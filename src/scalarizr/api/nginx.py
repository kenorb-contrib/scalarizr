import os
import glob
import shutil
import logging
import time
import cStringIO
import sys
import multiprocessing
from telnetlib import Telnet
from hashlib import sha1

from scalarizr import rpc
from scalarizr import linux
from scalarizr.bus import bus
from scalarizr.libs import metaconf
import scalarizr.libs.metaconf.providers
from scalarizr.node import __node__
from scalarizr.util import initdv2
from scalarizr.util import system2
from scalarizr.util import PopenError
from scalarizr.util import Singleton
from scalarizr.util import firstmatched
from scalarizr import linux
from scalarizr.linux import iptables
from scalarizr.linux import LinuxError
from scalarizr.linux import pkgmgr
from scalarizr import exceptions
from scalarizr.api import operation
from scalarizr.config import BuiltinBehaviours
from scalarizr.api import BehaviorAPI


__nginx__ = __node__['nginx']
try:
    # RedHat Software Collection
    nginx_rhscl_root = glob.glob('/opt/rh/nginx*/root')[0]
except IndexError:
    nginx_rhscl_root = None
if nginx_rhscl_root and linux.os.redhat:
    __nginx__['nginx.conf'] = os.path.join(nginx_rhscl_root, 'etc/nginx/nginx.conf')
    __nginx__['binary_path'] = os.path.join(nginx_rhscl_root, 'usr/sbin/nginx')
    __nginx__['service_name'] = '{0}-nginx'.format(os.path.basename(os.path.dirname(nginx_rhscl_root)))
else:
    __nginx__['nginx.conf'] = '/etc/nginx/nginx.conf'
    __nginx__['binary_path'] = firstmatched(lambda p: os.access(p, os.F_OK | os.X_OK),
                                ('/usr/sbin/nginx', '/usr/local/nginx/sbin/nginx'), '/usr/sbin/nginx')
    __nginx__['service_name'] = 'nginx'
__nginx__['app_include_path'] = os.path.join(os.path.dirname(__nginx__['nginx.conf']), 'app-servers.include')
__nginx__['https_include_path'] = os.path.join(os.path.dirname(__nginx__['nginx.conf']), 'https.include')



_logger = logging.getLogger(__name__)


class NginxInitScript(initdv2.ParametrizedInitScript):
    _nginx_binary = None

    def __init__(self):
        self._nginx_binary = __nginx__['binary_path']
        service_call = ['service', 'nginx'] if __nginx__['service_name'] == 'nginx' \
            else __nginx__['service_name']
        initdv2.ParametrizedInitScript.__init__(self,
                                                'nginx',
                                                service_call,
                                                socks=[])

    def _wait_workers(self):
        conf_dir = os.path.dirname(__nginx__['app_include_path'])
        conf_path = os.path.join(conf_dir, 'nginx.conf')
        conf = metaconf.Configuration('nginx')
        conf.read(conf_path)

        expected_workers_num = conf.get('worker_processes')
        if expected_workers_num == 'auto':
            expected_workers_num = multiprocessing.cpu_count()
        else:
            expected_workers_num = int(expected_workers_num)

        out = system2(['ps -C nginx --noheaders'], shell=True)[0]

        while len(out.splitlines()) - 1 < expected_workers_num:
            time.sleep(1)
            out = system2(['ps -C nginx --noheaders'], shell=True)[0]

    def status(self):
        status = initdv2.Status.UNKNOWN
        if self.socks:
            ip, port = self.socks[0].conn_address
            try:
                telnet = Telnet(ip, port)
            except:
                return status
            telnet.write('HEAD / HTTP/1.0\n\n')
            if 'server: nginx' in telnet.read_all().lower():
                return initdv2.Status.RUNNING
            return initdv2.Status.UNKNOWN
        else:
            args = self.initd_script + ['status']
            _, _, returncode = system2(args, raise_exc=False)
            if returncode == 0:
                return initdv2.Status.RUNNING
            else:
                if system2('ps -C nginx --noheaders', shell=True, raise_exc=False)[0].strip():
                    return initdv2.Status.RUNNING
                else:
                    return initdv2.Status.NOT_RUNNING

    def configtest(self, path=None):
        args = '%s -t' % self._nginx_binary
        if path:
            args += '-c %s' % path

        out = system2(args, shell=True)[1]
        if 'failed' in out.lower():
            raise initdv2.InitdError("Configuration isn't valid: %s" % out)

    def stop(self):
        if not self.running:
            return
        ret = initdv2.ParametrizedInitScript.stop(self)
        time.sleep(1)

    def restart(self):
        self.configtest()
        ret = initdv2.ParametrizedInitScript.restart(self)
        time.sleep(1)

    def start(self):
        self.configtest()

        if self.running:
            return

        try:
            args = [self.initd_script] \
                if isinstance(self.initd_script, basestring) \
                else list(self.initd_script)
            args.append('start')
            out, err, returncode = system2(args,
                                           close_fds=True,
                                           preexec_fn=os.setsid)
        except PopenError, e:
            raise initdv2.InitdError("Popen failed with error %s" % (e,))

        if returncode:
            raise initdv2.InitdError("Cannot start nginx. output= %s. %s" % (out, err),
                                     returncode)

        self._wait_workers()

    def reload(self):
        try:
            initdv2.ParametrizedInitScript.reload(self)
        except initdv2.InitdError, e:
            if 'is not running' in str(e):
                self.start()
            else:
                raise

    def set_port_to_check(self, port):
        _logger.debug('setting NginxInitScript port to check to: %s' % port)
        if port:
            self.socks = [initdv2.SockParam(port)]
        else:
            self.socks = []


def selinux_port(cmd, port):
    """ cmd can be either 'add' or 'delete' """
    if not port or not cmd:
        return False
    try:
        pkgmgr.installed('policycoreutils-python')
        system2(['semanage', 'port', '--%s' % cmd,
            '--type', 'http_port_t', '--proto', 'tcp', str(port)])
        return True
    except:
        e = sys.exc_info()[1]
        if not "already defined" in e:
            _logger.warning('{} port in selinux policy for nginx failed: {}'.format(cmd, e))
        return False


def open_port(port):
    if linux.os.redhat_family:
        selinux_port('add', port)
    if iptables.enabled():
        rule = {"jump": "ACCEPT", "protocol": "tcp", "match": "tcp", "dport": str(port)}
        iptables.FIREWALL.ensure([rule])


def close_port(port):
    # Cant know for sure what ports to close
    # if linux.os.redhat_family:
    #     selinux_port('delete', port)
    if iptables.enabled():
        rule = {"jump": "ACCEPT", "protocol": "tcp", "match": "tcp", "dport": str(port)}
        try:
            iptables.FIREWALL.remove(rule)
        except LinuxError:
            pass


def _bool_from_scalr_str(bool_str):
    if not bool_str:
        return False
    return int(bool_str) == 1


def _replace_string_in_file(file_path, s, new_s):
    raw = None
    with open(file_path, 'r') as fp:
        raw = fp.read()
        raw = raw.replace(s, new_s)
    with open(file_path, 'w') as fp:
        fp.write(raw)


def get_all_app_roles():
    _queryenv = bus.queryenv_service
    return _queryenv.list_roles(behaviour=BuiltinBehaviours.APP)


def _fix_ssl_keypaths(vhost_template):
    bad_keydir = '/etc/aws/keys/ssl/'
    good_keydir = os.path.join(bus.etc_path, "private.d/keys/")
    return vhost_template.replace(bad_keydir, good_keydir)


def _dump_config(obj):
    output = cStringIO.StringIO()
    obj.write_fp(output, close = False)
    return output.getvalue()


def _choose_host_ip(host, network=None):
    this_server_location = __node__['cloud_location']
    if network == None:
        use_internal_ip = this_server_location == host.cloud_location
    else:
        use_internal_ip = network == 'private'
    return host.internal_ip if use_internal_ip else host.external_ip


def get_role_servers(role_id=None, role_name=None, network=None):
    """ Method is used to get role servers from scalr """
    if type(role_id) is int:
        role_id = str(role_id)

    _queryenv = bus.queryenv_service
    roles = _queryenv.list_roles(farm_role_id=role_id, role_name=role_name)
    servers = []
    for role in roles:
        ips = [_choose_host_ip(h, network) for h in role.hosts]
        servers.extend(ips)

    return servers


def update_ssl_certificate(ssl_certificate_id, cert, key, cacert):
    """
    Updates ssl certificate. Returns paths to updated or created .key and
    .crt files
    """
    if not cert or not key:
        return (None, None)

    _logger.debug('Updating ssl certificate with id: %s' % ssl_certificate_id)

    if cacert:
        cert = cert + '\n' + cacert
    if ssl_certificate_id:
        ssl_certificate_id = '_' + str(ssl_certificate_id)
    else:
        ssl_certificate_id = ''

    keys_dir_path = os.path.join(bus.etc_path, "private.d/keys")
    if not os.path.exists(keys_dir_path):
        os.mkdir(keys_dir_path)

    cert_path = os.path.join(keys_dir_path, 'https%s.crt' % ssl_certificate_id)
    with open(cert_path, 'w') as fp:
        fp.write(cert)

    key_path = os.path.join(keys_dir_path, 'https%s.key' % ssl_certificate_id)
    with open(key_path, 'w') as fp:
        fp.write(key)

    return (cert_path, key_path)


def _fetch_ssl_certificate(ssl_certificate_id):
    """
    Gets ssl certificate and key from Scalr, writes them to files and
    returns paths to the files.
    """
    _queryenv = bus.queryenv_service
    cert, key, cacert = _queryenv.get_ssl_certificate(ssl_certificate_id)
    return update_ssl_certificate(ssl_certificate_id, cert, key, cacert)


class NginxAPI(BehaviorAPI):

    __metaclass__ = Singleton

    behavior = 'www'

    def __init__(self, app_inc_dir=None, proxies_inc_dir=None):
        """
        Basic API for configuring and managing Nginx service.

        Namespace::

            nginx
        """
        _logger.debug('Initializing nginx API.')
        self.service = NginxInitScript()
        self._op_api = operation.OperationAPI()
        self.error_pages_inc = None
        self.backend_table = {}
        self._selinux_opened_ports = []
        self.app_inc_path = None
        self.proxies_inc_dir = proxies_inc_dir
        self.proxies_inc_path = None

        if not app_inc_dir and __nginx__ and __nginx__['app_include_path']:
            app_inc_dir = os.path.dirname(__nginx__['app_include_path'])
        if app_inc_dir:
            self.app_inc_path = os.path.join(app_inc_dir, 'app-servers.include')

        if not proxies_inc_dir and __nginx__ and __nginx__['app_include_path']:
            self.proxies_inc_dir = os.path.dirname(__nginx__['app_include_path'])
        if self.proxies_inc_dir:
            self.proxies_inc_path = os.path.join(self.proxies_inc_dir, 'proxies.include')

    def init_service(self):
        _logger.debug('Initializing nginx API.')
        self._load_app_servers_inc()
        self.fix_app_servers_inc()
        self._load_proxies_inc()
        self._make_error_pages_include()
        self._queryenv = bus.queryenv_service

    def _make_error_pages_include(self):

        def _add_static_location(config, location, expires=None):
            xpath = 'location'
            locations_num = len(config.get_list(xpath))
            config.add(xpath, location)

            xpath = '%s[%i]' % (xpath, locations_num + 1)

            if expires:
                config.add('%s/expires' % xpath, expires)
            config.add('%s/root' % xpath, '/usr/share/nginx/html')

        error_pages_dir = os.path.dirname(__nginx__['app_include_path'])
        self.error_pages_inc = os.path.join(error_pages_dir,
                                            'error-pages.include')
        # error-pages.include is overwritten only if it is not exist,
        # so clients can modify it and be sure their changes are persist
        if not os.path.exists(self.error_pages_inc):
            error_pages_conf = metaconf.Configuration('nginx')
            _add_static_location(error_pages_conf, '/500.html', '0')
            _add_static_location(error_pages_conf, '/502.html', '0')
            _add_static_location(error_pages_conf, '/noapp.html')
            error_pages_conf.write(self.error_pages_inc)

    def _save_proxies_inc(self):
        self.proxies_inc.write(self.proxies_inc_path)

    def _load_proxies_inc(self):
        self.proxies_inc = metaconf.Configuration('nginx')
        if os.path.exists(self.proxies_inc_path):
            self.proxies_inc.read(self.proxies_inc_path)
        else:
            open(self.proxies_inc_path, 'w').close()

    def _save_app_servers_inc(self):
        self.app_servers_inc.write(self.app_inc_path)

    def _load_app_servers_inc(self):
        self.app_servers_inc = metaconf.Configuration('nginx')
        if os.path.exists(self.app_inc_path):
            _logger.debug('Reading app-servers.include')
            self.app_servers_inc.read(self.app_inc_path)
        else:
            _logger.debug('Creating app-servers.include')
            open(self.app_inc_path, 'w').close()

    def fix_app_servers_inc(self):
        _logger.debug('Fixing app servers include')
        https_inc_xpath = self.app_servers_inc.xpath_of('include',
                                                        '/etc/nginx/https.include')
        if https_inc_xpath:
            self.app_servers_inc.remove(https_inc_xpath)

        # Removing all existed servers
        for i, _ in enumerate(self.app_servers_inc.get_list('upstream')):
            _logger.debug('Removing existing backend servers from app-servers.include')
            backend_xpath = 'upstream[%i]' % (i + 1)
            # for j, _ in enumerate(self.app_servers_inc.get_list('%s/server' % backend_xpath)):
            self.app_servers_inc.remove('%s/server' % backend_xpath)

        self._save_app_servers_inc()

    def _clear_nginx_includes(self):
        _logger.debug('Clearing app-servers.include and proxies.include. '
                      'Old configs copied to .bak files.')
        if os.path.exists(self.app_inc_path):
            shutil.copyfile(self.app_inc_path, self.app_inc_path + '.bak')
        if os.path.exists(self.proxies_inc_path):
            shutil.copyfile(self.proxies_inc_path, self.proxies_inc_path + '.bak')

        with open(self.app_inc_path, 'w') as fp:
            fp.write('')
        with open(self.proxies_inc_path, 'w') as fp:
            fp.write('')
        self._load_app_servers_inc()
        self._load_proxies_inc()

    def _reload_service(self):
        if self.service.status() != initdv2.Status.RUNNING:
            self.service.start()
        else:
            self.service.reload()

    @rpc.command_method
    def start_service(self):
        """
        Starts Nginx service.

        Example::

            api.nginx.start_service()
        """
        self.service.start()

    @rpc.command_method
    def stop_service(self):
        """
        Stops Nginx service.

        :param reason: Message to appear in log before service is stopped.
        :type reason: str

        Example::

            api.nginx.stop_service("Configuring Nginx service.")
        """
        self.service.stop()

    @rpc.command_method
    def reload_service(self):
        """
        Reloads Nginx service.

        :param reason: Message to appear in log before service is reloaded.
        :type reason: str

        Example::

            api.nginx.reload("Applying proxy settings.")
        """
        self.service.reload()

    @rpc.command_method
    def restart_service(self):
        """
        Restarts Nginx service.

        :param reason: Message to appear in log before service is restarted.
        :type reason: str

        Example::

            api.nginx.stop_service("Applying new service configuration preset.")
        """
        self.service.restart()

    @rpc.command_method
    def configtest(self, reason=None):
        """
        Performs Nginx configtest.

        Example::

            api.nginx.configtest()
        """
        self.service.configtest()

    @rpc.command_method
    def get_service_status(self):
        return self.service.status()

    @rpc.command_method
    def recreate_proxying(self, proxy_list, reload_service=True):
        """
        Recreates Nginx proxying configuration.

        :param proxy_list: List of parameters for each proxy. Parameters are
            kwds-dict that passed to self.add_proxy()
        :type proxy_list: list

        :param reload_service: If True reloads nginx service after recreation.
        :type reload_service: bool
        
        Example:

        Recreating proxying with single proxy configuration::

            api.nginx.recreate_proxying([{'name': 'test.com',
                                          'backends': [{'host': '12.234.45.67', 'port': '80'}],
                                          'port': '80'}])
        """
        if not proxy_list:
            proxy_list = []

        _logger.debug('Recreating proxying with %s' % proxy_list)
        self._clear_nginx_includes()
        self.backend_table = {}
        if linux.os.redhat_family:
            for p in self._selinux_opened_ports:
                selinux_port('delete', p)
        self._selinux_opened_ports = []

        try:
            for proxy_parms in proxy_list:
                if 'hostname' in proxy_parms:
                    proxy_parms['name'] = proxy_parms.pop('hostname')
                self.add_proxy(reload_service=False, **proxy_parms)

            if reload_service:
                self._reload_service()
        except initdv2.InitdError, e:
            msg = "Can't add proxy %s: %s" % (proxy_parms['name'], e)
            raise Exception(msg)

    def _main_config_contains_server(self):
        config_dir = os.path.dirname(self.app_inc_path)
        nginx_conf_path = os.path.join(config_dir, 'nginx.conf')

        config = None
        result = False
        try:
            config = metaconf.Configuration('nginx')
            config.read(nginx_conf_path)
        except (Exception, BaseException), e:
            raise Exception('Cannot read/parse nginx main configuration file: %s' % str(e))

        try:
            result = config.get('http/server') != None
        except:
            pass

        return result

    def make_default_proxy(self, roles):
        # actually list_virtual_hosts() returns only 1 virtual host if it's
        # ssl virtual host. If there are no ssl vhosts in farm, it returns
        # empty list
        ssl_vhosts = self._queryenv.list_virtual_hosts()
        _logger.info('Making default proxy with roles: %s' % roles)
        servers = []
        for role in roles:
            servers_ips = []
            if type(role) is str:
                servers_ips = get_role_servers(role) or \
                    get_role_servers(role_name=role)
            else:
                cl = __node__['cloud_location']
                servers_ips = [h.internal_ip if cl == h.cloud_location else
                               h.external_ip
                               for h in role.hosts]
            servers.extend({'host': srv} for srv in servers_ips)

        if not servers:
            _logger.debug('No app roles in farm, making mock backend')
            servers = [{'host': '127.0.0.1',
                        'port': '80'}]

        _logger.debug('Clearing backend table')
        self.backend_table = {}
        
        _logger.debug('backend table is %s' % self.backend_table)
        write_proxies = not self._main_config_contains_server()
        self.add_proxy('backend',
                        backends=servers,
                        ssl=False,
                        backend_ip_hash=True,
                        hash_backend_name=False,
                        reload_service=False,
                        write_proxies=write_proxies)

        with open(self.proxies_inc_path, 'w') as fp:
            cert, key, cacert = self._queryenv.get_https_certificate()
            _logger.debug('updating certificates')
            update_ssl_certificate('', cert, key, cacert)

            if ssl_vhosts and cert and key:
                _logger.info('Writing SSL server configuration to proxies.conf. SSL on')
                raw_conf = _fix_ssl_keypaths(ssl_vhosts[0].raw)
                fp.write(raw_conf)
            else:
                _logger.info('Clearing SSL server configuration. SSL off')
                fp.write('')

        self._reload_service()

        # Uncomment if you want to ssl proxy to be generated and not be taken from template
        # if ssl_vhosts:
        #     _logger.debug('adding default ssl nginx server')
        #     write_proxies = not self._https_config_exists()
        #     self.make_proxy('backend.ssl',
        #                         backends=servers,
        #                         port=None,
        #                         ssl=True,
        #                         backend_ip_hash=True,
        #                         hash_backend_name=False,
        #                         write_proxies=write_proxies)
        # else:
        #     self.remove_proxy('backend.ssl')
        _logger.debug('After making proxy backend table is %s' % self.backend_table)
        _logger.debug('Default proxy is made')

    def _recreate_compat_mode(self):
        _logger.debug('Compatibility mode proxying recreation')
        roles_for_proxy = []
        if __nginx__['upstream_app_role']:
            roles_for_proxy = [__nginx__['upstream_app_role']]
        else:
            roles_for_proxy = get_all_app_roles()

        self.fix_app_servers_inc()
        self.make_default_proxy(roles_for_proxy)

        https_inc_path = os.path.join(os.path.dirname(self.app_inc_path),
                                      'https.include')
        nginx_dir = os.path.dirname(https_inc_path)
        for file_path in os.listdir(nginx_dir):
            if file_path.startswith('https.include'):
                _logger.debug('Removing %s' % file_path)
                os.remove(file_path)

    def _update_main_config(self, remove_server_section=True, reload_service=True):
        config_dir = os.path.dirname(self.app_inc_path)
        nginx_conf_path = os.path.join(config_dir, 'nginx.conf')

        config = None
        try:
            config = metaconf.Configuration('nginx')
            config.read(nginx_conf_path)
        except (Exception, BaseException), e:
            raise Exception('Cannot read/parse nginx main configuration file: %s' % str(e))

        _logger.debug('Update main configuration file')
        dump = _dump_config(config)

        gzip_vary = config.get_list('http/gzip_vary')
        if not gzip_vary:
            config.add('http/gzip_vary', 'on')
        gzip_proxied = config.get_list('http/gzip_proxied')
        if not gzip_proxied:
            config.add('http/gzip_proxied', 'any')
        gzip_types = config.get_list('http/gzip_types')
        if not gzip_types:
            types = 'text/plain text/css application/json application/x-javascript' \
                'text/xml application/xml application/xml+rss text/javascript'
            config.add('http/gzip_types', types)

        include_list = config.get_list('http/include')
        if not self.app_inc_path in include_list:
            _logger.debug('adding app-servers.include path to main config')
            config.add('http/include', self.app_inc_path)
        if not self.proxies_inc_path in include_list:
            _logger.debug('adding proxies.include path to main config')
            config.add('http/include', self.proxies_inc_path)
        else:
            _logger.debug('config contains proxies.include: %s \n%s' %
                               (self.proxies_inc_path, include_list))

        # First remove then rewrite or leave it removed
        _logger.debug('removing http/server section')
        try:
            config.remove('http/server')
        except (ValueError, IndexError):
            _logger.debug('no http/server section')
        
        if not remove_server_section:
            _logger.debug('Rewriting http/server section')
            config.read(os.path.join(bus.share_path, "nginx/server.tpl"))

        if linux.os.debian_family:
        # Comment /etc/nginx/sites-enabled/*
            try:
                i = config.get_list('http/include').index('/etc/nginx/sites-enabled/*')
                config.comment('http/include[%d]' % (i+1))
                _logger.debug('comment site-enabled include')
            except (ValueError, IndexError):
                _logger.debug('site-enabled include already commented')
        elif linux.os.redhat_family:
            def_host_path = '/etc/nginx/conf.d/default.conf'
            if os.path.exists(def_host_path):
                default_host = metaconf.Configuration('nginx')
                default_host.read(def_host_path)
                default_host.comment('server')
                default_host.write(def_host_path)

        if dump == _dump_config(config):
            _logger.debug("Main nginx config wasn`t changed")
        else:
            # Write new nginx.conf
            shutil.copy(nginx_conf_path, nginx_conf_path + '.bak')
            config.write(nginx_conf_path)
            if reload_service:
                self._reload_service()

    def do_reconfigure(self, op, proxies):
        backend_table_bak = self.backend_table.copy()
        selinux_opened_ports_bak = self._selinux_opened_ports
        main_conf_path = self.proxies_inc_dir + '/nginx.conf'
        try:
            self.app_inc_path = self.app_inc_path + '.new'
            self.proxies_inc_path = self.proxies_inc_path + '.new'

            _replace_string_in_file(main_conf_path,
                                    'proxies.include',
                                    'proxies.include.new')
            _replace_string_in_file(main_conf_path,
                                    'app-servers.include',
                                    'app-servers.include.new')
            self._update_main_config(remove_server_section=proxies!=None, reload_service=False)
            if proxies:
                self.recreate_proxying(proxies, reload_service=False)
            else:
                self._recreate_compat_mode()
            self.service.configtest()

        except:
            os.remove(self.app_inc_path)
            os.remove(self.proxies_inc_path)
            self.backend_table = backend_table_bak
            self._selinux_opened_ports = selinux_opened_ports_bak
            if linux.os.redhat_family:
                for p in self._selinux_opened_ports:
                    selinux_port('add', p)
            _replace_string_in_file(main_conf_path,
                                    'proxies.include.new',
                                    'proxies.include')
            _replace_string_in_file(main_conf_path,
                                    'app-servers.include.new',
                                    'app-servers.include')
            self.app_inc_path = self.app_inc_path[:-4]
            self.proxies_inc_path = self.proxies_inc_path[:-4]
            raise
        else:
            os.remove(self.app_inc_path[:-4])
            os.remove(self.proxies_inc_path[:-4])
            shutil.copyfile(self.app_inc_path, self.app_inc_path[:-4])
            shutil.copyfile(self.proxies_inc_path, self.proxies_inc_path[:-4])
            os.remove(self.app_inc_path)
            os.remove(self.proxies_inc_path)
            _replace_string_in_file(main_conf_path,
                                    'proxies.include.new',
                                    'proxies.include')
            _replace_string_in_file(main_conf_path,
                                    'app-servers.include.new',
                                    'app-servers.include')
            self._reload_service()
            self.app_inc_path = self.app_inc_path[:-4]
            self.proxies_inc_path = self.proxies_inc_path[:-4]

    @rpc.service_method
    def reconfigure(self, proxies, async=True):
        self._op_api.run('api.nginx.reconfigure',
                         func=self.do_reconfigure,
                         func_kwds={'proxies': proxies},
                         async=async,
                         exclusive=True)

    def _normalize_destinations(self, destinations):
        """
        Parses list of destinations. Dictionary example:

        .. code-block:: python
            {
            'farm_role_id': 123,
            'port': '80',
            'backup': True,
            # ...
            # other backend params
            # ...
            }

        or

        .. code-block:: python
            {
            'host': '12.234.45.67',
            'port': '80',
            'backup': True,
            # ...
            # other backend params
            # ...
            }

        Returns destination dictionaries with format like above
        plus servers list in 'servers' key.
        """
        if not destinations:
            return []

        normalized_dests = []
        for d in destinations:
            dest = d.copy()

            if 'backup' in dest:
                dest['backup'] = _bool_from_scalr_str(dest['backup'])
            if 'down' in dest:
                dest['down'] = _bool_from_scalr_str(dest['down'])

            dest['servers'] = []
            if 'farm_role_id' in dest:
                dest['id'] = str(dest['farm_role_id'])
                role_servers = get_role_servers(dest['id'], network=dest.get('network'))
                dest['servers'].extend(role_servers)
            if 'host' in dest:
                dest['servers'].append(dest['host'])

            normalized_dests.append(dest)

        return normalized_dests

    def _group_destinations(self, destinations):
        """
        Groups destinations by location in list of lists.
        If no location defined assumes that it's '/' location.
        """
        if not destinations:
            return []

        sorted_destinations = sorted(destinations,
                                     key=lambda x: x.get('location'),
                                     reverse=True)

        # Making backend dicts from destinations with similar location
        first_dest = sorted_destinations[0]
        if not first_dest.get('location'):
            first_dest['location'] = '/'
        grouped_destinations = [[first_dest]]
        # Grouping destinations with similar location
        for dest in sorted_destinations[1:]:
            if not dest.get('location'):
                dest['location'] = '/'
            if grouped_destinations[-1][0]['location'] == dest['location']:
                grouped_destinations[-1].append(dest)
            else:
                grouped_destinations.append([dest])

        return grouped_destinations

    def _group_templates(self, templates):
        """
        Groups list of temlate dictionaries with format:
        ``{'content': 'raw nginx configuration here', 'location': '/admin',
           'content': 'raw config 2', 'server': True,
           ...}``
        to dictionary of dictionaries, grouped by locations:
        ``{'/admin': {'content': 'raw nginx configuration here'},
           'server': {'content': 'raw config 2'},
           ...}``
        """
        if not templates:
            return {}

        result = {}
        for template in templates:
            key = None
            if _bool_from_scalr_str(template.get('server')):
                key = 'server'
            else:
                key = template['location']
            result[key] = {'content': template['content'] or ''}
        return result

    def _add_backend(self,
                     name,
                     destinations,
                     port=None,
                     ip_hash=True,
                     least_conn=False,
                     max_fails=None,
                     fail_timeout=None,
                     weight=None):
        """
        Adds backend to app-servers config, but without writing it to file.
        """
        if self.app_servers_inc.xpath_of('upstream', name):
            for dest in destinations:
                self.add_server(name, dest, False, False, False)
        else:
            backend = self._make_backend_conf(name,
                                              destinations,
                                              port=port,
                                              ip_hash=ip_hash,
                                              least_conn=least_conn,
                                              max_fails=max_fails,
                                              fail_timeout=fail_timeout,
                                              weight=weight)
            self.app_servers_inc.append_conf(backend)

    def _make_backend_conf(self,
                           name,
                           destinations,
                           port=None,
                           ip_hash=True,
                           least_conn=False,
                           max_fails=None,
                           fail_timeout=None,
                           weight=None):
        """Returns config for one backend server"""
        config = metaconf.Configuration('nginx')
        config.add('upstream', name or 'backend')
        if ip_hash:
            config.add('upstream/ip_hash', '')
        if least_conn:
            config.add('upstream/least_conn', '')

        for dest in destinations:
            servers = dest['servers']
            if len(servers) == 0:
                # if role destination has no running servers yet, 
                # adding mock server 127.0.0.1
                servers = ['127.0.0.1']
            for server in servers:
                if 'port' in dest or port:
                    server = '%s:%s' % (server, dest.get('port', port))

                if 'backup' in dest and dest['backup']:
                    server = '%s %s' % (server, 'backup')

                _max_fails = dest.get('max_fails', max_fails)
                if _max_fails:
                    server = '%s %s' % (server, 'max_fails=%s' % _max_fails)

                _fail_timeout = dest.get('fail_timeout', fail_timeout)
                if _fail_timeout:
                    server = '%s %s' % (server, 'fail_timeout=%ss' % _fail_timeout)

                if 'down' in dest and dest['down']:
                    server = '%s %s' % (server, 'down')

                _weight = dest.get('weight', weight)
                if _weight:
                    server = '%s %s' % (server, 'weight=%s' % _weight)

                config.add('upstream/server', server)

        return config

    def _backend_nameparts(self, backend_name):
        """ Takes name, location and roles from backend_name """
        parts = backend_name.split('_')
        name = parts[0]

        location = ''
        roles_index = -1
        for i, part in enumerate(parts[1:]):
            if part == '':
                roles_index = i + 2
                break
            location += part + '/'

        roles = []
        if roles_index != -1:
            roles = parts[roles_index:]

        return name, location, roles

    def _make_backend_name(self, name, location, roles, hash_name=True):
        role_namepart = '_'.join(map(str, roles))
        if hash_name:
            name = sha1(name).hexdigest()
        name = '%s%s__%s' % (name, 
                             (location.replace('/', '_')).rstrip('_'),
                             role_namepart)
        name = name.rstrip('_')

        return name

    def _add_backends(self,
                      hostname,
                      grouped_destinations,
                      port=None,
                      ip_hash=True,
                      least_conn=False,
                      max_fails=None,
                      fail_timeout=None,
                      weight=None,
                      hash_name=True):
        """
        Makes backend for each group of destinations and writes it to
        app-servers config file.

        Returns tuple of pairs with location and backend names:
        [[dest1, dest2], [dest3]] -> ((location1, name1), (location2, name2))

        Tuple of pairs is used instead of dict, because we need to keep order 
        saved.

        Name of backend is construct by pattern:

            ```hostname`[_`location`][__`role_id1`[_`role_id2`[...]]]``

        Example:

            ``test.com_somepage_123_345``
        """
        locations_and_backends = ()
        # making backend configs for each group
        for backend_destinations in grouped_destinations:
            location = backend_destinations[0]['location']

            # Find role ids that will be used in backend
            role_ids = set([dest.get('id') for dest in backend_destinations])
            role_ids.discard(None)

            name = self._make_backend_name(hostname, location, role_ids, hash_name)

            self._add_backend(name,
                              backend_destinations,
                              port=port,
                              ip_hash=ip_hash,
                              least_conn=least_conn,
                              max_fails=max_fails,
                              fail_timeout=fail_timeout,
                              weight=weight)

            locations_and_backends += ((location or '/', name),)

        return locations_and_backends

    def _is_redirector(self, conf, server_xpath):
        try:
            conf.get('%s/rewrite' % server_xpath)
        except metaconf.NoPathError:
            return False
        else:
            return True

    def _make_redirector_conf(self, hostname, port, ssl_port):
        """
        Makes config (metaconf.Configuration object) for server section of
        nginx config that is used to redirect http to https
        """
        if not port:
            port = '80'
        config = metaconf.Configuration('nginx')
        config.add('server', '')

        config.add('server/listen', str(port))
        config.add('server/server_name', hostname)

        redirect_regex = '^(.*)$ https://$host:%s$request_uri$is_args$args? permanent' % ssl_port
        config.add('server/rewrite', redirect_regex)

        return config

    def _add_noapp_handler(self, config):
        """ Adding proxy to noapp.html location if no app servers are found """
        config.add('server/if', '( $remote_addr = 127.0.0.1 )')
        config.add('server/if/rewrite', '^(.*)$ /noapp.html last')
        config.add('server/if/return', '302')

    def _old_style_ssl_on(self):
        """
        Returns True if nginx version is lesser than 0.8.21.
        ssl parameter in listen directive back than can be set only on default server,
        but multiple ssl servers could be set by ssl directive:
        `ssl on;` not `listen 443 ssl;`
        """
        out = system2(['nginx -v'], shell=True)[1]
        nginx_version_str = out.split('/')[1]
        nginx_version = nginx_version_str.split('.')
        # 0.8.21 version of nginx where default param for https listen is not needed
        old_nginx = nginx_version < ['0', '8', '21']
        _logger.debug('nginx version is: %s' % nginx_version_str)
        return old_nginx

    def _add_ssl_params(self,
                        config,
                        server_xpath,
                        ssl_port,
                        ssl_certificate_id,
                        http):
        old_style_ssl = self._old_style_ssl_on()

        listen_val = '%s%s' % ((ssl_port or '443'), ' ssl' if not old_style_ssl else '')
        config.add('%s/listen' % server_xpath, listen_val)

        if old_style_ssl:
            config.add('%s/ssl' % server_xpath, 'on')
        ssl_cert_path, ssl_cert_key_path = _fetch_ssl_certificate(ssl_certificate_id)
        config.add('%s/ssl_certificate' % server_xpath, ssl_cert_path)
        config.add('%s/ssl_certificate_key' % server_xpath, ssl_cert_key_path)


    def _add_default_template(self, config):
        config.add('server/proxy_set_header', 'Host $host')
        config.add('server/proxy_set_header', 'X-Real-IP $remote_addr')
        config.add('server/proxy_set_header', 'X-Forwarded-For $proxy_add_x_forwarded_for')
        config.add('server/client_max_body_size', '10m')
        config.add('server/client_body_buffer_size', '128k')
        config.add('server/proxy_buffering', 'on')
        config.add('server/proxy_connect_timeout', '15')
        config.add('server/proxy_intercept_errors', 'on')

        # default SSL params
        config.add('server/ssl_session_timeout', '10m')
        config.add('server/ssl_session_cache', 'shared:SSL:10m')
        config.add('server/ssl_protocols', 'SSLv2 SSLv3 TLSv1')
        config.add('server/ssl_ciphers', 
                   'ALL:!ADH:!EXPORT56:RC4+RSA:+HIGH:+MEDIUM:+LOW:+SSLv2:+EXP')
        config.add('server/ssl_prefer_server_ciphers', 'on')

        self._add_noapp_handler(config)

    def _make_server_conf(self,
                          hostname,
                          locations_and_backends,
                          port='80',
                          ssl=False,
                          ssl_port=None,
                          ssl_certificate_id=None,
                          grouped_templates=None):
        """
        Makes config (metaconf.Configuration object) for server section of
        nginx config
        """

        if not grouped_templates:
            grouped_templates = {}

        config = metaconf.Configuration('nginx')

        server_wide_template = grouped_templates.get('server')
        config.add('server', '')
        if server_wide_template and server_wide_template['content']:
            # TODO: this is ugly. Find the way to read conf from string
            temp_file = self.proxies_inc_dir + '/temalate.tmp'
            with open(temp_file, 'w') as fp:
                fp.write(server_wide_template['content'])
            template_conf = metaconf.Configuration('nginx')
            template_conf.read(temp_file)
            config.insert_conf(template_conf, 'server')
            os.remove(temp_file)
        else:
            self._add_default_template(config)
        
        if port:
            config.add('server/listen', str(port))
        try:
            config.get('server/server_name')
            config.set('server/server_name', hostname)
        except:
            config.add('server/server_name', hostname)

        # Configuring ssl
        if ssl:
            self._add_ssl_params(config, 'server', ssl_port, ssl_certificate_id, port!=None)

        config.add('server/include', self.error_pages_inc)
        
        # Adding locations leading to defined backends

        for i, (location, backend_name) in enumerate(locations_and_backends):
            location_xpath = 'server/location'
            config.add(location_xpath, location)

            location_xpath = '%s[%i]' % (location_xpath, i + 1)

            if grouped_templates.get(location) and grouped_templates[location]['content']:
                temp_file = self.proxies_inc_dir + '/temalate.tmp'
                # TODO: this is ugly. Find the way to read conf from string
                with open(temp_file, 'w') as fp:
                    fp.write(grouped_templates[location]['content'])
                template_conf = metaconf.Configuration('nginx')
                template_conf.read(temp_file)
                config.insert_conf(template_conf, location_xpath)
                os.remove(temp_file)

            config.add('%s/proxy_pass' % location_xpath, 'http://%s' % backend_name)

            if location is '/':
                config.add('%s/error_page' % location_xpath, '500 501 /500.html')
                config.add('%s/error_page' % location_xpath, '502 503 504 /502.html')

        return config

    def _add_nginx_server(self,
                          hostname,
                          locations_and_backends,
                          port='80',
                          http=True,
                          ssl=False,
                          ssl_port=None,
                          ssl_certificate_id=None,
                          grouped_templates=None,
                          redirector=True):
        """
        Adds server to https config, but without writing it to file.
        """
        if redirector:
            redirector_conf = self._make_redirector_conf(hostname,
                                                         port,
                                                         ssl_port)
            self.proxies_inc.append_conf(redirector_conf)

        server_config = self._make_server_conf(hostname,
                                               locations_and_backends,
                                               port if http else None,
                                               ssl,
                                               ssl_port,
                                               ssl_certificate_id,
                                               grouped_templates)

        self.proxies_inc.append_conf(server_config)

    def add_proxy(self,
                  name,
                  backends=[],
                  port='80',
                  http=True,
                  ssl=False,
                  ssl_port=None,
                  ssl_certificate_id=None,
                  backend_port=None,
                  backend_ip_hash=False,
                  backend_least_conn=False,
                  backend_max_fails=None,
                  backend_fail_timeout=None,
                  backend_weight=None,
                  templates=None,
                  reread_conf=True,
                  reload_service=True,
                  hash_backend_name=True,
                  write_proxies=True,
                  **kwds):
        """
        Adds proxy.

        All backend_* params are used for default values and can be overrided
        by values given for certain backend in backends list

        :param name: name for proxy. Used as hostname - server_name in nginx server section

        :param backends: is list of dictionaries which contains servers
        and/or roles with params and inner naming in this module for such dicts
        is ``destinations``. So keep in mind that ``backend`` word in all other
        places of this module means nginx upstream config.

        :param port: port for proxy to listen http

        :param http: if False proxy will not listen http port

        :param ssl: if True proxy will listen ssl port

        :param ssl_port: port for proxy to listen https

        :param ssl_certificate_id: scalr ssl certificate id. Will be fetched through queryenv

        :param backend_port: default port for backend servers to be proxied on

        :param backend_ip_hash: defines default presence of ip_hash in backend config

        :param backend_least_conn: defines default presence of least_conn in backend config

        :param backend_max_fails: default value of max_fails for servers in backends

        :param backend_fail_timeout: default value (in secs) of fail_timeout for servers in backends

        :param backend_weight: default value of weight for servers in backends

        :param templates: list of template dictionaries.
        Template dictionary consists of template content and location to be included in.
        'server' key determines that template is used for all proxy-server config part,
        not separate location.
        E.g.: ``[{'content': <raw_config>, 'location': '/admin'},
                 {'content': <another_raw>, 'server': True}]``

        :param reread_conf: if True app_servers_inc and proxies_inc will be reloaded from files
        before proxy addition

        :param reload_service: if True service will be reloaded after proxy will be added

        :param hash_backend_name: if True backend names will be hashed

        :param write_proxies: if False changes will not be written in proxies_inc file.
        This can be used if we only need to add backend.
        """
        # typecast is needed because scalr sends bool params as strings: '1' for True, '0' for False 
        ssl = _bool_from_scalr_str(ssl)
        http = _bool_from_scalr_str(http) if ssl else True
        backend_ip_hash = _bool_from_scalr_str(backend_ip_hash)
        backend_least_conn = _bool_from_scalr_str(backend_least_conn)
        reread_conf = _bool_from_scalr_str(reread_conf)
        reload_service = _bool_from_scalr_str(reload_service)
        hash_backend_name = _bool_from_scalr_str(hash_backend_name)
        write_proxies = _bool_from_scalr_str(write_proxies)

        _logger.debug('Adding proxy with name: %s' % name)
        destinations = self._normalize_destinations(backends)

        grouped_destinations = self._group_destinations(destinations)
        if not grouped_destinations:
            raise Exception('add_proxy() called with no destination list')
        if ssl_port == port and ssl_port != None:
            raise Exception("HTTP and HTTPS ports can't be the same")

        if reread_conf:
            self._load_app_servers_inc()
            self._load_proxies_inc()

        locations_and_backends = self._add_backends(name,
                                                    grouped_destinations,
                                                    port=backend_port,
                                                    ip_hash=backend_ip_hash,
                                                    max_fails=backend_max_fails,
                                                    fail_timeout=backend_fail_timeout,
                                                    least_conn=backend_least_conn,
                                                    weight=backend_weight,
                                                    hash_name=hash_backend_name)

        for backend_destinations, (_, backend_name) \
            in zip(grouped_destinations, locations_and_backends):

            if linux.os.redhat_family:
                for destination in backend_destinations:
                    p = destination.get('port')
                    if selinux_port('add', p):
                        self._selinux_opened_ports.append(p)

            self.backend_table[backend_name] = backend_destinations

        grouped_templates = self._group_templates(templates)

        # If it's an old nginx and proxy should work through ssl,
        # we need to make two different servers for http and https listening
        two_servers_are_needed = ssl and self._old_style_ssl_on()
        # making server that listens https
        self._add_nginx_server(name,
                               locations_and_backends,
                               port=port,
                               http=http and not two_servers_are_needed,
                               ssl=ssl,
                               ssl_port=ssl_port,
                               ssl_certificate_id=ssl_certificate_id,
                               grouped_templates=grouped_templates,
                               redirector=not http)
        # making server that listens http
        if two_servers_are_needed and http:
            self._add_nginx_server(name,
                                   locations_and_backends,
                                   port=port,
                                   http=http,
                                   grouped_templates=grouped_templates,
                                   redirector=False)

        if port:
            open_port(port)
        if ssl_port:
            open_port(ssl_port)

        self._save_app_servers_inc()
        if write_proxies:
            self._save_proxies_inc()

        if reload_service:
            self._reload_service()

        if port:
            self.service.set_port_to_check(port)

    def _remove_backend(self, name):
        """
        Removes backend with given name from app-servers config.
        """
        xpath = self.app_servers_inc.xpath_of('upstream', name)
        if xpath:
            self.app_servers_inc.remove(xpath)

    def _remove_nginx_server(self, name):
        """
        Removes server from proxies.include config. Also removes used backends.
        """

        xpaths_to_remove = []

        for i, _ in enumerate(self.proxies_inc.get_list('server')):

            server_xpath = 'server[%i]' % (i + 1)
            server_name = self.proxies_inc.get('%s/server_name' % server_xpath)

            if name == server_name:
                location_xpath = '%s/location' % server_xpath
                location_qty = len(self.proxies_inc.get_list(location_xpath))

                for j in xrange(location_qty):
                    xpath = location_xpath + ('[%i]' % (j + 1))
                    backend = self.proxies_inc.get(xpath + '/proxy_pass')
                    backend = backend.replace('http://', '')
                    self._remove_backend(backend)

                for addr in self.proxies_inc.get_list('%s/listen' % server_xpath):
                    port = addr.split()[0]
                    close_port(port)

                xpaths_to_remove.append(server_xpath)

        for xpath in reversed(xpaths_to_remove):
            self.proxies_inc.remove(xpath)

    def _get_any_port(self, config):
        port = None
        try:
            addr = self.proxies_inc.get('server[1]/listen')
            port = addr.split()[0]
        except metaconf.NoPathError:
            pass
        return port

    @rpc.command_method
    def remove_proxy(self, hostname, reload_service=True):
        """
        Removes proxy with given hostname. Removes created server and its backends.

        :param hostname: nginx proxy server name.
        :type hostname: str

        :param reload_service: If True reloads nginx service after proxy removal.
        :type reload_service: bool

        Examples:

        Remove proxy with name `test.com`::

            api.nginx.remove_proxy('test.com')

        Remove proxy with name `test.com` without service reload::

            api.nginx.remove_proxy('test.com', reload_service=True)
        """
        reload_service = _bool_from_scalr_str(reload_service)

        _logger.debug('Removing proxy with hostname: %s' % hostname)
        self._load_proxies_inc()
        self._load_app_servers_inc()

        self._remove_nginx_server(hostname)

        # remove each backend that were in use by this proxy from backend_table
        for backend_name in self.backend_table.keys():
            if hostname == self._backend_nameparts(backend_name)[0]:
                destinations = self.backend_table.pop(backend_name)

                if linux.os.redhat_family:
                    for destination in destinations:
                        p = destination.get('port')
                        if p and p in self._selinux_opened_ports:
                            selinux_port('delete', p)
                            self._selinux_opened_ports.remove(p)

        self.service.set_port_to_check(self._get_any_port(self.proxies_inc))

        self._save_proxies_inc()
        self._save_app_servers_inc()
        if reload_service:
            self._reload_service()

    @rpc.command_method
    def make_proxy(self, hostname, **kwds):
        """
        RPC method for adding or updating proxy configuration.
        Removes proxy with given hostname if exists and recreates it with given
        parameters. If some exception occures changes are reverted.

        :param hostname: nginx proxy server name.
        :type hostname: str

        See add_proxy() for detailed kwds description.

        Example:

        Make proxy with name `test.com`::

            api.nginx.make_proxy('test.com', 
                                 backends=[{'host': '123.321.111.1'}],
                                 port='8080',
                                 backend_port='80')
        """
        _logger.debug('making proxy: %s' % hostname)
        try:
            # trying to apply changes
            self._load_proxies_inc()
            self._load_app_servers_inc()

            self.proxies_inc.write(self.proxies_inc_path + '.bak')
            self.app_servers_inc.write(self.app_inc_path + '.bak')

            _logger.debug('deleting previously existed proxy')

            if kwds.get('write_proxies', True):
                self._remove_nginx_server(hostname)

            for backend_name in self.backend_table.keys():
                if hostname == self._backend_nameparts(backend_name)[0]:
                    self.backend_table.pop(backend_name)

            self.add_proxy(hostname, reread_conf=False, **kwds)

        except:
            # undo changes
            self.proxies_inc.read(self.proxies_inc_path + '.bak')
            self.app_servers_inc.read(self.app_inc_path + '.bak')
            self._save_proxies_inc()
            self._save_app_servers_inc()
            raise

    # TODO: use this method in backend conf making or smth.
    def _server_to_str(self, server):
        if type(server) is unicode:
            return str(server)
        if type(server) is str:
            return server

        result = server['host'] if 'host' in server else server['servers'][0]
        if 'port' in server:
            result = '%s:%s' % (result, server['port'])

        if 'backup' in server and _bool_from_scalr_str(server['backup']):
            result = '%s %s' % (result, 'backup')

        _max_fails = server.get('max_fails')
        if _max_fails:
            result = '%s %s' % (result, 'max_fails=%i' % _max_fails)

        _fail_timeout = server.get('fail_timeout')
        if _fail_timeout:
            result = '%s %s' % (result, 'fail_timeout=%is' % _fail_timeout)

        if 'down' in server and _bool_from_scalr_str(server['down']):
            result = '%s %s' % (result, 'down')

        _weight = server.get('weight')
        if _weight:
            result = '%s %s' % (result, 'weight=%s' % _weight)

        return result

    @rpc.command_method
    def add_server(self,
                   backend,
                   server,
                   update_conf=True,
                   reload_service=True,
                   update_backend_table=False):
        """
        Adds server to backend with given name pattern.
        Parameter server can be dict or string (ip addr)

        :param backend: backend's name to which server will be added.
        :type backend: str

        :param server: server configuration. Can be just IP of the server or
            dict of parameters (such as 'down', 'backup' or 'port')
        :type server: dict or str

        :param update_conf: if True updates app_servers_inc object from file
            before performing server addition.
        :type update_conf: bool

        :param reload_service: if True reloads nginx service after server addition.
        :type reload_service: bool

        :param update_backend_table: if True updates self.backend_table after server addition.
        :type update_backend_table: bool

        Examples:

        Adding server without parameters to backend `backend`::

            api.nginx.add_server('backend', '123.321.111.19')

        Adding server with non-standard port to backend `test`::

            api.nginx.add_server('test', {'host': '11.22.33.44', 'port': '8089'})
        """
        update_conf = _bool_from_scalr_str(update_conf)
        reload_service = _bool_from_scalr_str(reload_service)
        update_backend_table = _bool_from_scalr_str(update_backend_table)

        if update_conf:
            self._load_app_servers_inc()

        if not server:
            return

        _logger.debug('Adding server %s to backend %s' % (server, backend))

        xpath = self.app_servers_inc.xpath_of('upstream', backend + '*')

        server = self._server_to_str(server)
        already_added = self.app_servers_inc.xpath_of('%s/server' % xpath,
                                                      server)
        if not already_added:
            self.app_servers_inc.add('%s/server' % xpath, server)

            if update_backend_table:
                if self.backend_table[backend]:
                    dest = self.backend_table[backend][0]
                    dest['servers'].append(server)
                else:
                    location = self._backend_nameparts(backend)[1] or '/'
                    dest = {'location': location,
                            'servers': [server]}
                    self.backend_table[backend] = [dest]

        if update_conf:
            self._save_app_servers_inc()
        if reload_service:
            self._reload_service()

    @rpc.command_method
    def remove_server(self,
                      backend,
                      server,
                      update_conf=True,
                      reload_service=True,
                      update_backend_table=False):
        """
        Removes server from backend with given name pattern.
        Parameter server can be dict or string (ip addr)

        :param backend: backend's name from which server will be removed.
        :type backend: str

        :param server: server IP.
        :type server: str

        :param update_conf: if True updates app_servers_inc object from file
            before performing server removal.
        :type update_conf: bool

        :param reload_service: if True reloads nginx service after server removal.
        :type reload_service: bool

        :param update_backend_table: if True updates self.backend_table after server removal.
        :type update_backend_table: bool

        Example:

        Removing server from backend `backend`::

            api.nginx.remove_server('backend', '123.321.111.19')
        """
        update_conf = _bool_from_scalr_str(update_conf)
        reload_service = _bool_from_scalr_str(reload_service)
        update_backend_table = _bool_from_scalr_str(update_backend_table)

        if update_conf:
            self._load_app_servers_inc()

        if not server:
            return

        if type(server) is dict:
            server = server['host']

        backend_xpath = self.app_servers_inc.xpath_of('upstream', backend + '*')
        server_xpath = self.app_servers_inc.xpath_of('%s/server' % backend_xpath,
                                                     server + '*')
        if server_xpath:
            self.app_servers_inc.remove(server_xpath)

            if update_backend_table:
                empty_destinations = []
                for destination in self.backend_table[backend]:
                    if server in destination['servers']:
                        destination['servers'].remove(server)
                        if not destination['servers']:
                            empty_destinations.append(destination)
                for destination in empty_destinations:
                    self.backend_table[backend].remove(destination)

        if update_conf:
            self._save_app_servers_inc()
        if reload_service:
            self._reload_service()

    @rpc.command_method
    def add_server_to_role(self, server, role_id, update_conf=True, reload_service=True):
        """
        Adds server to each backend that uses given role. If role isn't used in
        any backend, does nothing

        :param server: server configuration. Can be just IP of the server or
            dict of parameters (such as 'down', 'backup' or 'port')
        :type server: dict or str

        :param role_id: Id of the role in which new server is up.
        :type role_id: str

        :param update_conf: if True updates app_servers_inc object from file
            before performing server addition.
        :type update_conf: bool

        :param reload_service: if True reloads nginx service after server addition.
        :type reload_service: bool

        Examples:

        Adding server without parameters to backends that are contain role `1234`::

            api.nginx.add_server_to_role('123.321.111.19', '1234')

        Adding server with non-standard port to backends that are contain
                role `4321`::

            api.nginx.add_server_to_role({'host': '11.22.33.44', 'port': '8089'},
                                             '4321')
        """
        update_conf = _bool_from_scalr_str(update_conf)
        reload_service = _bool_from_scalr_str(reload_service)

        if update_conf:
            self._load_app_servers_inc()

        if not server:
            return
        if not role_id:
            return
        if type(role_id) is not str:
            role_id = str(role_id)

        config_updated = False
        for backend_name, backend_destinations in self.backend_table.items():
            for dest in backend_destinations:
                if dest.get('id') == role_id and server not in dest['servers']:
                    srv = {'host': server}
                    # taking server parameters
                    srv.update(dest)
                    srv.pop('servers')
                    srv.pop('id')
                    
                    self.add_server(backend_name, srv, False, False)
                    if len(dest['servers']) == 0:
                        self.remove_server(backend_name, '127.0.0.1', False, False)
                    dest['servers'].append(server)
                    config_updated = True

        if config_updated:
            if update_conf:
                self._save_app_servers_inc()
            if reload_service:
                self._reload_service()

    @rpc.command_method
    def remove_server_from_role(self,
                                server,
                                role_id,
                                update_conf=True,
                                reload_service=True):
        """
        Removes server from each backend that uses given role. If role isn't
        used in any backend, does nothing

        :param server: server IP
        :type server: str

        :param role_id: Id of the role in which server is down.
        :type role_id: str

        :param update_conf: if True updates app_servers_inc object from file 
            before performing server removal.
        :type update_conf: bool

        :param reload_service: if True reloads nginx service after server removal.
        :type reload_service: bool

        Example:

        Removing server from backends that are contain role `1234`::

            api.nginx.remove_server_from_role('123.321.111.19', '1234')
        """
        update_conf = _bool_from_scalr_str(update_conf)
        reload_service = _bool_from_scalr_str(reload_service)

        if update_conf:
            self._load_app_servers_inc()

        if not server:
            return
        if not role_id:
            return
        if type(role_id) is not str:
            role_id = str(role_id)

        config_updated = False
        for backend_name, backend_destinations in self.backend_table.items():
            for dest in backend_destinations:
                if dest.get('id') == role_id and server in dest['servers']:
                    if len(dest['servers']) == 1:
                        self.add_server(backend_name, '127.0.0.1', False, False)
                    self.remove_server(backend_name, server, False, False)
                    dest['servers'].remove(server)
                    config_updated = True

        if config_updated:
            if update_conf:
                self._save_app_servers_inc()
            if reload_service:
                self._reload_service()


    @rpc.command_method
    def remove_server_from_all_backends(self,
                                        server,
                                        update_conf=True,
                                        reload_service=True):
        """
        Method is used to remove stand-alone servers, that aren't belong
        to any role. If role isn't used in any backend, does nothing

        :param server: Server IP.
        :type server: str

        :param update_conf: if True updates app_servers_inc object from file 
            before performing server removal.
        :type update_conf: bool

        :param reload_service: if True reloads nginx service after server removal.
        :type reload_service: bool

        Example:

        Removing server from all backends::

            api.nginx.remove_server_from_all_backends('123.321.111.19')
        """
        update_conf = _bool_from_scalr_str(update_conf)
        reload_service = _bool_from_scalr_str(reload_service)

        if update_conf:
            self._load_app_servers_inc()

        if not server:
            return

        config_updated = False
        for backend_name, backend_destinations in self.backend_table.items():
            for dest in backend_destinations:
                if server in dest['servers']:
                    self.remove_server(backend_name, server, False, False)
                    dest['servers'].remove(server)
                    config_updated = True

        if config_updated:
            if update_conf:
                self._save_app_servers_inc()
            if reload_service:
                self._reload_service()

    @rpc.command_method
    def enable_ssl(self,
                   hostname,
                   ssl_port=None,
                   ssl_certificate_id=None,
                   update_conf=True,
                   reload_service=True):
        """
        Enables SSL support on Nginx server.

        :param hostname: nginx proxy server name.
        :type hostname: str

        :param ssl_port: Port number.
        :type ssl_port: str

        :param ssl_certificate_id: Id of ssl certificate.
        :type ssl_certificate_id: str

        :param update_conf: if True updates app_servers_inc object from file 
            before performing ssl enabling.
        :type update_conf: bool

        :param reload_service: if True reloads nginx service after ssl enabling.
        :type reload_service: bool


        Example:

        Enable ssl on server with name `test.com`::

            api.nginx.enable_ssl('test.com', '443', '12345')
        """
        update_conf = _bool_from_scalr_str(update_conf)
        reload_service = _bool_from_scalr_str(reload_service)

        if update_conf:
            self._load_proxies_inc()

        if not hostname:
            return

        config_updated = False
        ssl_port = ssl_port or '443'
        for i, _ in enumerate(self.proxies_inc.get_list('server')):
            server_xpath = 'server[%i]' % (i + 1)
            server_name = self.proxies_inc.get('%s/server_name' % server_xpath)
            redirector = self._is_redirector(self.proxies_inc, server_xpath)

            if hostname == server_name and not redirector:
                listen_list = self.proxies_inc.get_list('%s/listen' % server_xpath)
                http = any(ssl_port not in listen for listen in listen_list)
                try:
                    # trying get ssl param from config
                    # if it raises exception, then we need to set up ssl
                    # like in first time
                    default_needed = self._old_style_ssl_on()
                    ssl_listen_xpath = self.proxies_inc.xpath_of('%s/listen' % server_xpath,
                                                                 '*ssl*')
                    if http and not ssl_listen_xpath:
                        val = '%s%s ssl' % (ssl_port, ' default' if default_needed else '')
                        self.proxies_inc.add('%s/listen' % server_xpath, val)
                    elif not http:
                        self.proxies_inc.get('%s/ssl' % server_xpath)
                        self.proxies_inc.set('%s/ssl' % server_xpath, 'on')
                except metaconf.NoPathError:
                    self._add_ssl_params(self.proxies_inc,
                                         server_xpath,
                                         ssl_port,
                                         ssl_certificate_id,
                                         http)
                break

        if config_updated:
            if update_conf:
                self._save_proxies_inc()
            if reload_service:
                self._reload_service()

    @rpc.command_method
    def disable_ssl(self, hostname, update_conf=True, reload_service=True):
        """
        Disables SSL support on Nginx server.

        :param hostname: nginx proxy server name.
        :type hostname: str

        :param update_conf: if True updates app_servers_inc object from file 
            before performing ssl disabling.
        :type update_conf: bool

        :param reload_service: if True reloads nginx service after ssl disabling.
        :type reload_service: bool

        Example:

        Disable ssl on server with name `test.com`::

            api.nginx.disable_ssl('test.com')
        """
        update_conf = _bool_from_scalr_str(update_conf)
        reload_service = _bool_from_scalr_str(reload_service)

        if update_conf:
            self._load_proxies_inc()

        if not hostname:
            return

        config_updated = False
        for i, _ in enumerate(self.proxies_inc.get_list('server')):
            server_xpath = 'server[%i]' % (i + 1)
            server_name = self.proxies_inc.get('%s/server_name' % server_xpath)
            redirector = self._is_redirector(self.proxies_inc, server_xpath)

            if hostname == server_name and not redirector:
                try:
                    if self.proxies_inc.get('%s/ssl' % server_xpath) is 'on':
                        self.proxies_inc.set('%s/ssl' % server_xpath, 'off')
                except metaconf.NoPathError:
                    # if there were no ssl option mentioned
                    ssl_listen_xpath = self.proxies_inc.xpath_of('%s/listen' % server_xpath,
                                                                 '*ssl*')
                    if ssl_listen_xpath:
                        self.proxies_inc.remove(ssl_listen_xpath)
                break

        if config_updated:
            if update_conf:
                self._save_proxies_inc()
            if reload_service:
                self._reload_service()

    @classmethod
    def do_check_software(cls, system_packages=None):
        return pkgmgr.check_any_software([['nginx'], ['nginx14']], system_packages)[0]

