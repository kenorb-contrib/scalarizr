from __future__ import with_statement
 
import os
import shutil
import logging
from telnetlib import Telnet
import time
from hashlib import sha1
 
from scalarizr import rpc
from scalarizr.bus import bus
from scalarizr.libs import metaconf
import scalarizr.libs.metaconf.providers
from scalarizr.node import __node__
from scalarizr.util import initdv2
from scalarizr.util import system2
from scalarizr.util import PopenError
from scalarizr.util import Singleton
from scalarizr.linux import iptables
from scalarizr.linux import LinuxError
 
 
__nginx__ = __node__['nginx']
 
 
_logger = logging.getLogger(__name__)
 
 
class NginxInitScript(initdv2.ParametrizedInitScript):
    _nginx_binary = None
 
    def __init__(self):
        self._nginx_binary = __nginx__['binary_path']
 
        pid_file = None
        '''
        Saw on 8.04:
        --pid-path=/var/run/nginx
        but actual pid-file is /var/run/nginx.pid
        try:
                nginx = software.whereis('nginx')
                if nginx:
                        out = system2((nginx[0], '-V'))[1]
                        m = re.search("--pid-path=(.*?)\s", out)
                        if m:
                                        pid_file = m.group(1)
        except:
                pass
        '''
 
        initdv2.ParametrizedInitScript.__init__(self,
                                                'nginx',
                                                '/etc/init.d/nginx',
                                                pid_file=pid_file,
                                                socks=[])
 
    def _wait_workers(self):
        conf_dir = os.path.dirname(__nginx__['app_include_path'])
        conf_path = os.path.join(conf_dir, 'nginx.conf')
        conf = metaconf.Configuration('nginx')
        conf.read(conf_path)
 
        expected_workers_num = int(conf.get('worker_processes'))
 
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
            args = [self.initd_script, 'status']
            _, _, returncode = system2(args, raise_exc=False)
            if returncode == 0:
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
        return
 
    def restart(self):
        self.configtest()
        ret = initdv2.ParametrizedInitScript.restart(self)
        time.sleep(1)
        return
 
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
 
def _open_port(port):
    if iptables.enabled():
        rule = {"jump": "ACCEPT", "protocol": "tcp", "match": "tcp", "dport": str(port)}
        iptables.FIREWALL.ensure([rule])
 
 
def _close_port(port):
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
 
 
class NginxAPI(object):
 
    __metaclass__ = Singleton
 
    def __init__(self, app_inc_dir=None, proxies_inc_dir=None):
        _logger.debug('Initializing nginx API.')
        self.service = NginxInitScript()
        self.error_pages_inc = None
        self.backend_table = {}
 
        if not app_inc_dir:
            app_inc_dir = os.path.dirname(__nginx__['app_include_path'])
        self.app_inc_path = os.path.join(app_inc_dir, 'app-servers.include')
        self._load_app_servers_inc()
        self.fix_app_servers_inc()
 
        if not proxies_inc_dir:
            proxies_inc_dir = os.path.dirname(__nginx__['app_include_path'])
        self.proxies_inc_dir = proxies_inc_dir
        self.proxies_inc_path = os.path.join(proxies_inc_dir, 'proxies.include')
        self._load_proxies_inc()
 
        self._make_error_pages_include()
 
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
        if self.service.status() == initdv2.Status.NOT_RUNNING:
            self.service.start()
        else:
            self.service.reload()
 
    @rpc.command_method
    def start_service(self):
        self.service.start()
 
    @rpc.command_method
    def stop_service(self):
        self.service.stop()
 
    @rpc.command_method
    def reload_service(self):
        self.service.reload()
 
    @rpc.command_method
    def restart_service(self):
        self.service.restart()
 
    @rpc.command_method
    def recreate_proxying(self, proxy_list, reload_service=True):
        if not proxy_list:
            proxy_list = []
 
        _logger.debug('Recreating proxying with %s' % proxy_list)
        self._clear_nginx_includes()
        self.backend_table = {}
 
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
 
    def _replace_string_in_file(self, file_path, s, new_s):
        raw = None
        with open(file_path, 'r') as fp:
            raw = fp.read()
            raw = raw.replace(s, new_s)
        with open(file_path, 'w') as fp:
            fp.write(raw)
 
    @rpc.service_method
    def reconfigure(self, proxy_list):
        # TODO: much like recreate_proxying() but with specs described in
        # https://scalr-labs.atlassian.net/browse/SCALARIZR-481?focusedCommentId=17428&page=com.atlassian.jira.plugin.system.issuetabpanels:comment-tabpanel#comment-17428
        # saving backend configuration table
        backend_table_bak = self.backend_table.copy()
        try:
            self.app_inc_path = self.app_inc_path + '.new'
            self.proxies_inc_path = self.proxies_inc_path + '.new'
            self.recreate_proxying(proxy_list, reload_service=False)
 
            main_conf_path = self.proxies_inc_dir + '/nginx.conf'
            self._replace_string_in_file(main_conf_path,
                                         'proxies.include',
                                         'proxies.include.new')
            self._replace_string_in_file(main_conf_path,
                                         'app-servers.include',
                                         'app-servers.include.new')
 
            self.service.configtest()
        except:
            os.remove(self.app_inc_path)
            os.remove(self.proxies_inc_path)
            self.backend_table = backend_table_bak
            self._replace_string_in_file(main_conf_path,
                                         'proxies.include.new',
                                         'proxies.include')
            self._replace_string_in_file(main_conf_path,
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
            self._replace_string_in_file(main_conf_path,
                                         'proxies.include.new',
                                         'proxies.include')
            self._replace_string_in_file(main_conf_path,
                                         'app-servers.include.new',
                                         'app-servers.include')
            self._reload_service()
            self.app_inc_path = self.app_inc_path[:-4]
            self.proxies_inc_path = self.proxies_inc_path[:-4]
 
 
    def get_role_servers(self, role_id=None, role_name=None):
        """ Method is used to get role servers from scalr """
        if type(role_id) is int:
            role_id = str(role_id)
 
        server_location = __node__['cloud_location']
        queryenv = bus.queryenv_service
        roles = queryenv.list_roles(farm_role_id=role_id, role_name=role_name)
        servers = []
        for role in roles:
            ips = [h.internal_ip if server_location == h.cloud_location else
                   h.external_ip
                   for h in role.hosts]
            servers.extend(ips)
 
        return servers
 
    def update_ssl_certificate(self, ssl_certificate_id, cert, key, cacert):
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
 
    def _fetch_ssl_certificate(self, ssl_certificate_id):
        """
        Gets ssl certificate and key from Scalr, writes them to files and
        returns paths to files.
        """
        queryenv = bus.queryenv_service
        cert, key, cacert = queryenv.get_ssl_certificate(ssl_certificate_id)
        return self.update_ssl_certificate(ssl_certificate_id, cert, key, cacert)
 
    def _normalize_destinations(self, destinations):
        """
        Parses list of destinations. They are dictionaries. Dictionary example:
 
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
                dest['servers'].extend(self.get_role_servers(dest['id']))
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
            result[key] = {'content': template['content']}
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
        ssl_cert_path, ssl_cert_key_path = self._fetch_ssl_certificate(ssl_certificate_id)
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
            _open_port(port)
        if ssl_port:
            _open_port(ssl_port)
 
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
                    _close_port(port)
 
                xpaths_to_remove.append(server_xpath)
 
        for xpath in reversed(xpaths_to_remove):
            self.proxies_inc.remove(xpath)
 
    def _get_any_port(self, config):
        port = None
        try:
            addr = self.proxies_inc.get('server[1]/listen' % server_xpath)
            port = addr.split()[0]
        except metaconf.NoPathError:
            pass
        return port
 
    @rpc.command_method
    def remove_proxy(self, hostname, reload_service=True):
        """
        Removes proxy with given hostname. Removes created server and its backends.
        """
        reload_service = _bool_from_scalr_str(reload_service)
 
        _logger.debug('Removing proxy with hostname: %s' % hostname)
        self._load_proxies_inc()
        self._load_app_servers_inc()
 
        self._remove_nginx_server(hostname)
 
        # remove each backend that were in use by this proxy from backend_table
        for backend_name in self.backend_table.keys():
            if hostname == self._backend_nameparts(backend_name)[0]:
                self.backend_table.pop(backend_name)
 
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
        parameters. If some exception occures, changes are reverted.
        See add_proxy() for detailed kwds description.
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
    def add_server_to_role(self, 
                           server,
                           role_id,
                           update_conf=True, 
                           reload_service=True):
        """
        Adds server to each backend that uses given role. If role isn't used in
        any backend, does nothing
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
 
