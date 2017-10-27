'''
Created on Aug 29, 2011
 
@author: marat
'''
 
from scalarizr import config
from scalarizr import handlers
from scalarizr import messaging
from scalarizr import storage
from scalarizr import util
from scalarizr.linux.rsync import rsync
from scalarizr.bus import bus
from scalarizr.services import cloudfoundry
 
 
import logging
import os
import shutil
 
 
def get_handlers():
    return (CloudControllerHandler(), SvssHandler(), MysqlHandler(), MainHandler())
 
 
LOG = logging.getLogger(__name__)
SERVICE_NAME = 'cloudfoundry'
DEFAULTS = {
        'home': '/root/cloudfoundry/vcap',
        'datadir': '/var/vcap'
}
 
 
_cnf = _ini = _bhs = _queryenv = _platform = None
_cf = _components = _services = None
_home = _datadir = None
 
 
def is_cloud_controller():
    return _bhs.cloud_controller in _bhs
 
def is_service():
    return _bhs.service in _bhs
 
def is_scalarizr_running():
    return _cnf.state == config.ScalarizrState.RUNNING
 
def public_ip():
    return _platform.get_public_ip()
 
def local_ip():
    return _platform.get_private_ip()
 
def from_cloud_controller(msg):
    return _bhs.cloud_controller in msg.behaviour
 
def its_me(msg):
    return msg.remote_ip == _platform.get_public_ip()
 
def svss():
    return _components + _services
 
class list_ex(list):
    def __setattr__(self, key, value):
        self.__dict__[key] = value
 
 
class MainHandler(handlers.Handler, handlers.FarmSecurityMixin):
 
    def __init__(self):
        handlers.FarmSecurityMixin.__init__(self, [4222, 9022, 12345])
        bus.on(init=self.on_init)
        self._init_globals()
 
    def _init_globals(self):
        global _cnf, _ini, _bhs, _queryenv, _platform
        global _cf, _components, _services
 
        _queryenv = bus.queryenv_service
        _platform = bus.platform
 
        # Apply configuration
        _cnf = bus.cnf
        _ini = _cnf.rawini
        for key, value in DEFAULTS.iteritems():
            if _ini.has_option(SERVICE_NAME, key):
                value = _ini.get(SERVICE_NAME, key)
            globals()['_' + key] = value
 
 
        _services = []
        _components, _bhs = [], list_ex()
        _cf = cloudfoundry.CloudFoundry(_home)
 
        behaviour_str = _ini.get('general', 'behaviour')
        for prop in dir(config.BuiltinBehaviours):
            if prop.startswith('CF') or prop == 'WWW':
                bh = getattr(config.BuiltinBehaviours, prop)
                if prop.startswith('CF'):
                    cmp = bh[3:]
                    setattr(_bhs, cmp, bh)
                else:
                    setattr(_bhs, 'www', bh)
                if bh in behaviour_str:
                    _bhs.append(bh)
                    if bh not in ('cf_service', 'www'):
                        _components.append(cmp)
        LOG.debug('Behaviors: %s', _bhs)
        LOG.debug('Components %s:', _components)
 
 
    def _start_services(self):
        _cf.start(*svss())
 
 
    def _stop_services(self):
        _cf.stop(*svss())
 
 
    def _locate_cloud_controller(self):
        util.wait_until(self._do_locate_cloud_controller, timeout=600, logger=LOG,
                                start_text='Locating cloud_controller server',
                                error_text='Cannot locate cloud_controller server')
 
 
    def _do_locate_cloud_controller(self):
        host = None
        if is_cloud_controller():
            host = local_ip()
        else:
            roles = _queryenv.list_roles(behaviour=_bhs.cloud_controller)
            if roles and roles[0].hosts:
                host = roles[0].hosts[0].internal_ip
        if host:
            _cf.cloud_controller = host
        return bool(host)
 
 
    def accept(self, message, queue, **kwds):
        result = message.name in (
                        messaging.Messages.HOST_INIT,
                        messaging.Messages.HOST_DOWN,
                        messaging.Messages.HOST_UP,
                        messaging.Messages.BEFORE_HOST_TERMINATE)
        return result
 
 
    def on_init(self, *args, **kwds):
        LOG.debug('Called on_init')
        bus.on(
                reload=self.on_reload,
                start=self.on_start,
                before_host_up=self.on_before_host_up,
                before_reboot_start=self.on_before_reboot_start
        )
 
 
    def on_reload(self, *args, **kwds):
        LOG.debug('Called on_reload')
        self._init_globals()
 
    def on_start(self):
        LOG.debug('Called on_start')
        if is_scalarizr_running():
            self._start_services()
 
 
    def on_before_host_up(self, msg):
        LOG.debug('Called on_before_host_up')
        log = bus.init_op.logger if bus.init_op else LOG
 
        log.info('Locating CloudController')
        self._locate_cloud_controller()
 
        log.info('Patching configuration files')
        LOG.debug('Setting ip route')
        for name in _components:
            _cf.components[name].ip_route = local_ip()
        LOG.debug('Creating log directories')
        for name in _components:
            cmp = _cf.components[name]
            if 'log_file' in cmp.config:
                dir = os.path.dirname(cmp.config['log_file'])
                if not os.path.exists(dir):
                    os.makedirs(dir)
        for name in _services:
            _cf.services[name].ip_route = local_ip()
 
        log.info('Starting services')
        self._start_services()
 
 
    def on_HostUp(self, msg):
        LOG.debug('Called on_HostUp')
        if from_cloud_controller(msg) and not its_me(msg):
            _cf.cloud_controller = msg.remote_ip
            self._start_services()
 
 
    def on_before_reboot_start(self, msg):
        LOG.debug('Called on_before_reboot_start')
        self._stop_services()
 
 
    def on_BeforeHostTerminate(self, msg):
        LOG.debug('Called on_BeforeHostTerminate')
        if its_me(msg):
            self._stop_services()
 
 
class CloudControllerHandler(handlers.Handler):
    LOG = logging.getLogger(__name__ + '.cloud_controller')
 
    def __init__(self):
        super(CloudControllerHandler, self).__init__()
        bus.on(init=self.on_init)
 
    def _set_volume_config(self, cnf):
        volume_dir = os.path.dirname(self.volume_path)
        if not os.path.exists(volume_dir):
            os.makedirs(volume_dir)
        if cnf:
            storage.Storage.backup_config(cnf, self.volume_path)
        self._volume_config = cnf
 
    def _get_volume_config(self):
        if not self._volume_config:
            self._volume_config = storage.Storage.restore_config(self.volume_path)
        return self._volume_config
 
 
    volume_config = property(_get_volume_config, _set_volume_config)
 
    def _init_objects(self):
        self.volume_path = _cnf.private_path('storage/cloudfoundry.json')
        self.volume_config = None
        self.volume = None
 
 
    def _os_hosts(self, ipaddr, hostname):
        hosts_file = '/etc/hosts'
        hosts_bck_file = hosts_file + '.bck'
        if not os.path.exists(hosts_bck_file):
            shutil.copy(hosts_file, hosts_bck_file)
 
        lines = open(hosts_file).readlines()
        newline = '%s %s\n' % (ipaddr, hostname)
 
        fp = open(hosts_file, 'w')
        updated = False
        for line in lines:
            if hostname in line:
                line = newline
                updated = True
            fp.write(line)
        if not updated:
            fp.write(newline)
        fp.close()
 
 
    def _locate_nginx(self):
        util.wait_until(self._do_locate_nginx, timeout=600, logger=LOG,
                                start_text='Locating nginx frontend server',
                                error_text='Cannot locate nginx frontend server')
 
 
    def _do_locate_nginx(self):
        host = None
        if 'www' in _bhs:
            host = local_ip()
        else:
            roles = _queryenv.list_roles(behaviour='www')
            if roles and roles[0].hosts:
                host = roles[0].hosts[0].internal_ip
        if host:
            self._os_hosts(host, _cf.components['cloud_controller'].config['external_uri'])
        return bool(host)
 
 
    @property
    def cf_tags(self):
        return handlers.build_tags(SERVICE_NAME, 'active')
 
 
    def _plug_storage(self, vol=None, mpoint=None):
        vol = vol or self.volume_config
        mpoint = mpoint or _datadir
        if type(vol) == dict:
            vol['tags'] = self.cf_tags
        if not hasattr(vol, 'id'):
            vol = storage.Storage.create(vol)
 
        try:
            if not os.path.exists(mpoint):
                os.makedirs(mpoint)
            if not vol.mounted():
                vol.mount(mpoint)
        except storage.StorageError, e:
            ''' XXX: Crapy. We need to introduce error codes from mount '''
            if 'you must specify the filesystem type' in str(e):
                vol.mkfs()
                vol.mount(mpoint)
            else:
                raise
        return vol
 
 
    #def accept(self, message, queue, behaviour=None, platform=None, os=None, dist=None):
    #       result = is_cloud_controller() and message.name == messaging.Messages.BEFORE_HOST_UP and its_me(message)
    #       return result
 
 
    def on_init(self):
        if is_cloud_controller():
            bus.on(
                    start=self.on_start,
                    host_init_response=self.on_host_init_response,
                    before_host_up=self.on_before_host_up,
                    reload=self.on_reload
            )
            self._init_objects()
 
    def on_reload(self):
        self._init_objects()
 
 
    def on_start(self):
        if is_scalarizr_running():
            self._locate_nginx()
            self._plug_storage()
 
 
    def on_host_init_response(self, msg):
        '''
        Store volume configuration from HIR message
        '''
        log = bus.init_op.logger if bus.init_op else self.LOG
        self.LOG.debug('Called on_host_init_response')
        ini = msg.body.get(_bhs.cloud_controller, {})
        self.volume_config = ini.pop('volume_config', dict(
            type='loop', 
            file='/mnt/cfdata.loop',
            size=500
        ))
 
        '''
        Plug storage, initialize database
        Why here? cause before_host_up routines could be executed after MysqlHandler
        and it will lead to fail
        '''
 
        log.info('Creating VCAP data storage')
        # Initialize storage
        tmp_mpoint = '/mnt/tmp.vcap'
        try:
            self.volume = self._plug_storage(mpoint=tmp_mpoint)
            if not _cf.valid_datadir(tmp_mpoint):
                LOG.info('Copying data from %s to storage', _datadir)
                rsync(_datadir + '/', tmp_mpoint, archive=True, delete=True)
 
            LOG.debug('Mounting storage to %s', _datadir)
            self.volume.umount()
            self.volume.mount(_datadir)
        except:
            LOG.exception('Failed to initialize storage')
        finally:
            if os.path.exists(tmp_mpoint):
                os.removedirs(tmp_mpoint)
        self.volume_config = self.volume.config()
 
        log.info('Locating Nginx frontend')
        _cf.components['cloud_controller'].allow_external_app_uris = True
        self._locate_nginx()
 
        log.info('Creating CloudController database')
        _cf.init_db()
 
 
    def on_before_host_up(self, msg):
        msg.body[_bhs.cloud_controller] = dict(volume_config=self.volume_config)
 
 
 
class SvssHandler(handlers.Handler):
 
    def accept(self, message, queue, **kwds):
        return is_service() and message.name in (messaging.Messages.HOST_INIT_RESPONSE, )
 
    def on_HostInitResponse(self, msg):
        globals()['_services'] = msg.body.get(_bhs.service, {}).keys()
 
 
class MysqlHandler(handlers.Handler):
    def __init__(self):
        self.enabled = False
        bus.on(init=self.on_init)
 
 
    #def accept(self, message, queue, behaviour=None, platform=None, os=None, dist=None):
    #       return self.enabled and message.name in (messaging.Messages.BEFORE_HOST_UP, )
 
 
    def on_init(self):
        bus.on(host_init_response=self.on_host_init_response)
 
 
    def on_host_init_response(self, msg):
        ini = msg.body.get(_bhs.service, {})
        self.enabled = 'mysql' in ini
        if self.enabled:
            svs_conf = ini['mysql'].copy()
 
            svs = _cf.services['mysql']
            svs.node_config['mysql']['host'] = svs_conf['hostname']
            svs.node_config['mysql']['user'] = svs_conf['user']
            svs.node_config['mysql']['password'] = svs_conf['password']
            svs.flush_node_config()
 
