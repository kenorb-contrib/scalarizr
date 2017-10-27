from __future__ import with_statement
'''
Created on Jul 23, 2010
 
@author: marat
@author: Dmytro Korsakov
'''
 
from __future__ import with_statement
 
# Core
from scalarizr.bus import bus
from scalarizr.config import BuiltinBehaviours
from scalarizr.services import PresetProvider, BaseConfig
from scalarizr.api import service as preset_service
from scalarizr.handlers import ServiceCtlHandler, HandlerError, FarmSecurityMixin
from scalarizr.messaging import Messages
 
# Libs
from scalarizr.util import disttool, initdv2
 
 
# Stdlibs
import logging, re, os
 
 
if disttool._is_debian_based:
    mcd_conf_path = '/etc/memcached.conf'
    expression = re.compile('^\s*-m\s*\d*$', re.M)
    mem_re = re.compile('^-m\s+(?P<memory>\d+)\s*$', re.M)
    template = '-m AMOUNT'
else:
    mcd_conf_path = '/etc/sysconfig/memcached'
    expression = re.compile('^\s*CACHESIZE\s*=\s*"\d*"$', re.M)
    mem_re = re.compile('^\s*CACHESIZE\s*=\s*"(?P<memory>\d+)"\s*$', re.M)
    template = 'CACHESIZE="AMOUNT"'
 
def set_cache_size(sub):
    mcd_conf = None
    with open(mcd_conf_path, 'r') as fp:
        mcd_conf = fp.read()
 
    if mcd_conf:
        if expression.findall(mcd_conf):
            with open(mcd_conf_path, 'w') as fp:
                fp.write(re.sub(expression, sub, mcd_conf))
        else:
            with open(mcd_conf_path, 'a') as fp:
                fp.write(sub)
 
def get_cache_size():
    mcd_conf = None
    with open(mcd_conf_path, 'r') as fp:
        mcd_conf = fp.read()
    if mcd_conf:
        result = re.search(mem_re, mcd_conf)
        if result:
            return result.group('memory')
        else:
            return '400'
 
 
class MemcachedInitScript(initdv2.ParametrizedInitScript):
    def __init__(self):
 
        pid_file = None
        if disttool.is_redhat_based():
            pid_file = "/var/run/memcached/memcached.pid"
        elif disttool.is_debian_based():
            pid_file = "/var/run/memcached.pid"
 
        initd_script = '/etc/init.d/memcached'
        if not os.path.exists(initd_script):
            raise HandlerError("Cannot find Memcached init script at %s. Make sure that memcached is installed" % initd_script)
 
        initdv2.ParametrizedInitScript.__init__(self, 'memcached', initd_script, pid_file, socks=[initdv2.SockParam(11211)])
 
initdv2.explore('memcached', MemcachedInitScript)
BEHAVIOUR = SERVICE_NAME = BuiltinBehaviours.MEMCACHED
 
def get_handlers():
    return [MemcachedHandler()]
 
class MemcachedHandler(ServiceCtlHandler, FarmSecurityMixin):
 
    _logger = None
    _queryenv = None
    _ip_tables = None
    _port = None
 
    def __init__(self):
        self.preset_provider = MemcachedPresetProvider()
        preset_service.services[BEHAVIOUR] = self.preset_provider
        FarmSecurityMixin.__init__(self, [11211])
        ServiceCtlHandler.__init__(self, BEHAVIOUR, MemcachedInitScript())
        self._logger = logging.getLogger(__name__)
        self._queryenv = bus.queryenv_service
        bus.on("init", self.on_init)
 
    def on_init(self):
        bus.on(before_host_up=self.on_before_host_up, host_init_response = self.on_host_init_response)
 
    def accept(self, message, queue, behaviour=None, platform=None, os=None, dist=None):
        return message.name in (Messages.HOST_INIT, Messages.HOST_DOWN, Messages.UPDATE_SERVICE_CONFIGURATION) \
                        and BEHAVIOUR in behaviour
 
    def on_before_host_up(self, message):
        # Service configured
        bus.fire('service_configured', service_name=SERVICE_NAME)
 
    def on_host_init_response(self, message):
        if hasattr(message, BEHAVIOUR):
            data = getattr(message, BEHAVIOUR)
            if data and 'preset' in data:
                self.initial_preset = data['preset'].copy()
 
class MemcachedConf(BaseConfig):
 
    config_type = 'app'
    config_name = 'apache2.conf' if disttool.is_debian_based() else 'httpd.conf'
 
 
class MemcachedPresetProvider(PresetProvider):
 
    def __init__(self):
        service = initdv2.lookup(SERVICE_NAME)
        config_mapping = {'memcached.conf':MemcachedConf(mcd_conf_path)}
        PresetProvider.__init__(self, service, config_mapping)
 
