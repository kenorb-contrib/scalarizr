'''
Created on Jul 23, 2010

@author: marat
@author: Dmytro Korsakov
'''

# Core
from scalarizr.bus import bus
from scalarizr.config import BuiltinBehaviours
from scalarizr.services import PresetProvider, BaseConfig
from scalarizr.api import service as preset_service
from scalarizr.api import memcached as memcached_api
from scalarizr.handlers import ServiceCtlHandler, FarmSecurityMixin
from scalarizr.messaging import Messages
from scalarizr import linux

# Libs
from scalarizr.util import initdv2


# Stdlibs
import logging, re


if linux.os.debian_family:
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
        FarmSecurityMixin.__init__(self)
        self.init_farm_security([11211])
        ServiceCtlHandler.__init__(self, BEHAVIOUR, memcached_api.MemcachedInitScript())
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
    config_name = 'apache2.conf' if linux.os.debian_family else 'httpd.conf'


class MemcachedPresetProvider(PresetProvider):

    def __init__(self):
        service = initdv2.lookup(SERVICE_NAME)
        config_mapping = {'memcached.conf':MemcachedConf(mcd_conf_path)}
        PresetProvider.__init__(self, service, config_mapping)
