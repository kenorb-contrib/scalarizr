import os
import logging

from scalarizr import rpc
from scalarizr import linux
from scalarizr.util import initdv2
from scalarizr.linux import pkgmgr
from scalarizr.util import Singleton
from scalarizr import exceptions
from scalarizr.api import BehaviorAPI


LOG = logging.getLogger(__name__)


class MemcachedAPI(BehaviorAPI):

    __metaclass__ = Singleton

    behavior = 'memcached'

    def __init__(self):
        self.service = MemcachedInitScript()

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
    def get_service_status(self):
        return self.service.status()

    @classmethod
    def do_check_software(cls, system_packages=None):
        return pkgmgr.check_software(['memcached'], system_packages)[0]


class MemcachedInitScript(initdv2.ParametrizedInitScript):

    def __init__(self):
        pid_file = None
        if linux.os.redhat_family:
            pid_file = "/var/run/memcached/memcached.pid"
        elif linux.os.debian_family:
            pid_file = "/var/run/memcached.pid"

        if linux.os.redhat_family and linux.os["release"].version[0] == 7:
            initd_script = ("/sbin/service", "memcached")
        else:
            initd_script = '/etc/init.d/memcached'
            if not os.path.exists(initd_script):
                msg = "Cannot find Memcached init script at %s. Make sure that memcached is installed"
                raise Exception(msg % initd_script)

        initdv2.ParametrizedInitScript.__init__(self,
                'memcached', initd_script, pid_file, socks=[initdv2.SockParam(11211)])

initdv2.explore('memcached', MemcachedInitScript)

