import os
import signal
import logging

from scalarizr import rpc
from scalarizr import linux
from scalarizr.linux import pkgmgr
from scalarizr.util import Singleton, initdv2
from scalarizr import exceptions
from scalarizr.api import BehaviorAPI
from scalarizr.util import software


LOG = logging.getLogger(__name__)


class ChefInitScript(initdv2.ParametrizedInitScript):
    _default_init_script = '/etc/init.d/chef-client'

    def __init__(self):
        self._env = None
        super(ChefInitScript, self).__init__('chef', None, '/var/run/chef-client.pid')


    def start(self, env=None):
        self._env = env or os.environ
        super(ChefInitScript, self).start()


    # Uses only pid file, no init script involved
    def _start_stop_reload(self, action):
        chef_client_bin = linux.which('chef-client')
        if action == "start":
            if not self.running:
                # Stop default chef-client init script
                if os.path.exists(self._default_init_script):
                    linux.system(
                        (self._default_init_script, "stop"), 
                        close_fds=True, 
                        preexec_fn=os.setsid, 
                        raise_exc=False
                    )

                cmd = (chef_client_bin, '--daemonize', '--logfile', 
                        '/var/log/chef-client.log', '--pid', self.pid_file)
                out, err, rcode = linux.system(cmd, close_fds=True, 
                            preexec_fn=os.setsid, env=self._env,
                            stdout=open(os.devnull, 'w+'), 
                            stderr=open(os.devnull, 'w+'), 
                            raise_exc=False)
                if rcode == 255:
                    LOG.debug('chef-client daemon already started')
                elif rcode:
                    msg = (
                        'Chef failed to start daemonized. '
                        'Return code: %s\nOut:%s\nErr:%s'
                        )
                    raise initdv2.InitdError(msg % (rcode, out, err))

        elif action == "stop":
            if self.running:
                with open(self.pid_file) as f:
                    pid = int(f.read().strip())
                try:
                    os.getpgid(pid)
                except OSError:
                    os.remove(self.pid_file)
                else:
                    os.kill(pid, signal.SIGTERM)

    def restart(self):
        self._start_stop_reload("stop")
        self._start_stop_reload("start")

initdv2.explore('chef', ChefInitScript)


class ChefAPI(BehaviorAPI):
    """
    Basic API for managing Chef service status.

    Namespace::

        chef
    """
    __metaclass__ = Singleton

    behavior = 'chef'

    def __init__(self):
        self.service = ChefInitScript()

    @rpc.command_method
    def start_service(self):
        """
        Starts Chef service.

        Example::

            api.chef.start_service()
        """
        self.service.start()

    @rpc.command_method
    def stop_service(self):
        """
        Stops Chef service.

        Example::

            api.chef.stop_service()
        """
        self.service.stop()

    @rpc.command_method
    def reload_service(self):
        """
        Reloads Chef configuration.

        Example::

            api.chef.reload_service()
        """
        self.service.reload()

    @rpc.command_method
    def restart_service(self):
        """
        Restarts Chef service.

        Example::

            api.chef.restart_service()
        """
        self.service.restart()

    @rpc.command_method
    def get_service_status(self):
        """
        Checks Chef service status.

        RUNNING = 0
        DEAD_PID_FILE_EXISTS = 1
        DEAD_VAR_LOCK_EXISTS = 2
        NOT_RUNNING = 3
        UNKNOWN = 4

        :return: Status num.
        :rtype: int


        Example::

            >>> api.chef.get_service_status()
            0
        """
        return self.service.status()

    @classmethod
    def do_check_software(cls, system_packages=None):
        try:
            si = software.chef_software_info()
            return ('chef', '.'.join(map(str, si.version)))
        except software.SoftwareError:
            raise pkgmgr.NotInstalledError('chef')
        # if linux.os.windows:
        #     if not linux.which('chef-client'):
        #         msg = ("Can't find chef-client in %PATH%, "
        #                 "check that chef was properly installed")
        #         raise Exception(msg)
        #     return (('chef', ))
        # return pkgmgr.check_software(['chef'], system_packages)[0]

