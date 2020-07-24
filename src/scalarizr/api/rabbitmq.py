from scalarizr.util.cryptotool import pwgen
from scalarizr.services import rabbitmq as rabbitmq_module
from scalarizr import rpc
from scalarizr import linux
from scalarizr.util import Singleton
from scalarizr.linux import pkgmgr
from scalarizr import exceptions
from scalarizr.api import BehaviorAPI


class RabbitMQAPI(BehaviorAPI):
    """
    Basic API for managing RabbitMQ service.

    Namespace::

        rabbitmq
    """
    __metaclass__ = Singleton

    behavior = 'rabbitmq'

    def __init__(self):
        self.service = rabbitmq_module.RabbitMQInitScript()
        self.rabbitmq = rabbitmq_module.RabbitMQ()

    @rpc.command_method
    def start_service(self):
        """
        Starts RabbitMQ service.

        Example::

            api.rabbitmq.start_service()
        """
        self.service.start()

    @rpc.command_method
    def stop_service(self):
        """
        Stops RabbitMQ service.

        Example::

            api.rabbitmq.stop_service()
        """
        self.service.stop()

    @rpc.command_method
    def reload_service(self):
        """
        Reloads RabbitMQ configuration.

        Example::

            api.rabbitmq.reload_service()
        """
        self.service.reload()

    @rpc.command_method
    def restart_service(self):
        """
        Restarts RabbitMQ service.

        Example::

            api.rabbitmq.restart_service()
        """
        self.service.restart()

    @rpc.command_method
    def get_service_status(self):
        """
        Checks RabbitMQ service status.

        RUNNING = 0
        DEAD_PID_FILE_EXISTS = 1
        DEAD_VAR_LOCK_EXISTS = 2
        NOT_RUNNING = 3
        UNKNOWN = 4

        :return: Status num.
        :rtype: int


        Example::

            >>> api.rabbitmq.get_service_status()
            0
        """
        return self.service.status()

    @rpc.command_method
    def reset_password(self, new_password=None):
        """
        Resets password for RabbitMQ user 'scalr_main'.

        :param new_password: New password. If not provided, 10-character string will be generated.
        :type new_password: str

        :returns: New password.
        :rtype: str

        """
        if not new_password:
            new_password = pwgen(10)
        self.rabbitmq.check_main_user(new_password)
        return new_password

    @classmethod
    def do_check_software(cls, system_packages=None):
        os_name = linux.os['name'].lower()
        os_vers = linux.os['version']
        requirements = None
        if os_name == 'ubuntu':
            requirements = ['rabbitmq-server>=2.6,<3.5']
        elif os_name == 'debian':
            requirements = ['rabbitmq-server>=3.0,<3.5']
        elif linux.os.redhat_family:
            if os_vers >= '6':
                requirements = ['rabbitmq-server>=3.1,<3.5']
            elif os_vers >= '5':
                raise exceptions.UnsupportedBehavior(
                        cls.behavior,
                        "Not supported by Scalr on {0} {1}".format(linux.os['name'], linux.os['version']))
        if requirements is None:
            raise exceptions.UnsupportedBehavior(
                    cls.behavior,
                    "Not supported on {0} os family".format(linux.os['family']))
        return pkgmgr.check_software(requirements, system_packages)[0]

