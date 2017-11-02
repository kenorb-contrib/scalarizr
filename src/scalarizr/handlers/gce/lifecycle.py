from __future__ import with_statement
__author__ = 'Nick Demyanchuk'

from scalarizr.bus import bus
from scalarizr import handlers
from scalarizr import linux


def get_handlers ():
    return [GceLifeCycle()]

class GceLifeCycle(handlers.Handler):

    def __init__(self):
        super(GceLifeCycle, self).__init__()
        bus.on(init=self.on_init)


    def on_init(self, *args, **kwargs):
        bus.on(before_hello=self.on_before_hello)
        try:
            linux.system(('ntpdate', '-u', 'metadata.google.internal'))
        except:
            pass


    def on_before_hello(self, message):
        """
        @param message: Hello message
        """

        pl = bus.platform
        message.body['gce'] = {
            'serverId': pl.get_instance_id(),
            'cloudLocation ': pl.get_zone(),
            'serverName': pl.get_hostname().split('.')[0],
            'machineType': pl.get_machine_type()
        }

