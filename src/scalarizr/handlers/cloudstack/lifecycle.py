from __future__ import with_statement
'''
Created on Sep 8, 2011

@author: marat
'''

from scalarizr import linux
from scalarizr.bus import bus
from scalarizr import util
from scalarizr.handlers import Handler
import logging


LOG = logging.getLogger(__name__)


def get_handlers ():
    return [CloudStackLifeCycleHandler()]


class CloudStackLifeCycleHandler(Handler):

    def __init__(self):
        bus.on(init=self.on_init)


    def on_init(self, *args, **kwargs):
        bus.on(before_hello=self.on_before_hello)
        if not linux.os.windows_family:
            ssh_key = bus.platform.get_ssh_pub_key()
            if ssh_key:
                util.add_authorized_key(ssh_key)


    def on_before_hello(self, message):
        """
        @param message: Hello message
        """

        pl = bus.platform
        message.body['cloudstack'] = {
                'instance_id': pl.get_instance_id(),
                'avail_zone': pl.get_avail_zone()
        }
