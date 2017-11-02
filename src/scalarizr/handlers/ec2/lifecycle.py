from __future__ import with_statement
'''
Created on Mar 2, 2010

@author: marat
'''

import os
import re
import sys
import logging

from scalarizr import linux
from scalarizr.bus import bus
from scalarizr.node import __node__
from scalarizr.config import STATE
from scalarizr.handlers import Handler
from scalarizr.util import system2, add_authorized_key
from scalarizr.linux import mount, system, os as os_dist


__ec2__ = __node__['ec2']

def get_handlers ():
    return [Ec2LifeCycleHandler()]


class Ec2LifeCycleHandler(Handler):
    _logger = None
    _platform = None
    """
    @ivar scalarizr.platform.ec2.Ec2Platform:
    """

    def __init__(self):
        self._logger = logging.getLogger(__name__)
        bus.on(init=self.on_init, reload=self.on_reload)
        self.on_reload()

    def on_init(self, *args, **kwargs):
        bus.on("before_hello", self.on_before_hello)
        bus.on("before_host_init", self.on_before_host_init)
        bus.on("before_restart", self.on_before_restart)
        bus.on("before_reboot_finish", self.on_before_reboot_finish)

        try:
            system(('ntpdate', '-u', '0.amazon.pool.ntp.org'))
        except:
            pass

        msg_service = bus.messaging_service
        producer = msg_service.get_producer()
        producer.on("before_send", self.on_before_message_send)

        if not os_dist.windows_family and not __node__['base'].get('hostname'):
            # Set the hostname to this instance's public hostname
            try:
                hostname_as_pubdns = int(__ec2__['hostname_as_pubdns'])
            except:
                hostname_as_pubdns = True

            if hostname_as_pubdns:
                pub_hostname = self._platform.get_public_hostname()
                self._logger.debug('Setting hostname to %s' % pub_hostname)
                system2("hostname " + pub_hostname, shell=True)

        if linux.os.ubuntu:
            # Ubuntu cloud-init scripts may disable root ssh login
            for path in ('/etc/ec2-init/ec2-config.cfg', '/etc/cloud/cloud.cfg'):
                if os.path.exists(path):
                    c = None
                    with open(path, 'r') as fp:
                        c = fp.read()
                    c = re.sub(re.compile(r'^disable_root[^:=]*([:=]).*', re.M), r'disable_root\1 0', c)
                    with open(path, 'w') as fp:
                        fp.write(c)

        if not linux.os.windows_family:
            # Add server ssh public key to authorized_keys
            ssh_key = self._platform.get_ssh_pub_key()
            if ssh_key:
                add_authorized_key(ssh_key)

            system2('mount -a', shell=True, raise_exc=False)

    def on_before_reboot_finish(self, *args, **kwds):
        STATE['ec2.t1micro_detached_ebs'] = []


    def on_reload(self):
        self._platform = bus.platform

    def on_before_hello(self, message):
        """
        @param message: Hello message
        """

        message.aws_instance_id = self._platform.get_instance_id()
        message.aws_instance_type = self._platform.get_instance_type()
        message.aws_ami_id = self._platform.get_ami_id()
        message.aws_avail_zone = self._platform.get_avail_zone()


    def on_before_host_init(self, message):
        """
        @param message: HostInit message
        """

        message.ssh_pub_key = self._platform.get_ssh_pub_key()

    def on_before_restart(self, message):
        """
        @param message: Restart message
        @type message: scalarizr.messaging.Message
        """

        """
        @todo Update ips, reset platform meta-data
        @see http://docs.amazonwebservices.com/AWSEC2/latest/DeveloperGuide/index.html?Concepts_BootFromEBS.html#Stop_Start
        """
        pass

    def on_before_message_send(self, queue, message):
        """
        @todo: add aws specific here
        """

        pass
