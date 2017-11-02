import logging
import os
import shutil
import sys
import time

from scalarizr.api.image import ImageAPIDelegate
from scalarizr.api.image import ImageAPIError
from scalarizr.node import __node__
from scalarizr.util import software
from scalarizr.util import system2
from scalarizr.util import wait_until
from scalarizr import linux


LOG = logging.getLogger(__name__)


class OpenStackWindowsImageTaker(object):

    def prepare(self):
        # XXX: server is terminated during sysprep.
        # we should better understand how it works
        #shutil.copy(r'C:\Windows\System32\sysprep\RunSysprep_2.cmd', r'C:\windows\system32\sysprep\RunSysprep.cmd')
        #shutil.copy(r'C:\Windows\System32\sysprep\SetupComplete_2.cmd', r'C:\windows\setup\scripts\SetupComplete.cmd')
        #linux.system((r'C:\windows\system32\sysprep\RunSysprep.cmd', ))
        return software.system_info()

    def snapshot(self, rolename):
        pass

    def finalize(self):
        pass


class OpenStackLinuxImageTaker(object):

    def prepare(self):
        rulename = '70-persistent-net.rules'
        if os.path.exists('/etc/udev/rules.d/'+rulename):
            if os.path.exists('/tmp/'+rulename):
                os.remove('/tmp/'+rulename)
            shutil.move('/etc/udev/rules.d/'+rulename, '/tmp')

    def snapshot(self, role_name):
        image_name = role_name + "-" + time.strftime("%Y%m%d%H%M%S")
        nova = __node__['openstack']['new_nova_connection']
        nova.connect()

        server_id = __node__['openstack']['server_id']
        system2("sync", shell=True)
        LOG.info('Creating server image (server_id: %s)', server_id)
        image_id = nova.servers.create_image(server_id, image_name)
        LOG.info('Server image %s created', image_id)

        result = [None]
        def image_completed():
            try:
                result[0] = nova.images.get(image_id)
                return result[0].status in ('ACTIVE', 'FAILED')
            except:
                e = sys.exc_info()[1]
                if 'Unhandled exception occurred during processing' in str(e):
                    return
                raise

        wait_until(image_completed, start_text='Polling image status', sleep=30)

        image_id = result[0].id
        if result[0].status == 'FAILED':
            raise ImageAPIError('Image %s becomes FAILED', image_id)
        LOG.info('Image %s completed and available for use!', image_id)
        return image_id

    def finalize(self):
        rulename = '70-persistent-net.rules'
        if os.path.exists('/tmp/'+rulename):
            shutil.move('/tmp/'+rulename, '/etc/udev/rules.d')


class OpenStackImageAPIDelegate(ImageAPIDelegate):
    
    def __init__(self):
        if linux.os.windows_family:
            self._image_taker = OpenStackWindowsImageTaker()
        else:
            self._image_taker = OpenStackLinuxImageTaker()

    def prepare(self, op, name):
        return self._image_taker.prepare()

    def snapshot(self, op, name):
        return self._image_taker.snapshot(name)

    def finalize(self, op, name):
        return self._image_taker.finalize()
