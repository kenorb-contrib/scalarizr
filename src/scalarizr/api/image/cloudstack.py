import os
import time
import shutil
import logging

from scalarizr.api.image import ImageAPIDelegate
from scalarizr.api.image import ImageAPIError
from scalarizr.node import __node__
from scalarizr.platform.cloudstack import voltool


LOG = logging.getLogger(__name__)


class CloudStackImageAPIDelegate(ImageAPIDelegate):
    IMAGE_MPOINT = '/mnt/img-mnt'
    IMAGE_NAME_MAXLEN = 32

    def get_os_type_id(self, conn, instance_id):
        pl = __node__['platform']
        vm = conn.listVirtualMachines(id=instance_id)[0]
        return vm.guestosid

    def snapshot(self, op, name):
        now = time.strftime('%Y%m%d%H%M%S')
        if len(name) > self.IMAGE_NAME_MAXLEN - len(now) - 1:
            image_name = name[0:len(now)+2] + '--' + now
        else:
            image_name = name + "-" + now

        pl = __node__['platform']
        conn = pl.new_cloudstack_conn()

        root_vol = None
        instance_id = pl.get_instance_id()
        for vol in conn.listVolumes(virtualMachineId=instance_id):
            if vol.type == 'ROOT':
                root_vol = vol
                break
        else:
            raise ImageAPIError("Can't find root volume for virtual machine %s" % 
                instance_id)

        instance = conn.listVirtualMachines(id=instance_id)[0]

        LOG.info('Creating ROOT volume snapshot (volume: %s)', root_vol.id)
        snap = voltool.create_snapshot(conn,
            root_vol.id,
            wait_completion=True,
            logger=LOG)
        LOG.info('ROOT volume snapshot created (snapshot: %s)', snap.id)

        LOG.info('Creating image')
        image = conn.createTemplate(image_name, 
            image_name,
            self.get_os_type_id(conn, instance_id),
            snapshotId=snap.id,
            passwordEnabled=instance.passwordenabled)
        LOG.info('Image created (template: %s)', image.id)

        return image.id

    def prepare(self, op, name=None):
        rulename = '70-persistent-net.rules'
        if os.path.exists('/etc/udev/rules.d/'+rulename):
            if os.path.exists('/tmp/'+rulename):
                os.remove('/tmp/'+rulename)
            shutil.move('/etc/udev/rules.d/'+rulename, '/tmp')

    def finalize(self, op, name=None):
        rulename = '70-persistent-net.rules'
        if os.path.exists('/tmp/'+rulename):
            shutil.move('/tmp/'+rulename, '/etc/udev/rules.d')
