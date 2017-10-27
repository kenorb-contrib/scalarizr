from __future__ import with_statement
'''
Created on Sep 9, 2011
 
@author: marat
'''
 
import os
import time
 
from scalarizr.bus import bus
from scalarizr.handlers import HandlerError
from scalarizr.handlers import rebundle as rebundle_hdlr
from scalarizr.util import disttool
from scalarizr.platform.cloudstack import voltool
import shutil
 
LOG = rebundle_hdlr.LOG
 
def get_handlers():
    return [CloudStackRebundleHandler()]
 
 
class CloudStackRebundleHandler(rebundle_hdlr.RebundleHandler):
    IMAGE_MPOINT = '/mnt/img-mnt'
    IMAGE_NAME_MAXLEN = 32
 
    def get_os_type_id(self, conn):
        pl = bus.platform
        vm = conn.listVirtualMachines(id=pl.get_instance_id())[0]
        return vm.guestosid
 
 
    def rebundle(self):
        now = time.strftime('%Y%m%d%H%M%S')
        if len(self._role_name) > self.IMAGE_NAME_MAXLEN - len(now) - 1:
            image_name = self._role_name[0:16] + '--' + now
        else:
            image_name = self._role_name + "-" + now
 
        pl = bus.platform
        conn = pl.new_cloudstack_conn()
 
        try:
            root_vol = filter(lambda x: x.type == 'ROOT',
                    conn.listVolumes(virtualMachineId=pl.get_instance_id()))[0]
        except IndexError:
            raise HandlerError(
                            "Can't find root volume for virtual machine %s" % pl.get_instance_id())
 
        instance = conn.listVirtualMachines(id=pl.get_instance_id())[0]
 
        try:
            # Create snapshot
            LOG.info('Creating ROOT volume snapshot (volume: %s)', root_vol.id)
            snap = voltool.create_snapshot(conn, root_vol.id,
                                                                                    wait_completion=True, logger=LOG)
            LOG.info('ROOT volume snapshot created (snapshot: %s)', snap.id)
 
            LOG.info('Creating image')
            image = conn.createTemplate(image_name, image_name,
                                            self.get_os_type_id(conn),
                                            snapshotId=snap.id,
                                            passwordEnabled=instance.passwordenabled)
            LOG.info('Image created (template: %s)', image.id)
 
            return image.id
        finally:
            pass
 
 
    def before_rebundle(self):
        if os.path.exists('/etc/udev/rules.d/70-persistent-net.rules'):
            shutil.move('/etc/udev/rules.d/70-persistent-net.rules', '/tmp')
 
 
    def after_rebundle(self):
        if os.path.exists('/tmp/70-persistent-net.rules'):
            shutil.move('/tmp/70-persistent-net.rules', '/etc/udev/rules.d')
 
