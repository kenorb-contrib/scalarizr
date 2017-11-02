'''
Created on Oct 13, 2011

@author: marat
'''

import os
import sys
import logging

from scalarizr.bus import bus
from scalarizr import storage2, linux
from scalarizr import handlers
from scalarizr.node import __node__
from scalarizr.messaging import Messages
from scalarizr.util import wait_until
from scalarizr.linux import mount


LOG = logging.getLogger(__name__)

class BlockDeviceHandler(handlers.Handler):
    _platform = None
    _queryenv = None
    _msg_service = None
    _vol_type = None
    _config = None

    def __init__(self, vol_type):
        super(BlockDeviceHandler, self).__init__()
        self._vol_type = vol_type
        self._volumes = []
        self.on_reload()
        
        bus.on(init=self.on_init, reload=self.on_reload)
        bus.define_events(
            # Fires when volume is attached to instance
            # @param device: device name, ex: /dev/sdf
            "block_device_attached", 
            
            # Fires when volume is detached from instance
            # @param device: device name, ex: /dev/sdf 
            "block_device_detached",
            
            # Fires when volume is mounted
            # @param device: device name, ex: /dev/sdf
            "block_device_mounted"
        )
        
        
    def on_reload(self):
        self._platform = bus.platform
        self._queryenv = bus.queryenv_service

    def accept(self, message, queue, **kwds):
        return message.name in (Messages.INT_BLOCK_DEVICE_UPDATED, 
                Messages.MOUNTPOINTS_RECONFIGURE)

    def on_init(self):
        bus.on(
            host_init_response=self.on_host_init_response,
            before_host_up=self.on_before_host_up,
            block_device_mounted=self.on_block_device_mounted
        )

        try:
            handlers.script_executor.skip_events.add(Messages.INT_BLOCK_DEVICE_UPDATED)
        except AttributeError:
            pass
        self.add_udev_rules()
        if __node__['state'] == 'running':
            volumes = self._queryenv.list_farm_role_params(__node__['farm_role_id']).get('params', {}).get('volumes', [])
            volumes = volumes or []  # Cast to list
            for vol in volumes:
                vol = storage2.volume(vol)
                try:
                    vol.ensure(mount=bool(vol.mpoint))
                except:
                    # It may be because of missing cloud credentials, we shouldn't stop initialization
                    LOG.warn("Can't ensure volume {0}. Error: {1}".format(dict(vol), sys.exc_info()[1]))

    def add_udev_rules(self):
        if linux.os.windows_family:
            return
        LOG.debug("Ensure udev rule for EBS devices")
        try:
            script_path = os.path.join(__node__['scripts_dir'], 'udev')
            with open("/etc/udev/rules.d/84-ebs.rules", "w+") as fp:
                fp.write('KERNEL=="sd*", ACTION=="add|remove", RUN+="' + script_path + '"\n')
                fp.write('KERNEL=="xvd*", ACTION=="add|remove", RUN+="' + script_path + '"')
        except (OSError, IOError), e:
            LOG.error("Cannot add udev rule into '/etc/udev/rules.d' Error: %s", str(e))
            raise


    def on_host_init_response(self, hir):
        bus.init_op.logger.info('Configuring storage volumes')
        # volumes from QueryEnv.list_ebs_mountpoints()
        wait_until(self._plug_old_style_volumes, sleep=10)
        # volumes assigned to this role on Farm Designer
        volumes = hir.body.get('volumes', []) or []
        self._plug_new_style_volumes(volumes)


    def on_before_host_up(self, hostup):
        if self._volumes:
            LOG.debug('update hostup with volumes. HostUp message body: %s', hostup.body)
            hostup.body['volumes'] = self._volumes


    def on_block_device_mounted(self, volume):
        self.send_message(Messages.BLOCK_DEVICE_MOUNTED, {
            'device_name': volume.device,
            'volume_id': volume.id,
            'mountpoint': volume.mpoint            
            })


    def _plug_new_style_volumes(self, volumes):
        for vol in volumes:
            vol = storage2.volume(**vol)
            #vol.tags.update(build_tags()) # [UI-343]
            self._log_ensure_volume(vol)
            vol.ensure(mount=bool(vol.mpoint), mkfs=True)
            self._volumes.append(dict(vol))


    def _log_ensure_volume(self, vol):
        '''
        Log messages I want to see:

        'Ensure ebs: take vol-12345678, mount to /mnt/volume'
        'Ensure ebs: create volume, create ext3 filesystem, mount to /mnt/volume'
        'Ensure ebs: create volume from snap-12345678'

        'Ensure raid1: take raid-vol-123432 (ebs disks: vol-12345678, vol-12345678, vol-12345678, vol-12345678), mount to /mnt/raid'
        'Ensure raid5: create 4 ebs volumes, create xfs filesystem, mount to /mnt/raid5'
        'Ensure raid10: create 4 ebs volumes from snapshots (snap-12345678, snap-87654321, snap-11111111, snap-22222222), mount to /mnt/raid10'

        '''

        common_actions = {
            'mkfs': 'make {0} filesystem',
            'mount': 'mount to {0}'        
        }
        persistent_actions = {
            'take': 'take {0}',
            'new': 'create volume',
            'snap': 'create volume from {0}'
        }
        raid_actions = {
            'take': 'take {0} ({1} {2} disks: {3})',
            'new': 'create {0} {1} volumes',
            'snap': 'create {0} {1} volumes from snapshots ({2})'
        }

        is_raid = vol.type == 'raid'

        acts = []
        if vol.id:
            if is_raid:
                act = raid_actions['take'].format(vol.id, 
                        len(vol.disks), 
                        vol.disks[0].type, 
                        ', '.join(str(disk.id) for disk in vol.disks))
            else:
                act = persistent_actions['take'].format(vol.id)
            acts.append(act)
        elif vol.snap:
            if is_raid:
                act = raid_actions['snap'].format(len(vol.disks), vol.snap['disks'][0]['type'], 
                        ', '.join(snap['id'] for snap in vol.snap['disks']))
            else:
                act = persistent_actions['snap'].format(vol.snap['id'])
            acts.append(act)
        else:
            if is_raid:
                act = raid_actions['new'].format(len(vol.disks), vol.disks[0].type)
            else:
                act = persistent_actions['new']
            acts.append(act)
            if vol.mpoint:
                act = common_actions['mkfs'].format(vol.fstype)
                acts.append(act)
        if vol.mpoint:
            act = common_actions['mount'].format(vol.mpoint)
            acts.append(act)

        if is_raid:
            msg = 'Ensure {0}{1}: '.format(vol.type, vol.level)
        else:
            msg = 'Ensure {0}: '.format(vol.type)
        msg += ', '.join(acts)
        log = bus.init_op.logger if bus.init_op else LOG
        log.info(msg)


    def _plug_old_style_volumes(self):
        unplugged = 0
        plugged_names = []
        for qe_mpoint in self._queryenv.list_ebs_mountpoints():
            if qe_mpoint.name in plugged_names:
                continue
            if qe_mpoint.name == 'vol-creating':
                unplugged += 1
            else:
                self._plug_volume(qe_mpoint)
                plugged_names.append(qe_mpoint.name)
        return not unplugged


    def _plug_volume(self, qe_mpoint):
        try:
            assert len(qe_mpoint.volumes), 'Invalid mpoint info %s. Volumes list is empty' % qe_mpoint
            qe_volume = qe_mpoint.volumes[0]
            mpoint = qe_mpoint.dir or None
            assert qe_volume.volume_id, 'Invalid volume info %s. volume_id should be non-empty' % qe_volume
            
            vol = storage2.volume(
                type=self._vol_type, 
                id=qe_volume.volume_id, 
                name=qe_volume.device,
                mpoint=mpoint
            )
            LOG.info("Plugging volume with tags: %s" % str(vol.tags))
            if mpoint:
                logger = bus.init_op.logger if bus.init_op else LOG
                logger.info('Ensure %s: take %s, mount to %s', self._vol_type, vol.id, vol.mpoint)

                vol.ensure(mount=True, mkfs=True, fstab=True)

            # vol._create_tags_async(qe_volume.volume_id, build_tags())  # [SCALARIZR-1012] upd [UI-343]

        except:
            LOG.exception("Can't attach volume")


    def get_devname(self, devname):
        return devname


    def on_MountPointsReconfigure(self, message):
        LOG.info("Reconfiguring mountpoints")
        for qe_mpoint in self._queryenv.list_ebs_mountpoints():
            self._plug_volume(qe_mpoint)
        LOG.debug("Mountpoints reconfigured")


    def on_IntBlockDeviceUpdated(self, message):
        if not message.devname:
            return
        
        if message.action == "add":
            LOG.debug("udev notified me that block device %s was attached", message.devname)
            
            self.send_message(
                Messages.BLOCK_DEVICE_ATTACHED, 
                {"device_name" : self.get_devname(message.devname)}, 
                broadcast=True
            )
            
            bus.fire("block_device_attached", device=message.devname)
            
        elif message.action == "remove":
            LOG.debug("udev notified me that block device %s was detached", message.devname)
            fstab = mount.fstab()
            fstab.remove(message.devname)
            
            self.send_message(
                Messages.BLOCK_DEVICE_DETACHED, 
                {"device_name" : self.get_devname(message.devname)}, 
                broadcast=True
            )
            
            bus.fire("block_device_detached", device=message.devname)





