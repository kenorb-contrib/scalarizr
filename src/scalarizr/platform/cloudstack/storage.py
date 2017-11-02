from __future__ import with_statement
'''
Created on Sep 12, 2011

@author: marat
'''

import logging
import os
import sys

from scalarizr.bus import bus
from scalarizr.storage import Storage, Volume, VolumeProvider, StorageError, devname_not_empty, \
        VolumeConfig, Snapshot
from . import voltool
from scalarizr.util import wait_until


LOG = logging.getLogger(__name__)


class CSConfig(VolumeConfig):
    type = 'csvol'
    snapshot_id = None
    zone_id = None
    disk_offering_id = None
    size = None

class CSVolume(Volume, CSConfig):
    pass


class CSSnapshot(Snapshot, CSConfig):
    _ignores = ('snapshot_id',)


class CSVolumeProvider(VolumeProvider):
    type = CSConfig.type
    vol_class = CSVolume
    snap_class = CSSnapshot
    snapshot_state_map = {
            'Creating' : Snapshot.CREATING,
            'BackingUp' : Snapshot.CREATED,
            'BackedUp' : Snapshot.COMPLETED,
            'error' : Snapshot.FAILED
    }

    def _new_conn(self):
        try:
            return bus.platform.new_cloudstack_conn()
        except:
            pass

    def _create(self, **kwargs):
        '''
        @param id: volume id
        @param device: Device name
        @param size: Size in GB
        @param zone_id: Availability zone
        @param disk_offering_id: Disk offering ID
        @param snapshot_id: Snapshot id
        '''
        native_vol = None
        pl = bus.platform
        conn = self._new_conn()

        if conn:
            LOG.debug('storage._create kwds: %s', kwargs)

            # Find free devname
            device = kwargs.get('device')
            if device and not os.path.exists(device):
                device_id = voltool.get_deviceid(device)
            else:
                device_id = voltool.get_free_deviceid(conn, pl.get_instance_id())

            # Take volume and snapshot ids
            volume_id = kwargs.get('id')
            snap_id = kwargs.get('snapshot_id')
            if snap_id:
                volume_id = None
            attached = False

            try:
                if volume_id:
                    LOG.debug('Volume %s has been already created', volume_id)
                    try:
                        native_vol = conn.listVolumes(id=volume_id)[0]
                    except IndexError:
                        raise StorageError("Volume %s doesn't exist" % volume_id)
                    else:
                        snap_id = None

                if snap_id or not volume_id:
                    LOG.debug('Creating new volume')
                    disk_offering_id = kwargs.get('disk_offering_id')
                    if not disk_offering_id:
                        # Any size you want
                        disk_offering_id = [dskoffer for dskoffer in conn.listDiskOfferings()
                                        if not dskoffer.disksize and dskoffer.iscustomized][0].id


                    native_vol = voltool.create_volume(conn,
                            name='%s-%02d' % (pl.get_instance_id(), device_id),
                            zone_id=pl.get_avail_zone_id(),
                            size=kwargs.get('size'),
                            disk_offering_id=disk_offering_id,
                            snap_id=snap_id,
                            logger=LOG
                    )


                if voltool.volume_attached(native_vol):
                    if native_vol.virtualmachineid == pl.get_instance_id():
                        LOG.debug('Volume %s is attached to this instance', volume_id)
                        device = voltool.get_system_devname(native_vol.deviceid)
                        attached = True
                    else:
                        LOG.warning('Volume %s is not available. '
                                                                'It is attached to different instance %s. '
                                                                'Now scalarizr will detach it',
                                                                volume_id, native_vol.virtualmachineid)
                        if native_vol.vmstate == 'Stopping':
                            # We should wait for state chage
                            def vm_state_changed():
                                native_vol = conn.listVolumes(id=volume_id or native_vol.id)[0]
                                return voltool.volume_detached(native_vol) or \
                                                native_vol.vmstate != 'Stopping'
                            wait_until(vm_state_changed)

                        if voltool.volume_attached(native_vol):
                            # If stil attached, detaching
                            voltool.detach_volume(conn, volume_id)
                            LOG.debug('Volume %s detached', volume_id)

                if not attached:
                    LOG.debug('Attaching volume %s to this instance', volume_id)
                    device = voltool.attach_volume(conn, native_vol, pl.get_instance_id(), device_id,
                            to_me=True, logger=LOG)[1]

            except:
                exc_type, exc_value, exc_trace = sys.exc_info()
                if native_vol:
                    LOG.debug('Detaching volume')
                    try:
                        conn.detachVolume(id=native_vol.id)
                    except:
                        pass

                raise StorageError, 'Volume construction failed: %s' % exc_value, exc_trace



            kwargs['device'] = device
            kwargs['id'] = native_vol.id
            kwargs['zone_id'] = native_vol.zoneid
            kwargs['disk_offering_id'] = getattr(native_vol, 'diskofferingid', None)

        return super(CSVolumeProvider, self).create(**kwargs)

    create = _create

    def create_from_snapshot(self, **kwargs):
        '''
        @param zone_id: Availability zone
        @param id: Snapshot id
        '''
        return self._create(**kwargs)

    def create_snapshot(self, vol, snap, **kwargs):
        native_snap = voltool.create_snapshot(self._new_conn(), vol.id)
        snap.id = native_snap.id
        return snap

    def get_snapshot_state(self, snap):
        snapshots = self._new_conn().listSnapshots(id=snap.id)
        if not snapshots:
            raise StorageError('listSnapshots returned empty list for snapshot %s' % snap.id)
        state = snapshots[0].state
        return self.snapshot_state_map[state]

    def blank_config(self, cnf):
        cnf.pop('snapshot_id', None)

    def destroy(self, vol, force=False, **kwargs):
        '''
        @type vol: CSVolume
        '''
        super(CSVolumeProvider, self).destroy(vol)
        conn = self._new_conn()
        if conn:
            voltool.detach_volume(conn, vol.id, LOG)
            voltool.delete_volume(conn, vol.id, LOG)
        vol.device = None

    def destroy_snapshot(self, snap):
        conn = self._new_conn()
        if conn:
            LOG.debug('Deleting EBS snapshot %s', snap.id)
            conn.deleteSnapshot(id=snap.id)

    @devname_not_empty
    def detach(self, vol, force=False):
        super(CSVolumeProvider, self).detach(vol)
        conn = self._new_conn()
        if conn:
            voltool.detach_volume(conn, vol.id, LOG)
        vol.device = None


Storage.explore_provider(CSVolumeProvider)
