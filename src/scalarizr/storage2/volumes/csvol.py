'''
Created on Jan 16, 2013

@author: marat, uty
'''
from __future__ import with_statement

import logging
import os
import sys
import time
import threading
import uuid

from scalarizr import node, linux
from scalarizr.linux import coreutils
from scalarizr import util
from scalarizr.platform import NoCredentialsError
from scalarizr import storage2
from scalarizr.storage2.volumes import base



__cloudstack__ = node.__node__['cloudstack']
LOG = logging.getLogger(__name__)


class CSVolume(base.Volume):
    attach_lock = threading.Lock()

    _global_timeout = 3600

    def __init__(self,
                 name=None,
                 snapshot_id=None,
                 zone_id=None,
                 disk_offering_id=None,
                 size=None,
                 **kwds):
        '''
        :type name: string
        :param name: Volume name

        :type snapshot_id: string
        :param snapshot_id: Snapshot id

        :type zone_id: int
        :param zone_id: Availability zone id

        :param disk_offering_id: Disk offering ID

        :type size: int or string
        :param size: Volume size in GB
        '''
        base.Volume.__init__(self,
                             name=name,
                             snapshot_id=snapshot_id,
                             zone_id=zone_id,
                             size=size and int(size) or None,
                             disk_offering_id=disk_offering_id,
                             **kwds)
        self._native_vol = None
        self.error_messages.update({
            'no_id_or_conn': 'Volume has no ID and Cloudstack connection '
            'required for volume construction is not available'
        })
        self.error_messages.update({
            'no_connection': 'Cloudstack connection should be available '
            'to perform this operation'})

    def _new_conn(self):
        try:
            return __cloudstack__.connect_cloudstack()
        except NoCredentialsError:
            return False

    def _check_connection(self):
        self._conn = self._new_conn()
        assert self._conn, self.error_messages['no_connection']

    def _clone(self, config):
        config.pop('device', None)
        config.pop('zone_id', None)

    def _check_attachement(self):
        self._native_vol = self._conn.listVolumes(id=self.id)[0]
        if self._attached():
            if self._native_vol.virtualmachineid == __cloudstack__['instance_id']:
                LOG.debug('Volume %s is attached to this instance', self.id)
                return
            self.device = None  # Volume will have a new device name

            LOG.warning('Volume %s is not available. '
                        'It is attached to different instance %s. '
                        'Now scalarizr will detach it',
                        self.id, self._native_vol.virtualmachineid)
            # We should wait for state chage
            if self._native_vol.vmstate == 'Stopping':
                def vm_state_changed():
                    self._native_vol = self._conn.listVolumes(id=self._native_vol.id)[0]
                    return not hasattr(self._native_vol, 'virtualmachineid') or \
                        self._native_vol.vmstate != 'Stopping'
                util.wait_until(vm_state_changed)

            # If stil attached, detaching
            if hasattr(self._native_vol, 'virtualmachineid'):
                self._detach()
                LOG.debug('Volume %s detached', self.id)

        return self._attach(__cloudstack__['instance_id'])


    def _ensure(self):
        self._native_vol = None
        snapshot_id = None
        self._conn = self._new_conn()
        devname = self.device

        if self._conn:
            try:
                if self.snap:
                    snapshot_id = self.snap['id']
                    self.id = None
                    self.size = None

                if self.id:
                    LOG.debug('Volume %s has been already created', self.id)
                    int_errs = 0
                    while True:
                        try:
                            LOG.debug('XXX: Volumes attached to terminated instances ' \
                                    'are not visible in listVolumes view. ' \
                                    'Calling detachVolume to force volume be visibile')
                            self._conn.detachVolume(id=self.id)
                        except Exception, e:
                            msg = str(e)
                            if 'does not exist' in msg:
                                raise storage2.VolumeNotExistsError(self.id)
                            if 'not attached' in msg:
                                break
                            if 'Internal error executing command' in msg:
                                int_errs += 1
                                if int_errs >= 10:
                                    raise
                                time.sleep(30)
                                continue
                            # pass other errors
                        break

                    try:
                        vol_list = self._conn.listVolumes(id=self.id)
                    except:
                        if 'Expected list, got null' in str(sys.exc_info()[1]):
                            raise storage2.VolumeNotExistsError(self.id)
                        raise
                    else:
                        if len(vol_list) == 0:
                            raise storage2.VolumeNotExistsError(self.id)
                    self._native_vol = vol_list[0]
                    devname = self._check_attachement()                    

                if not self.id:
                    LOG.debug('Creating new volume')
                    if not self.disk_offering_id:
                        # Any size you want
                        for dskoffer in self._conn.listDiskOfferings():
                            if not dskoffer.disksize and dskoffer.iscustomized:
                                self.disk_offering_id = dskoffer.id
                                break
                    self._native_vol = self._create_volume(
                        name=getattr(self, 'scalr_storage_id', str(uuid.uuid4())),
                        zone_id=__cloudstack__['zone_id'],
                        size=self.size,
                        disk_offering_id=self.disk_offering_id,
                        snap_id=snapshot_id)
                    self.id = self._native_vol.id
                    devname = self._attach(__cloudstack__['instance_id'])
                    self._native_vol = self._conn.listVolumes(id=self.id)[0]
            except storage2.StorageError:
                raise
            except:
                exc_type, exc_value, exc_trace = sys.exc_info()
                if self._native_vol:
                    LOG.debug('Detaching volume')
                    try:
                        self._conn.detachVolume(id=self._native_vol.id)
                    except:
                        pass

                raise storage2.StorageError, \
                    'Volume construction failed: %s' % exc_value, \
                    exc_trace

            self._config.update({
                'id': self._native_vol.id,
                'size': self._native_vol.size / (1024 * 1024 * 1024),
                'device': devname,
                'snap': None,
                'zone_id': self._native_vol.zoneid,
                'disk_offering_id': getattr(self._native_vol,
                                            'diskofferingid',
                                            None)})
            self._native_vol = None

    def _snapshot(self, description, tags, **kwds):
        '''
        @type nowait: bool
        @param nowait: Wait for snapshot completion. Default: True
        '''

        self._check_connection()
        snapshot = self._create_snapshot(self.id, kwds.get('nowait', True))
        return storage2.snapshot(
            type='csvol',
            id=snapshot.id,
            description=description,
            tags=tags)

    def _destroy(self, force=False, **kwargs):
        self._detach()
        self._delete()

    def _create_snapshot(self, volume_id, nowait=True):
        self._check_connection()
        LOG.debug('Creating snapshot of volume %s', volume_id)
        util.system2('sync', shell=True)
        snap = self._conn.createSnapshot(volume_id)
        LOG.debug('Snapshot %s created for volume %s', snap.id, volume_id)

        if not nowait:
            self._wait_snapshot(snap)

        return snap

    def _wait_snapshot(self, snap_id):
        '''
        Waits until snapshot becomes 'completed' or 'error'
        '''
        self._check_connection()
        if hasattr(snap_id, 'id'):
            snap_id = snap_id.id

        LOG.debug('Checking that snapshot %s is completed', snap_id)

        def exit_condition():
            return self._conn.listSnapshots(id=snap_id)[0].state == 'BackedUp'
        util.wait_until(exit_condition,
                   logger=LOG,
                   timeout=self._global_timeout,
                   error_text="Snapshot %s wasn't completed in a reasonable time" % snap_id)
        LOG.debug('Snapshot %s completed', snap_id)

    def _create_volume(self,
                       name,
                       zone_id,
                       size=None,
                       disk_offering_id=None,
                       snap_id=None):
        self._check_connection()
        if snap_id:
            disk_offering_id = None

        msg = "Creating volume '%s' in zone %s%s%s%s" % (
            name,
            zone_id,
            size and ' (size: %sG)' % size or '',
            snap_id and ' from snapshot %s' % snap_id or '',
            disk_offering_id and ' with disk offering %s' % disk_offering_id or '')
        LOG.debug(msg)

        if snap_id:
            self._wait_snapshot(snap_id)

        vol = self._conn.createVolume(name=name,
                                      size=size,
                                      diskOfferingId=disk_offering_id,
                                      snapshotId=snap_id,
                                      zoneId=zone_id)
        LOG.debug('Volume %s created%s', vol.id, snap_id and ' from snapshot %s' % snap_id or '')

        if vol.state not in ('Allocated', 'Ready'):
            LOG.debug('Checking that volume %s is available', vol.id)
            util.wait_until(
                lambda: self._conn.listVolumes(id=vol.id)[0].state in ('Allocated', 'Ready'),
                logger=LOG,
                timeout=self._global_timeout,
                error_text="Volume %s wasn't available in a reasonable time" % vol.id
            )
            LOG.debug('Volume %s available', vol.id)

        return vol

    def _attach(self, instance_id):
        self._check_connection()
        volume_id = self.id or self._native_vol.id

        with self.attach_lock:
            LOG.debug('Attaching CloudStack volume %s', volume_id)
            taken_before = base.taken_devices()
            self._conn.attachVolume(volume_id, instance_id)

            def device_plugged():
                # Rescan SCSI bus
                scsi_host = '/sys/class/scsi_host'
                for name in os.listdir(scsi_host):
                    with open(scsi_host + '/' + name + '/scan', 'w') as fp:
                        fp.write('- - -')
                return base.taken_devices() > taken_before

            util.wait_until(device_plugged,
                    start_text='Checking that volume %s is available in OS' % volume_id,
                    timeout=30,
                    sleep=1,
                    error_text='Volume %s attached but not available in OS' % volume_id)

            devices = list(base.taken_devices() - taken_before)
            if len(devices) > 1:
                msg = "While polling for attached device, got multiple new devices: %s. " \
                    "Don't know which one to select".format(devices)
                raise Exception(msg)
            return devices[0]

        LOG.debug('Checking that volume %s is attached', volume_id)


    def _detach(self, force=False, **kwds):
        self._check_connection()
        volume_id = self.id or self._native_vol.id

        # Remove volume from SCSI host
        if self.device and \
            ((self._native_vol and self._native_vol.virtualmachineid == __cloudstack__['instance_id']) or \
                not self._native_vol):
            if linux.which('lsscsi'):
                scsi = coreutils.lsscsi().get(self.device)
                if scsi:
                    LOG.debug('Removing device from SCSI host')
                    name = '/sys/class/scsi_host/host{host}/device/target{host}:{bus}:{target}/{host}:{bus}:{target}:{lun}/delete'.format(**scsi)
                    with open(name, 'w') as fp:
                        fp.write('1')

        LOG.debug('Detaching volume %s', volume_id)
        try:
            self._conn.detachVolume(volume_id)
        except Exception, e:
            if 'not attached' in str(e) or \
                'Please specify a VM that is either running or stopped' in str(e):
                pass
            else:
                raise

        LOG.debug('Checking that volume %s is available', volume_id)
        util.wait_until(self._detached,
                   logger=LOG,
                   timeout=self._global_timeout,
                   error_text="Volume %s wasn't available in a reasonable time" % volume_id)
        LOG.debug('Volume %s is available', volume_id)


    def _delete(self):
        self._check_connection()
        volume_id = self.id or self._native_vol.id
        LOG.debug('Deleting volume %s', volume_id)
        self._conn.deleteVolume(volume_id)
        self.id = None

    def _attached(self):
        vol = None
        if self.id:
            vol = self._conn.listVolumes(id=self.id)[0]
        else:
            vol = self._native_vol
        return hasattr(vol, 'virtualmachineid')

    def _detached(self):
        return not self._attached()


class CSSnapshot(base.Snapshot):

    _status_map = {'Creating': base.Snapshot.IN_PROGRESS,
                   'BackingUp': base.Snapshot.IN_PROGRESS,
                   'BackedUp': base.Snapshot.COMPLETED,
                   'error': base.Snapshot.FAILED}

    def _new_conn(self):
        try:
            return __cloudstack__.connect_cloudstack()
        except NoCredentialsError:
            return False

    def _check_connection(self):
        self._conn = self._new_conn()
        assert self._conn, self.error_messages['no_connection']

    def __init__(self, **kwds):
        base.Snapshot.__init__(self, **kwds)

    def _status(self):
        snapshots = self._new_conn().listSnapshots(id=self.id)
        if not snapshots:
            raise storage2.StorageError('listSnapshots returned empty list for snapshot %s' % self.id)
        return self._status_map[snapshots[0].state]

    def _destroy(self):
        self._check_conn()
        self._conn.deleteSnapshot(id=self.id)


storage2.volume_types['csvol'] = CSVolume
storage2.snapshot_types['csvol'] = CSSnapshot
