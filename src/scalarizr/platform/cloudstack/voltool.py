from __future__ import with_statement
'''
Created on Aug 25, 2010

@author: marat
'''

from __future__ import with_statement

from scalarizr.util import wait_until, system2

import logging
import os
import string
import operator


DEFAULT_TIMEOUT = 2400          # 40 min
SNAPSHOT_TIMEOUT = 3600         # 1 h
LOG = logging.getLogger(__name__)

AVAIL_STATES = ('Allocated', 'Ready')


def create_snapshot(conn, volume_id, logger=None, timeout=SNAPSHOT_TIMEOUT, wait_completion=False):
    if hasattr(volume_id, 'id'):
        volume_id = volume_id.id
    logger = logger or LOG

    # Create snapshot
    logger.debug('Creating snapshot of volume %s', volume_id)
    system2('sync', shell=True)
    snap = conn.createSnapshot(volume_id)
    logger.debug('Snapshot %s created for volume %s', snap.id, volume_id)


    if wait_completion:
        wait_snapshot(conn, snap, logger, timeout)

    return snap


def wait_snapshot(conn, snap_id, logger=None, timeout=SNAPSHOT_TIMEOUT):
    '''
    Waits until snapshot becomes 'completed' or 'error'
    '''
    logger = logger or LOG
    if hasattr(snap_id, 'id'):
        snap_id = snap_id.id

    logger.debug('Checking that snapshot %s is completed', snap_id)
    wait_until(
            lambda: conn.listSnapshots(id=snap_id)[0].state == 'BackedUp',
            logger=logger, timeout=timeout,
            error_text="Ssnapshot %s wasn't completed in a reasonable time" % snap_id
    )
    logger.debug('Snapshot %s completed', snap_id)


def create_volume(conn, name, zone_id, size=None, disk_offering_id=None, snap_id=None,
                                logger=None, timeout=DEFAULT_TIMEOUT):
    logger = logger or LOG
    if snap_id:
        disk_offering_id = None

    msg = "Creating volume '%s' in zone %s%s%s%s" % (
            name, zone_id,
            size and ' (size: %sG)' % size or '',
            snap_id and ' from snapshot %s' % snap_id or '',
            disk_offering_id and ' with disk offering %s' % disk_offering_id or ''
    )
    logger.debug(msg)

    if snap_id:
        wait_snapshot(conn, snap_id, logger)

    vol = conn.createVolume(name, size=size, diskOfferingId=disk_offering_id, snapshotId=snap_id, zoneId=zone_id)
    logger.debug('Volume %s created%s', vol.id, snap_id and ' from snapshot %s' % snap_id or '')

    if vol.state not in AVAIL_STATES:
        logger.debug('Checking that volume %s is available', vol.id)
        wait_until(
                lambda: conn.listVolumes(id=vol.id)[0].state in AVAIL_STATES,
                logger=logger, timeout=timeout,
                error_text="Volume %s wasn't available in a reasonable time" % vol.id
        )
        logger.debug('Volume %s available', vol.id)

    return vol


def attach_volume(conn, volume_id, instance_id, device_id=None,
                                to_me=False, logger=None, timeout=DEFAULT_TIMEOUT):
    logger = logger or LOG
    if hasattr(volume_id, 'id'):
        volume_id = volume_id.id

    msg = 'Attaching volume %s%s%s' % (volume_id,
                            device_id and ' as device %s' % get_system_devname(device_id) or '',
                            not to_me and ' instance %s' % instance_id or '')
    logger.debug(msg)
    conn.attachVolume(volume_id, instance_id, device_id)


    logger.debug('Checking that volume %s is attached', volume_id)
    wait_until(
            lambda: volume_attached(conn.listVolumes(id=volume_id)[0]),
            logger=logger, timeout=timeout,
            error_text="Volume %s wasn't attached in a reasonable time"
                            " (vm_id: %s)." % (
                            volume_id, instance_id)
    )
    logger.debug('Volume %s attached',  volume_id)

    vol = conn.listVolumes(id=volume_id)[0]
    devname = get_system_devname(vol.deviceid)

    def scsi_attached():
        # Rescan all SCSI buses
        scsi_host = '/sys/class/scsi_host'
        for name in os.listdir(scsi_host):
            with open(scsi_host + '/' + name + '/scan', 'w') as fp:
                fp.write('- - -')
        return os.access(devname, os.F_OK | os.R_OK)

    if to_me:
        logger.debug('Checking that device %s is available', devname)
        wait_until(
                scsi_attached,
                sleep=5, logger=logger, timeout=timeout,
                error_text="Device %s wasn't available in a reasonable time" % devname
        )
        logger.debug('Device %s is available', devname)

    return vol, devname


def get_disk_offering_id(conn, size):
    sizes = [(item.id, item.disksize) for item in conn.listDiskOfferings()]
    sizes = sorted(sizes, key=operator.itemgetter(1))
    LOG.debug('sizes: %s', sizes)

    for id, sz in sizes:
        if sz >= size:
            return id
    return sizes[-1][0]


def get_system_devname(deviceid):
    if isinstance(deviceid, int):
        return '/dev/%sd%s' % ('xv' if os.path.exists('/dev/xvda') else 's', string.ascii_letters[deviceid])
    return deviceid


def get_deviceid(devname):
    if isinstance(devname, basestring):
        return string.ascii_letters.index(devname[-1])
    return devname


def get_free_deviceid(conn, instance_id):
    busy = set([vol.deviceid for vol in conn.listVolumes(virtualMachineId=instance_id)])
    avail = set(range(0, 3) + range(4, 16))
    avail.difference_update(busy)
    if len(avail):
        return avail.pop()
    raise Exception('No free devices available (instance: %s)' % instance_id)


def detach_volume(conn, volume_id, force=False, logger=None, timeout=DEFAULT_TIMEOUT):
    logger = logger or LOG
    if hasattr(volume_id, 'id'):
        volume_id = volume_id.id

    logger.debug('Detaching volume %s', volume_id)
    try:
        conn.detachVolume(volume_id)
    except Exception, e:
        if 'not attached' not in str(e):
            raise

    logger.debug('Checking that volume %s is available', volume_id)
    wait_until(
            lambda: volume_detached(conn.listVolumes(id=volume_id)[0]),
            logger=logger, timeout=timeout,
            error_text="Volume %s wasn't available in a reasonable time" % volume_id
    )
    logger.debug('Volume %s is available', volume_id)


def delete_volume(conn, volume_id, logger=None):
    logger = logger or LOG
    if hasattr(volume_id, 'id'):
        volume_id = volume_id.id
    logger.debug('Deleting volume %s', volume_id)
    conn.deleteVolume(volume_id)


def volume_attached(vol):
    return hasattr(vol, 'virtualmachineid')

def volume_detached(vol):
    return not volume_attached(vol)
