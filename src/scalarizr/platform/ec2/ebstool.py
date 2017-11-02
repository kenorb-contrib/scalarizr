from __future__ import with_statement
'''
Created on Aug 25, 2010

@author: marat
'''

import sys
import logging
import os

from boto.ec2.volume import Volume
from boto.exception import BotoServerError
from boto.ec2.snapshot import Snapshot

from scalarizr.bus import bus
from scalarizr.util import wait_until, system2
from scalarizr.platform import PlatformError, UserDataOptions
from scalarizr import storage


DEFAULT_TIMEOUT = 2400          # 40 min
SNAPSHOT_TIMEOUT = 3600         # 1 h

def create_snapshot(ec2_conn, volume_id, description=None, logger=None, timeout=SNAPSHOT_TIMEOUT, wait_completion=False, tags=None):
    if isinstance(volume_id, Volume):
        volume_id = volume_id.id
    logger = logger or logging.getLogger(__name__)

    # Create snapshot
    logger.debug('Creating snapshot of EBS volume %s', volume_id)
    system2('sync', shell=True)
    snap = ec2_conn.create_snapshot(volume_id, description)
    logger.debug('Snapshot %s created for EBS volume %s', snap.id, volume_id)

    # Apply tags
    if not tags:
        logger.debug('No tags to apply to EBS snapshot %s' % snap.id)
    else:
        try:
            logger.debug('Applying tags to EBS snapshot %s : %s' % (snap.id, tags))
            ec2_conn.create_tags((snap.id, ), tags)
        except:
            logger.warn('Cannot apply tags to EBS snapshot %s', snap.id)

    if wait_completion:
        wait_snapshot(ec2_conn, snap, logger, timeout)

    return snap

def wait_snapshot(ec2_conn, snap_id, logger=None, timeout=SNAPSHOT_TIMEOUT):
    '''
    Waits until snapshot becomes 'completed' or 'error'
    '''
    logger = logger or logging.getLogger(__name__)

    if isinstance(snap_id, basestring):
        snap = Snapshot(ec2_conn)
        snap.id = snap_id
    else:
        snap = snap_id

    logger.debug('Checking that snapshot %s is completed', snap.id)
    wait_until(
            lambda: snap.update() and snap.status != 'pending',
            logger=logger, timeout=timeout,
            error_text="EBS snapshot %s wasn't completed in a reasonable time" % snap.id
    )
    if snap.status == 'error':
        raise PlatformError('Snapshot %s creation failed' % snap.id)
    elif snap.status == 'completed':
        logger.debug('Snapshot %s completed', snap.id)


def create_volume(ec2_conn, size, avail_zone, snap_id=None, volume_type=None, iops=None,
                                logger=None, timeout=DEFAULT_TIMEOUT, tags=None):
    logger = logger or logging.getLogger(__name__)

    msg = 'Creating EBS volume%s%s in avail zone %s' % (
            size and ' (size: %sG)' % size or '',
            snap_id and ' from snapshot %s' % snap_id or '',
            avail_zone
    )
    logger.debug(msg)

    if snap_id:
        wait_snapshot(ec2_conn, snap_id, logger)

    vol = ec2_conn.create_volume(size, avail_zone, snapshot=snap_id, volume_type=volume_type, iops=iops)
    logger.debug('EBS volume %s created%s', vol.id, snap_id and ' from snapshot %s' % snap_id or '')

    logger.debug('Checking that EBS volume %s is available', vol.id)
    wait_until(
            lambda: vol.update() == "available",
            logger=logger, timeout=timeout,
            error_text="EBS volume %s wasn't available in a reasonable time" % vol.id
    )
    logger.debug('EBS volume %s available', vol.id)

    if not tags:
        logger.debug('No tags to apply to volume %s' % vol.id)
    else:
        try:
            logger.debug('Applying tags to EBS volume %s : %s' % (vol.id, tags))
            ec2_conn.create_tags((vol.id, ), tags)
        except:
            logger.warn('Cannot apply tags to EBS volume %s', vol.id)

    return vol

def attach_volume(ec2_conn, volume_id, instance_id, devname, to_me=False, logger=None, timeout=DEFAULT_TIMEOUT):
    logger = logger or logging.getLogger(__name__)
    if isinstance(volume_id, basestring):
        vol = Volume(ec2_conn)
        vol.id = volume_id
    else:
        vol = volume_id

    def attach():
        try:
            vol.attach(instance_id, devname)
        except BotoServerError, e:
            if e.status == 400 and not e.code:
                # RHEL here can raise Null body error
                return
            else:
                raise
        return 1
    wait_until(attach, logger=logger, timeout=30,
                    start_text='Attaching volume %s as device %s%s' % (vol.id, devname, not to_me and ' instance %s' % instance_id or ''),
                    error_text="Failed to attach EBS volume %s. AttachVolume operation respond with 400 code without any details" % vol.id)

    logger.debug('Checking that volume %s is attached', vol.id)
    wait_until(
            lambda: vol.update() and vol.attachment_state() == 'attached',
            logger=logger, timeout=timeout,
            error_text="EBS volume %s wasn't attached in a reasonable time"
                            " (status=%s attachment_state=%s)." % (
                            vol.id, vol.status, vol.attachment_state())
    )
    logger.debug('Volume %s attached',  vol.id)

    devname = real_devname(devname)
    if to_me:
        logger.debug('Checking that device %s is available', devname)
        wait_until(
                lambda: os.access(devname, os.F_OK | os.R_OK),
                sleep=1, logger=logger, timeout=timeout,
                error_text="Device %s wasn't available in a reasonable time" % devname
        )
        logger.debug('Device %s is available', devname)

    return vol, devname


get_system_devname = real_devname= storage.get_system_devname

get_ebs_devname = storage.get_cloud_devname


def detach_volume(ec2_conn, volume_id, force=False, logger=None, timeout=DEFAULT_TIMEOUT):
    logger = logger or logging.getLogger(__name__)
    if isinstance(volume_id, basestring):
        vol = Volume(ec2_conn)
        vol.id = volume_id
    else:
        vol = volume_id

    logger.debug('Detaching volume %s', vol.id)
    try:
        vol.detach(force)
    except BotoServerError, e:
        if e.code != 'IncorrectState':
            raise
    logger.debug('Checking that volume %s is available', vol.id)
    wait_until(
            lambda: vol.update() == 'available',
            logger=logger, timeout=timeout,
            error_text="EBS volume %s wasn't available in a reasonable time" % vol.id
    )
    logger.debug('Volume %s is available', vol.id)


def delete_volume(ec2_conn, volume_id, logger=None):
    logger = logger or logging.getLogger(__name__)
    logger.debug('Deleting volume %s', volume_id)
    ec2_conn.delete_volume(isinstance(volume_id, basestring) and volume_id or volume_id.id)

def apply_tags(ec2_conn, resources, tags=None, logger=None):
    logger = logger or logging.getLogger(__name__)
    tags = tags or dict()
    tags.update(_std_tags())
    resources_str = ', '.join('%s %s' % item for item in resources.iteritems())
    try:
        logger.debug('Applying tags to resource(s) %s', resources_str)
        ec2_conn.create_tags(resources.keys(), tags)
    except:
        logger.warn('Cannot apply tags to resource(s) %s. %s: %s', resources_str, *sys.exc_info()[0:2])


def _std_tags():
    pl = bus.platform
    return {
            'farm' : pl.get_user_data(UserDataOptions.FARM_ID),
            'role' : pl.get_user_data(UserDataOptions.ROLE_NAME)
    }
