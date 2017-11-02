from __future__ import with_statement
'''
Created on Nov 24, 2010

@author: marat
@author: Dmytro Korsakov
'''
from __future__ import with_statement

from scalarizr.bus import bus
from scalarizr.platform import PlatformError
from scalarizr.storage import Storage, Volume, VolumeProvider, StorageError, devname_not_empty, \
        VolumeConfig, Snapshot
from scalarizr.storage.transfer import TransferProvider, TransferError
from . import ebstool

import os
import re
import sys
import logging
import urlparse
import threading
import string

from boto.s3.key import Key
from boto.exception import BotoServerError, S3ResponseError
from scalarizr.util import firstmatched, wait_until
from scalarizr import linux


class EbsConfig(VolumeConfig):
    type = 'ebs'
    tags = None
    snapshot_id = None
    avail_zone = None
    size = None
    volume_type = None
    iops = None

class EbsVolume(Volume, EbsConfig):
    @property
    def ebs_device(self):
        return ebstool.get_ebs_devname(self.device)

class EbsSnapshot(Snapshot, EbsConfig):
    _ignores = ('snapshot_id',)

    def destroy(self):
        try:
            pl = bus.platform
            conn = pl.new_ec2_conn()
        except:
            pass
        else:
            conn.delete_snapshot(self.id)
        finally:
            self.snapshot_id = None

class EbsVolumeProvider(VolumeProvider):
    type = 'ebs'
    vol_class = EbsVolume
    snap_class = EbsSnapshot

    letters_lock = threading.Lock()

    # Workaround: rhel 6 returns "Null body" when attach to /dev/sdf
    all_letters = set(string.ascii_lowercase[7 if linux.os.rhel else 5:16])
    acquired_letters = set()


    snapshot_state_map = {
            'pending' : Snapshot.CREATED,
            'completed' : Snapshot.COMPLETED,
            'error' : Snapshot.FAILED
    }

    _logger = None

    def __init__(self):
        self._logger = logging.getLogger(__name__)

    def _new_ec2_conn(self):
        try:
            pl = bus.platform
            return pl.new_ec2_conn()
        except:
            if sys.exc_type.__name__ not in ('AttributeError', 'NoAuthHandlerFound', 'PlatformError'):
                raise
        return None

    def _create(self, **kwargs):
        '''
        @param id: EBS volume id
        @param device: Device name
        @param size: Size in GB
        @param avail_zone: Availability zone
        @param snapshot_id: Snapshot id
        '''
        ebs_vol = None
        pl = bus.platform
        conn = self._new_ec2_conn()

        if conn:
            device = kwargs.get('device')

            def get_free_devname(device):
                if device:
                    device = ebstool.get_ebs_devname(device)

                used_letters = set(row['device'][-1]
                                        for row in Storage.volume_table()
                                        if row['device'] and ( \
                                                row['state'] == 'attached' or ( \
                                                pl.get_instance_type() == 't1.micro' and row['state'] == 'detached')))

                with self.letters_lock:

                    avail_letters = list(set(self.all_letters) - used_letters - self.acquired_letters)

                    volumes = conn.get_all_volumes(filters={'attachment.instance-id': pl.get_instance_id()})

                    for volume in volumes:
                        volume_device = volume.attach_data.device
                        volume_device = re.sub('\d+', '', volume_device)
                        try:
                            avail_letters.remove(volume_device[-1])
                        except ValueError:
                            pass

                    if not device or not (device[-1] in avail_letters) or os.path.exists(device):
                        letter = firstmatched(
                                lambda l: not os.path.exists(ebstool.real_devname('/dev/sd%s' % l)), avail_letters
                        )
                        if letter:
                            device = '/dev/sd%s' % letter
                            self.acquired_letters.add(letter)
                        else:
                            raise StorageError('No free letters for block device name remains')

                return device

            self._logger.debug('storage._create kwds: %s', kwargs)
            volume_id = kwargs.get('id')

            # TODO: hotfix
            if volume_id and volume_id.startswith('snap-'):
                volume_id = None

            snap_id = kwargs.get('snapshot_id')
            ebs_vol = None
            delete_snap = False
            volume_attached = False
            try:
                if volume_id:
                    self._logger.debug('EBS has been already created')
                    try:
                        ebs_vol = conn.get_all_volumes([volume_id])[0]
                    except IndexError:
                        raise StorageError("EBS volume '%s' doesn't exist." % volume_id)

                    if ebs_vol.zone != pl.get_avail_zone():
                        self._logger.warn('EBS volume %s is in the different availability zone (%s). ' +
                                                        'Snapshoting it and create a new EBS volume in %s',
                                                        ebs_vol.id, ebs_vol.zone, pl.get_avail_zone())
                        volume_id = None
                        delete_snap = True
                        snap_id = ebstool.create_snapshot(conn, ebs_vol, logger=self._logger, wait_completion=True, tags=kwargs.get('tags')).id
                    else:
                        snap_id = None

                if snap_id or not volume_id:
                    self._logger.debug('Creating new EBS')
                    kwargs['avail_zone'] = pl.get_avail_zone()
                    ebs_vol = ebstool.create_volume(conn, kwargs.get('size'),
                                                                            kwargs.get('avail_zone'), snap_id,
                                                                            kwargs.get('volume_type'), kwargs.get('iops'),
                                                                            logger=self._logger, tags=kwargs.get('tags'))

                if 'available' != ebs_vol.volume_state():
                    if ebs_vol.attachment_state() == 'attaching':
                        wait_until(lambda: ebs_vol.update() and ebs_vol.attachment_state() != 'attaching', timeout=600,
                                        error_text='EBS volume %s hangs in attaching state' % ebs_vol.id)

                    if ebs_vol.attach_data.instance_id != pl.get_instance_id():
                        self._logger.debug('EBS is attached to another instance')
                        self._logger.warning("EBS volume %s is not available. Detaching it from %s",
                                                                ebs_vol.id, ebs_vol.attach_data.instance_id)
                        ebstool.detach_volume(conn, ebs_vol, force=True, logger=self._logger)
                    else:
                        self._logger.debug('EBS is attached to this instance')
                        device = ebstool.real_devname(ebs_vol.attach_data.device)
                        wait_until(lambda: os.path.exists(device), sleep=1, timeout=300,
                                        error_text="Device %s wasn't available in a reasonable time" % device)
                        volume_attached = True

                if not volume_attached:
                    device = kwargs.get('device')
                    device = get_free_devname(device)

                    self._logger.debug('Attaching EBS to this instance')
                    device = ebstool.attach_volume(conn, ebs_vol, pl.get_instance_id(), device,
                            to_me=True, logger=self._logger)[1]

            except:
                self._logger.debug('Caught exception')
                if ebs_vol:
                    self._logger.debug('Detaching EBS')
                    if ebs_vol.update() and ebs_vol.attachment_state() != 'available':
                        ebstool.detach_volume(conn, ebs_vol, force=True, logger=self._logger)

                raise StorageError, 'EBS volume construction failed: %s' % str(sys.exc_value), sys.exc_traceback

            finally:
                if delete_snap and snap_id:
                    conn.delete_snapshot(snap_id)
                if device and device[-1] in self.acquired_letters:
                    self.acquired_letters.remove(device[-1])


            kwargs['device'] = device
            kwargs['id'] = ebs_vol.id

        elif kwargs.get('device'):
            kwargs['device'] = ebstool.get_system_devname(kwargs['device'])

        return super(EbsVolumeProvider, self).create(**kwargs)

    create = _create

    def create_from_snapshot(self, **kwargs):
        '''
        @param size: Size in GB
        @param avail_zone: Availability zone
        @param id: Snapshot id
        '''
        return self._create(**kwargs)

    def create_snapshot(self, vol, snap, **kwargs):
        conn = self._new_ec2_conn()
        ebs_snap = ebstool.create_snapshot(conn, vol.id, snap.description, tags=kwargs.get('tags'))
        snap.id = ebs_snap.id
        return snap

    def get_snapshot_state(self, snap):
        conn = self._new_ec2_conn()
        state = conn.get_all_snapshots((snap.id,))[0].status
        return self.snapshot_state_map[state]

    def blank_config(self, cnf):
        cnf.pop('snapshot_id', None)

    def destroy(self, vol, force=False, **kwargs):
        '''
        @type vol: EbsVolume
        '''
        super(EbsVolumeProvider, self).destroy(vol)
        conn = self._new_ec2_conn()
        if conn:
            if not vol.detached:
                ebstool.detach_volume(conn, vol.id, self._logger)
            ebstool.delete_volume(conn, vol.id, self._logger)
        vol.device = None

    def destroy_snapshot(self, snap):
        conn = self._new_ec2_conn()
        if conn:
            self._logger.debug('Deleting EBS snapshot %s', snap.id)
            conn.delete_snapshot(snap.id)

    @devname_not_empty
    def detach(self, vol, force=False):
        super(EbsVolumeProvider, self).detach(vol)

        try:
            pl = bus.platform
            conn = pl.new_ec2_conn()
            vol.detached = True
        except AttributeError:
            pass
        else:
            ebstool.detach_volume(conn, vol.id, self._logger)
        finally:
            vol.device = None



Storage.explore_provider(EbsVolumeProvider, default_for_snap=True)

class S3TransferProvider(TransferProvider):
    schema  = 's3'
    urlparse.uses_netloc.append(schema)

    acl     = None

    _logger = None
    _bucket = None

    def __init__(self, acl='aws-exec-read'):
        self._logger = logging.getLogger(__name__)
        self.acl = acl

    def configure(self, remote_path):
        self._parse_path(remote_path)

    def put(self, local_path, remote_path):
        self._logger.info("Uploading '%s' to S3 under '%s'", local_path, remote_path)
        bucket_name, key_name = self._parse_path(remote_path)
        key_name = os.path.join(key_name, os.path.basename(local_path))

        try:
            connection = self._get_connection()

            if not self._bucket_check_cache(bucket_name):
                try:
                    bck = connection.get_bucket(bucket_name)
                except S3ResponseError, e:
                    if e.code == 'NoSuchBucket':
                        pl = bus.platform
                        try:
                            location = location_from_region(pl.get_region())
                        except:
                            location = ''
                        bck = connection.create_bucket(
                                bucket_name,
                                location=location,
                                policy=self.acl
                        )
                    else:
                        raise
                # Cache bucket
                self._bucket = bck

            file = None
            try:
                key = Key(self._bucket)
                key.name = key_name
                file = open(local_path, "rb")
                key.set_contents_from_file(file, policy=self.acl)
                return self._format_path(bucket_name, key_name)
            finally:
                if file:
                    file.close()

        except:
            exc = sys.exc_info()
            raise TransferError, exc[1], exc[2]


    def get(self, remote_path, local_path):
        self._logger.info('Downloading %s from S3 to %s' % (remote_path, local_path))
        bucket_name, key_name = self._parse_path(remote_path)
        dest_path = os.path.join(local_path, os.path.basename(remote_path))

        try:
            connection = self._get_connection()

            try:
                if not self._bucket_check_cache(bucket_name):
                    self._bucket = connection.get_bucket(bucket_name, validate=False)
                key = self._bucket.get_key(key_name)
            except S3ResponseError, e:
                if e.code in ('NoSuchBucket', 'NoSuchKey'):
                    raise TransferError("S3 path '%s' not found" % remote_path)
                raise

            key.get_contents_to_filename(dest_path)
            return dest_path

        except:
            exc = sys.exc_info()
            raise TransferError, exc[1], exc[2]

    def list(self, remote_path):
        bucket_name, key_name = self._parse_path(remote_path)
        connection = self._get_connection()
        bkt = connection.get_bucket(bucket_name, validate=False)
        files = [self._format_path(self.bucket.name, key.name) for key in bkt.list(prefix=key_name)]
        return tuple(files)

    def _bucket_check_cache(self, bucket):
        if self._bucket and self._bucket.name != bucket:
            self._bucket = None
        return self._bucket

    def _get_connection(self):
        pl = bus.platform
        return pl.new_s3_conn()

    def _format_path(self, bucket, key):
        return '%s://%s/%s' % (self.schema, bucket, key)

    def _parse_path(self, path):
        o = urlparse.urlparse(path)
        if o.scheme != self.schema:
            raise TransferError('Wrong schema')
        return o.hostname, o.path[1:]


def location_from_region(region):
    if region == 'us-east-1' or not region:
        return ''
    elif region == 'eu-west-1':
        return 'EU'
    else:
        return region


# Workaround over bug when EBS volumes cannot be reattached on the same letter,
# and instance need to be rebooted to fix this issue.

def _cleanup_volume_table(*args, **kwargs):
    conn = bus.db
    cur = conn.cursor()
    cur.execute("DELETE FROM storage where (device LIKE '/dev/sd%' or type = 'ebs') and state = 'detached'")
    conn.commit()

bus.on(init=lambda *args, **kwargs: bus.on(before_reboot_finish=_cleanup_volume_table))
