from __future__ import with_statement
'''
Created on Jan 6, 2011

@author: marat
'''
from __future__ import with_statement
from . import VolumeConfig, Volume, Snapshot, VolumeProvider, Storage, StorageError
from .transfer import Transfer
from .util.lvm2 import Lvm2, lvm_group_b64, Lvm2Error
from .util import ramdisk

from scalarizr.libs.metaconf import Configuration
from scalarizr.util.software import which
from scalarizr.util import firstmatched
from scalarizr import linux
from scalarizr.linux import mount, pkgmgr

from Queue import Queue, Empty
from tempfile import mkdtemp

import subprocess
import threading
import cStringIO
import binascii
import hashlib
import logging
import time
import os
import sys

LVM_EXTENT_SIZE = 4*1024*1024

class EphConfig(VolumeConfig):
    type = 'eph'
    vg = None
    lvm_group_cfg = None
    disk = None
    size = None
    path = None
    snap_backend = None
    snap_strategy = None

class EphVolume(Volume, EphConfig):
    _ignores = ('path', 'snap_strategy')

class EphSnapshot(Snapshot, EphConfig):
    pass

TRANZIT_VOL_MPOINT      = '/mnt/tranzit'
TRANZIT_VOL_SIZE        = 205

class EphVolumeProvider(VolumeProvider):
    type = 'eph'
    vol_class = EphVolume
    snap_class = EphSnapshot

    _lvm = None
    _snap_pvd = None

    def __init__(self):
        self._lvm = Lvm2()
        self._snap_pvd = EphSnapshotProviderLite()

    def _create_layout(self, pv, vg, size):
        '''
        Creates LV layout
              [Disk]
                |
               [VG]
              /   \
          [Data] [Tranzit]
        '''

        # Create PV
        self._lvm.create_pv(pv)

        # Create VG
        if not isinstance(vg, dict):
            vg = dict(name=vg)
        vg_name = vg['name']
        del vg['name']
        vg = self._lvm.create_vg(vg_name, [pv], **vg)
        vg = os.path.basename(vg)

        # Create data volume
        lv_kwargs = dict()

        size = size or '80%'
        size = str(size)
        if size[-1] == '%':
            lv_kwargs['extents'] = '%sVG' % size
        else:
            lv_kwargs['size'] = int(size)

        data_lv = self._lvm.create_lv(vg, 'data', **lv_kwargs)

        # Create tranzit volume (should be 5% bigger then data vol)
        #lvi = self._lvm.lv_info(data_lv)
        #size_in_KB = 0
        #with open('/sys/block/dm-%s/size' % lvi.lv_kernel_minor, 'r') as fp:
        #    size_in_KB = int(fp.read()) / 2
        #tranzit_lv = self._lvm.create_lv(vg, 'tranzit', size='%dK' % (size_in_KB*1.05,))

        return (vg, data_lv, size)

    def _destroy_layout(self, vg, data_lv):
        # Find PV
        pv = None
        pvi = firstmatched(lambda pvi: vg in pvi.vg, self._lvm.pv_status())
        if pvi:
            pv = pvi.pv

        # Remove storage VG
        self._lvm.change_lv(data_lv, available=False)
        self._lvm.remove_vg(vg)

        if pv:
            # Remove PV if it doesn't belongs to any other VG
            pvi = self._lvm.pv_info(pv)
            if not pvi.vg:
                self._lvm.remove_pv(pv)

    def create(self, **kwargs):
        '''
        @param disk: Physical volume
        @param vg: Uniting volume group
        @param size: Useful storage size (in % of physican volume or MB)
        @param snap_backend: Snapshot backend

        Example:
        Storage.create({
                'type': 'eph',
                'disk': '/dev/sdb',
                'size': '40%',
                'vg': {
                        'name': 'mysql_data',
                        'ph_extent_size': 10
                },
                'snap_backend': 'cf://mysql_backups/cloudsound/production',
                'snap_strategy' : 'data'|'device'
        })
        '''
        initialized = False
        if 'device' in kwargs:
            try:
                self._lvm.pv_scan()
                self._lvm.change_vg(kwargs['vg'], available=True)
                lvi = self._lvm.lv_info(kwargs['device'])
                gvi = self._lvm.vg_info(kwargs['vg'])
                initialized = lvi.path == kwargs['device'] and gvi.vg == kwargs['vg']
            except (LookupError, Lvm2Error):
                pass

        if not initialized:
            if kwargs.get('lvm_group_cfg'):
                self._lvm.restore_vg(kwargs['vg'], cStringIO.StringIO(kwargs['lvm_group_cfg']))
            else:
                # Create LV layout
                kwargs['disk'].umount()
                kwargs['vg'], kwargs['device'], kwargs['size'] = self._create_layout(
                                kwargs['disk'].device, vg=kwargs.get('vg'), size=kwargs.get('size'))

        # Accept snapshot backend
        if not isinstance(kwargs['snap_backend'], dict):
            kwargs['snap_backend'] = dict(path=kwargs['snap_backend'])

        return super(EphVolumeProvider, self).create(**kwargs)

    def create_from_snapshot(self, **kwargs):
        '''
        ...
        @param path: Path to snapshot manifest on remote storage

        Example:
        Storage.create(**{
                'disk' : {
                        'type' : 'loop',
                        'file' : '/media/storage',
                        'size' : 1000
                }
                'snapshot': {
                        'type': 'eph',
                        'description': 'Last winter mysql backup',
                        'path': 'cf://mysql_backups/cloudsound/production/snap-14a356de.manifest.ini'
                        'size': '40%',
                        'vg': 'mysql_data'
                }
        })
        '''
        _kwargs = kwargs.copy()
        if 'id' in _kwargs:
            del _kwargs['id']

        if not 'snap_backend' in _kwargs:
            _kwargs['snap_backend'] = os.path.dirname(_kwargs['path'])
        vol = self.create(**_kwargs)

        snap = self.snapshot_factory(**kwargs)
        """
        # Free ram check disabled (wrong ram detection on GCE instances)

        free_ram, free_swap = ramdisk.free()
        if (free_ram + free_swap) < TRANZIT_VOL_SIZE:
                raise Exception('Instance has no enough free ram to create tranzit ramdisk')
        """
        ramdisk.create(TRANZIT_VOL_SIZE, TRANZIT_VOL_MPOINT)

        try:
            self._snap_pvd.download_and_restore(vol, snap, TRANZIT_VOL_MPOINT)
        finally:
            ramdisk.destroy(TRANZIT_VOL_MPOINT, force=True)

        """
        try:
                self._snap_pvd.prepare_tranzit_vol(vol.tranzit_vol)
                self._snap_pvd.download(vol, snap, vol.tranzit_vol.mpoint)
                self._snap_pvd.restore(vol, snap, vol.tranzit_vol.mpoint)
        finally:
                self._snap_pvd.cleanup_tranzit_vol(vol.tranzit_vol)
        """
        return vol

    def create_snapshot(self, vol, snap, **kwargs):
        ramdisk.create(TRANZIT_VOL_SIZE, TRANZIT_VOL_MPOINT)
        cleanup = lambda: ramdisk.destroy(TRANZIT_VOL_MPOINT, force=True)
        return self._snap_pvd.create(vol, snap, TRANZIT_VOL_MPOINT, cleanup)


    def get_snapshot_state(self, snap):
        return self._snap_pvd.get_snapshot_state(snap)

    def blank_config(self, cnf):
        cnf.pop('lvm_group_cfg', None)
        cnf['disk'] = Storage.blank_config(cnf['disk'])

    def detach(self, vol, force=False):
        '''
        @type vol: EphVolume
        '''
        super(EphVolumeProvider, self).detach(vol, force)
        if vol.vg:
            vol.lvm_group_cfg = lvm_group_b64(vol.vg)
            self._destroy_layout(vol.vg, vol.device)
        vol.disk.detach(force)
        return vol.config()

    def destroy(self, vol, force=False, **kwargs):
        super(EphVolumeProvider, self).destroy(vol, force, **kwargs)
        self._destroy_layout(vol.vg, vol.device)
        vol.disk.destroy(force=force)

Storage.explore_provider(EphVolumeProvider)


def clear_queue(queue):
    while True:
        try:
            queue.get_nowait()
        except Empty:
            return

class EphSnapshotProviderLite(object):

    MANIFEST_NAME           = 'manifest.ini'
    SNAPSHOT_LV_NAME        = 'snap'

    chunk_size = None
    '''     Data chunk size in Mb '''

    _logger         = None
    _transfer       = None
    _lvm            = None
    _state_map      = None
    _upload_queue= None
    _chunks_md5 = None
    _read_finished = None
    _inner_exc_info = None
    _pigz_bin = None

    def __init__(self, chunk_size=100):
        self.chunk_size                 = chunk_size
        self._logger                    = logging.getLogger(__name__)
        self._lvm                               = Lvm2()
        self._state_map                 = dict()
        self._upload_queue              = Queue(2)
        self._download_queue    = Queue()
        self._writer_queue              = Queue(2)
        self._read_finished     = threading.Event()
        self._download_finished = threading.Event()
        self._slot_available    = threading.Semaphore(2)
        self._transfer_cls      = Transfer
        self._inner_exc_info    = None
        self._return_ev                 = threading.Event()
        self._pigz_bin                  = '/usr/bin/pigz'


    def create(self, volume, snapshot, tranzit_path, complete_cb=None):
        try:
            if snapshot.id in self._state_map:
                raise StorageError('Snapshot %s is already %s. Cannot create it again' % (
                                snapshot.id, self._state_map[snapshot.id]))

            clear_queue(self._upload_queue)

            if not os.path.exists(self._pigz_bin):
                if linux.os['family'] == 'Debian' and linux.os['release'] >= (10, 4):
                    pkgmgr.installed('pigz')
                elif linux.os['family'] == 'RedHat' and linux.os['release'] >= (6, 0):
                    pkgmgr.epel_repository()
                    pkgmgr.installed('pigz')


            self._chunks_md5 = {}
            self._state_map[snapshot.id] = Snapshot.CREATING
            #self.prepare_tranzit_vol(volume.tranzit_vol)
            snap_lv = self._lvm.create_lv_snapshot(volume.device, self.SNAPSHOT_LV_NAME, extents='100%FREE')
            self._logger.info('Created LVM snapshot %s for volume %s', snap_lv, volume.device)
            self._return_ev.clear()
            t = threading.Thread(name='%s creator' % snapshot.id, target=self._create,
                                                    args=(volume, snapshot, snap_lv, tranzit_path, complete_cb))
            t.start()
            self._return_ev.wait()
        except:
            if complete_cb:
                complete_cb()
            raise
        snapshot.snap_strategy = 'data'
        snapshot.path = os.path.join(volume.snap_backend['path'],
                                                                '%s.%s' % (snapshot.id, self.MANIFEST_NAME))
        return snapshot

    def _create(self, volume, snapshot, snap_lv, tranzit_path,  complete_cb):
        try:
            chunk_prefix = '%s.data' % snapshot.id
            snapshot.path = None
            snap_mpoint = mkdtemp()
            try:
                opts = []
                if volume.fstype == 'xfs':
                    opts += ['-o', 'nouuid,ro']
                mount.mount(snap_lv, snap_mpoint, *opts)
                tar_cmd = ['tar', 'cp', '-C', snap_mpoint, '.']

                if which('pigz'):
                    compress_cmd = [which('pigz'), '-5']
                else:
                    compress_cmd = ['gzip', '-5']

                self._logger.debug("Creating and compressing snapshot data.")
                tar = subprocess.Popen(tar_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, close_fds=True)
                compress = subprocess.Popen(compress_cmd, stdin=tar.stdout, stdout=subprocess.PIPE, stderr=subprocess.PIPE, close_fds=True)
                tar.stdout.close() # Allow tar to receive a SIGPIPE if compress exits.
                split = threading.Thread(target=self._split, name='split',
                                  args=(compress.stdout, tranzit_path, chunk_prefix, snapshot))
                split.start()

                uploaders = []
                for i in range(2):
                    uploader = threading.Thread(name="Uploader-%s" % i, target=self._uploader,
                                                                      args=(volume.snap_backend['path'], snapshot))
                    self._logger.debug("Starting uploader '%s'", uploader.getName())

                    uploader.start()
                    uploaders.append(uploader)
                self._logger.debug('uploaders started. waiting compress')

                compress.wait()
                self._logger.debug('compress completed (code: %s). waiting split', compress.returncode)
                if compress.returncode:
                    raise StorageError('Compress process terminated with exit code %s. <err>: %s' % (compress.returncode, compress.stderr.read()))

                split.join()
                self._logger.debug('split completed. waiting uploaders')

                for uploader in uploaders:
                    uploader.join()
                self._logger.debug('uploaders completed')

                if self._inner_exc_info:
                    t, e, s = self._inner_exc_info
                    raise t, e, s

            finally:
                self._return_ev.set()
                mount.umount(snap_mpoint)
                os.rmdir(snap_mpoint)
                self._lvm.remove_lv(snap_lv)
                self._inner_exc_info = None
            self._state_map[snapshot.id] = Snapshot.COMPLETED
        except (Exception, BaseException), e:
            self._state_map[snapshot.id] = Snapshot.FAILED
            self._logger.exception('Snapshot creation failed. %s' % e)
        finally:
            try:
                if complete_cb:
                    complete_cb()
            except:
                self._logger.warn('complete_cb() failed', exc_info=sys.exc_info())

    def _split(self, stdin, tranzit_path, chunk_prefix, snapshot):
        chunk_fp = None
        try:
            self._read_finished.clear()
            chunk_max_size = 100*1024*1024
            piece_rest = ''
            index = 0
            chunk_size = 0
            chunk_md5 = hashlib.md5()
            chunk_path = os.path.join(tranzit_path, chunk_prefix + '.tar.gz.%03d'%index)
            chunk_fp = open(chunk_path, 'wb' )

            while True:
                piece = stdin.read(LVM_EXTENT_SIZE)
                if not piece and not piece_rest:

                    if not chunk_fp.closed:
                        chunk_fp.close()
                    if chunk_size:
                        self._logger.debug('Putting chunk %s to upload queue' % chunk_path)
                        self._upload_queue.put(chunk_path)
                        self._chunks_md5[os.path.basename(chunk_path)] = binascii.hexlify(chunk_md5.digest())

                    manifest_path = self._write_manifest(snapshot, tranzit_path)
                    self._upload_queue.put(manifest_path)
                    break

                if piece_rest:
                    piece = piece_rest + piece
                    piece_rest = ''

                if (chunk_size + len(piece)) > chunk_max_size:
                    rest_len = chunk_size + len(piece) - chunk_max_size
                    piece_rest = piece[-rest_len:]
                    piece = piece[:-rest_len]


                if chunk_fp.closed:
                    with self._slot_available:
                        chunk_path = os.path.join(tranzit_path, chunk_prefix + '.tar.gz.%03d'%index)
                        chunk_fp = open(chunk_path, 'wb' )


                chunk_fp.write(piece)
                chunk_size += len(piece)
                chunk_md5.update(piece)


                if chunk_size == chunk_max_size:
                    chunk_fp.close()
                    self._logger.debug('Putting chunk %s to upload queue' % chunk_path)
                    self._upload_queue.put(chunk_path)
                    self._chunks_md5[os.path.basename(chunk_path)] = binascii.hexlify(chunk_md5.digest())
                    chunk_md5 = hashlib.md5()
                    index += 1
                    chunk_size = 0
        except:
            self._inner_exc_info = sys.exc_info()
        finally:
            self._read_finished.set()
            stdin.close()
            if chunk_fp and not chunk_fp.closed:
                chunk_fp.close()



    def _uploader(self, dst, snapshot):
        """
        @rtype: tuple
        """
        try:
            transfer = self._transfer_cls()

            def _upload():
                with self._slot_available:
                    self._return_ev.set()
                    link = transfer.upload([chunk_path], dst)[0]
                    os.remove(chunk_path)

                if 'manifest.ini' in link:
                    snapshot.path = link

            while True:
                try:
                    chunk_path = self._upload_queue.get(False)
                    self._logger.debug('Uploader got chunk %s' % chunk_path)
                except Empty:
                    if self._read_finished.is_set():
                        while True:
                            try:
                                chunk_path = self._upload_queue.get(False)
                                _upload()
                            except Empty:
                                break
                        self._logger.debug('Upload is finished.')
                        break
                    continue

                _upload()

        except:
            self._inner_exc_info = sys.exc_info()


    def _downloader(self, tranzit_path):
        transfer = self._transfer_cls()
        while True:
            if self._download_queue.empty():
                self._download_finished.set()
                break

            if not self._writer_queue.empty():
                continue

            link, md5 = self._download_queue.get()

            transfer.download((link,), tranzit_path)
            chunk_path = os.path.join(tranzit_path, os.path.basename(link))
            if self._md5sum(chunk_path) != md5:
                raise Exception('Md5sum is not correct')
            self._writer_queue.put(chunk_path)

    def _write_manifest(self, snapshot, tranzit_path):
        ''' Make snapshot manifest '''
        manifest_path = os.path.join(tranzit_path, '%s.%s' % (snapshot.id, self.MANIFEST_NAME))
        self._logger.info('Writing snapshot manifest file in %s', manifest_path)
        config = Configuration('ini')
        config.add('snapshot/description', snapshot.description, force=True)
        config.add('snapshot/created_at', time.strftime("%Y-%m-%d %H:%M:%S"))
        config.add('snapshot/pack_method', 'pigz') # Not used yet
        for chunk, md5 in self._chunks_md5.iteritems():
            config.add('chunks/%s' % chunk, md5, force=True)

        config.write(manifest_path)

        return manifest_path


    def get_snapshot_state(self, snapshot):
        return self._state_map[snapshot.id]

    def download_and_restore(self, volume, snapshot, tranzit_path):
        # Load manifest
        clear_queue(self._writer_queue)
        clear_queue(self._download_queue)
        self._download_finished.clear()
        transfer = self._transfer_cls()
        mnf_path = transfer.download(snapshot.path, tranzit_path)
        mnf = Configuration('ini')
        mnf.read(mnf_path)

        volume.fs_created = False
        volume.mkfs(snapshot.fstype)

        remote_path = os.path.dirname(snapshot.path)
        # Get links with md5 sums
        links = [(os.path.join(remote_path, chunk[0]), chunk[1]) for chunk in mnf.items('chunks')]
        links.sort()

        # Download 2 first chunks
        for link in links[:2]:
            transfer.download(link[0], tranzit_path)
            chunk_path = os.path.join(tranzit_path, os.path.basename(link[0]))
            if self._md5sum(chunk_path) != link[1]:
                raise Exception("md5sum of chunk %s is not correct." % chunk_path)
            self._writer_queue.put(chunk_path)

        if hasattr(snapshot, 'snap_strategy') and snapshot.snap_strategy == 'data':
            restore_strategy = DataRestoreStrategy(self._logger)
        else:
            restore_strategy = DeviceRestoreStrategy(self._logger)

        writer = threading.Thread(target=restore_strategy.restore, name='writer',
                                                        args=(self._writer_queue, volume, self._download_finished))
        writer.start()

        # Add remaining files to download queue
        for link in links[2:]:
            self._download_queue.put(link)

        downloader = threading.Thread(name="Downloader", target=self._downloader,
                                                                  args=(tranzit_path,))
        downloader.start()
        downloader.join()
        writer.join()

    def _md5sum(self, file, block_size=4096):
        fp = open(file, 'rb')
        try:
            md5 = hashlib.md5()
            while True:
                data = fp.read(block_size)
                if not data:
                    break
                md5.update(data)
            return binascii.hexlify(md5.digest())
        finally:
            fp.close()


class RestoreStrategy:
    def __init__(self, logger):
        #self._logger = logging.getLogger(__name__)
        self._logger = logger

    def concat_chunks(self, queue, download_finished, stdout):
        while True:
            try:
                chunk_path = queue.get(False)
            except Empty:
                if download_finished.is_set():
                    break
                continue
            chunk_fp = open(chunk_path)
            while True:
                piece = chunk_fp.read(LVM_EXTENT_SIZE)
                if not piece:
                    chunk_fp.close()
                    os.unlink(chunk_path)
                    break
                stdout.write(piece)

class DataRestoreStrategy(RestoreStrategy):
    def restore(self, queue, volume, download_finished):
        tmp_mpoint = mkdtemp()
        volume.mount(tmp_mpoint)
        try:
            try:
                cmd1 = (which('pigz'), '-d')
            except LookupError:
                cmd1 = ('gzip', '-d')
            cmd2 = ('tar', 'px', '-C', tmp_mpoint)

            compressor = subprocess.Popen(cmd1, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, close_fds=True)
            tar      = subprocess.Popen(cmd2, stdin=compressor.stdout, stdout=subprocess.PIPE, stderr=subprocess.PIPE, close_fds=True)
            self.concat_chunks(queue, download_finished, compressor.stdin)

            compressor.stdin.close()
            r_code = compressor.wait()
            if r_code:
                raise Exception('Archiver finished with return code %s' % r_code)

            r_code = tar.wait()
            if r_code:
                raise Exception('Tar finished with return code %s' % r_code)
        finally:
            mount.umount(tmp_mpoint)

class DeviceRestoreStrategy(RestoreStrategy):
    def restore(self, queue, volume, download_finished):
        device_fp = open(volume.device, 'w')
        if which('pigz'):
            compress_cmd = [which('pigz'), '-d']
        else:
            compress_cmd = ['gzip', '-d']
        compressor = subprocess.Popen(compress_cmd, stdin=subprocess.PIPE, stdout=device_fp, stderr=subprocess.PIPE, close_fds=True)
        self.concat_chunks(queue, download_finished, compressor.stdin)

        compressor.stdin.close()

        ret_code = compressor.wait()
        if ret_code:
            raise StorageError('Snapshot decompression failed.')
