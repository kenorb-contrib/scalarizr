from __future__ import with_statement

__author__ = 'Nick Demyanchuk'

import os
import re
import sys
import time
import Queue
import base64
import logging
import tempfile
import threading
import itertools


from scalarizr import storage2, util
from scalarizr.linux import mdadm, lvm2, coreutils, os as os_detect
from scalarizr.storage2.volumes import base


LOG = logging.getLogger(__name__)


class RaidVolume(base.Volume):


    lv_re = re.compile(r'Logical volume "([^\"]+)" created')


    def __init__(self, disks=None, raid_pv=None, level=None, lvm_group_cfg=None,
                 vg=None, pv_uuid=None, **kwds):
        '''
        :type disks: list
        :param disks: Raid disks

        :type raid_pv: string
        :param raid_pv: Raid device name (e.g: /dev/md0)

        :type level: int
        :param level: Raid level. Valid values are
                * 0
                * 1
                * 5
                * 10

        :type lvm_group_cfg: string
        :param lvm_group_cfg: LVM volume group configuration (base64 encoded)

        :type vg: string
        :param vg: LVM volume group name

        :type pv_uuid: string
        :param pv_uuid: Mdadm device physical volume id
        '''
        # Backward compatibility with old storage
        if vg is not None:
            vg = os.path.basename(vg)
        self._v1_compat = False
        if disks:
            disks = [storage2.volume(disk) for disk in disks]
        super(RaidVolume, self).__init__(disks=disks or [],
                        raid_pv=raid_pv, level=level and int(level),
                        lvm_group_cfg=lvm_group_cfg,
                        vg=vg, pv_uuid=pv_uuid, **kwds)
        self.features.update({'restore': True, 'grow': True})


    def _disable_autoassembly(self):
        try:
            mdadm_conf_path = '/etc/mdadm.conf' if os_detect.redhat_family else '/etc/mdadm/mdadm.conf'
            if os.path.exists(mdadm_conf_path):
                with open(mdadm_conf_path) as f:
                    mdadm_conf = f.read()
            else:
                mdadm_conf = str()
                
            # TODO: augeas candidate
            with open(mdadm_conf_path, 'w') as f:
                if mdadm.version() >= (2, 6, 8):
                    if 'AUTO' in mdadm_conf:
                        mdadm_conf = re.sub(re.compile('^(AUTO\s+(?!-all).*)$', re.M), '#\g<0>', mdadm_conf)
                    if not re.search(re.compile('^AUTO\s+-all\s*$', re.M), mdadm_conf):
                        mdadm_conf += '\nAUTO -all'

                if 'DEVICE' in mdadm_conf:
                    mdadm_conf = re.sub(re.compile('^(DEVICE\s+(?!/dev/null).*)$', re.M), '#\g<0>', mdadm_conf)
                if not re.search(re.compile('^DEVICE\s+/dev/null\s*$', re.M), mdadm_conf):
                    mdadm_conf += '\nDEVICE /dev/null'

                f.write(mdadm_conf)
        except:
            LOG.warning('Autoassembly was not disabled.', exc_info=sys.exc_info())

    def _ensure(self):
        self._v1_compat = self.snap and len(self.snap['disks']) and \
                                        isinstance(self.snap['disks'][0], dict) and \
                                        'snapshot' in self.snap['disks'][0]
        if self.snap:
            disks = []
            snaps = []
            try:
                # @todo: create disks concurrently
                for disk_snap in self.snap['disks']:
                    if self._v1_compat:
                        disk_snap = disk_snap['snapshot']
                    snap = storage2.snapshot(disk_snap)
                    snaps.append(snap)

                if self.disks:
                    if len(self.disks) != len(snaps):
                        raise storage2.StorageError('Volume disks count is not equal to '
                                                                                'snapshot disks count')
                    self.disks = map(storage2.volume, self.disks)

                # Mixing snapshots to self.volumes (if exist) or empty volumes
                disks = self.disks or [storage2.volume(type=s['type']) for s in snaps]

                for disk, snap in zip(disks, snaps):
                    disk.snap = snap

            except:
                with util.capture_exception(logger=LOG):
                    for disk in disks:
                        disk.destroy()

            self.disks = disks

            if self._v1_compat:
                # is some old snapshots /dev/vgname occured
                self.vg = os.path.basename(self.snap['vg'])
            else:
                self.vg = self.snap['vg']
            self.level = int(self.snap['level'])
            self.pv_uuid = self.snap['pv_uuid']
            self.lvm_group_cfg = self.snap['lvm_group_cfg']

            self.snap = None

        self._check_attr('level')
        self._check_attr('vg')
        self._check_attr('disks')

        assert int(self.level) in (0, 1, 5, 10), 'Unknown raid level: %s' % self.level

        # Making sure autoassembly is disabled before attaching disks
        self._disable_autoassembly()

        disks = []
        for disk in self.disks:
            disk = storage2.volume(disk)
            disk.ensure()
            disks.append(disk)
        self.disks = disks

        disks_devices = [disk.device for disk in self.disks]

        if self.lvm_group_cfg:
            time.sleep(2)  # Give a time to device manager 
            try:
                raid_device = mdadm.mdfind(*disks_devices)
            except storage2.StorageError:
                raid_device = mdadm.findname()
                """
                if self.level in (1, 10):
                        for disk in disks_devices:
                                mdadm.mdadm('misc', None, disk,
                                                        zero_superblock=True, force=True)

                        try:
                                kwargs = dict(force=True, metadata='default',
                                                          level=self.level, assume_clean=True,
                                                          raid_devices=len(disks_devices))
                                mdadm.mdadm('create', raid_device, *disks_devices, **kwargs)
                        except:
                                if self.level == 10 and self._v1_compat:
                                        self._v1_repair_raid10(raid_device)
                                else:
                                        raise
                else:
                """
                mdadm.mdadm('assemble', raid_device, *disks_devices)
                mdadm.mdadm('misc', None, raid_device, wait=True, raise_exc=False)

            # Restore vg config
            vg_restore_file = tempfile.mktemp()
            with open(vg_restore_file, 'w') as f:
                f.write(base64.b64decode(self.lvm_group_cfg))

            # Ensure RAID physical volume
            try:
                lvm2.pvs(raid_device)
            except:
                lvm2.pvcreate(raid_device, uuid=self.pv_uuid,
                                        restorefile=vg_restore_file)
            finally:
                lvm2.vgcfgrestore(self.vg, file=vg_restore_file)
                os.remove(vg_restore_file)


            # Check that logical volume exists
            lv_infos = lvm2.lvs(self.vg)
            if not lv_infos:
                raise storage2.StorageError(
                        'No logical volumes found in %s vol. group')
            lv_name = lv_infos.popitem()[1].lv_name
            self.device = lvm2.lvpath(self.vg, lv_name)

            # Activate volume group
            lvm2.vgchange(self.vg, available='y')

            # Wait for logical volume device file
            util.wait_until(lambda: os.path.exists(self.device),
                                    timeout=120, logger=LOG,
                                    error_text='Logical volume %s not found' % self.device)

        else:
            raid_device = mdadm.findname()
            kwargs = dict(force=True, level=self.level, assume_clean=True,
                                      raid_devices=len(disks_devices), metadata='default')
            mdadm.mdadm('create', raid_device, *disks_devices, **kwargs)
            mdadm.mdadm('misc', None, raid_device, wait=True, raise_exc=False)

            lvm2.pvcreate(raid_device, force=True)
            self.pv_uuid = lvm2.pvs(raid_device)[raid_device].pv_uuid

            lvm2.vgcreate(self.vg, raid_device)

            out, err = lvm2.lvcreate(self.vg, extents='100%FREE')[:2]
            try:
                clean_out = out.strip().split('\n')[-1].strip()
                vol = re.match(self.lv_re, clean_out).group(1)
                self.device = lvm2.lvpath(self.vg, vol)
            except:
                e = 'Logical volume creation failed: %s\n%s' % (out, err)
                raise Exception(e)

            self.lvm_group_cfg = lvm2.backup_vg_config(self.vg)

        self.raid_pv = raid_device


    def _v1_repair_raid10(self, raid_device):
        '''
        Situation is the following:
        raid10 creation from the only half of snapshots failed.
        '''
        disks_devices = [disk.device for disk in self.disks]
        missing_devices = disks_devices[::2]
        md0_devices = [disks_devices[i] if i % 2 else 'missing' \
                                        for i in range(0, len(disks_devices))]


        # Stop broken raid
        if os.path.exists('/dev/md127'):
            mdadm.mdadm('stop', '/dev/md127', force=True)

        # Create raid with missing disks
        kwargs = dict(force=True, metadata='default',
                                  level=self.level, assume_clean=True,
                                  raid_devices=len(disks_devices))
        mdadm.mdadm('create', raid_device, *md0_devices, **kwargs)
        mdadm.mdadm('misc', raid_device, wait=True)

        # Add missing devices one by one
        for device in missing_devices:
            mdadm.mdadm('add', raid_device, device)
        mdadm.mdadm('misc', raid_device, wait=True)


    def _detach(self, force, **kwds):
        self.lvm_group_cfg = lvm2.backup_vg_config(self.vg)
        lvm2.vgremove(self.vg, force=True)
        lvm2.pvremove(self.raid_pv, force=True)

        mdadm.mdadm('misc', None, self.raid_pv, stop=True, force=True)
        try:
            mdadm.mdadm('manage', None, self.raid_pv, remove=True, force=True)
        except (Exception, BaseException), e:
            if not 'No such file or directory' in str(e):
                raise

        try:
            os.remove(self.raid_pv)
        except:
            pass

        self.raid_pv = None

        for disk in self.disks:
            disk = storage2.volume(disk)
            disk.detach(force=force)

        self.device = None


    def _snapshot(self, description, tags, **kwds):
        coreutils.sync()
        lvm2.dmsetup('suspend', self.device)
        try:
            description = 'Raid%s disk ${index}%s' % (self.level, \
                                            '. %s' % description if description else '')
            disks_snaps = storage2.concurrent_snapshot(
                    volumes=self.disks,
                    description=description,
                    tags=tags, **kwds
            )

            return storage2.snapshot(
                    type='raid',
                    disks=disks_snaps,
                    lvm_group_cfg=lvm2.backup_vg_config(self.vg),
                    level=self.level,
                    pv_uuid=self.pv_uuid,
                    vg=self.vg
            )
        finally:
            lvm2.dmsetup('resume', self.device)


    def _destroy(self, force, **kwds):
        remove_disks = kwds.get('remove_disks')
        if remove_disks:
            for disk in self.disks:
                disk = storage2.volume(disk)
                disk.destroy(force=force)
            self.disks = []


    def _clone(self, config):
        disks = []
        for disk_cfg_or_obj in self.disks:
            disk = storage2.volume(disk_cfg_or_obj)
            disk_clone = disk.clone()
            disks.append(disk_clone)

        config['disks'] = disks
        for attr in ('pv_uuid', 'lvm_group_cfg', 'raid_pv', 'device'):
            config.pop(attr, None)


    def check_growth(self, **growth):
        if int(self.level) in (0, 10):
            raise storage2.StorageError("Raid%s doesn't support growth" % self.level)

        disk_growth = growth.get('disks')
        change_disks = False

        if disk_growth:
            for disk_cfg_or_obj in self.disks:
                disk = storage2.volume(disk_cfg_or_obj)
                try:
                    disk.check_growth(**disk_growth)
                    change_disks = True
                except storage2.NoOpError:
                    pass

        new_len = growth.get('disks_count')
        current_len = len(self.disks)
        change_size = new_len and int(new_len) != current_len

        if not change_size and not change_disks:
            raise storage2.NoOpError('Configurations are equal. Nothing to do')

        if change_size and int(new_len) < current_len:
            raise storage2.StorageError('Disk count can only be increased.')

        if change_size and int(self.level) in (0, 10):
            raise storage2.StorageError("Can't add disks to raid level %s"
                                                                                                                    % self.level)

    def _grow(self, new_vol, **growth):
        if int(self.level) in (0, 10):
            raise storage2.StorageError("Raid%s doesn't support growth" % self.level)

        disk_growth = growth.get('disks')

        current_len = len(self.disks)
        new_len = int(growth.get('disks_count', 0))
        increase_disk_count = new_len and new_len != current_len

        new_vol.lvm_group_cfg = self.lvm_group_cfg
        new_vol.pv_uuid = self.pv_uuid

        growed_disks = []
        added_disks = []
        try:
            if disk_growth:

                def _grow(index, disk, cfg, queue):
                    try:
                        ret = disk.grow(resize_fs=False, **cfg)
                        queue.put(dict(index=index, result=ret))
                    except:
                        e = sys.exc_info()[1]
                        queue.put(dict(index=index, error=e))

                # Concurrently grow each descendant disk
                queue = Queue.Queue()
                pool = []
                for index, disk_cfg_or_obj in enumerate(self.disks):
                    # We use index to save disk order in raid disks
                    disk = storage2.volume(disk_cfg_or_obj)

                    t = threading.Thread(
                            name='Raid %s disk %s grower' % (self.id, disk.id),
                            target=_grow, args=(index, disk, disk_growth, queue))
                    t.daemon = True
                    t.start()
                    pool.append(t)

                for thread in pool:
                    thread.join()

                # Get disks growth results
                res = []
                while True:
                    try:
                        res.append(queue.get_nowait())
                    except Queue.Empty:
                        break

                res.sort(key=lambda p: p['index'])
                growed_disks = [r['result'] for r in res if 'result' in r]

                # Validate concurrent growth results
                assert len(res) == len(self.disks), ("Not enough data in "
                                "concurrent raid disks grow result")

                if not all(map(lambda x: 'result' in x, res)):
                    errors = '\n'.join([str(r['error']) for r in res if 'error' in r])
                    raise storage2.StorageError('Failed to grow raid disks.'
                                    ' Errors: \n%s' % errors)

                assert len(growed_disks) == len(self.disks), ("Got malformed disks"
                                        " growth result (not enough data).")

                new_vol.disks = growed_disks
                new_vol.pv_uuid = self.pv_uuid
                new_vol.lvm_group_cfg = self.lvm_group_cfg

                new_vol.ensure()

            if increase_disk_count:
                if not disk_growth:
                    """ It means we have original disks in self.disks
                            We need to snapshot it and make new disks.
                    """
                    new_vol.disks = []
                    snaps = storage2.concurrent_snapshot(self.disks,
                                    'Raid %s temp snapshot No.${index} (for growth)' % self.id,
                                    tags=dict(temp='1'))
                    try:
                        for disk, snap in zip(self.disks, snaps):
                            new_disk = disk.clone()
                            new_disk.snap = snap
                            new_vol.disks.append(new_disk)
                            new_disk.ensure()
                    finally:
                        for s in snaps:
                            try:
                                s.destroy()
                            except:
                                e = sys.exc_info()[1]
                                LOG.debug('Failed to remove temporary snapshot: %s' % e)

                    new_vol.ensure()

                existing_raid_disk = new_vol.disks[0]
                add_disks_count = new_len - current_len
                for _ in range(add_disks_count):
                    disk_to_add = existing_raid_disk.clone()
                    added_disks.append(disk_to_add)
                    disk_to_add.ensure()

                added_disks_devices = [d.device for d in added_disks]
                mdadm.mdadm('manage', new_vol.raid_pv, add=True,
                                                                                                *added_disks_devices)
                new_vol.disks.extend(added_disks)

                mdadm.mdadm('grow', new_vol.raid_pv, raid_devices=new_len)

            mdadm.mdadm('misc', None, new_vol.raid_pv, wait=True, raise_exc=False)
            mdadm.mdadm('grow', new_vol.raid_pv, size='max')
            mdadm.mdadm('misc', None, new_vol.raid_pv, wait=True, raise_exc=False)

            lvm2.pvresize(new_vol.raid_pv)
            try:
                lvm2.lvresize(new_vol.device, extents='100%VG')
            except:
                e = sys.exc_info()[1]
                if (self.level == 1 and 'matches existing size' in str(e) and not disk_growth):
                    LOG.debug('Raid1 actual size has not changed')
                else:
                    raise
        except:
            err_type, err_val, trace = sys.exc_info()
            if growed_disks or added_disks:
                LOG.debug("Removing %s successfully growed disks and "
                                        "%s additional disks",
                                        len(growed_disks), len(added_disks))
                for disk in itertools.chain(growed_disks, added_disks):
                    try:
                        disk.destroy(force=True)
                    except:
                        e = sys.exc_info()[1]
                        LOG.error('Failed to remove raid disk: %s' % e)

            raise err_type, err_val, trace


    def replace_disk(self, index, disk):
        '''
        :param: index RAID disk index. Starts from 0
        :type index: int
        :param: disk  Replacement disk. 
        :type: disk dict/Volume
        '''

        disk_replace = storage2.volume(disk)
        replace_is_new = not disk_replace.id

        try:
            disk_replace.ensure()
            disk_find = self.disks[index]

            mdadm.mdadm('manage', self.raid_pv, '--fail', disk_find.device)
            mdadm.mdadm('manage', self.raid_pv, '--remove', disk_find.device)
            mdadm.mdadm('manage', self.raid_pv, '--add', disk_replace.device)

            self.disks[index] = disk_replace
        except:
            with util.capture_exception(logger=LOG):
                if replace_is_new:
                    disk_replace.destroy(force=True)
        else:
            disk_find.destroy(force=True)



class RaidSnapshot(base.Snapshot):

    def __init__(self, **kwds):
        super(RaidSnapshot, self).__init__(**kwds)
        self.disks = map(storage2.snapshot, self.disks)


    def _destroy(self):
        for disk in self.disks:
            disk.destroy()


    def _status(self):
        if all((snap.status() == self.COMPLETED for snap in self.disks)):
            return self.COMPLETED
        elif any((snap.status() == self.FAILED for snap in self.disks)):
            return self.FAILED
        elif any((snap.status() == self.IN_PROGRESS for snap in self.disks)):
            return self.IN_PROGRESS
        return self.UNKNOWN


storage2.volume_types['raid'] = RaidVolume
storage2.snapshot_types['raid'] = RaidSnapshot
