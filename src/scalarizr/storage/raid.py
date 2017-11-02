from __future__ import with_statement
'''
Created on Jan 6, 2011

@author: marat
'''

import os
import sys
import copy
import time
import logging
import binascii
import threading


from . import VolumeConfig, Volume, Snapshot, VolumeProvider, Storage, \
                StorageError, system, devname_not_empty
from .util.mdadm import Mdadm
from .util.lvm2 import Lvm2, lvm_group_b64

from scalarizr.util import wait_until


logger = logging.getLogger(__name__)


class RaidConfig(VolumeConfig):
    type    = 'raid'
    level   = None
    raid_pv = None
    vg              = None
    lvm_group_cfg = None
    disks   = None
    pv_uuid = None


    def config(self, as_snapshot=False):
        cnf = VolumeConfig.config(self, as_snapshot)

        if isinstance(cnf['raid_pv'], Volume):
            cnf['raid_pv'] = cnf['raid_pv'].config()

        return cnf



class RaidVolume(Volume, RaidConfig):

    def __init__(self, device=None, mpoint=None, fstype=None, type=None, *args, **kwargs):
        super(RaidVolume, self).__init__(device, mpoint, fstype, type, *args, **kwargs)
        self.define_events(
                'before_replace_disk', 'after_replace_disk', 'replace_disk_failed',
                'before_add_disk', 'after_add_disk', 'add_disk_failed'
        )


    def replace_disk(self, old, new):
        self.fire('before_replace_disk')
        try:
            pvd = Storage.lookup_provider(self.type)
            pvd.replace_disk(self, old, new)
        except:
            self.fire('replace_disk_failed')
            raise
        else:
            self.fire('after_replace_disk')


    def add_disks(self, disks):
        self.fire('before_add_disk')
        try:
            pvd = Storage.lookup_provider(self.type)
            pvd.add_disks(self, disks)
        except:
            self.fire('add_disk_failed')
            raise
        else:
            self.fire('after_add_disk')



class RaidSnapshot(Snapshot, RaidConfig):
    pass



class RaidVolumeProvider(VolumeProvider):
    type = 'raid'
    vol_class = RaidVolume
    snap_class = RaidSnapshot


    def __init__(self):
        self._mdadm = Mdadm()
        self._lvm = Lvm2()
        self._logger = logging.getLogger(__name__)
        self._lvm_backup_filename = '/tmp/lvm_backup'


    def create(self, **kwargs):
        '''
        @param disks: Raid disks
        @type disks: list(Volume)

        @param level: Raid level 0, 1, 5, 10 - are valid values
        @type level: int

        @param vg: Volume group over RAID PV
        @type vg: str|dict

        '''

        if kwargs.get('lvm_group_cfg'):
            volume = self.create_from_snapshot(**kwargs)
        else:
            raid_pv = self._mdadm.create(list(vol.devname for vol in kwargs['disks']), kwargs['level'])
            if not isinstance(kwargs['vg'], dict):
                kwargs['vg'] = dict(name=kwargs['vg'])
            vg_name = kwargs['vg']['name']
            del kwargs['vg']['name']
            vg_options = kwargs['vg']

            self._create_pv(raid_pv)
            kwargs['vg'] = self._create_vg(vg_name, raid_pv, **vg_options)
            kwargs['device'] = self._create_lv(vg_name, kwargs.get('device'))
            kwargs['raid_pv'] = raid_pv
            kwargs['pv_uuid'] = self._lvm.pv_info(raid_pv).uuid
            kwargs['lvm_group_cfg'] = lvm_group_b64(kwargs['vg'])
            volume = super(RaidVolumeProvider, self).create(**kwargs)
        return volume


    def _create_pv(self, device, uuid=None):
        self._lvm.pv_scan()
        try:
            self._lvm.pv_info(device)
        except LookupError:
            self._logger.debug('Creating physical volume. %s ' % (uuid and 'uuid=%s' % uuid or ''))
            self._lvm.create_pv(device, uuid=uuid)


    def _create_vg(self, name, device, **options):
        try:
            self._lvm.vg_info(name)
            return '/dev/%s' % name
        except:
            return self._lvm.create_vg(name, (device,), **options)


    def _create_lv(self, vg_name, device=None):
        if device:
            try:
                lvi = self._lvm.lv_info(device)
                return lvi.path
            except:
                pass

        return self._lvm.create_lv(vg_name, extents='100%FREE')


    def create_from_snapshot(self, **kwargs):
        '''
        @param level: Raid level 0, 1, 5 - are valid values
        @param vg: Volume group name to restore
        @param lvm_group_cfg: Base64 encoded RAID volume group configuration
        @param disks: Volumes
        '''

        vg = kwargs['vg']
        level = kwargs['level']
        raw_vg = os.path.basename(vg)

        if int(level) in (1, 10):
            raid_pv = self._mdadm.create([vol.devname for vol in kwargs['disks']], level)
        else:
            raid_pv = self._mdadm.assemble([vol.devname for vol in kwargs['disks']])

        pv_uuid = kwargs.get('pv_uuid')
        self._create_pv(raid_pv, pv_uuid )

        lvm_raw_backup = binascii.a2b_base64(kwargs['lvm_group_cfg'])
        with open(self._lvm_backup_filename, 'w') as fp:
            fp.write(lvm_raw_backup)

        try:
            self._lvm.restore_vg(vg, self._lvm_backup_filename)
        finally:
            os.unlink(self._lvm_backup_filename)

        lvinfo = self._lvm.lv_info(kwargs['device'])
        self._lvm.change_vg(raw_vg, available=True)
        wait_until(lambda: os.path.exists(kwargs['device']), logger=self._logger)
        pv_info = self._lvm.pv_info(raid_pv)

        return RaidVolume(      lvinfo.lv_path,
                                                raid_pv = raid_pv,
                                                vg              = vg,
                                                disks   = kwargs['disks'],
                                                level   = kwargs['level'],
                                                pv_uuid = pv_info.uuid,
                                                lvm_group_cfg = kwargs['lvm_group_cfg'],
                                                mpoint = kwargs.get('mpoint'),
                                                type = self.type)


    @devname_not_empty
    def create_snapshot(self, vol, snap, **kwargs):

        snap.level              = vol.level
        snap.vg                 = vol.vg
        snap.pv_uuid    = vol.pv_uuid
        snap.disks              = []
        snap.lvm_group_cfg = vol.lvm_group_cfg

        tags = kwargs.get('tags')
        snapshots = []
        errors = []
        threads = []

        # Suspend logical volumes
        self._lvm.suspend_lv(vol.device)

        def snapshot(i, vol, description, tags):
            try:
                pvd = Storage.lookup_provider(vol.type)
                snap = pvd.snapshot_factory(description)
                pvd.create_snapshot(vol, snap, tags=tags)
                snapshots.append((i, snap))
            except:
                e = sys.exc_info()[1]
                errors.append(e)

        try:
            # Creating RAID members snapshots

            for i, _vol in enumerate(vol.disks):
                if int(vol.level) in (1, 10) and (i % 2):
                    continue

                description = 'RAID%s disk #%d - %s' % (vol.level, i, snap.description)
                t = threading.Thread(target=snapshot, args=(i, _vol, description, tags))
                t.start()
                threads.append(t)

            for t in threads:
                t.join()

        except:
            e, t = sys.exc_info()[1:]
            raise StorageError, "Error occured during Raid snapshot creation: '%s'" % e, t

        finally:
            self._lvm.resume_lv(vol.device)

        if errors:
            self._logger.debug('Some snapshots failed. Remove successfull snapshots.')
            for _snap in snapshots.itervalues():
                try:
                    _snap.destroy()
                except:
                    e = sys.exc_info()[1]
                    self._logger.debug('Snapshot remove failed: %s' % e)

            raise StorageError('Raid snapshot failed: some of disks failed to snapshot. '
                                               'Original errors: \n%s' % ''.join([str(e) for e in errors]))

        # Sort snapshots by volume index
        snapshots.sort(lambda x,y: cmp(x[0], y[0]))

        for i, _snap in snapshots:
            snap.disks.append(_snap)

            if int(vol.level) in (1, 10) and not (i % 2):
                snap_copy = copy.copy(_snap)
                description = 'RAID%s disk #%d - %s' % (vol.level, i+1, snap.description)
                snap_copy.description = description
                snap.disks.append(snap_copy)

        return snap


    def destroy(self, vol, force=False, **kwargs):
        super(RaidVolumeProvider, self).destroy(vol, force, **kwargs)

        remove_disks=kwargs.get('remove_disks')
        if not vol.detached:
            self._remove_lvm(vol, force)
            # Check if sleeping is necessary
            time.sleep(1)
            self._mdadm.delete(vol.raid_pv)
        if remove_disks:
            if getattr(vol.disks, '__iter__', False):
                for disk in vol.disks:
                    disk.destroy(force=force)



    @devname_not_empty
    def detach(self, vol, force=False):
        self._logger.debug('Detaching volume %s' % vol.devname)
        super(RaidVolumeProvider, self).detach(vol, force)
        pv_uuid = system(('pvs', '-o', 'pv_uuid', vol.raid_pv))[0].splitlines()[1].strip()

        vol.lvm_group_cfg = lvm_group_b64(vol.vg)
        self._remove_lvm(vol)
        self._mdadm.delete(vol.raid_pv, zero_superblock=False)
        for disk in vol.disks:
            disk.detach(force)

        ret = vol.config()
        ret['pv_uuid'] = pv_uuid
        vol.detached = True
        return ret


    def _remove_lvm(self, vol, force=False):
        self._lvm.remove_vg(vol.vg)
        self._lvm.remove_pv(vol.raid_pv)
        vol.device = None


    def replace_disk(self, raid_vol, old, new):
        try:
            self._mdadm.replace_disk(raid_vol.raid_pv, old.device, new.device)
        except:
            e, t = sys.exc_info()[1:]
            try:
                array_devices = self._mdadm.get_array_devices(raid_vol.raid_pv)
                if new.device in array_devices:
                    self.remove_disks(raid_vol, new)
                if not old.device in array_devices:
                    self.add_disks(raid_vol, old)
            except:
                pass
            finally:
                raise StorageError, str(e), t
        else:
            # Delete failed device
            index = raid_vol.disks.index(old)
            raid_vol.disks[index] = new


    def add_disks(self, raid_vol, *disks):
        for disk in disks:
            self._mdadm.add_disk(raid_vol.raid_pv, disk.device)


    def remove_disks(self, raid_vol, *disks):
        for disk in disks:
            self._mdadm.remove_disk(raid_vol.raid_pv, disk.device)


    def status(self, raid_vol):
        return self._mdadm.get_array_info(raid_vol.raid_pv)



Storage.explore_provider(RaidVolumeProvider)
