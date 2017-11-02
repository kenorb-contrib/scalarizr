from __future__ import with_statement
'''
Created on Aug 22, 2012

@author: marat
'''
import os
import itertools

from scalarizr import storage2, util
from scalarizr.storage2.volumes import base
from scalarizr.linux import lvm2, coreutils


class LvmVolume(base.Volume):

    def __init__(self,
                            pvs=None,
                            vg=None,
                            name=None,
                            size=None,
                            **kwds):
        '''
        :type pvs: list
        :param pvs: Physical volumes

        :type vg: string
        :param vg: Volume group name

        :type name: string
        :param name: Logical volume name

        :type size: int or string
        :param size: Logical volume size <int>[bBsSkKmMgGtTpPeE]
                or %{VG|PVS|FREE|ORIGIN}
        '''
        super(LvmVolume, self).__init__(pvs=pvs or [], vg=vg, name=name,
                        size=size, **kwds)
        self.features['restore'] = True

    def _lvinfo(self):
        return lvm2.lvs(lvm2.lvpath(self.vg, self.name)).values()[0]


    def _ensure(self):
        def get_lv_size_kwarg(size):
            kwd = dict()
            if '%' in str(size):
                kwd['extents'] = size
            else:
                try:
                    int(size)
                    kwd['size'] = '%sG' % size
                except:
                    kwd['size'] = size
            return kwd

        if self.snap:
            pvs = []
            try:
                for snap in self.snap['pv_snaps']:
                    snap = storage2.snapshot(snap)
                    vol = storage2.volume(type=snap.type, snap=snap)
                    vol.ensure()
                    pvs.append(vol)
            except:
                for pv in pvs:
                    pv.destroy()
                raise
            self.pvs = pvs
            self.vg = self.snap['vg']
            self.name = self.snap['name']

        pv_volumes = []
        for pv_volume in self.pvs:
            pv_volume = storage2.volume(pv_volume)
            pv_volume.ensure()

            pvs = lvm2.pvs()
            if pv_volume.device not in pvs:
                if pv_volume.mounted_to():
                    pv_volume.umount()
                lvm2.pvcreate(pv_volume.device)
            pv_volumes.append(pv_volume)
        self.pvs = pv_volumes

        self._check_attr('vg')
        try:
            lv_info = self._lvinfo()
        except lvm2.NotFound:
            self._check_attr('size')

            try:
                lvm2.vgs(self.vg)
            except lvm2.NotFound:
                lvm2.vgcreate(self.vg, *[disk.device for disk in self.pvs])

            kwds = {'name': self.name}
            kwds.update(get_lv_size_kwarg(self.size))

            lvm2.lvcreate(self.vg, **kwds)
            lv_info = self._lvinfo()

        self._config.update({
                'device': lv_info.lv_path,
                'snap': None
        })

        pvs_to_extend_vg = []
        for pv in self.pvs:
            pv_info = lvm2.pvs(pv.device).popitem()[1]

            if not pv_info.vg_name:
                pvs_to_extend_vg.append(pv_info.pv_name)
                continue

            if os.path.basename(self.vg) != pv_info.vg_name:
                raise storage2.StorageError(
                        'Can not add physical volume %s to volume group %s: already'
                        ' in volume group %s' %
                        (pv_info.pv_name, self.vg, pv_info.vg_name))

        if pvs_to_extend_vg:
            lvm2.vgextend(self.vg, *pvs_to_extend_vg)
            lvm2.lvextend(self.device, **get_lv_size_kwarg(self.size))
            if self.is_fs_created():
                fs = storage2.filesystem(self.fstype)
                if fs.features.get('resizable'):
                    fs.resize(self.device)

        if lv_info.lv_attr[4] == '-':
            lvm2.lvchange(self.device, available='y')
            util.wait_until(
                    lambda: os.path.exists(self.device), sleep=1, timeout=30,
                    start_text='Waiting for device %s' % self.device,
                    error_text='Device %s not available' % self.device
            )


    def lvm_snapshot(self, name=None, size=None):
        long_kwds = {
                'name': name or '%ssnap' % self.name,
                'snapshot': '%s/%s' % (self.vg, self.name)
        }
        if size:
            size=str(size)
            if '%' in size:
                long_kwds['extents'] = size
            else:
                long_kwds['size'] = size
        else:
            long_kwds['extents'] = '1%ORIGIN'

        lvol = '%s/%s' % (self.vg, long_kwds['name'])
        if lvol in lvm2.lvs():
            lvm2.lvremove(lvol)
        lvm2.lvcreate(**long_kwds)
        lv_info = lvm2.lvs(lvol).values()[0]

        return storage2.snapshot(
                        type='lvm_native',
                        name=lv_info.lv_name,
                        vg=lv_info.vg_name,
                        device=lv_info.lv_path)


    def _snapshot(self, description, tags, **kwds):
        active = os.path.exists(self.device)
        if active:
            coreutils.dmsetup('suspend', self.device)
        try:
            if not description:
                description = self.id
            description += ' PV-${index}'
            pv_snaps = storage2.concurrent_snapshot(self.pvs,
                                                            description, tags, **kwds)
            return storage2.snapshot(
                            type='lvm',
                            pv_snaps=pv_snaps,
                            vg=self.vg,
                            name=self.name,
                            size=self.size)
        finally:
            if active:
                coreutils.dmsetup('resume', self.device)


    def _detach(self, force, **kwds):
        lvm2.lvchange(self.device, available='n')


    def _destroy(self, force, **kwds):
        try:
            lvm2.lvremove(self.device)
        except lvm2.NotFound:
            pass

        if force:
            try:
                vg_info = lvm2.vgs(self.vg).values()[0]
            except lvm2.NotFound:
                pass
            else:
                if not (int(vg_info.snap_count) and not int(vg_info.lv_count)):
                    pv_disks = [device for device, pv_info in lvm2.pvs().items()
                                            if pv_info.vg_name == self.vg]
                    lvm2.vgremove(self.vg)
                    for device in pv_disks:
                        lvm2.pvremove(device)

                    for pv in self.pvs:
                        pv.destroy(force=True)



    def _clone(self, config):
        config['pvs'] = [storage2.volume(pv).clone() for pv in config['pvs']]


class LvmNativeSnapshot(base.Snapshot):
    def _destroy(self):
        lvm2.lvremove(self.device)


    def _status(self):
        try:
            lvm2.lvs(self.device)
            return self.COMPLETED
        except lvm2.NotFound:
            return self.FAILED


class LvmSnapshot(base.Snapshot):
    def _destroy(self):
        for snap in self.pv_snaps:
            if isinstance(snap, dict):
                snap = storage2.snapshot(**snap)
            snap.destroy()


    def _status(self):
        if all((snap.status() == self.COMPLETED for snap in self.pv_snaps)):
            return self.COMPLETED
        elif any((snap.status() == self.FAILED for snap in self.pv_snaps)):
            return self.FAILED
        return self.UNKNOWN


storage2.volume_types['lvm'] = LvmVolume
storage2.snapshot_types['lvm'] = LvmSnapshot
storage2.snapshot_types['lvm_native'] = LvmNativeSnapshot
