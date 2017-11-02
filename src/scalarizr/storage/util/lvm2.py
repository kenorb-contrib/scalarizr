from __future__ import with_statement
'''
Created on Nov 11, 2010

@author: Dmytro Korsakov
@author: marat
'''
from __future__ import with_statement

import re
import os
import time
import random
import logging
import binascii

from scalarizr.util import wait_until

try:
    from collections import namedtuple
except ImportError:
    from scalarizr.externals.collections import namedtuple

from scalarizr.util import system2, firstmatched, PopenError
from scalarizr.util.software import which
from scalarizr.linux import coreutils, pkgmgr
from scalarizr.storage import StorageError


logger = logging.getLogger(__name__)


class Lvm2Error(PopenError):
    pass

if not os.path.exists('/sbin/pvs'):
    pkgmgr.installed('lvm2')


try:
    PVS = which('pvs')
    VGS = which('vgs')
    LVS = which('lvs')

    PVSCAN = which('pvscan')
    PVCREATE = which('pvcreate')
    VGCREATE = which('vgcreate')
    LVCREATE = which('lvcreate')

    LVCHANGE = which('lvchange')
    VGCHANGE = which('vgchange')
    VGEXTEND = which('vgextend')
    VGREDUCE = which('vgreduce')
    VGCFGRESTORE = which('vgcfgrestore')

    PVREMOVE = which('pvremove')
    VGREMOVE = which('vgremove')
    LVREMOVE = which('lvremove')
    DMSETUP  = which('dmsetup')

except LookupError:
    raise Lvm2Error('Some of lvm2 executables were not found.')

def system(*args, **kwargs):
    kwargs['logger'] = logger
    kwargs['close_fds'] = True
    '''
    To prevent this garbage in stderr (Fedora/CentOS):
    File descriptor 6 (/tmp/ffik4yjng (deleted)) leaked on lv* invocation.
    Parent PID 29542: /usr/bin/python
    '''
    kwargs['exc_class'] = Lvm2Error
    return system2(*args, **kwargs)

class PVInfo(namedtuple('PVInfo', 'pv vg format attr size free uuid')):
    COMMAND = (PVS, '-o', 'pv_name,vg_name,pv_fmt,pv_attr,pv_size,pv_free,pv_uuid')
    pass

class VGInfo(namedtuple('VGInfo', 'vg num_pv num_lv num_sn attr size free')):
    COMMAND = (VGS,)
    @property
    def path(self):
        return '/dev/%s' % (self[0],)

_columns = ('vg_name','lv_uuid','lv_name','lv_attr',
                        'lv_major','lv_minor','lv_read_ahead',
                        'lv_kernel_major','lv_kernel_minor','lv_kernel_read_ahead',
                        'lv_size','seg_count','origin','origin_size','snap_percent','copy_percent',
                        'move_pv','convert_lv','lv_tags','mirror_log','modules')
class LVInfo(namedtuple('LVInfo', ' '.join(_columns))):
    COMMAND = (LVS, '-o', ','.join(_columns))
    @property
    def lv_path(self):
        return lvpath(self.vg_name, self.lv_name)
    path = lv_path
del _columns



def lvpath(group, lvol):
    return '/dev/mapper/%s-%s' % (group.replace('-', '--'), lvol.replace('-', '--'))

def extract_vg_lvol(lvolume):
    '''
    Return (vg, lvol) from logical device name
    Example:
            /dev/mapper/vg0-vol0 -> ('vg0', 'vol0')
            /dev/mapper/my--volume--group-data -> ('my-volume-group' ,'data')
    '''
    vg_lvol = os.path.basename(lvolume).split('-')
    if len(vg_lvol) > 2:
        ret = []
        for s in vg_lvol:
            if len(ret) and not ret[-1][-1] or not s:
                ret[-1].append(s)
            else:
                ret.append([s])
        vg_lvol = map(lambda x: '-'.join(filter(None, x)), ret)
    return tuple(vg_lvol)

def normalize_lvname(lvolume):
    if '/dev/mapper' in lvolume:
        return '/dev/%s/%s' % extract_vg_lvol(lvolume)
    else:
        return lvolume


def lvm_group_b64(vg):
    vgfile = '/etc/lvm/backup/%s' % os.path.basename(vg)
    if os.path.exists(vgfile):
        file_content = None
        with open(vgfile, 'r') as fp:
            file_content = fp.read()
        return binascii.b2a_base64(file_content)


class Lvm2:
    '''
    Object-oriented interface to lvm2
    '''

    _usable = None

    @staticmethod
    def usable():
        if Lvm2._usable is None:
            Lvm2._usable = False
            try:
                coreutils.modprobe('dm_snapshot')
                coreutils.modprobe('dm_mod')
                Lvm2._usable = True
            except:
                try:
                    import platform
                    release = platform.release()
                    kernel_config_path = "/boot/config-" + release
                    if os.path.isfile(kernel_config_path):

                        with open(kernel_config_path) as f:
                            kernel_config = f.readlines()

                        drivers_compiled = dict(CONFIG_BLK_DEV_DM=False,
                                                                        CONFIG_DM_SNAPSHOT=False)

                        for line in kernel_config:
                            for driver_name in drivers_compiled.keys():
                                if line.startswith(driver_name):
                                    drivers_compiled[driver_name] = line.strip().split('=')[1] == 'y'

                        if all(drivers_compiled.values()):
                            Lvm2._usable = True
                except:
                    pass

        return Lvm2._usable

    def __init__(self):
        if not Lvm2.usable():
            raise StorageError('LVM2 is not usable. Please check that kernel compiled with dm_mod, dm_snapshot')

    def _parse_status_table(self, cmd, ResultClass):
        if isinstance(ResultClass, tuple):
            raise ValueError('ResultClass should be a namedtuple subclass. %s taken' % type(ResultClass))
        args = list(cmd)
        args += ('--separator', '|')
        out = system(args)[0].strip()
        if out:
            return tuple(ResultClass(*line.strip().split('|')) for line in out.split('\n')[1:])
        return ()

    def _status(self, cmd, ResultClass, column=None):
        rows = self._parse_status_table(cmd, ResultClass)
        if column:
            return tuple(getattr(o, column) for o in rows)
        return rows

    def pv_status(self, column=None):
        return self._status(PVInfo.COMMAND, PVInfo, column)

    def vg_status(self, column=None):
        return self._status(VGInfo.COMMAND, VGInfo, column)

    def lv_status(self, column=None):
        return self._status(LVInfo.COMMAND, LVInfo, column)

    def pv_info(self, ph_volume):
        info = firstmatched(lambda inf: inf.pv == ph_volume, self.pv_status())
        if info:
            return info
        raise LookupError('Physical volume %s not found' % ph_volume)

    def pv_scan(self):
        system((PVSCAN,), error_text='Physical volumes scan failed')

    def vg_info(self, group):
        group = os.path.basename(group)
        info = firstmatched(lambda inf: inf.vg == group, self.vg_status())
        if info:
            return info
        raise LookupError('Volume group %s not found' % group)

    def lv_info(self, lvolume=None, group=None, name=None):
        lvolume = lvolume if lvolume else lvpath(group, name)
        info = firstmatched(lambda inf: inf.lv_path == lvolume, self.lv_status())
        if info:
            return info
        raise LookupError('Logical volume %s not found' % lvolume)


    def create_pv(self, device, uuid=None):
        cmd = [PVCREATE]
        if uuid is not None:
            cmd += ['-u', uuid]
        cmd.append(device)
        system(cmd, error_text='Cannot initiate a disk for use by LVM')

    def create_vg(self, group, ph_volumes, ph_extent_size=4):
        group = os.path.basename(group)
        system([VGCREATE, '-s', ph_extent_size, group] + list(ph_volumes),
                        error_text='Cannot create a volume group %s' % group)
        return '/dev/%s' % group

    def create_lv(self, group=None, name=None, extents=None, size=None, segment_type=None, ph_volumes=None):
        args = [LVCREATE]
        if name:
            args += ('-n', name)
        if extents:
            args += ('-l', extents)
        elif size:
            args += ('-L', size)
        if segment_type:
            args += ('--type=' + segment_type,)
        if group and segment_type != 'snapshot':
            args.append(group)

        if ph_volumes:
            args += ph_volumes

        out, err, ret_code = system(args, raise_exc=False)
        out = out.strip()
        if not ret_code:
            vol = re.match(r'Logical volume "([^\"]+)" created', out.split('\n')[-1].strip()).group(1)
            if not vol:
                raise Lvm2Error('Cannot create logical volume: %s' % err)

        elif ret_code == 5:
            logger.debug('Lvcreate exited with non-zero code. Trying to find '
                                    'target device manually')

            device_to_find = lvpath(os.path.basename(group), name)
            if not os.path.exists(device_to_find):
                raise Lvm2Error("Couldn't create logical volume %s: %s" % (device_to_find, err))
            return device_to_find

        else:
            raise Lvm2Error('Cannot create logical volume: %s' % err)

        device_path = lvpath(os.path.basename(group), vol)
        wait_until(lambda: os.path.exists(device_path), timeout=30)
        return device_path



    def create_lv_snapshot(self, lvolume, name=None, extents=None, size=None):
        vg = extract_vg_lvol(lvolume)[0]
        return self.create_lv(vg, name, extents, size, segment_type='snapshot', ph_volumes=(normalize_lvname(lvolume),))


    def change_lv(self, lvolume, available=None):
        cmd = [LVCHANGE]
        if available is not None:
            cmd.append('-ay' if available else '-an')
        cmd.append(normalize_lvname(lvolume))
        system(cmd, error_text='Cannot change logical volume attributes')

    def remove_pv(self, ph_volume):
        vg_of_ph_volume = self.pv_info(ph_volume).vg
        if vg_of_ph_volume:
            system((VGREDUCE, '-f', vg_of_ph_volume, ph_volume), error_text='Cannot reduce volume group')
        system((PVREMOVE, '-ff', ph_volume), error_text='Cannot remove a physical volume')

    def remove_vg(self, group):
        system((VGREMOVE, '-ff', group), error_text='Cannot remove volume group')

    def remove_lv(self, lvolume):
        vol = '%s/%s' % extract_vg_lvol(lvolume)
        # test that volume could be removed
        # see https://bugzilla.redhat.com/show_bug.cgi?id=570359
        for _ in range(0, 3):
            if not system((LVREMOVE, '--test', '--force', vol), raise_exc=False)[2]:
                break
            time.sleep(1)
        system((LVREMOVE, '--force', vol), error_text='Cannot remove logical volume')

        # On GCE lvremove finishes with the following stderr:
        #
        #   The link /dev/<GroupName>/<VolumeName> should have been removed by udev but it is still present.
        #   Falling back to direct link removal.
        #
        # After that snapshot cow is still presented and prevents new snapshots creation
        cow = '%s-%s-cow' % extract_vg_lvol(lvolume)
        if os.path.exists('/dev/mapper/%s' % cow):
            system((DMSETUP, 'remove', cow))


    def extend_vg(self, group, *ph_volumes):
        system([VGEXTEND, group] + list(ph_volumes), error_text='Cannot extend volume group')

    def repair_vg(self, group):
        system((VGREDUCE, '--removemissing', group))
        system((VGCHANGE, '-a', 'y', group))

    def restore_vg(self, group, backup_file):
        rmfile = False
        if hasattr(backup_file, 'read'):
            # File-like object
            fpi = backup_file
            backup_file = '/tmp/lvmvg-%s' % random.randint(100, 999)
            fpo = open(backup_file, 'w+')
            try:
                fpo.write(fpi.read())
            finally:
                fpo.close()
            rmfile = True

        try:
            cmd = ((VGCFGRESTORE, '-f', backup_file, group))
            system(cmd, error_text='Cannot restore volume group %s from backup file %s' % (group, backup_file))
        finally:
            if rmfile:
                os.remove(backup_file)

    def change_vg(self, group, available=None):
        cmd = [VGCHANGE]
        if available is not None:
            cmd.append('-ay' if available else '-an')
        cmd.append(group)
        system(cmd, error_text='Cannot volume group attributes')


    def suspend_lv(self, lv):
        system2((DMSETUP, 'suspend', lv))


    def resume_lv(self, lv):
        system2((DMSETUP, 'resume', lv))


    # Untested --->

    def get_lv_size(self, lv_name):
        lv_info = self.get_logic_volumes_info()
        if lv_info:
            for lv in lv_info:
                if lv[0] == lv_name:
                    return lv[3]
        return 0

    def get_vg_free_space(self, group=None):
        '''
        @return tuple('amount','suffix')
        '''
        if not group: group = self.group
        for group_name in self.get_vg_info():
            if group_name[0]==group:
                raw = re.search('(\d+\.*\d*)(\D*)',group_name[-1])
                if raw:
                    return (raw.group(1), raw.group(2))
                raise Lvm2Error('Cannot determine available free space in group %s' % group)
        raise Lvm2Error('Group %s not found' % group)
