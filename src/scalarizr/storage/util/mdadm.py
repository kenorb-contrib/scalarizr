from __future__ import with_statement
'''
Created on Nov 11, 2010

@author: spike
@author: marat
'''

import logging
import os
import re
import time

from scalarizr.linux import pkgmgr
from scalarizr.util import system2
from scalarizr.util import wait_until
from scalarizr.util import firstmatched
from scalarizr.util import PopenError
from scalarizr import linux


MDADM_EXEC='/sbin/mdadm'
logger = logging.getLogger(__name__)

class MdadmError(PopenError):
    pass

def system(*popenargs, **kwargs):
    kwargs['logger'] = logger
    kwargs['exc_class'] = MdadmError
    return system2(*popenargs, **kwargs)

class Mdadm:


    def __init__(self):
        if not os.path.exists(MDADM_EXEC):
            if linux.os.redhat_family:
                system2(('/usr/bin/yum', '-d0', '-y', 'install', 'mdadm', '-x', 'exim'), raise_exc=False)
            else:
                pkgmgr.installed('mdadm')
        for location in ['/etc ', '/lib']:
            path = os.path.join(location, 'udev/rules.d/85-mdadm.rules')
            if os.path.exists(path):

                rule = None
                with open(path, 'r') as fp:
                    rule = fp.read()

                if rule:
                    rule = re.sub(re.compile('^([^#])', re.M), '#\\1', rule)
                    with open(path, 'w') as fp:
                        fp.write(rule)

        self._raid_devices_re   = re.compile('Raid\s+Devices\s+:\s+(?P<count>\d+)')
        self._total_devices_re  = re.compile('Total\s+Devices\s+:\s+(?P<count>\d+)')
        self._state_re          = re.compile('State\s+:\s+(?P<state>.+)')
        self._rebuild_re        = re.compile('Rebuild\s+Status\s+:\s+(?P<percent>\d+)%')
        self._level_re                  = re.compile('Raid Level : (?P<level>.+)')


    def _get_existed_array_name(self, devices, level=None):
        array = None
        for device in devices:
            try:
                array = self._get_array_by_device(device)
                break
            except:
                pass

        if array:
            array_disks = self.get_array_devices(array)
            array_level = int(self.get_array_info(array)['level'])
            if sorted(array_disks) == sorted(devices) and (
                            level is None or int(level) == array_level):
                return array


    def create(self, devices, level=1):
        # Validate RAID level
        if not int(level) in (0,1,5,10):
            raise MdadmError('Unknown RAID level: %s' % level)

        existed = self._get_existed_array_name(devices, level)
        if existed:
            return existed

        # Select RAID device name
        devname = self._get_free_md_devname()
        for device in devices:
            try:
                self._zero_superblock(device)
            except:
                pass

        # Create RAID device
        cmd = [MDADM_EXEC, '--create', devname, '--level=%s' % level, '--assume-clean', '-f', '-e', 'default', '-n', len(devices)]
        cmd.extend(devices)
        system(cmd, error_text='Error occured during raid device creation')
        self._wait(devname)

        return devname


    def delete(self, array, zero_superblock=True):
        if not os.path.exists(array):
            raise MdadmError('Device %s does not exist' % array)

        # Stop raid
        devices = self.get_array_devices(array)
        self._wait(array)
        cmd = (MDADM_EXEC, '-S', '-f', array)
        try:
            system(cmd, error_text='Error occured during array stopping')
        except (Exception, BaseException), e:
            if not 'Device or resource busy' in str(e):
                raise
            time.sleep(5)
            system(cmd, error_text='Error occured during array stopping')

        # Delete raid
        try:
            cmd = (MDADM_EXEC, '--remove', '-f', array)
            system(cmd, error_text='Error occured during array deletion')
        except (Exception, BaseException), e:
            if not 'No such file or directory' in str(e):
                raise

        system(('rm', '-f', array))

        if zero_superblock:
            for device in devices:
                self._zero_superblock(device)


    def assemble(self, devices):
        existed = self._get_existed_array_name(devices)
        if existed:
            return existed
        md_devname = self._get_free_md_devname()
        cmd = (MDADM_EXEC, '--assemble', md_devname) + tuple(devices)
        system(cmd, error_text="Error occured during array assembling")
        self._wait(md_devname)
        return md_devname


    def add_disk(self, array, device, grow=True):
        info = self.get_array_info(array)
        if info['level'] in ('raid0', 'raid10'):
            raise MdadmError("Can't add devices to %s." % info['level'])

        self._wait(array)
        cmd = (MDADM_EXEC, '--add', array, device)
        system(cmd, error_text='Error occured during device addition')

        if grow:
            array_info = self.get_array_info(array)
            raid_devs = array_info['raid_devices']
            total_devs = array_info['total_devices']

            if total_devs > raid_devs:
                cmd = (MDADM_EXEC, '--grow', array, '--raid-devices=%s' % total_devs)
                system(cmd, error_text='Error occured during array "%s" growth')

        self._wait(array)


    def remove_disk(self, array, device):
        array_disks = self.get_array_devices(array)

        if not device in array_disks:
            raise MdadmError('Disk %s is not part of %s array' % (device, array))

        self._wait(array)

        cmd = (MDADM_EXEC, array, '-f', '--fail', device)
        system(cmd, error_text='Error occured while marking device as failed')

        cmd = (MDADM_EXEC, array, '-f', '--remove', device)
        system(cmd, error_text='Error occured during device removal')


    def replace_disk(self, array, old, new):
        if self.get_array_info(array)['level'] == 'raid0':
            raise MdadmError("Can't replace disk in raid level 0.")

        self.remove_disk(array, old)
        self.add_disk(array, new, False)
        self._wait(array)


    def get_array_info(self, array):
        ret = {}
        details = self._get_array_details(array)

        disk_stats = re.findall('([a-zA-Z\s]+/dev/[\w]+)\n', details)
        ret['devices'] = {}
        for stat in disk_stats:
            status, devname = stat.rsplit(None, 1)
            status = status.strip()
            ret['devices'][devname] = status


        ret['raid_devices']   = int(re.search(self._raid_devices_re, details).group('count'))
        ret['total_devices']  = int(re.search(self._total_devices_re, details).group('count'))
        ret['state']              = re.search(self._state_re, details).group('state')
        level                             = re.search(self._level_re, details).group('level')
        if level.startswith('raid'):
            level = level[4:]
        ret['level']              = level
        rebuild_res               = re.search(self._rebuild_re, details)
        ret['rebuild_status'] = rebuild_res.group('percent') if rebuild_res else None
        return ret


    def _get_array_by_device(self, device):
        devname = os.path.basename(device)
        out = None
        with open('/proc/mdstat', 'r') as fp:
            out = fp.read()

        if not out:
            raise Exception("Can't get array info from /proc/mdstat.")

        for line in out.splitlines():
            if devname in line:
                array = line.split()[0]
                break
        else:
            raise Exception("Device %s isn't part of any array." % device)

        return '/dev/%s' % array


    def _zero_superblock(self, device):
        devname = os.path.basename(device)
        cmd = (MDADM_EXEC, '--zero-superblock', '-f', '/dev/%s' % devname)
        system(cmd, error_text='Error occured during zeroing superblock on %s' % device)


    def _get_free_md_devname(self):
        return '/dev/md%s' % firstmatched(lambda x: not os.path.exists('/dev/md%s' % x), range(100))


    def get_array_devices(self, array):
        details = self._get_array_details(array)
        return re.findall('(/dev/[\w]+)\n', details)


    def _get_array_details(self, array):
        cmd = (MDADM_EXEC, '-D', array)
        error_text = 'Error occured while obtaining array %s info' % array
        return system(cmd, error_text=error_text)[0]


    def _wait(self, array):
        """ Wait for array to finish any resync, recover or reshape activity """
        system2((MDADM_EXEC, '-W', array), raise_error=False)
