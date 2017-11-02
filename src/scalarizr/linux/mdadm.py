from __future__ import with_statement

import logging
import re
import os
from scalarizr import linux
from scalarizr.linux import coreutils
from scalarizr.storage2 import StorageError


if not linux.which('mdadm'):
    from scalarizr.linux import pkgmgr
    pkgmgr.installed('mdadm', updatedb=True)

mdadm_binary = linux.which('mdadm')

if not os.path.exists('/proc/mdstat'):
    coreutils.modprobe('md_mod')

LOG = logging.getLogger(__name__)

def mdadm(mode, md_device=None, *devices, **long_kwds):
    """
    Example:
    mdadm.mdadm('create', '/dev/md0', '/dev/loop0', '/dev/loop1',
                            level=0, metadata='default',
                            assume_clean=True, raid_devices=2)
    """
    raise_exc = long_kwds.pop('raise_exc', True)
    return linux.system(linux.build_cmd_args(
                                    mdadm_binary,
                                    ['--%s' % mode] + ([md_device] if md_device else []),
                                    long_kwds, devices), raise_exc=raise_exc)


def mdfind(*devices):
    """ Return md name that contains passed devices """
    devices_base = map(os.path.basename, devices)

    with open('/proc/mdstat') as f:
        stat = f.readlines()
        
    LOG.debug('mdstat: %s', stat)

    for line in stat:
        if all(map(lambda x: x in line, devices_base)):
            array = '/dev/%s' % line.split()[0]
            md_info = detail(array)
            md_devices = md_info['devices'].keys()
            md_devices = map(os.path.realpath, md_devices)
            if sorted(md_devices) == sorted(devices):
                return array
    else:
        raise StorageError(
                "Devices aren't part of any array: %s" % ', '.join(devices))

def findname():
    """ Return unused md device name """
    for i in range(1000):
        dev = '/dev/md%s' % i
        if not os.path.exists(dev):
            return dev

    raise StorageError("No unused raid device name left")

_raid_devices_re        = re.compile('Raid\s+Devices\s+:\s+(?P<count>\d+)')
_total_devices_re       = re.compile('Total\s+Devices\s+:\s+(?P<count>\d+)')
_state_re               = re.compile('State\s+:\s+(?P<state>.+)')
_rebuild_re             = re.compile('Rebuild\s+Status\s+:\s+(?P<percent>\d+)%')
_level_re               = re.compile('Raid Level : (?P<level>.+)')


def version():
    out = mdadm('misc', None, '--version')[1].strip()
    return tuple(map(int, out.split('-')[1].strip()[1:].split('.')))

def detail(md_device):
    """
    Example:
    >> mdadm.detail('/dev/md0')
    >> {
            'version': '1.2',
            'creation_time': 'Tue Sep 11 23:20:21 2012',
            'raid_level':
            ...
            'devices_detail': [{
                    'raiddevice': 0,
                    'state': 'active sync',
                    'device': '/dev/loop0'
            }, ...]
    }
    """
    ret = dict()
    details = mdadm('misc', None, md_device, detail=True)[0]

    disk_stats = re.findall('([a-zA-Z\s]+/dev/[\w]+)\n', details)
    ret['devices'] = {}
    for stat in disk_stats:
        status, devname = stat.rsplit(None, 1)
        status = status.strip()
        ret['devices'][devname] = status

    ret['raid_devices'] = int(re.search(
                                    _raid_devices_re, details).group('count'))
    ret['total_devices'] = int(re.search(
                                    _total_devices_re, details).group('count'))
    ret['state'] = re.search(_state_re, details).group('state')

    level = re.search(_level_re, details).group('level')
    if level.startswith('raid'):
        level = level[4:]
    ret['level'] = level

    rebuild_res = re.search(_rebuild_re, details)
    ret['rebuild_status'] = int(rebuild_res.group('percent')) if rebuild_res \
                                                    else None

    return ret
