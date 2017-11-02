from __future__ import with_statement
'''
Created on Aug 28, 2012

@author: marat
'''

import os
import glob
import shutil
import logging
from math import ceil

from scalarizr import linux
from scalarizr.linux import os as os_info

try:
    from collections import namedtuple
except ImportError:
    from scalarizr.externals.collections import namedtuple

def sync():
    return linux.system(('/bin/sync', ))


def dd(**kwds):
    short = []
    for k, v in kwds.items():
        short.append('%s=%s' % (k, v))
    return linux.system(linux.build_cmd_args(
                executable='/bin/dd',
                short=short))


def sfdisk():
    raise NotImplementedError()


def modprobe(module_name, **long_kwds):
    if not os_info['mods_enabled']:
        return (None, None, 0)

    return linux.system(linux.build_cmd_args(
                executable='/sbin/modprobe', 
                long=long_kwds,
                params=[module_name]), 
            error_text='Kernel module %s is not available' % module_name)
    

def dmsetup(action, *params, **long_kwds):
    if not os.path.exists('/sbin/dmsetup'):
        from scalarizr.linux import pkgmgr
        if linux.os.debian_family:
            package = 'dmsetup'
        else:
            package = 'device-mapper'
        pkgmgr.installed(package)

    return linux.system(linux.build_cmd_args(
            executable='/sbin/dmsetup', 
            short=[action], 
            long=long_kwds, 
            params=params)) 
    
    
def losetup(*args, **long_kwds):
    return linux.system(linux.build_cmd_args(
                executable='/sbin/losetup', 
                long=long_kwds, 
                params=args))

    
def losetup_all(flip=False):
    '''
    Alias to losetup --all with parsing output into python dict.
    When flip=True filenames becomes keys and devices are values
    Example:
        {'/dev/loop0': '/mnt/loop0',
        '/dev/loop1': '/mnt/loop1',
        '/dev/loop2': '/mnt/loop2'}
    '''
    ret = list()
    if os_info['family'] == 'RedHat' and os_info['version'] < (6, 0):
        out = losetup('-a')[0].strip()
    else:
        out = losetup(all=True)[0].strip()
    for line in out.splitlines():
        cols = line.split()
        device = cols[0][:-1]
        filename = cols[-1][1:-1]
        ret.append((device, filename) if not flip else (filename, device))
    return dict(ret)


def touch(filename):
    open(filename, "w+").close()


def chown_r(path, owner, group=None):
    return linux.system(linux.build_cmd_args(
                executable='/bin/chown', 
                long={'recursive': True}, 
                params=[owner if not group else owner + ':' + group, path]))    

def chmod_r(path, mode):
    if os.path.isdir(path):
        for root, dirs, files in os.walk(path):
            for f in dirs + files:
                os.chmod(os.path.join(root, f), mode)
    else:
        os.chmod(path, mode)


def remove(path):
    if os.path.exists(path):
        if os.path.isdir(path):
            shutil.rmtree(path)
        else: 
            os.remove(path) 


def clean_dir(path, recursive=True):
    if not os.path.isdir(path):
        raise Exception('No such directory: %s' % path)

    content = glob.glob(os.path.join(path, '*'))
    for item in content:
        if recursive or (not recursive and os.path.isfile(item)):
            remove(item)


def mkdir(path, mode=0777):
    if not os.path.exists(path):
        os.makedirs(path, mode)


def blkid(device_path, **kwargs):
    if not os.path.exists(device_path):
        raise Exception("Device %s doesn't exist")

    ret = dict()

    args = ['/sbin/blkid']
    for k, v in kwargs.items():
        if type(v) == bool:
            args.append('-%s' % k)
        else:
            args.extend(['-%s' % k, str(v)])

    args.append(device_path)

    out = linux.system(args, raise_exc=False)[0]
    if out.strip():
        pairs = out.split()[1:]
        for line in pairs:
            line = line.strip()
            if line:
                k, v = line.split('=', 1)
                ret[k.lower()] = v[1:-1]

    return ret


BUFFER_SIZE = 1024 * 1024   # Buffer size in bytes.
PART_SUFFIX = '.part.'  


def _write_chunk(source_file, chunk_file, chunk_size):
    bytes_written = 0  # Bytes written.
    bytes_left = chunk_size # Bytes left to write in this chunk.
    
    while bytes_left > 0:
        size = BUFFER_SIZE if BUFFER_SIZE < bytes_left else bytes_left
        buf = source_file.read(size)
        chunk_file.write(buf)
        bytes_written += len(buf)
        bytes_left = chunk_size - bytes_written
        if len(buf) < size:
            bytes_left = 0 # EOF


def split(filename, part_name_prefix, chunk_size, dest_dir):
    logger = logging.getLogger(__name__)
    f = None
    try:
        try:
            f = open(filename, "rb")
        except (OSError, IOError):
            logger.error("Cannot open file to split '%s'", filename)
            raise
        
        # Create the part file upfront to catch any creation/access errors
        # before writing out data.
        num_parts = int(ceil(float(os.path.getsize(filename))/chunk_size))
        part_names = []
        logger.debug("Splitting file '%s' into %d chunks", filename, num_parts)
        for i in range(num_parts):
            part_name_suffix = PART_SUFFIX + str(i).rjust(2, "0")
            part_name = part_name_prefix + part_name_suffix
            part_names.append(part_name)
            
            part_filename = os.path.join(dest_dir, part_name)
            try:
                touch(part_filename)
            except OSError:
                logger.error("Cannot create part file '%s'", part_filename)
                raise
                    
        # Write parts to files.
        for part_name in part_names:
            part_filename = os.path.join(dest_dir, part_name)
            cf = open(part_filename, "wb")
            try:
                logger.debug("Writing chunk '%s'", part_filename)
                _write_chunk(f, cf, chunk_size)
            except OSError:
                logger.error("Cannot write chunk file '%s'", part_filename)
                raise
            
        return part_names
    finally:
        if f is not None:
            f.close()


def truncate(filename):
    f = open(filename, "w+")
    f.truncate(0)
    f.close()


def statvfs(path='/'):
    statvfs_result = os.statvfs(path)
    result = dict()
    result['size'] = statvfs_result.f_bsize * statvfs_result.f_blocks
    free = statvfs_result.f_bsize * statvfs_result.f_bfree
    result['used'] = result['size'] - free
    result['avail'] = statvfs_result.f_bsize * statvfs_result.f_bavail
    return result

    
_dfrow = namedtuple('df', 'device size mpoint')

def _parse_int(val):
    try:
        return int(val)
    except ValueError:
        return 0

def df():
    out = linux.system(('df', '-Pk'))[0]
    return [_dfrow(line[0], _parse_int(line[1]), line[-1]) 
            for line in map(str.split, out.splitlines()[1:])]


def lsscsi():
    out = linux.system(('lsscsi', ))[0]
    ret = {}
    for line in out.splitlines():
        parts  = filter(None, line.split(' '))
        target, device = parts[0], parts[-1]
        host, bus, tgt, lun = target[1:-1].split(':')
        ret[device] = {
            'host': host,
            'bus': bus,
            'target': tgt,
            'lun': lun,
            'device': device
        }
    return ret


