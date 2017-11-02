'''
Created on Aug 27, 2012

@author: marat
'''

import re
import os
import collections
import itertools

from scalarizr import linux


class NoFileSystem(linux.LinuxError):
    pass


_MountEntry = collections.namedtuple('_MountEntry', 
                'device mpoint fstype options dump fsck_order')


class _Mounts(object):
    filename = '/proc/mounts'   
    _entries = None
    _entry_re = None
    
    def __init__(self, filename=None):
        if filename:
            self.filename = filename
        self._entry_re = re.compile("\\s+")
        self._reload()

    def _reload(self):
        self._entries = []
        for line in open(self.filename):
            if line[0] != "#":
                m = filter(None, self._entry_re.split(line))
                if m:
                    m.extend(itertools.repeat('', 6-len(m)))
                    if os.path.islink(m[0]):
                        m[0] = os.path.realpath(m[0])
                    self._entries.append(_MountEntry(*m))

    def __getitem__(self, device_or_mpoint):
        matched = [entry for entry in self 
                    if self._entry_matches(entry, device_or_mpoint)]
        if matched:
            return matched[0]       
        raise KeyError(device_or_mpoint)

    def __contains__(self, device_or_mpoint):
        return any([self._entry_matches(entry, device_or_mpoint)
                    for entry in self])

    def __len__(self):
        self._reload()
        return len(self._entries)

    def __iter__(self):
        self._reload()
        return iter(self._entries)

    list_entries = __iter__

    def __delitem__(self, device_or_mpoint):
        tmpname = self.filename + '.tmp'
        with open(tmpname, 'w+') as fp:
            for entry in self:
                if not self._entry_matches(entry, device_or_mpoint):
                    self._write_entry(entry, fp)
        os.rename(tmpname, self.filename)

    remove = __delitem__

    def __setitem__(self, device, entry):
        pass    

    def add(self, device, mpoint, fstype, options='auto', dump=0, fsck_order=0):
        with open(self.filename, 'a+') as fp:
            self._write_entry(_MountEntry(device, mpoint, fstype, options, dump, fsck_order), fp)

    def _entry_matches(self, entry, device_or_mpoint):
        return entry.device == device_or_mpoint or \
                entry.mpoint == device_or_mpoint

    def _write_entry(self, entry, fp):
        line = ' '.join(map(str, entry))
        fp.write('\n')
        fp.write(line)


class _Fstab(_Mounts):
    filename = '/etc/fstab'
        

def mounts(filename=None):
    return _Mounts(filename)


def fstab(filename=None):
    return _Fstab(filename) 


def mount(device, mpoint, *short_args, **long_kwds):
    args = linux.build_cmd_args(
        executable='/bin/mount',
        short=short_args,
        long=long_kwds, 
        params=(device, mpoint)
    )
    if not os.path.exists(mpoint):
        os.makedirs(mpoint)
    try:
        msg = 'Cannot mount %s -> %s' % (device, mpoint)
        return linux.system(args, error_text=msg)
    except linux.LinuxError, e:
        if 'you must specify the filesystem type' in e.err:
            raise NoFileSystem(device)
        raise


def umount(device_or_mpoint, **long_kwds):
    args = linux.build_cmd_args(
            executable='/bin/umount',
            short=('-f', device_or_mpoint),
            long=long_kwds)
    try:
        linux.system(args, error_text='Cannot umount %s' % device_or_mpoint)
    except linux.LinuxError, e:
        if 'not mounted' in e.err or 'not found' in e.err:
            return
        raise

class MountError(BaseException):
    NO_FS = -100
    CANNOT_MOUNT = -101
    CANNOT_UMOUNT = -102
    CANNOT_CREATE_FS = -103
    
    message = None
    code = None
    
    def __init__(self, *args):
        BaseException.__init__(self, *args)
        self.message = args[0]
        if(len(args) > 1):
            self.code = args[1]
    

def mount_ex(device, 
             mpoint='/mnt',
             options=None,
             make_fs=False,
             fstype='ext3',
             auto_mount=False):
    if not os.path.exists(mpoint):
        os.makedirs(mpoint)
    
    options = options or ('-t', 'auto')
    
    if make_fs:
        from scalarizr import storage2
        storage2.filesystem(fstype).mkfs(device)
    
    out = mount(device, mpoint, *options)[0]
    
    if " ".join(options).find("loop") == -1:
        mtab = mounts()     
        if not mtab.contains(device):
            raise MountError("Cannot mount device '%s'. %s" % (device, out),
                             MountError.CANNOT_MOUNT)
    
    if auto_mount:
        _fstab = fstab()
        if not _fstab.contains(device, mpoint=mpoint, reload=True):
            opts = "defaults"
            if linux.os.ubuntu and linux.os['version'] >= (10, 4):
                opts += ',comment=cloudconfig,nobootwait'
            _fstab.append(device, mpoint, options=opts)
