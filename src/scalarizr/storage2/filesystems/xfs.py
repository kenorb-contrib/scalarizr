"""
Created on Aug 29, 2012

@author: marat
"""

import re

from scalarizr.storage2 import filesystems
from scalarizr import storage2
from scalarizr.linux import mount


XFS_ADMIN_EXEC  = '/usr/sbin/xfs_admin'
XFS_GROWFS_EXEC = '/usr/sbin/xfs_growfs'
XFS_FREEZE_EXEC = '/usr/sbin/xfs_freeze'


def _device_mpoint(device):
    try:
        return mount.mounts()[device].mpoint
    except KeyError:
        return False


class XfsFileSystem(filesystems.FileSystem):
    type = 'xfs'

    os_packages = ('xfsprogs', )

    def __init__(self):
        super(XfsFileSystem, self).__init__()
        self._label_re = re.compile(r'label\s+=\s+"(?P<label>.*)"',
                                                                re.IGNORECASE)

    def set_label(self, device, label):
        cmd = (XFS_ADMIN_EXEC, '-L', label, device)
        filesystems.system(cmd,
                        error_text=self.error_messages['set_label'] % device)

    def get_label(self, device):
        cmd = (XFS_ADMIN_EXEC, '-l', device)
        res = re.search(self._label_re, filesystems.system(
                                cmd,
                                error_text=self.error_messages['get_label'] % device)[0])
        return res.group('label') if res else ''


    def resize(self, device, size=None, *short_args, **long_kwds):
        mpoint = _device_mpoint(device)
        if mpoint:
            cmd = (XFS_GROWFS_EXEC, mpoint)
            filesystems.system(cmd,
                            error_text=self.error_messages['resize'] % device)
        else:
            raise filesystems.FileSystemError(
                                    self.error_messages['not_mounted'] % device)


    def freeze(self, device):
        mpoint = _device_mpoint(device)
        if mpoint:
            cmd = (XFS_FREEZE_EXEC, '-f', mpoint)
            filesystems.system(cmd,
                            error_text=self.error_messages['freeze'] % device)
        else:
            raise filesystems.FileSystemError(
                            self.error_messages['not_mounted'] % device)


    def unfreeze(self, device):
        mpoint = _device_mpoint(device)
        if mpoint:
            cmd = (XFS_FREEZE_EXEC, '-u', mpoint)
            filesystems.system(cmd,
                            error_text=self.error_messages['unfreeze'] % device)
        else:
            raise filesystems.FileSystemError(
                            self.error_messages['not_mounted'] % device)


storage2.filesystem_types[XfsFileSystem.type] = XfsFileSystem
