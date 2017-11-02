from __future__ import with_statement
'''
Created on Nov 11, 2010

@author: spike
@author: marat
'''

from . import FileSystem, device_should_exists, system

import re


XFS_ADMIN_PATH  = "/usr/sbin/xfs_admin"
XFS_GROWFS_PATH = "/usr/sbin/xfs_growfs"
XFS_FREEZE_PATH = "/usr/sbin/xfs_freeze"



class XfsFileSystem(FileSystem):
    name = 'xfs'
    umount_on_resize = False
    os_packages = ('xfsprogs', )

    def __init__(self):
        FileSystem.__init__(self)
        self._label_re  = re.compile('label\s+=\s+"(?P<label>.*)"', re.IGNORECASE)


    @device_should_exists
    def set_label(self, device, label):
        cmd = (XFS_ADMIN_PATH, '-L', label, device)
        system(cmd, error_text=self.E_SET_LABEL % device)

    @device_should_exists
    def get_label(self, device):
        cmd = (XFS_ADMIN_PATH, '-l', device)
        res = re.search(self._label_re, system(cmd, error_text=self.E_GET_LABEL % device)[0])
        return res.group('label') if res else ''

    @device_should_exists
    def resize(self, device, size=None, **options):
        mpoint = self._check_mounted(device)
        cmd = (XFS_GROWFS_PATH, mpoint)
        system(cmd, error_text=self.E_RESIZE % device)

    @device_should_exists
    def freeze(self, device):
        mpoint = self._check_mounted(device)
        cmd = (XFS_FREEZE_PATH, '-f', mpoint)
        system(cmd, error_text=self.E_FREEZE % device)

    @device_should_exists
    def unfreeze(self, device):
        mpoint = self._check_mounted(device)
        cmd = (XFS_FREEZE_PATH, '-u', mpoint)
        system(cmd, error_text=self.E_UNFREEZE % device)

__filesystem__ = XfsFileSystem
