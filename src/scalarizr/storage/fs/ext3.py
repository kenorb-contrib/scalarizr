from __future__ import with_statement
'''
Created on Nov 11, 2010

@author: spike
@author: marat
'''
from . import FileSystem, system, device_should_exists


E2LABEL_EXEC            = "/sbin/e2label"
RESIZE2FS_EXEC          = "/sbin/resize2fs"
E2FSCK_EXEC                     = "/sbin/e2fsck"
MAX_LABEL_LENGTH        = 16


class ExtFileSystem(FileSystem):
    umount_on_resize        = True

    def mkfs(self, device, options=None):
        FileSystem.mkfs(self, device, ('-F',))

    @device_should_exists
    def resize(self, device, size=None, **options):
        cmd = (E2FSCK_EXEC, '-fy', device)
        error_text = "Error occured during filesystem check on device '%s'" % device
        system(cmd, error_text=error_text)

        cmd = (RESIZE2FS_EXEC, device)
        system(cmd, error_text=self.E_RESIZE % device)

    @device_should_exists
    def set_label(self, device, label):
        cmd     = (E2LABEL_EXEC, device, label[:MAX_LABEL_LENGTH])
        system(cmd, error_text=self.E_SET_LABEL % device)

    @device_should_exists
    def get_label(self, device):
        cmd = (E2LABEL_EXEC, device)
        return system(cmd, error_text=self.E_GET_LABEL % device)[0].strip()


class Ext3FileSystem(ExtFileSystem):
    name = 'ext3'


__filesystem__ = Ext3FileSystem
