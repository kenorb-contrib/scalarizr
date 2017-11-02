"""
Created on Aug 29, 2012

@author: marat
"""

from scalarizr import storage2
from scalarizr.storage2 import filesystems


E2LABEL_EXEC            = "/sbin/e2label"
RESIZE2FS_EXEC          = "/sbin/resize2fs"
E2FSCK_EXEC                     = "/sbin/e2fsck"
MAX_LABEL_LENGTH        = 16


class ExtFileSystem(filesystems.FileSystem):

    features = filesystems.FileSystem.features.copy()
    features['umount_on_resize'] = True

    error_messages = filesystems.FileSystem.error_messages.copy()
    error_messages['fsck'] = 'Error occured during filesystem check on device %s'

    os_packages = ('e2fsprogs', )


    def mkfs(self, device, *short_args):
        short_args = list(short_args)
        short_args += list(opt for opt in ('-F', '-q') if opt not in short_args)
        super(ExtFileSystem, self).mkfs(device, *short_args)


    def resize(self, device, size=None, *short_args, **long_kwds):
        cmd = (E2FSCK_EXEC, '-fy', device)
        rcode = filesystems.system(cmd, raise_exc=False,
                                                error_text=self.error_messages['fsck'] % device)[2]
        if rcode not in (0, 1):
            raise storage2.StorageError('Fsck failed to correct file system errors')
        cmd = (RESIZE2FS_EXEC, device)
        filesystems.system(cmd, error_text=self.error_messages['resize'] % device)


    def set_label(self, device, label):
        cmd     = (E2LABEL_EXEC, device, label[:MAX_LABEL_LENGTH])
        filesystems.system(cmd, error_text=self.error_messages['set_label'] % device)


    def get_label(self, device):
        cmd = (E2LABEL_EXEC, device)
        return filesystems.system(cmd, error_text=self.error_messages['get_label'] % device)[0].strip()


class Ext3FileSystem(ExtFileSystem):
    type = 'ext3'


storage2.filesystem_types[Ext3FileSystem.type] = Ext3FileSystem
