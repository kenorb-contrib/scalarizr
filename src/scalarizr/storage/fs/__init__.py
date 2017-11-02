from __future__ import with_statement
'''
Created on Nov 11, 2010

@author: spike
@author: marat
'''

from .. import MKFS_EXEC, MOUNT_EXEC

from scalarizr.linux import coreutils, pkgmgr
from scalarizr.util import system2, PopenError

import os
import re
import sys
import logging

logger = logging.getLogger(__name__)

class FileSystemError(PopenError):
    pass

def system(*args, **kwargs):
    kwargs['logger'] = logger
    kwargs['exc_class'] = FileSystemError
    kwargs['warn_stderr'] = False
    return system2(*args, **kwargs)

def device_should_exists(f):
    def d(*args):
        if not os.path.exists(args[1]):
            raise FileSystemError("Device %s doesn't exist" % args[1])
        return f(*args)
    return d

class FileSystem:
    name = None
    freezable = False
    resizable = True
    umount_on_resize = False
    os_packages = None

    E_MKFS          = 'Error during filesystem creation on device %s'
    E_RESIZE        = 'Error during filesystem resize on device %s'
    E_SET_LABEL = 'Error while setting label for device %s'
    E_GET_LABEL = 'Error while getting label for device %s'
    E_FREEZE        = 'Error during filesystem freeze on device %s'
    E_UNFREEZE      = 'Error during filesystem un-freeze on device %s'
    E_NOT_MOUNTED = 'Device %s should be mounted'

    def __init__(self):
        if not os.path.exists('/sbin/mkfs.%s' % self.name):
            try:
                coreutils.modprobe(self.name)
            except:
                e = sys.exc_info()[1]
                error_text="Cannot load '%s' kernel module: %s" % (self.name, e)
                raise Exception(error_text)

            if self.os_packages:
                for package in self.os_packages:
                    pkgmgr.installed(package)



    @device_should_exists
    def mkfs(self, device, options=None):
        cmd = [MKFS_EXEC, '-t', self.name]
        if options:
            cmd.extend(options)
        cmd.append(device)
        system(cmd, error_text=self.E_MKFS % device)

    def resize(self, device, size=None, **options):
        '''
        Resize filesystem on given device to given size (default: to the size of partition)
        '''
        pass

    def set_label(self, device, label):
        pass

    def get_label(self, device):
        pass

    def freeze(self, device):
        pass

    def unfreeze(self, device):
        pass

    def _check_mounted(self, device):
        res = re.search('%s\s+on\s+(?P<mpoint>.+)\s+type' % device, system(MOUNT_EXEC)[0])
        if not res:
            raise FileSystemError(self.E_NOT_MOUNTED % device)
        return res.group('mpoint')
