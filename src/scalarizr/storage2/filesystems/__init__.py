"""
Created on Nov 11, 2010

@author: spike
@author: marat
"""

import os
import re
import logging

from scalarizr import linux
from scalarizr.linux import mount, coreutils
from scalarizr.linux import pkgmgr


LOG = logging.getLogger(__name__)


class FileSystemError(linux.LinuxError):
    pass

def system(*args, **kwargs):
    kwargs['logger'] = LOG
    kwargs['exc_class'] = FileSystemError
    kwargs['warn_stderr'] = False
    return linux.system(*args, **kwargs)


class FileSystem(object):
    type = None

    features = {
            'freezable': False,
            'resizable': True,
            'umount_on_resize': False
    }

    error_messages = {
            'mkfs': 'Error during filesystem creation on device %s',
            'resize': 'Error during filesystem resize on device %s',
            'set_label': 'Error while setting label for device %s',
            'get_label': 'Error while getting label for device %s',
            'freeze': 'Error during filesystem freeze on device %s',
            'unfreeze': 'Error during filesystem un-freeze on device %s',
            'not_mounted': 'Device %s should be mounted to perform this operation'
    }

    os_packages = []


    def __init__(self):
        if not os.path.exists('/sbin/mkfs.%s' % self.type):
            coreutils.modprobe(self.type)
            if self.os_packages:
                LOG.debug('Installing OS packages')

                for package in self.os_packages:
                    pkgmgr.installed(package)


    def mkfs(self, device, *short_args):
        short_args = list(short_args)
        short_args = ['-t', self.type] + short_args
        args = linux.build_cmd_args(
                                executable='/sbin/mkfs',
                                short=short_args,
                                params=[device])
        system(args, error_text=self.error_messages['mkfs'] % device)


    def resize(self, device, size=None, *short_args, **long_kwds):
        '''
        Resize filesystem on given device to a
        given size (default: to the size of partition)
        '''
        raise NotImplementedError()


    def set_label(self, device, label):
        raise NotImplementedError()


    def get_label(self, device):
        raise NotImplementedError()


    def freeze(self, device):
        # pylint: disable=W0613
        if self.features['freezable']:
            raise NotImplementedError()


    def unfreeze(self, device):
        # pylint: disable=W0613
        if self.features['freezable']:
            raise NotImplementedError()
