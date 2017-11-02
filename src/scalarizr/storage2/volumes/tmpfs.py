from __future__ import with_statement

import os

from scalarizr.linux import mount
from scalarizr import storage2
from scalarizr.storage2.volumes import base


class TmpfsVolume(base.Volume):

    def __init__(self, size=None, **kwds):
        '''
        :type size: int
        :param size: Volume size in Mb
        '''
        super(TmpfsVolume, self).__init__(size=size, **kwds)
        self.error_messages.update({
                'invalid_size': 'Volume size required and should be int value in Mb'
        })
        self.features['restore'] = False

    def _ensure(self):
        expr = isinstance(self.size, int) and self.size
        assert expr, self.error_messages['invalid_size']
        self._check_attr('mpoint')
        if not self.device:
            if not os.path.exists(self.mpoint):
                os.makedirs(self.mpoint)
            mount.mount('tmpfs', self.mpoint,
                            '-t', 'tmpfs',
                            '-o', 'size=%sM' % self.size)
            self.device = self.mpoint


storage2.volume_types['tmpfs'] = TmpfsVolume
