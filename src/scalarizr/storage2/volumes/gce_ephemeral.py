import os

from scalarizr import storage2
from scalarizr.storage2.volumes import base


class GceEphemeralVolume(base.Volume):
    def __init__(self, name=None, **kwds):
        super(GceEphemeralVolume, self).__init__(
                name=name, **kwds)

    def _ensure(self):
        self._check_attr('name')
        if self.name.startswith('google-'):
            self.name = self.name[7:]
        device = '/dev/disk/by-id/google-%s' % self.name
        if not os.path.exists(device):
            msg = "Device '%s' not found" % device
            raise storage2.StorageError(msg)
        self.device = os.path.realpath(device)


storage2.volume_types['gce_ephemeral'] = GceEphemeralVolume
