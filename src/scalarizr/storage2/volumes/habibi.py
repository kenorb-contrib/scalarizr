__author__ = 'spike'

import os
import stat
import json
import urllib
import functools

from scalarizr import storage2
from scalarizr.bus import bus
from scalarizr.storage2.volumes import base

service_url = bus.platform.get_user_data('storage_service_url')

class StorageClient(object):

    @classmethod
    def request(cls, name, **params):
        post_data = json.dumps(dict(method=name, params=params))
        r = urllib.urlopen(service_url, post_data)
        response = json.loads(r.read())
        if response.get('status') == 'error':
            raise Exception(response.get('error'))
        return response['payload']


    def __getattr__(self, item):
        return functools.partial(self.request, item)



class HabibiVolume(base.Volume):

    def _ensure(self):
        if not self.id:
            self._check(False, False, size=True)
            resp = StorageClient.request('create_volume', size=self.size)
            self._config.update(resp)

        vol = StorageClient.request('describe_volume', id=self.id)
        server_id = bus.platform.get_user_data('serverid')

        if vol['attached_to'] and vol['attached_to'] != server_id:
            StorageClient.request('detach_volume', volume_id=self.id, instance_id=vol['attached_to'])

        StorageClient.attach_volume(volume_id=self.id, instance_id=server_id)

        dev_no = os.makedev(int(self.maj), int(self.min))
        device = os.path.join('/dev', self.id)
        os.mknod(device, 644 | stat.S_IFBLK, dev_no)
        self.device = device


    def _snapshot(self, description, tags, **kwds):
        snapshot = StorageClient.request('create_snapshot', volume_id=self.id)
        return HabibiSnapshot(**snapshot)


class HabibiSnapshot(base.Snapshot):
    pass

storage2.volume_types['habibi'] = HabibiVolume