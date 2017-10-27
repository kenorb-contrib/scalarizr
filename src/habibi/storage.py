__author__ = 'Nick Demyanchuk'
 
 
import os
import sys
import json
import glob
import uuid
import logging
from scalarizr.linux import lvm2, system
from habibi import events
 
LOG = logging.getLogger(__name__)
 
vg_name = 'tests'
cgroup_mpoint = '/sys/fs/cgroup' # Ubuntu default
snap_size = '100M'
snapshot_dir = '/tmp/snapshots'
port = 12345
 
class StorageError(Exception):
    pass
 
 
class StorageMgr():
 
    def __init__(self, farm):
        self.farm = farm
        self.volumes = dict()
        self.snapshots = dict()
        # Server_id -> [volumes attached]
        self.attachments = dict()
 
    @events.listener(event='server_terminated')
    def _server_terminated(self, server):
        if self.attachments.get(server['id']):
            for volume in self.attachments[server['id']]:
                self.detach_volume(volume['id'], server['id'])
 
 
    def cleanup(self):
        # Remove all volumes of volume group
        try:
            lvm2.vgs(vg_name)
        except lvm2.NotFound:
            pass
        else:
            lvm2.vgremove(vg_name)
 
 
    def __call__(self, environ, start_response):
        try:
            try:
                length = int(environ['CONTENT_LENGTH'])
                data = environ['wsgi.input'].read(length)
                data = json.loads(data)
 
                method = getattr(self, data['method'])
                params = data['params']
                LOG.debug('Storage service call. Method: %s, params: %s', method, params)
            except:
                start_response('400 Bad request', [], sys.exc_info())
                return str(sys.exc_info()[1])
 
            try:
                payload = method(**params)
                result = dict(status='ok')
                if payload:
                    result['payload'] = payload
            except:
                e = sys.exc_info()
                if not isinstance(e[1], (AssertionError, StorageError)):
                    LOG.error('Storage internal error occured', exc_info=e)
                result = dict(status='error', error=str(e[1]))
 
            result = json.dumps(result)
 
            headers = [('Content-type', 'application/json'),
                        ('Content-length', str(len(result))),]
            start_response('200 OK', headers)
            return result
        except:
            start_response('500 Internal Server Error', [], sys.exc_info())
            LOG.exception('Unhandled exception')
            return ''
 
 
    def create_volume(self, **params):
        size = params.get('size')
        snapshot_id = params.get('snapshot')
 
        assert size or snapshot_id, 'Not enough params to create volume'
        if snapshot_id:
            snapshot = self.describe_snapshot(snapshot_id)
            if size:
                if int(size) < int(snapshot['size']):
                    raise StorageError('Size you specified is smaller than snapshot')
        else:
            # Size in Gigabytes
            size = int(size)
 
        id = 'vol-%s' % str(uuid.uuid4())[:7]
 
        lvm2.lvcreate(vg_name, name=id, size='%sG' % size)
        lvinfo = lvm2.lvs(lvm2.lvpath(vg_name, id)).values()[0]
        device = os.path.realpath(lvinfo.lv_path)
        if snapshot_id:
            # Apply snapshot
            system('dd if=%s of=%s' % (self._get_snapshot_path(snapshot_id), device), shell=True)
 
        stat = os.stat(device)
        maj, min = (os.major(stat.st_rdev), os.minor(stat.st_rdev))
 
        self.volumes[id] = dict(id=id, attached_to=None, maj=maj, min=min,
                      host_path=device, size=str(size), source_snapshot=snapshot_id)
        return self.volumes[id]
 
 
    def attach_volume(self, volume_id, instance_id):
        assert volume_id in self.volumes, 'Volume "%s" not found' % volume_id
        volume = self.volumes[volume_id]
        attached_to = volume['attached_to']
        assert attached_to == None, 'Volume already attached to instance "%s"' % attached_to
 
        allow_path_wildcard = os.path.join(cgroup_mpoint, 'devices/lxc/%s*/devices.allow' % instance_id)
        allow_path = glob.glob(allow_path_wildcard)[0]
 
        with open(allow_path, 'w') as f:
            f.write("b %s:%s rwm\n" % (volume['maj'], volume['min']))
        volume['attached_to'] = instance_id
        if self.attachments.get(instance_id) is None:
            self.attachments[instance_id] = []
        self.attachments[instance_id].append(volume)
        return dict(volume=volume)
 
 
    def detach_volume(self, volume_id, instance_id):
        assert volume_id in self.volumes, 'Volume "%s" not found' % volume_id
        volume = self.volumes[volume_id]
        attached_to = volume['attached_to']
        assert attached_to == instance_id, 'Volume not atached to instance "%s"' % instance_id
 
        deny_path_wildcard = os.path.join(cgroup_mpoint, 'devices/lxc/%s*/devices.deny' % instance_id)
        try:
            deny_path = glob.glob(deny_path_wildcard)[0]
        except IndexError:
            # Server is no longer exist
            pass
        else:
            try:
                # Again. does not exist
                with open(deny_path, 'w') as f:
                    f.write("b %s:%s rwm\n" % (volume['maj'], volume['min']))
            except IOError:
                pass
 
        volume['attached_to'] = None
        self.attachments[instance_id].remove(volume)
 
 
    def create_snapshot(self, volume_id):
        assert volume_id in self.volumes, 'Volume "%s" not found' % volume_id
        volume = self.volumes[volume_id]
 
        snapshot_id = str(uuid.uuid4())[:7]
        if not os.path.isdir(snapshot_dir):
            os.makedirs(snapshot_dir)
        snap_path = self._get_snapshot_path(snapshot_id)
 
        lvm2.lvcreate(os.path.join('/dev', vg_name, volume['id']), snapshot=True, name=snapshot_id, size=snap_size)
        lv_info = None
        try:
            lv_info = lvm2.lvs(lvm2.lvpath(vg_name, snapshot_id)).values()[0]
            system('dd if=%s | cp --sparse=always /dev/stdin %s' % (lv_info.lv_path, snap_path), shell=True)
        finally:
            if lv_info:
                lvm2.lvremove(lv_info.lv_path)
            else:
                lvm2.lvremove(os.path.join('/dev', vg_name, snapshot_id))
 
        snapshot = dict(id=snapshot_id, size=volume['size'])
        self.snapshots[snapshot_id] = snapshot
        return snapshot
 
 
    def _get_snapshot_path(self, snapshot_id):
        return os.path.join(snapshot_dir, snapshot_id)
 
 
    def describe_snapshot(self, id):
        try:
            return self.snapshots[id]
        except KeyError:
            raise StorageError('Snapshot %s not found' % id)
 
 
    def describe_volume(self, id):
        try:
            return self.volumes[id]
        except KeyError:
            raise StorageError('Volume %s not found' % id)
 
 
    def destroy_volume(self, id):
        volume = self.describe_volume(id)
        attached_to = volume.get('attached_to')
 
        if attached_to:
            raise StorageError('Can not destroy volume: volume attached to instance %s' % attached_to)
        lvm2.lvremove(volume['host_path'])
 
 
    def destroy_snapshot(self, id):
        snapshot = self.describe_snapshot(id)
        if not snapshot['status'] == 'ready':
            raise StorageError('Snapshot is not ready yet')
