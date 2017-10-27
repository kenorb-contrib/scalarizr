__author__ = 'Nick Demyanchuk'
 
import mock
import unittest
 
from scalarizr.storage2.volumes import gce_persistent
 
 
class TestGcePersistentStorage(unittest.TestCase):
 
    @mock.patch('os.path.exists')
    @mock.patch('scalarizr.storage2.volumes.gce_persistent.__node__')
    @mock.patch('scalarizr.storage2.volumes.gce_persistent.wait_for_operation_to_complete')
    def test_snapshot(self, waiter, node, exists):
        exists.return_value = True
        compute = mock.MagicMock()
        compute._baseUrl = 'http://some/base/url'
 
        project_id = '1234567'
        node_content = dict(gce=dict(project_id=project_id,
                                                                 compute_connection=compute))
        node.__getitem__.side_effect = lambda x: node_content[x]
 
        waiter.return_value = True
        compute.snapshots.return_value.insert.return_value.execute.return_value = dict(
                name='test_operation')
 
        snapshot_info = dict(id='123123123', name='name', diskSizeGb='1', selfLink='somelink')
        compute.snapshots.return_value.get.return_value.execute.return_value = snapshot_info
 
        vol = gce_persistent.GcePersistentVolume(name='myvolume', id='123123123')
        vol.ensure()
        snap = vol.snapshot()
 
        waiter.assert_called_once_with(compute, project_id, 'test_operation')
 
        compute.snapshots.return_value.insert.return_value.execute.assert_called_once_with()
        compute.snapshots.return_value.insert.assert_called_once_with(
                project=project_id, body=dict(
                        name=mock.ANY, description=None, sourceDisk=vol.link,
                        sourceDiskId=vol.id
                )
        )
 
        assert isinstance(snap, gce_persistent.GcePersistentSnapshot)
        assert snap.name == snapshot_info['name']
        assert snap.id == snapshot_info['id']
        assert snap.size == snapshot_info['diskSizeGb']
        assert snap.link == snapshot_info['selfLink']
 
 
    @mock.patch('scalarizr.storage2.volumes.gce_persistent.__node__')
    @mock.patch('scalarizr.storage2.volumes.gce_persistent.wait_for_operation_to_complete')
    def test_destroy_snapshot(self, waiter, node):
        project_id = 'kabato'
        compute = mock.MagicMock()
        node_content = dict(gce=dict(project_id=project_id,
                                                                 compute_connection=compute))
        node.__getitem__.side_effect = lambda x: node_content[x]
 
        snap_name = 'itsasnap'
        snap = gce_persistent.GcePersistentSnapshot(name='itsasnap')
 
        op_name = 'operation123'
        compute.snapshots.return_value.delete.return_value.execute.return_value = dict(
                name=op_name
        )
 
        snap.destroy()
 
        compute.snapshots.return_value.delete.assert_called_once_with(
                project=project_id, snapshot=snap_name)
 
        waiter.assert_called_once_with(compute, project_id, op_name)
 
 
    @mock.patch('scalarizr.storage2.volumes.gce_persistent.__node__')
    def test_link(self, node):
        project_id = 'ninjutsu'
        compute = mock.MagicMock()
        node_content = dict(gce=dict(project_id=project_id,
                                                                 compute_connection=compute))
        node.__getitem__.side_effect = lambda x: node_content[x]
 
        compute._baseUrl = 'http://some/base/url/'
 
        vol_name = 'test'
        vol = gce_persistent.GcePersistentVolume(name=vol_name)
 
        link = vol.link
        assert link == '%s%s/disks/%s' % (compute._baseUrl, project_id,vol_name)
 
