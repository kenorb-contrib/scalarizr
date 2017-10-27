'''
Created on Sep 4, 2012
 
@author: marat
'''
 
import mock
from nose.tools import raises
 
from scalarizr.storage2 import StorageError
from scalarizr.storage2.volumes import base
from scalarizr.storage2.volumes import ebs
from scalarizr.linux import coreutils
 
 
def test_name2device():
    pass
 
 
@mock.patch('os.path.exists', return_value=True)
def test_name2device_xen(*args):
    device = ebs.name2device('/dev/sda1')
    assert device == '/dev/xvda1'
 
 
@mock.patch('os.path.exists', return_value=True)
@mock.patch.object(ebs, 'storage2')
def test_name2device_rhel_bug(s, exists):
    s.RHEL_DEVICE_ORDERING_BUG = True
    device = ebs.name2device('/dev/sda1')
    assert device == '/dev/xvde1'
 
 
def test_name2device_device_passed():
    pass
 
def test_device2name():
    pass
 
def test_device2name_xen():
    pass
 
def test_device2name_rhel_bug():
    pass
 
def test_device2name_name_passed():
    pass
 
 
class TestFreeDeviceLetterMgr(object):
    def setup(self):
        self.mgr = ebs.FreeDeviceLetterMgr()
 
    @mock.patch('glob.glob')
    @mock.patch.object(ebs, '__node__', new={'ec2': {
                                    't1micro_detached_ebs': None}})
    def test_acquire(self, glob):
        glob_returns = [['/dev/sdf1'], []]
        def globfn(*args, **kwds):
            return glob_returns.pop(0)
        glob.side_effect = globfn
 
        letter = self.mgr.__enter__().get()
 
        glob.assert_called_with('/dev/sd%s*' % letter)
        assert glob.call_count == 2
 
 
    def test_acquire_concurrent(self):
        pass
 
 
    @mock.patch('glob.glob', return_value=[])
    @mock.patch.object(ebs, '__node__', new={'ec2': {
                                    't1micro_detached_ebs': ['/dev/sdg', '/dev/sdf']}})
    def test_acquire_t1micro(self, glob):
        letter = self.mgr.__enter__().get()
        assert letter not in ('g', 'f')
 
 
Ebs = ebs.EbsVolume
@mock.patch.object(ebs, '__node__', new={'ec2': {
                                'instance_id': 'i-12345678',
                                'instance_type': 'm1.small',
                                'avail_zone': 'us-east-1a'}})
@mock.patch.object(ebs, 'name2device',
                                side_effect=lambda name: name.replace('/sd', '/xvd'))
@mock.patch.object(Ebs, '_free_device_letter_mgr',
                                **{'get.return_value' : 'b'})
@mock.patch.object(Ebs, '_connect_ec2')
class TestEbsVolume(object):
 
    @mock.patch.object(Ebs, '_attach_volume')
    @mock.patch.object(Ebs, '_create_volume')
    def test_ensure_new(self, _create_volume, *args):
        ebs = mock.Mock(
                id='vol-12345678',
                size=1,
                zone='us-east-1a',
                **{'volume_state.return_value': 'available',
                        'attach_data.device': '/dev/sdb'}
        )
        _create_volume.return_value = ebs
 
        self.vol = Ebs(type='ebs')
        self.vol.ensure()
 
        assert self.vol.id == ebs.id
        assert self.vol.size == ebs.size
        assert self.vol.avail_zone == 'us-east-1a'
        assert self.vol.name == '/dev/sdb'
        assert self.vol.device == '/dev/xvdb'
        assert self.vol.config()
 
 
    @mock.patch.object(Ebs, '_attach_volume')
    def test_ensure_existed(self, av, _connect_ec2, *args):
        conn = _connect_ec2.return_value
        ebs = mock.Mock(
                id='vol-12345678',
                size=1,
                zone='us-east-1a',
                **{'volume_state.return_value': 'available'}
        )
        conn.get_all_volumes.return_value = [ebs]
 
        vol = Ebs(type='ebs', id='vol-12345678')
        vol.ensure()
 
        assert vol.id == ebs.id
        assert vol.size == ebs.size
        assert vol.avail_zone == 'us-east-1a'
        assert vol.name == '/dev/sdb'
        assert vol.device == '/dev/xvdb'
        conn.get_all_volumes.assert_called_once_with(['vol-12345678'])
        assert vol.config()
 
 
    @mock.patch.object(Ebs, '_attach_volume')
    @mock.patch.object(Ebs, '_create_volume')
    @mock.patch.object(Ebs, '_create_snapshot')
    def test_ensure_existed_in_different_zone(self, _create_snapshot,
                            _create_volume, av, _connect_ec2, *args):
        conn = _connect_ec2.return_value
        ebs = mock.Mock(
                id='vol-12345678',
                size=1,
                zone='us-east-1b',
                **{'volume_state.return_value': 'available'}
        )
        ebs2 = mock.Mock(
                id='vol-87654321',
                size=1,
                zone='us-east-1a',
                **{'volume_state.return_value': 'available'}
        )
        conn.get_all_volumes.return_value = [ebs]
        _create_volume.return_value = ebs2
        _create_snapshot.return_value = mock.Mock(id='snap-12345678')
 
        vol = Ebs(type='ebs', id='vol-12345678')
        vol.ensure()
 
        assert vol.id == ebs2.id
 
 
    @mock.patch.object(Ebs, '_attach_volume')
    @mock.patch.object(Ebs, '_detach_volume')
    def test_ensure_existed_attached_to_other_instance(self, av, dv,
                                    _connect_ec2, *args):
        conn = _connect_ec2.return_value
        ebs = mock.Mock(
                id='vol-12345678',
                size=1,
                zone='us-east-1a',
                **{'volume_state.return_value': 'available',
                        'attachment_state.return_value': 'attached'}
        )
        conn.get_all_volumes.return_value = [ebs]
 
        vol = Ebs(type='ebs', id='vol-12345678')
        vol.ensure()
 
        assert vol.id == ebs.id
        vol._detach_volume.assert_called_once_with(ebs)
        vol._attach_volume.assert_called_once_with(ebs, '/dev/sdb')
 
 
    @mock.patch.object(Ebs, '_attach_volume')
    @mock.patch.object(Ebs, '_detach_volume')
    @mock.patch.object(Ebs, '_wait_attachment_state_change')
    def test_ensure_existed_attaching_to_other_instance(self,
                                    av, dv, wasc, _connect_ec2, *args):
        ebs = mock.Mock(
                id='vol-12345678',
                size=1,
                zone='us-east-1a',
                **{'volume_state.return_value': 'available',
                        'attachment_state.return_value': 'attaching'}
        )
        _connect_ec2.return_value.get_all_volumes.return_value = [ebs]
 
        vol = Ebs(type='ebs', id='vol-12345678')
        vol.ensure()
 
        assert vol.id == ebs.id
        assert vol._detach_volume.call_count == 0
        vol._attach_volume.assert_called_once_with(ebs, '/dev/sdb')
 
 
    @mock.patch.object(Ebs, '_create_volume')
    @mock.patch.object(Ebs, '_attach_volume')
    def test_ensure_restore(self, vc, av, *args):
        snap_config = {
                'id':'snap-12345678',
                'tags':{'ta':'gs'}
        }
        size = 1
        vol = Ebs(size=size, snap=snap_config)
        vol.ensure()
 
        vol._create_volume.assert_called_once_with(
                        zone=mock.ANY,
                        size=size,
                        snapshot=snap_config['id'],
                        tags=snap_config['tags'],
                        volume_type=mock.ANY,
                        iops=mock.ANY)
 
 
    @mock.patch.object(Ebs, '_create_snapshot')
    def test_snapshot(self, cs, _connect_ec2, *args):
        description='test'
        tags={'ta':'gs'}
        vol = Ebs(type='ebs', id='vol-12345678')
 
        vol.snapshot(description, tags=tags, nowait=True)
        vol._create_snapshot.assert_called_once_with(
                                vol.id, description, tags, True)
 
 
    @mock.patch.object(coreutils, 'sync')
    def test_snapshot_tags(self, sync, _connect_ec2, *args):
        snapshot = mock.Mock(id='snap-12345678')
        conn = _connect_ec2.return_value
        conn.configure_mock(**{'create_snapshot.return_value': snapshot,
                                                        'create_tags': mock.Mock()})
 
        tags = {'ta':'gs'}
        vol = Ebs(type='ebs', id='vol-12345678')
        snap = vol.snapshot('test', tags=tags)
 
        conn.create_tags.assert_called_once_with([snapshot.id], tags)
 
 
    @mock.patch.object(Ebs, '_wait_snapshot')
    def test_snapshot_wait(self, ws, _connect_ec2, *args):
        snapshot = mock.Mock(id='snap-12345678')
        _connect_ec2.return_value.configure_mock(**{
                        'create_snapshot.return_value': snapshot,
                        'create_tags': mock.Mock()})
 
        vol = Ebs(type='ebs', id='vol-12345678', tags={'ta': 'gs'})
        snap = vol.snapshot('test', tags=vol.tags, nowait=False)
 
        vol._wait_snapshot.assert_called_once_with(snapshot)
 
 
    @raises(AssertionError)
    def test_snapshot_no_connection(self, _connect_ec2, *args):
        _connect_ec2.return_value = None
        self.vol = Ebs(type='ebs', id='vol-12345678')
        snap = self.vol.snapshot('test')
 
 
    @mock.patch.object(Ebs, 'umount')
    @mock.patch.object(Ebs, '_detach_volume')
    def test_detach(self, *args):
        vol = Ebs(type='ebs', device='/dev/xvdp', id='vol-12345678')
        vol.detach()
        vol._detach_volume.assert_called_with(vol.id, False)
 
 
    @mock.patch.object(Ebs, '_detach_volume')
    @mock.patch.object(Ebs, '_instance_type', return_value='t1.micro')
    def test_detach_t1micro(self, *args):
        ebs.__node__['ec2']['t1micro_detached_ebs'] = None
        vol = Ebs(name='/dev/sdf', type='ebs', id='vol-12345678')
        vol._detach(True)
        assert ebs.__node__['ec2']['t1micro_detached_ebs'] == [vol.name,]
 
 
    @raises(AssertionError)
    def test_detach_no_connection(self, _connect_ec2, *args):
        _connect_ec2.return_value = None
        vol = Ebs(type='ebs', id='vol-12345678', device='/dev/xvdp')
        vol._detach(False)
 
 
    def test_destroy(self, _connect_ec2, *args):
        vol = Ebs(type='ebs', id='vol-12345678')
        vol.destroy(True)
        conn = _connect_ec2.return_value
        conn.delete_volume.assert_called_once_with(vol.id)
 
 
    @raises(AssertionError)
    def test_destroy_no_connection(self, _connect_ec2, *args):
        _connect_ec2.return_value = None
        vol = Ebs(type='ebs', id='vol-12345678')
        vol.destroy(True)
 
 
EbsSnapshot = ebs.EbsSnapshot
@mock.patch.object(EbsSnapshot, '_connect_ec2')
class TestEbsSnapshot(object):
 
    @mock.patch.object(EbsSnapshot, '_ebs_snapshot')
    def test_status(self, _ebs_snapshot, _connect_ec2, *args, **kwargs):
        snap = EbsSnapshot(id='vol-123456ab')
        snapshot = mock.Mock(id='vol-123456ab')
        snapshot.status = 'pending'
        _ebs_snapshot.return_value = snapshot
        assert snap.status() == base.Snapshot.IN_PROGRESS
        _ebs_snapshot.assert_called_once_with('vol-123456ab')
        snapshot.update.assert_called_with()
        snapshot.status = 'completed'
        assert snap.status() == base.Snapshot.COMPLETED
        snapshot.status = 'error'
        assert snap.status() == base.Snapshot.FAILED
 
 
    @raises(AssertionError)
    def test_status_no_connection(self, _connect_ec2, *args, **kwargs):
        _connect_ec2.return_value = None
        snap = EbsSnapshot(id='vol-123456ab')
        snap.status()
 
 
    def test_destroy(self, _connect_ec2, *args, **kwargs):
        snap = EbsSnapshot(id='vol-123456ab')
        snap.destroy()
        conn = _connect_ec2.return_value
        conn.delete_snapshot.assert_called_once_with(snap.id)
 
