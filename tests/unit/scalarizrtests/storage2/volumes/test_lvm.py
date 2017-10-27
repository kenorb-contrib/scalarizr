'''
Created on Aug 22, 2012
 
@author: marat
'''
 
import mock
import unittest
 
from scalarizr.storage2.volumes import lvm
from scalarizr.storage2.volumes import base
from scalarizr import storage2
 
@mock.patch('scalarizr.storage2.volumes.base.mod_mount')
@mock.patch('scalarizr.storage2.volumes.lvm.lvm2')
class TestLvmVolume(unittest.TestCase):
 
 
    def test_ensure_new(self, lvm2, mod_mount):
        lvm2.NotFound = Exception
        lvs_returns = [
                lvm2.NotFound,
                {'data/vol1': mock.Mock(lv_path='/dev/mapper/data-vol1', lv_attr='-wi-ao')}
        ]
        def lvs(*args, **kwds):
            ret = lvs_returns.pop(0)
            if isinstance(ret, Exception):
                raise ret
            return ret
 
        lvm2.lvs.side_effect = lvs
        lvm2.pvs.side_effect = [{}, {}] + [{'/dev/sdb': mock.Mock(vg_name=None)},
                                                                           {'/dev/sdc': mock.Mock(vg_name=None)}]*2
        lvm2.vgs.side_effect = lvm2.NotFound
 
        vol = lvm.LvmVolume(
                name='vol1',
                vg='data',
                pvs=['/dev/sdb', '/dev/sdc'],
                size='98%FREE')
        vol.ensure()
 
        assert len(vol.pvs) == 2
        assert isinstance(vol.pvs[0], base.Volume)
        assert isinstance(vol.pvs[1], base.Volume)
        assert vol.device == '/dev/mapper/data-vol1'
        calls = [mock.call('/dev/sdb'),  mock.call('/dev/sdc')]
        lvm2.pvcreate.assert_has_calls(calls)
        lvm2.vgcreate.assert_called_once_with('data', '/dev/sdb', '/dev/sdc')
        lvm2.lvcreate.assert_called_once_with('data', name='vol1', extents='98%FREE')
 
 
    def test_ensure_new_with_existed_layout(self, lvm2, mod_mount):
        lvm2.NotFound = Exception
        lvs_returns = [
                lvm2.NotFound,
                {'data/vol1': mock.Mock(lv_path='/dev/mapper/data-vol1', lv_attr='-wi---')}
        ]
        def lvs(*args, **kwds):
            ret = lvs_returns.pop(0)
            if isinstance(ret, Exception):
                raise ret
            return ret
        lvm2.lvs.side_effect = lvs
        lvm2.pvs.return_value = {'/dev/sdb': mock.Mock(vg_name='data'), '/dev/sdc': mock.Mock(vg_name='data')}
 
        vol = lvm.LvmVolume(
                        name='vol1',
                        vg='data',
                        pvs=['/dev/sdb', '/dev/sdc'],
                        size='98%FREE')
        vol.ensure()
 
        assert len(vol.pvs) == 2
        assert isinstance(vol.pvs[0], base.Volume)
        assert isinstance(vol.pvs[1], base.Volume)
        assert vol.device == '/dev/mapper/data-vol1'
        assert lvm2.pvs.mock_calls == [mock.call(),
                                                                   mock.call(),
                                                                   mock.call('/dev/sdb'),
                                                                   mock.call('/dev/sdc')]
        assert lvm2.pvcreate.call_count == 0
        lvm2.vgs.assert_called_once_with('data')
        lvm2.lvchange.assert_called_once_with(vol.device, available='y')
 
 
    def test_detach(self, lvm2, mod_mount):
        vol = self._create_vol(lvm2)
        device = vol.device
        vol.detach()
        lvm2.lvchange.assert_called_once_with(device, available='n')
 
    def test_destroy(self, lvm2, mod_mount):
        vol = self._create_vol(lvm2)
        vol.destroy()
        lvm2.lvremove.assert_called_once_with(vol.device)
        self.assertFalse(lvm2.vgremove.mock_calls)
        self.assertFalse(lvm2.pvremove.mock_calls)
 
        vol = self._create_vol(lvm2)
        lvm2.vgs.return_value = {'data': mock.MagicMock(snap_count=0, lv_count=0)}
        lvm2.pvs.return_value = {'/dev/sdb': mock.MagicMock(vg_name='data'),
                                                         '/dev/sdc': mock.MagicMock(vg_name='data')}
        vol.destroy(force=True)
        lvm2.lvremove.assert_called_once_with(vol.device)
        lvm2.vgs.assert_called_once_with(vol.vg)
 
        lvm2.pvs.assert_called_once_with()
        lvm2.vgremove.assert_called_once_with(vol.vg)
        self.assertEqual(lvm2.pvremove.mock_calls,
                                         [mock.call('/dev/sdb'), mock.call('/dev/sdc')])
 
 
 
 
    def _create_vol(self, lvm2):
        """
        Creates mocked lvm volume with 2 pvs
        """
        # storage2.volume('/dev/sdd')
        lvm2.NotFound = Exception
        lvs_returns = [
                lvm2.NotFound,
                {'data/vol1': mock.Mock(lv_path='/dev/mapper/data-vol1', lv_attr='-wi-ao')}
        ]
        def lvs(*args, **kwds):
            ret = lvs_returns.pop(0)
            if isinstance(ret, Exception):
                raise ret
            return ret
 
        lvm2.lvs.side_effect = lvs
        lvm2.pvs.side_effect = lambda *args, **kwds: {'/dev/sdb': mock.Mock(vg_name=None),
                                                                        '/dev/sdc':  mock.Mock(vg_name=None)}
        lvm2.vgs.side_effect = lvm2.NotFound
 
        vol = lvm.LvmVolume(
                name='vol1',
                vg='data',
                pvs=['/dev/sdb', '/dev/sdc'],
                size='98%FREE')
        vol.ensure()
 
        lvm2.reset_mock()
        lvm2.lvs.side_effect = lvm2.pvs.side_effect = lvm2.vgs.side_effect = None
        return vol
 
 
    @mock.patch('scalarizr.storage2.volumes.lvm.storage2.concurrent_snapshot')
    @mock.patch('scalarizr.storage2.volumes.lvm.coreutils')
    @mock.patch('os.path.exists', return_value=True)
    def test_snapshot(self, ex, coreutils, conc_snap, lvm2, mod_mount):
        vol = self._create_vol(lvm2)
 
        """ Successfull snapshot """
        snap = vol.snapshot(description='test_descr', tags=dict(tag='val'))
 
        assert coreutils.dmsetup.mock_calls == [
                mock.call('suspend', vol.device), mock.call('resume', vol.device)
        ]
        conc_snap.assert_called_once_with(vol.pvs,'test_descr PV-${index}',
                                                                                                                        dict(tag='val'))
 
        assert type(snap) == lvm.LvmSnapshot
 
        """ Pv snapshots failed """
        conc_snap.reset_mock()
        coreutils.dmsetup.reset_mock()
        conc_snap.side_effect = Exception
        self.assertRaises(Exception, vol.snapshot,
                                          description='test_descr', tags=dict(tag='val'))
        assert coreutils.dmsetup.mock_calls == [
                mock.call('suspend', vol.device), mock.call('resume', vol.device)
        ]
        conc_snap.assert_called_once_with(vol.pvs,'test_descr PV-${index}',
                                                                          dict(tag='val'))
 
 
    def test_lvm_snapshot(self, lvm2, mod_mount):
        vol = self._create_vol(lvm2)
 
        lvm_snap_info = mock.Mock(lv_path='/dev/mapper/data-vol1snap',
                                                          lv_name='vol1snap', vg_name='data')
        lvm2.lvs.side_effect = lambda *args, **kwargs: {'data/vol1snap': lvm_snap_info}
 
        lvm2.reset_mock()
        lvm_snap = vol.lvm_snapshot('lvm_snap_test', '2%')
        lvm2.lvcreate.assert_called_once_with(name='lvm_snap_test',
                                                snapshot='data/vol1', extents='2%')
        lvm2.lvs.assert_called_once_with('data/lvm_snap_test')
        assert type(lvm_snap) == lvm.LvmNativeSnapshot
 
        lvm2.reset_mock()
        lvm_snap = vol.lvm_snapshot()
        lvm2.lvcreate.assert_called_once_with(name='vol1snap',
                                                                snapshot='data/vol1', extents='1%ORIGIN')
        assert type(lvm_snap) == lvm.LvmNativeSnapshot
 
        lvm2.reset_mock()
        lvm_snap = vol.lvm_snapshot(size=300)
        lvm2.lvcreate.assert_called_once_with(name='vol1snap',
                                                                        snapshot='data/vol1', size='300')
        assert type(lvm_snap) == lvm.LvmNativeSnapshot
 
 
    @mock.patch('scalarizr.storage2.volumes.lvm.storage2.filesystem')
    def test_extend_underlying_pvs(self, fs_fn, lvm2, mod_mount):
        lvm2.pvs.side_effect = [{}, {'/dev/sdb': mock.Mock(vg_name='data')}]
        lvm2.NotFound = Exception
        lvm2.vgs.side_effect = lvm2.NotFound
        lvs_returns = [
                lvm2.NotFound,
                {'data/vol1': mock.Mock(lv_path='/dev/mapper/data-vol1', lv_attr='-wi-ao')}
        ]
        def lvs(*args, **kwds):
            ret = lvs_returns.pop(0)
            if isinstance(ret, Exception):
                raise ret
            return ret
        lvm2.lvs.side_effect = lvs
        vol = lvm.LvmVolume(
                name='vol1',
                vg='data',
                pvs=['/dev/sdb'],
                size='98%FREE')
        vol.ensure()
 
        vol.pvs.append('/dev/sdc')
 
        lvm2.pvs.side_effect = [{'/dev/sdb': mock.Mock(vg_name='data')}]*3 + \
                                                         [{'/dev/sdc': mock.Mock(vg_name=None)}]
        lvs_returns.append(
                {'data/vol1': mock.Mock(lv_path='/dev/mapper/data-vol1', lv_attr='-wi-ao')}
        )
        fs = fs_fn.return_value
        fs.features.get.return_value = True
        with mock.patch.object(vol, 'is_fs_created') as f:
            f.return_value = True
            vol.ensure()
            f.assert_called_once_with()
 
        fs_fn.assert_called_once_with('ext3')
        fs.features.get.assert_called_once_with('resizable')
        lvm2.pvcreate.reset_mock
        lvm2.pvcreate.assert_called_with('/dev/sdc')
        lvm2.vgextend.assert_called_with('data', '/dev/sdc')
        lvm2.lvextend.assert_called_once_with('/dev/mapper/data-vol1',
                                                                                  extents='98%FREE')
        fs.resize.assert_called_once_with('/dev/mapper/data-vol1')
 
