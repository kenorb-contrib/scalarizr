__author__ = 'Nick Demyanchuk'
 
import sys
import mock
import unittest
 
from scalarizr.storage2.volumes import raid
from scalarizr.linux import mount
 
@mock.patch('__builtin__.open')
@mock.patch('scalarizr.storage2.volumes.raid.base64')
@mock.patch('scalarizr.storage2.volumes.raid.tempfile')
@mock.patch('scalarizr.storage2.volumes.raid.os.remove')
@mock.patch('scalarizr.storage2.volumes.raid.os.path.exists')
@mock.patch('scalarizr.storage2.volumes.raid.storage2')
@mock.patch('scalarizr.storage2.volumes.raid.lvm2')
@mock.patch('scalarizr.storage2.volumes.raid.mdadm')
class RaidVolumeTest(unittest.TestCase):
 
    def test_ensure_new(self, mdadm, lvm2, storage2, exists, rm, tfile,
                                            b64, op):
        disks = [mock.MagicMock(type='loop', device='/dev/loop%s' % x) for x in range(2)]*2
        storage2.volume.side_effect = disks
        disks_devices = [d.device for d in disks[:2]]
        mdadm.findname.return_value = '/dev/md1'
 
        lvm2.pvs.return_value.__getitem__.return_value.pv_uuid = 'pvuuid'
        lvm2.lvcreate.return_value = ('Logical volume "lvol0" created', '', 0)
 
        raid_vol = raid.RaidVolume(type='raid', vg='test', level=1,
                                                           disks=[dict(type='loop', size=0.01)]*2)
 
        raid_vol.ensure()
 
        for disk in disks:
            disk.ensure.assert_called_once_with()
 
        mdadm.findname.assert_called_once_with()
 
        calls = [mock.call('create', '/dev/md1', *disks_devices, force=True, level=1,
                                assume_clean=True, raid_devices=2, metadata='default'),
                        mock.call('misc', None, '/dev/md1', wait=True, raise_exc=False)]
        self.assertSequenceEqual(mdadm.mdadm.mock_calls, calls)
 
        lvm2.pvcreate.assert_called_once_with('/dev/md1', force=True)
        lvm2.pvs.assert_called_once_with('/dev/md1')
        lvm2.vgcreate.assert_called_once_with('test', '/dev/md1')
        lvm2.lvcreate.assert_called_once_with('test', extents='100%FREE')
        lvm2.backup_vg_config.assert_called_once_with('test')
 
        self.assertEqual(raid_vol.raid_pv, '/dev/md1')
        self.assertEqual(raid_vol.lvm_group_cfg,
                                         lvm2.backup_vg_config.return_value)
        self.assertEqual(raid_vol.pv_uuid, 'pvuuid')
        self.assertEqual(raid_vol.disks, disks[:2])
        self.assertEqual(raid_vol.level, 1)
 
 
 
    def test_ensure_from_snapshot_autodetect(self, mdadm, lvm2, storage2,
                                                                                     exists, rm, tfile, b64, op):
        exists.side_effect = [True]
        disks_snaps = [dict(type='loop', size=0.01)]*2
        disks =  [mock.MagicMock(), mock.MagicMock()]
        storage2.snapshot.side_effect = disks
        storage2.volume.side_effect = disks
        mdadm.mdfind.return_value = '/dev/md2'
        lv_info = mock.MagicMock()
        lvm2.lvs.return_value = {'test': lv_info}
 
        raid_vol = raid.RaidVolume(type='raid',
                                                           snap=dict(vg='test', level=1,
                                                           disks=disks_snaps,
                                                           pv_uuid='pvuuid',
                                                           lvm_group_cfg='base64_encoded_cfg'))
 
        raid_vol.ensure()
        self.assertSequenceEqual(raid_vol.disks, disks)
 
        for d in raid_vol.disks:
            assert d.ensure.call_count == 1
        mdadm.mdfind.assert_called_once_with(
                                        *[r.device for r in raid_vol.disks])
        lvm2.pvs.assert_called_once_with('/dev/md2')
 
        tfile.mktemp.assert_called_once_with()
        tmpfile = tfile.mktemp.return_value
        op.assert_called_once_with(tmpfile, 'w')
        f = op.return_value.__enter__.return_value
        f.write.assert_called_once_with(b64.b64decode.return_value)
        b64.b64decode.assert_called_once_with('base64_encoded_cfg')
        lvm2.vgcfgrestore.assert_called_once_with('test', file=tmpfile)
        rm.assert_called_once_with(tmpfile)
 
        lvm2.lvs.assert_called_once_with('test')
        lvm2.lvpath.assert_called_once_with('test', lv_info.lv_name)
        lvm2.vgchange.assert_called_once_with('test', available='y')
 
        exists.assert_called_with(lvm2.lvpath.return_value)
 
 
    def test_ensure_from_snapshot_raid_1_10(self, mdadm, lvm2, storage2,
                                                                                    exists, rm, tfile, b64, op):
        for lvl in (1, 10):
            disks_snaps = [dict(type='loop', size=0.01)]*2
            disks =  [mock.MagicMock(), mock.MagicMock()]
            storage2.snapshot.side_effect = disks
            storage2.volume.side_effect = disks
            storage2.StorageError = Exception
            mdadm.mdfind.side_effect = Exception()
 
            raid_vol = raid.RaidVolume(type='raid',
                                                               snap=dict(vg='test', level=lvl,
                                                             disks=disks_snaps,
                                                             pv_uuid='pvuuid',
                                                             lvm_group_cfg='base64_encoded_cfg'))
 
            raid_vol.ensure()
 
            self.assertSequenceEqual(raid_vol.disks, disks)
            for d in raid_vol.disks:
                assert d.ensure.call_count == 1
            disks_devices = [r.device for r in raid_vol.disks]
            mdadm.mdfind.assert_called_once_with(
                    *disks_devices)
 
            mdadm.findname.assert_called_once_with()
 
            raid_device = mdadm.findname.return_value
 
            calls = [mock.call('assemble', raid_device, *disks_devices),
                                    mock.call('misc', None, raid_device, wait=True, raise_exc=False)]
            self.assertSequenceEqual(mdadm.mdadm.mock_calls, calls)
            mdadm.reset_mock()
            storage2.reset_mock()
 
 
    def test_ensure_from_snapshot_raid_0_5(self, mdadm, lvm2, storage2,
                                                                               exists, rm, tfile, b64, op):
        for lvl in (0, 5):
            disks_snaps = [dict(type='loop', size=0.01)]*2
            disks = [mock.MagicMock(), mock.MagicMock()]
            storage2.snapshot.side_effect = disks
            storage2.volume.side_effect = disks
            storage2.StorageError = Exception
            mdadm.mdfind.side_effect = Exception()
 
            raid_vol = raid.RaidVolume(type='raid',
                                                               snap=dict(
                                                                            vg='test', level=lvl,
                                                                            disks=disks_snaps,
                                                                            pv_uuid='pvuuid',
                                                                            lvm_group_cfg='base64_encoded_cfg'
                                                               ))
            raid_vol.ensure()
 
            self.assertSequenceEqual(raid_vol.disks, disks)
            for d in raid_vol.disks:
                assert d.ensure.call_count == 1
            disks_devices = [r.device for r in raid_vol.disks]
            mdadm.mdfind.assert_called_once_with(
                    *disks_devices)
 
            mdadm.findname.assert_called_once_with()
 
            raid_device = mdadm.findname.return_value
            calls = [mock.call('assemble', raid_device, *disks_devices),
                                    mock.call('misc', None, raid_device, wait=True, raise_exc=False)]
            self.assertSequenceEqual(mdadm.mdadm.mock_calls, calls)
            mdadm.reset_mock()
            storage2.reset_mock()
 
 
 
    def test_ensure_from_snap_disks_destroy_on_fail(self,
                            mdadm, lvm2, storage2, exists, rm, tfile, b64, op):
 
        disks_snaps = [dict(type='loop', size=0.01)]*2
        snaps = [mock.MagicMock(), mock.MagicMock()]
        snaps[1].restore.side_effect = Exception
 
        storage2.snapshot.side_effect = snaps
        raid_vol = raid.RaidVolume(type='raid',
                                                           snap=dict(
                                                                   vg='test', level=1,
                                                                   disks=disks_snaps,
                                                                   pv_uuid='pvuuid',
                                                                   lvm_group_cfg='base64_encoded_cfg'
                                                           ))
        self.assertRaises(Exception, raid_vol.ensure)
 
        snaps[0].restore.return_value.destroy.assert_called_once_with()
 
 
    def test_ensure_from_snap_pv_not_detected(self, mdadm, lvm2,
                                                    storage2, exists, rm, tfile, b64, op):
        disks_snaps = [dict(type='loop', size=0.01)]*2
        lvm2.pvs.side_effect = Exception
        tempfile_mock = mock.MagicMock()
        tfile.mktemp.return_value = tempfile_mock
        raid_vol = raid.RaidVolume(type='raid',
                                                           snap=dict(
                                                                   vg='test', level=1,
                                                                   disks=disks_snaps,
                                                                   pv_uuid='pvuuid',
                                                                   lvm_group_cfg='base64_encoded_cfg'
                                                           ))
        raid_vol.ensure()
        raid_dev = mdadm.mdfind.return_value
        lvm2.pvs.assert_called_once_with(raid_dev)
        lvm2.pvcreate.assert_called_once_with(raid_dev, uuid='pvuuid',
                                                                                  restorefile=tempfile_mock)
 
 
    def test_ensure_existed(self, mdadm, lvm2,
                                                    storage2, exists, rm, tfile, b64, op):
        disks = [mock.MagicMock() for _ in xrange(4)]
        storage2.volume.side_effect = disks
        raid_vol = raid.RaidVolume(type='raid',
                                                vg='test', level=1,
                                                disks=disks, pv_uuid='pvuuid',
                                                lvm_group_cfg='base64_encoded_cfg'
        )
        storage2.volume.side_effect = disks
        raid_vol.ensure()
 
        mdadm.reset_mock()
        lvm2.reset_mock()
 
        map(mock.Mock.reset_mock, raid_vol.disks)
        storage2.volume.side_effect = disks
        raid_vol.ensure()
 
        self.assertSequenceEqual(disks, raid_vol.disks)
        for d in raid_vol.disks:
            d.ensure.assert_called_once_with()
 
        disks_devices = [r.device for r in raid_vol.disks]
        mdadm.mdfind.assert_called_once_with(*disks_devices)
        raid_dev = mdadm.mdfind.return_value
        lvm2.pvs.assert_called_once_with(raid_dev)
        tmp_file = tfile.mktemp.return_value
 
        lvm2.lvs.assert_called_once_with('test')
 
        lvm2.vgcfgrestore.assert_called_once_with('test', file=tmp_file)
        lvm2.vgchange.assert_called_once_with('test', available='y')
 
 
    @mock.patch.object(mount, 'umount')
    def test_detach(self, um, mdadm, lvm2, storage2,
                                    exists, rm, tfile, b64, op):
        lvm2.lvcreate.return_value = ('Logical volume "lvol0" created', '', 0)
        raid_vol = raid.RaidVolume(type='raid', vg='test', level=1,
                                                           disks=[dict(type='loop', size=0.01)]*2)
        disks =  [mock.MagicMock() for _ in xrange(2)]*2
        storage2.volume.side_effect = disks
        raid_vol.ensure()
        lvm2.reset_mock()
        mdadm.reset_mock()
 
        raid_vol.detach()
 
        lvm2.backup_vg_config.assert_called_once_with('test')
        lvm2.vgremove.assert_called_once_with('test', force=True)
        raid_device = mdadm.findname.return_value
        calls = [mock.call('misc', None, raid_device, stop=True, force=True),
                         mock.call('manage', None, raid_device, remove=True, force=True)
        ]
        assert mdadm.mdadm.mock_calls == calls
        rm.assert_called_once_with(raid_device)
        for d in disks:
            d.detach.assert_called_once_with(force=False)
 
        assert raid_vol.raid_pv is None
 
 
    @mock.patch.object(mount, 'umount')
    def test_destroy(self, um, mdadm, lvm2, storage2,
                                     exists, rm, tfile, b64, op):
        lvm2.lvcreate.return_value = ('Logical volume "lvol0" created', '', 0)
        raid_vol = raid.RaidVolume(type='raid', vg='test', level=1,
                                                           disks=[dict(type='loop', size=0.01)]*2)
        disks =  [mock.MagicMock() for _ in xrange(2)]*2
        storage2.volume.side_effect = disks
 
        raid_vol.ensure()
 
        lvm2.reset_mock()
        mdadm.reset_mock()
 
        raid_vol.destroy()
        for d in disks:
            d.detach.assert_called_once_with(force=False)
            assert d.destroy.call_count == 0
 
        self.assertSequenceEqual(raid_vol.disks, disks[:2])
 
 
 
    @mock.patch.object(mount, 'umount')
    def test_destroy_remove_disks(self, um, mdadm, lvm2, storage2,
                                     exists, rm, tfile, b64, op):
        lvm2.lvcreate.return_value = ('Logical volume "lvol0" created', '', 0)
        raid_vol = raid.RaidVolume(type='raid', vg='test', level=1,
                                                           disks=[dict(type='loop', size=0.01)]*2)
        disks = [mock.MagicMock() for _ in xrange(2)]*2
        storage2.volume.side_effect = disks
 
        raid_vol.ensure()
 
        lvm2.reset_mock()
        mdadm.reset_mock()
 
        storage2.volume.side_effect = disks
 
        raid_vol.destroy(force=True, remove_disks=True)
        for d in disks:
            d.detach.assert_called_once_with(force=True)
            d.destroy.assert_called_once_with(force=True)
 
        assert raid_vol.disks == []
 
 
    @mock.patch('scalarizr.storage2.volumes.raid.coreutils')
    def test_snapshot(self, coreutils, mdadm, lvm2, storage2,
                                      exists, rm, tfile, b64, op):
        lvm2.lvcreate.return_value = ('Logical volume "lvol0" created', '', 0)
        raid_vol = raid.RaidVolume(type='raid', vg='test', level=1,
                                                           disks=[dict(type='loop', size=0.01)]*2)
        disks =  [mock.MagicMock(), mock.MagicMock()]
        storage2.volume.side_effect = disks
 
        raid_vol.ensure()
 
        mdadm.reset_mock()
        lvm2.reset_mock()
 
        s = raid_vol.snapshot(description='descr', tags={'t': 'v'})
        coreutils.sync.assert_called_once_with()
 
        calls = [mock.call('suspend', raid_vol.device),
                         mock.call('resume', raid_vol.device)]
 
        self.assertSequenceEqual(lvm2.dmsetup.mock_calls, calls)
        storage2.concurrent_snapshot.assert_called_once_with(
                volumes=disks, description=mock.ANY, tags={'t': 'v'}
        )
        lvm2.backup_vg_config.assert_called_once_with('test')
        storage2.snapshot.assert_called_once_with(
                type='raid', disks=storage2.concurrent_snapshot.return_value,
                lvm_group_cfg=lvm2.backup_vg_config.return_value,
                level=1, pv_uuid=raid_vol.pv_uuid, vg='test'
        )
 
        assert s is storage2.snapshot.return_value
 
 
    @mock.patch('scalarizr.storage2.volumes.raid.coreutils')
    def test_snapshot_resume_lvm_if_failed(self, coreutils, mdadm, lvm2,
                                                                    storage2, exists, rm, tfile, b64, op):
        lvm2.lvcreate.return_value = ('Logical volume "lvol0" created', '', 0)
        raid_vol = raid.RaidVolume(type='raid', vg='test', level=1,
                                                           disks=[dict(type='loop', size=0.01)]*2)
        disks =  [mock.MagicMock(), mock.MagicMock()]
        storage2.volume.side_effect = disks
 
        raid_vol.ensure()
 
        mdadm.reset_mock()
        lvm2.reset_mock()
 
        storage2.concurrent_snapshot.side_effect = Exception
 
        self.assertRaises(Exception, raid_vol.snapshot)
 
        calls = [mock.call('suspend', raid_vol.device),
                         mock.call('resume', raid_vol.device)]
 
        self.assertSequenceEqual(lvm2.dmsetup.mock_calls, calls)
 
 
class RaidVolumeTest2(unittest.TestCase):
 
    @mock.patch('scalarizr.storage2.volumes.raid.mdadm')
    @mock.patch('scalarizr.storage2.volumes.raid.lvm2')
    def test_replace_disk(self, lvm2, mdadm):
        lvm2.lvcreate.return_value = ('Logical volume "lvol0" created', '', 0)
        raid_vol = raid.RaidVolume(type='raid', vg='test', level=1,
                disks=[dict(type='loop', size=0.01)]*2)
        raid_vol.replace_disk(0, {'device':'/dev/loop0', 'id':'vol-987654321'})
 
        with mock.patch('scalarizr.storage2.volumes.base.Base._genid') as mock_genid:
            mock_genid.return_value = None
            raid_vol = raid.RaidVolume(type='raid', vg='test', level=1,
                    disks=[dict(type='loop', size=0.01)]*2)
            self.assertRaises(Exception, raid_vol.replace_disk, 0, {'device':'/dev/loop0'})
 
 
