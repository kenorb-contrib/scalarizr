import os
import unittest
 
import mock
 
from scalarizr.storage2.volumes import eph
 
volume = {
        'snap_backend': {
                'path': 's3://scalr-3414-ap-northeast-1/data-bundles/5071/percona'
        },
        'vg': 'percona',
        'fs_created': True,
        'fstype': 'xfs',
        'lvm_group_cfg': None,
        'mpoint': '/mnt/dbstorage',
        'device': '/dev/mapper/percona-data',
        'disk': {
                'fs_created': None,
                'fstype': None,
                'mpoint': None,
                'device': '/dev/sda2',
                'type': 'base',
                'id': 'base-vol-1fdb2bf0'
        },
        'type': 'eph',
        'id': 'eph-vol-2a3bd1c8',
        'size': '80%'
}
 
snapshot = {
        'snap_backend' : {
                'path': 's3://scalr-3414-ap-northeast-1/data-bundles/5071/percona'
        },
        'vg': 'percona',
        'description': 'MySQL data bundle (farm: 5071 role: percona64-centos6)',
        'snap_strategy': 'data',
        'fs_created': True,
        'fstype': 'xfs',
        'lvm_group_cfg': None,
        'mpoint': '/mnt/dbstorage',
        'device': '/dev/mapper/percona-data',
        'path': 's3://scalr-3414-ap-northeast-1/data-bundles/5071/percona/eph-snap-fe9ac9ce.manifest.ini',
        'disk': {
                'fs_created': None,
                'fstype': None,
                'mpoint': None,
                'device': '/dev/sda2',
                'type': 'base',
                'id': 'base-vol-1fdb2bf0'
        },
        'type': 'eph',
        'id': 'eph-snap-fe9ac9ce',
}
 
@mock.patch('scalarizr.storage2.volumes.eph.storage2')
class EphVolumeTest(unittest.TestCase):
 
 
    def test_10_compatible(self, storage2):
        vol = eph.EphVolume(**volume)
        self.assertFalse(hasattr(vol, 'snap_backend'))
        self.assertEqual(vol.cloudfs_dir, volume['snap_backend']['path'] + '/')
 
 
    def test_ensure(self, storage2):
        vol = eph.EphVolume(**volume)
        vol.ensure()
 
        storage2.volume.assert_called_once_with(
                type='lvm', pvs=[volume['disk']], size='%sVG' % volume['size'],
                vg=volume['vg'], name='data'
        )
 
        lvm_vol = storage2.volume.return_value
        lvm_vol.ensure.assert_called_once_with()
        self.assertEqual(lvm_vol.device, vol.device)
 
    @mock.patch('scalarizr.linux.coreutils.df')
    @mock.patch('scalarizr.storage2.volumes.eph.os.rmdir')
    @mock.patch('scalarizr.storage2.volumes.eph.tempfile')
    @mock.patch('scalarizr.storage2.volumes.eph.cloudfs')
    @mock.patch('scalarizr.storage2.volumes.eph.EphVolume.mkfs')
    def test_ensure_from_snap(self, mkfs, cfs, tfile, rmdir, df, storage2):
        storage2.StorageError = Exception
        snap = storage2.snapshot.return_value
        snap.size = 5000
        disk = mock.MagicMock()
        vol = eph.EphVolume(type='eph', snap=snapshot, size='80%',
                                                vg='mongo', disk=disk)
 
        tmp_mpoint = 'test_temp'
        df.return_value = [mock.MagicMock(), mock.MagicMock(),
                                           mock.MagicMock(mpoint=tmp_mpoint, free=10000)]
        tfile.mkdtemp.return_value = tmp_mpoint
        with mock.patch.multiple(vol, mount=mock.DEFAULT, umount=mock.DEFAULT):
            vol.ensure()
            vol.mount.assert_called_once_with()
            vol.umount.assert_called_once_with()
 
 
        storage2.snapshot.assert_called_once_with(snapshot)
        snap = storage2.snapshot.return_value
        storage2.volume.assert_called_once_with(
                pvs=[disk], size='80%VG', vg='mongo', name='data', type='lvm'
        )
        lvm_vol = storage2.volume.return_value
        lvm_vol.ensure.assert_called_once_with()
        self.assertEqual(lvm_vol.device, vol.device)
        mkfs.assert_called_once_with()
 
        tfile.mkdtemp.assert_called_once_with()
 
        cfs.LargeTransfer.assert_called_once_with(snap.path, tmp_mpoint + '/')
        tr = cfs.LargeTransfer.return_value
 
        tr.run.assert_called_once
        rmdir.assert_called_once_with(tmp_mpoint)
 
        self.assertEqual(vol.mpoint, None)
 
 
        """ Cleanup on transfer failure """
        tr.run.side_effect = Exception
        rmdir.reset_mock()
        vol = eph.EphVolume(type='eph', snap=snapshot, size='80%',
                                                vg='mongo', disk=disk)
 
        with mock.patch.multiple(vol, mount=mock.DEFAULT, umount=mock.DEFAULT):
            self.assertRaises(storage2.StorageError, vol.ensure)
            vol.mount.assert_called_once_with()
            vol.umount.assert_called_once_with()
 
        rmdir.assert_called_once_with(tmp_mpoint)
 
        """ Snapshot size bigger than free space"""
        tr.reset_mock()
        snap.size = 20000
        vol = eph.EphVolume(type='eph', snap=snapshot, size='80%',
                                                vg='mongo', disk=disk)
        with mock.patch.multiple(vol, mount=mock.DEFAULT, umount=mock.DEFAULT):
            self.assertRaisesRegexp(storage2.StorageError,
                                                            'Not enough free space',
                                                            vol.ensure)
        self.assertFalse(tr.mock_calls)
 
 
    def test_detach(self, storage2):
        vol = eph.EphVolume(**volume)
        vol.ensure()
 
        with mock.patch.object(vol, 'umount') as um:
            vol.detach(force=True)
            um.assert_called_once_with()
            vol._lvm_volume.detach.assert_called_once_with(force=True)
 
 
    def test_destroy(self, storage2):
        vol = eph.EphVolume(**volume)
        vol.ensure()
 
        with mock.patch.object(vol, 'umount') as um:
            vol.destroy(force=True)
            um.assert_called_once_with()
            vol._lvm_volume.destroy.assert_called_once_with(force=True)
 
        assert vol.device is None
 
 
    @mock.patch('scalarizr.storage2.volumes.eph.os.rmdir')
    @mock.patch('scalarizr.linux.coreutils.df')
    @mock.patch('scalarizr.storage2.volumes.eph.tempfile')
    @mock.patch('scalarizr.storage2.volumes.eph.cloudfs')
    def test_snapshot(self, cfs, tmpf, df, rmdir, storage2):
        vol = eph.EphVolume(**volume)
        vol.ensure()
        storage2.reset_mock()
 
        lvm_vol = vol._lvm_volume
 
        tmp_mpoint = 'temporary_dir'
        tmpf.mkdtemp.return_value = tmp_mpoint
 
        snap = mock.MagicMock(id='snapshot_id')
        lvm_snap_vol = mock.MagicMock()
        lvm_snap_vol.mpoint = tmp_mpoint
 
        storage2.snapshot.return_value = snap
        storage2.volume.return_value = lvm_snap_vol
 
        df.return_value = [mock.MagicMock(mpoint='random'),
                                           mock.MagicMock(mpoint=tmp_mpoint, used=400000)]
 
        final_snap = vol.snapshot()
 
        lvm_vol.lvm_snapshot.assert_called_once_with(size='100%FREE')
        lvm_snap = lvm_vol.lvm_snapshot.return_value
        storage2.snapshot.assert_called_once_with(type='eph')
 
        snap_path = os.path.join(volume['snap_backend']['path'],
                                                         'snapshot_id.manifest.ini')
 
        self.assertEqual(snap_path, final_snap.path)
        tmpf.mkdtemp.assert_called_once_with()
        storage2.volume.assert_called_once_with(device=lvm_snap.device,
                                                                                        mpoint=tmp_mpoint)
 
        lvm_snap_vol.ensure.assert_called_once_with(mount=True)
        df.assert_called_once_with()
        self.assertEqual(final_snap.size, 400000)
 
        cfs.LargeTransfer.assert_called_once_with(src='temporary_dir/',
                                dst=snap_path, tar_it=True, gzip_it=True, tags=None)
 
        cfs.LargeTransfer.return_value.run.assert_called_once_with()
 
        lvm_snap_vol.umount.assert_called_once_with()
        lvm_snap.destroy.assert_called_once_with()
        rmdir.assert_called_once_with(tmp_mpoint)
 
 
class EphSnapshotTest(unittest.TestCase):
 
    @mock.patch('__builtin__.open')
    @mock.patch('scalarizr.storage2.volumes.eph.tempfile')
    @mock.patch('scalarizr.storage2.volumes.eph.os.path.join')
    @mock.patch('scalarizr.storage2.volumes.eph.os.remove')
    @mock.patch('scalarizr.storage2.volumes.eph.cloudfs')
    @mock.patch('scalarizr.storage2.volumes.eph.metaconf')
    def test_destroy_snapshot(self, metaconf, cloudfs, rm, join, tmpf, open):
        snap = eph.EphSnapshot(type='eph', size=400000, path='http://test')
        c = metaconf.Configuration.return_value
        chunk_names = [mock.MagicMock() for x in range(10)]
        chunk_paths = [mock.MagicMock() for x in range(10)]
        c.children.return_value = chunk_names
        join.side_effect = chunk_paths
 
        snap.destroy()
 
        cloudfs.cloudfs.assert_called_once_with('http')
        storage_drv = cloudfs.cloudfs.return_value
 
        tmpf.mktemp.assert_called_once_with()
        manifest = tmpf.mktemp.return_value
 
        open.assert_called_once_with(manifest, 'w')
        f = open.return_value.__enter__.return_value
        storage_drv.get.assert_called_once_with('http://test', f)
 
        metaconf.Configuration.assert_called_once_with('ini')
        c.read.assert_called_once_with(manifest)
 
        c.children.assert_called_once_with('./chunks/')
 
        base_url = os.path.dirname('http://test')
 
        join_calls = [mock.call(base_url, x) for x in chunk_names]
        self.assertEqual(join_calls, join.mock_calls)
 
        drv_del_calls = [mock.call(x) for x in chunk_paths]
        drv_del_calls.append(mock.call('http://test'))
        self.assertEqual(storage_drv.delete.mock_calls, drv_del_calls)
 
        rm.assert_called_once_with(manifest)
 
