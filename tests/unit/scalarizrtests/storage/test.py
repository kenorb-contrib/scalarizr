'''
Created on Nov 25, 2010
 
@author: marat
'''
import unittest
 
from scalarizr.util import system2 as system, firstmatched
from scalarizr.storage import Volume, Snapshot, Storage, StorageError, VolumeProvider,\
        VolumeConfig
from scalarizr.storage.util.loop import mkloop
from scalarizr.storage.eph import EphSnapshot
from scalarizr.storage.fs import FileSystem
from szr_unittest import switch_reset_db
 
import os
import time
from random import randint
 
 
class TestMkloop(unittest.TestCase):
    filename = None
    loop = None
    SIZE = 100
 
    def setUp(self):
        self.loop = None
        self.filename = '/tmp/loop%s' % randint(11, 99)
 
    def tearDown(self):
        if self.loop:
            system('/sbin/losetup -d %s' % self.loop, shell=True)
        if os.path.exists(self.filename):
            os.remove(self.filename)
 
    def test_create_quick(self):
        t1 = time.time()
        self.loop = mkloop(self.filename, size=self.SIZE, quick=True)
        t2 = time.time()
        self.assertTrue(t2 - t1 < 0.1)
        self.assert_size()
 
    def test_create(self):
        t1 = time.time()
        self.loop = mkloop(self.filename, size=self.SIZE, quick=False)
        t2 = time.time()
        self.assertTrue(t2 - t1 > 0.1)
        self.assert_size()
 
    def test_create_on_exiting_file(self):
        system('/bin/dd if=/dev/zero of=%s bs=1M seek=%d count=1' % (self.filename, self.SIZE-1), shell=True)
        self.loop = mkloop(self.filename)
        self.assert_size()
 
    def assert_size(self):
        self.assertEqual(os.path.getsize(self.filename), self.SIZE * 1024 * 1024)
 
class TestResourceManagement(unittest.TestCase):
    mgr = None
    def setUp(self):
        self.mgr = Storage
        self.mgr._fs_drivers = {}
 
    def tearDown(self):
        pass
 
    def test_singleton_fs(self):
        myfs = 'myfs'
        class FSDriver(FileSystem):
            name = myfs
        self.mgr.explore_filesystem(myfs, FSDriver)
        o1 = self.mgr.lookup_filesystem(myfs)
        self.assertTrue(isinstance(o1, FSDriver))
        self.assertEqual(o1, self.mgr.lookup_filesystem(myfs))
 
    def test_lookup_std_fs(self):
        fs = self.mgr.lookup_filesystem('ext4')
        self.assertEqual(fs.name, 'ext4')
 
 
class TestVolume(unittest.TestCase):
    def setUp(self):
        self.device = mkloop('/tmp/testvolume', size=100)
        self.mpoint = '/mnt/storage'
        if not os.path.exists(self.mpoint):
            os.makedirs(self.mpoint)
 
    def tearDown(self):
        if self.device:
            system(('/sbin/losetup', '-d', self.device))
        if os.path.exists(self.mpoint):
            os.removedirs(self.mpoint)
 
    def test(self):
        vol = Volume(self.device, self.mpoint)
 
        # When trying to do filesystem operations without having a fs on volume ValueError raises
        self.assertRaises(StorageError, vol.freeze)
        self.assertRaises(StorageError, vol.unfreeze)
 
        # After creating a filesystem ValueError dissapears
        vol.mkfs('ext3')
        self.assertTrue(vol.fstype, 'ext3')
        vol.freeze()
        vol.unfreeze()
 
        # When volume is mounted without mpoint specify, it gets from volume mpoint property
        self.assertFalse(vol.mounted())
        vol.mount()
        self.assertTrue(vol.mpoint, self.mpoint)
        self.assertTrue(vol.mounted())
 
        # When volume is unmounted, volume mpoint property remains
        vol.umount()
        self.assertEqual(vol.mpoint, self.mpoint)
        self.assertFalse(vol.mounted())
 
        snap = vol.snapshot(description='test snap')
        self.assertTrue(isinstance(snap, Snapshot))
        self.assertEqual(snap.description, 'test snap')
 
    def test_config(self):
        vol = Volume('/dev/smd0', '/mnt/trace', 'xfs')
        cnf = vol.config()
        self.assertTrue(isinstance(cnf, dict))
        self.assertTrue('device' in cnf)
        self.assertEqual(cnf['device'], '/dev/smd0')
        self.assertTrue('type' in cnf)
        self.assertEqual(cnf['type'], 'base')
 
class TestStorageProviders(unittest.TestCase):
    _save_snap_pvd = None
    _save_vol_pvd = None
    _save_pvds = None
 
    class MyPvd(VolumeProvider):
        type = 'myvol'
 
    def setUp(self):
        self._save_pvds = Storage.providers.copy()
        Storage.providers.clear()
        self._save_snap_pvd, Storage.default_snap_provider = Storage.default_snap_provider, None
        self._save_vol_pvd, Storage.default_vol_provider = Storage.default_vol_provider, None
 
    def tearDown(self):
        Storage.providers = self._save_pvds
        Storage.default_snap_provider = self._save_snap_pvd
        Storage.default_vol_provider = self._save_vol_pvd
 
    def test_explore_provider(self):
        Storage.explore_provider(self.MyPvd)
        self.assertFalse(Storage.default_snap_provider)
        self.assertFalse(Storage.default_vol_provider)
        self.assertTrue(isinstance(Storage.lookup_provider(self.MyPvd.type), self.MyPvd))
 
    def test_explore_default_provider(self):
        Storage.explore_provider(self.MyPvd, True)
        self.assertFalse(Storage.default_snap_provider)
        self.assertEqual(Storage.default_vol_provider, self.MyPvd.type)
 
        self.assertTrue(isinstance(Storage.lookup_provider(self.MyPvd.type), self.MyPvd))
        self.assertTrue(isinstance(Storage.lookup_provider(), self.MyPvd))
 
    def test_explore_default_provider2(self):
        Storage.explore_provider(self.MyPvd, default_for_snap=True)
        self.assertEqual(Storage.default_snap_provider, self.MyPvd.type)
        self.assertFalse(Storage.default_vol_provider)
 
        self.assertTrue(isinstance(Storage.lookup_provider(self.MyPvd.type), self.MyPvd))
        self.assertTrue(isinstance(Storage.lookup_provider(None, True), self.MyPvd))
 
    def test_snapshot_factory(self):
        Storage.providers = self._save_pvds
        pvd = Storage.lookup_provider('eph')
        snap = pvd.snapshot_factory('hom')
        self.assertEqual(snap.type, 'eph')
        self.assertEqual(snap.description, 'hom')
        self.assertTrue(isinstance(snap, EphSnapshot))
 
 
class TestStorageCreate(unittest.TestCase):
    class Vol(Volume):
        def __init__(self, *args, **kwargs):
            if kwargs:
                for k, v in kwargs.items():
                    setattr(self, k, v)
            Volume.__init__(self, *args, **kwargs)
 
    class VolPvd(VolumeProvider):
        type = 'myvol'
 
    def setUp(self):
        self.VolPvd.vol_class = self.Vol
        Storage.explore_provider(self.VolPvd)
 
    def tearDown(self):
        pass
 
    def test_create_by_string_args(self):
        vol = Storage.create('/dev/sdb')
        self.assertEqual(vol.devname, '/dev/sdb')
 
    def test_create_over_disk(self):
        vol = Storage.create(type='myvol', device='/dev/lvolume', disk='/dev/sdb')
        self.assertEqual(vol.disk.devname, '/dev/sdb')
 
        vol = Storage.create(
                type='myvol',
                device='/dev/ldevice2',
                disk=dict(
                        type='myvol',
                        device='/dev/sdb',
                        param1='value1'
                )
        )
        self.assertEqual(vol.disk.devname, '/dev/sdb')
        self.assertEqual(vol.disk.param1, 'value1')
 
    def test_create_vol_container(self):
        vol = Storage.create(
                type='myvol',
                device='/dev/gp0',
                disks=('/dev/sdb', dict(type='myvol', device='/dev/sdd'))
        )
        self.assertEqual(len(vol.disks), 2)
        self.assertEqual(vol.disks[0].devname, '/dev/sdb')
        self.assertEqual(vol.disks[1].devname, '/dev/sdd')
        self.assertEqual(vol.disks[1].type, 'myvol')
 
    def test_create_from_snapshot(self):
        vol = Storage.create(
                snapshot=dict(
                        type='base',
                        device='/dev/sdb',
                        mpoint='/mnt/dbstorage',
                        fstype='xfs'
                )
        )
        self.assertEqual(vol.devname, '/dev/sdb')
        self.assertEqual(vol.mpoint, '/mnt/dbstorage')
 
        vol = Storage.create(
                device='/dev/sdd',
                snapshot=dict(
                        type='myvol',
                        device='/dev/lvol',
                        param1='value1',
                        param2='value2'
                )
        )
        self.assertEqual(vol.devname, '/dev/sdd')
        self.assertEqual(vol.type, 'myvol')
        self.assertEqual(vol.param1, 'value1')
 
class SnapshotFieldsTest(unittest.TestCase):
    def test_base_volume(self):
        device = '/dev/sdo'
        mpoint = '/mnt/media-server-flvs'
        fstype = 'ext4'
 
        vol = Storage.create(device=device, mpoint=mpoint, fstype=fstype)
        snap = vol.snapshot('snap #00')
 
        snap_cnf = snap.config()
        vol_cnf = vol.config()
        self.assertEqual(snap_cnf['type'], vol.type)
        self.assertEqual(snap_cnf['mpoint'], vol.mpoint)
        self.assertEqual(snap_cnf['device'], vol.device)
 
 
    def test_with_ignores(self):
        class VolConfig(VolumeConfig):
            vg = None
            base64_whatever = None
            only_in_volume_config = None
            only_in_snapshot_config = None
 
        class Vol(VolConfig, Volume):
            _ignores = ('only_in_snapshot_config')
 
        class Snap(VolConfig, Snapshot):
            _ignores = ('only_in_volume_config',)
 
        class VolPvd(VolumeProvider):
            type = 'mimimi'
            vol_class = Vol
            snap_class = Snap
        Storage.explore_provider(VolPvd)
 
 
        vol = Storage.create(
                type='mimimi',
                device='/dev/sdo',
                vg='vg0',
                only_in_volume_config='4u',
                only_in_snapshot_config='4s'
        )
        snap = vol.snapshot()
        snap_cnf = snap.config()
        vol_cnf = vol.config()
 
        self.assertFalse('only_in_volume_config' in snap_cnf)
        self.assertEqual(snap_cnf['vg'], 'vg0')
 
        self.assertFalse('only_in_snapshot_config' in vol_cnf)
        self.assertTrue(vol_cnf['base64_whatever'] is None)
 
class VolumeTableTest(unittest.TestCase):
    def setUp(self):
        Storage.maintain_volume_table = True
        switch_reset_db()
 
    def tearDown(self):
        pass
 
    def test_1(self):
        v1 = Storage.create(device='/dev/sdo')
        v2 = Storage.create(device='/dev/sdm')
        table = Storage.volume_table()
        self.assertEqual(len(table), 2)
        v1row = firstmatched(lambda row: row['device'] == '/dev/sdo', table)
        self.assertTrue(v1row)
        self.assertEqual(v1row['volume_id'], v1.id)
        self.assertEqual(v1row['device'], v1.device)
        self.assertEqual(v1row['type'], v1.type)
        self.assertEqual(v1row['state'], 'attached')
 
        v2.detach()
        table = Storage.volume_table()
        self.assertEqual(len(table), 2)
        v2row = firstmatched(lambda row: row['device'] == '/dev/sdm', table)
        self.assertEqual(v2row['state'], 'detached')
 
 
if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()
 
