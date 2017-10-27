'''
Created on Dec 3, 2010
 
@author: marat
'''
import unittest
 
from scalarizr.util import system2 as system, wait_until
from scalarizr.storage import Storage, Snapshot
from scalarizr.storage.util.loop import mkloop
 
import os
from random import randint
import shutil
import logging
 
 
class TestEphStorageCreate(unittest.TestCase):
    filename = None
    device = None
    vol = None
 
    def setUp(self):
        self.filename = '/tmp/pv%s' % randint(11, 99)
        self.device = mkloop(self.filename, size=50, quick=True)
        self.mpoint = '/mnt/ephstorage'
        if not os.path.exists(self.mpoint):
            os.makedirs(self.mpoint)
 
    def tearDown(self):
        if self.vol:
            self.vol.destroy()
        if self.device:
            system(('/sbin/losetup', '-d', self.device))
        if os.path.exists(self.filename):
            os.remove(self.filename)
        os.removedirs(self.mpoint)
 
    def test_1(self):
        self.vol = vol = Storage.create(
                type = 'eph',
                disk = self.device,
                vg = 'dbstorage',
                fstype = 'ext3',
                snap_backend='my://secretphase/backups'
        )
 
        self.assertEqual(vol.type, 'eph')
        self.assertTrue(os.path.exists(vol.devname))
        self.assertEqual(vol.disk.devname, self.device)
        self.assertTrue(os.path.exists(vol.tranzit_vol.devname))
 
        config = self.vol.config()
        self.assertEqual(config['type'], 'eph')
        self.assertTrue(config['size'] is not None)
        self.assertTrue(config['snap_backend'] is not None)
        self.assertTrue(isinstance(config['disk'], dict))
 
        # Reinitialize storage from config
        eph = Storage.create(config)
        self.assertEqual(eph.device, self.vol.device)
 
 
class TestEphStorageSnapshot(unittest.TestCase):
 
    def setUp(self):
        self.filenames = []
        self.devices = []
        self.mpoints = ('/mnt/storage', '/mnt/snapshot', '/mnt/storage2')
        self.vols = [None, None, None]
        for i in range(3):
            self.filenames.append('/tmp/pv%s' % randint(11, 99))
            self.devices.append(mkloop(self.filenames[i], size=100, quick=False))
            if not os.path.exists(self.mpoints[i]):
                os.makedirs(self.mpoints[i])
 
    def tearDown(self):
        for vol in self.vols:
            if vol:
                vol.destroy()
        for device in self.devices:
            system(('/sbin/losetup', '-d', device))
        for file in self.filenames:
            os.remove(file)
        for mpoint in self.mpoints:
            os.rmdir(mpoint)
 
    def test_1(self):
        class TransferMock(object):
            SCHEMA = 'file://'
            def __init__(self):
                self._logger = logging.getLogger(__name__)
                pass
 
            def upload(self, files, remote_dst):
                remote_path = os.path.normpath(remote_dst[len(self.SCHEMA):])
                ret = []
                for file in files:
                    self._logger.debug('Copy %s -> %s/', file, remote_path)
                    shutil.copy(file, remote_path)
                    ret.append('file://%s/%s' % (remote_path, os.path.basename(file)))
                print system(('ls', '-la', remote_path))[0]
                return tuple(ret)
 
            def download(self, remote_files, dst, recursive=False):
                if isinstance(remote_files, basestring):
                    remote_files = (remote_files,)
                files = list(os.path.normpath(path[len(self.SCHEMA):]) for path in remote_files)
 
                ret = []
                for file in files:
                    self._logger.debug('Copy %s -> %s/', file, dst)
                    shutil.copy(file, dst)
                    ret.append(os.path.join(dst, os.path.basename(file)))
                return ret
 
 
        Storage.lookup_provider('eph')._snap_pvd._transfer = TransferMock()
 
        # Create snapshot strage volume (Remote storage emulation)
        self.vols[1] = Storage.create(
                device=self.devices[1],
                mpoint=self.mpoints[1],
                fstype='ext3'
        )
        self.vols[1].mkfs()
        self.vols[1].mount()
 
 
 
        # Create and mount EPH storage
        self.vols[0] = Storage.create(
                type='eph',
                disk=self.devices[0],
                vg='casstorage',
                snap_backend = '%s%s' % (TransferMock.SCHEMA, self.mpoints[1]),
                fstype = 'ext3',
                mpoint = self.mpoints[0]
        )
        self.vols[0].mkfs()
        self.vols[0].mount()
 
        # Create big file
        bigfile = os.path.join(self.mpoints[0], 'bigfile')
        system(('dd', 'if=/dev/urandom', 'of=%s' % bigfile, 'bs=1M', 'count=15'))
        bigsize = os.path.getsize(bigfile)
        self.assertTrue(bigsize > 0)
        md5sum = system(('/usr/bin/md5sum', bigfile))[0].strip().split(' ')[0]
 
        # Snapshot storage
        snap = self.vols[0].snapshot(description='Bigfile with us forever')
        self.assertEqual(snap.type, 'eph')
        self.assertEqual(snap.vg, 'casstorage')
        self.assertEqual(snap.state, Snapshot.CREATING)
 
        wait_until(lambda: snap.state in (Snapshot.COMPLETED, Snapshot.FAILED))
        print snap.config()
        if snap.state == Snapshot.FAILED:
            raise Exception('Snapshot creation failed. See log for more details')
 
        # Destroy original storage
        self.vols[0].destroy()
        self.vols[0] = None
 
        # Restore snapshot
        self.vols[2] = Storage.create(disk=self.devices[2], snapshot=snap)
        self.vols[2].mount(self.mpoints[2])
        bigfile2 = os.path.join(self.mpoints[2], 'bigfile')
 
        self.assertTrue(os.path.exists(bigfile2))
 
        md5sum2 = system(('/usr/bin/md5sum', bigfile2))[0].strip().split(' ')[0]
        self.assertEqual(md5sum, md5sum2)
 
 
if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()
 
