'''
Created on Aug 27, 2012
 
@author: marat
'''
 
import os
import shutil
 
from scalarizr import linux
from scalarizr.linux import mount
 
import mock
from nose.tools import raises
 
 
def test_mount():
    with mock.patch('scalarizr.linux.system') as m:
        mount.mount('/dev/sdb', '/mnt')
        assert m.called
 
 
@raises(mount.NoFileSystem)
def test_mount_no_filesystem():
    m = mock.Mock(side_effect=linux.LinuxError('', '', 'mount: you must specify the filesystem type\n', 32, ()))
    with mock.patch('scalarizr.linux.system', m):
        mount.mount('/dev/sdb', '/mnt')
 
 
def test_umount():
    with mock.patch('scalarizr.linux.system') as m:
        mount.umount('/mnt/mpoint')
        assert m.called
 
 
def test_umount_not_mounted():
    m = mock.Mock(side_effect=linux.LinuxError('', '', 'umount: /mnt: not mounted\n', 1, ()))
    with mock.patch('scalarizr.linux.system', m):
        mount.umount('/mnt')
        assert m.called
 
 
@raises(linux.LinuxError)
def test_umount_raise_error():
    m = mock.Mock(side_effect=linux.LinuxError('', '', 'umount: /: device is busy.\n', 1, ()))
    with mock.patch('scalarizr.linux.system', m):
        mount.umount('/')
 
 
 
class TestMounts(object):
    orig_filename = os.path.abspath(__file__ + '/../../../fixtures/linux/proc.mounts')
    test_filename = orig_filename + '.test'
 
    def setup(self):
        shutil.copy(self.orig_filename, self.test_filename)
        self.mounts = mount._Mounts(self.test_filename)
 
    def teardown(self):
        if os.path.exists(self.test_filename):
            os.remove(self.test_filename)
 
    def test_getitem_by_unique_device(self):
        assert 'sunrpc' in self.mounts
        sunrpc = self.mounts['sunrpc']
        assert sunrpc.mpoint == '/var/lib/nfs/rpc_pipefs'
        assert sunrpc.fstype == 'rpc_pipefs'
        assert sunrpc.options == 'rw,relatime'
        assert sunrpc.dump == '0'
        assert sunrpc.fsck_order == '0'
 
    def test_getitem_by_unique_mpoint(self):
        assert '/sys/fs/cgroup/freezer' in self.mounts
        cgroup = self.mounts['/sys/fs/cgroup/freezer']
        assert cgroup.device == 'cgroup'
 
    @raises(KeyError)
    def test_getitem_no_device_or_mpoint(self):
        assert '/dev/sda2' not in self.mounts
        self.mounts['/dev/sda2']
 
    def test_getitem_device_not_unique(self):
        assert 'cgroup' in self.mounts
        cgroup = self.mounts['cgroup']
        assert cgroup.mpoint == '/sys/fs/cgroup/systemd', 'First matched line'
 
    def test_getitem_nfs(self):
        nfs = self.mounts['/share']
        assert nfs.fsck_order == ''
        assert nfs.dump == ''
        assert nfs.fstype == 'nfs'
 
    def test_add_entry(self):
        mpoint = '/mnt/storage'
        device = '/dev/vdh'
        fstype = 'ext4'
        self.mounts.add(device, mpoint, fstype)
 
        assert device in self.mounts
        assert mpoint in self.mounts
        assert len(self.mounts) > 1
 
        entry = self.mounts[device]
        assert entry.fstype == fstype
        assert entry.options == 'auto'
        assert entry.fsck_order == '0'
        assert entry.dump == '0'
 
    def test_remove_entry(self):
        assert 'systemd-1' in self.mounts
        assert len(self.mounts) == 36
 
        del self.mounts['systemd-1']
 
        assert 'systemd-1' not in self.mounts
        assert len(self.mounts) == 31
 
