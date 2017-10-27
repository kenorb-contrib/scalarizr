'''
Created on Aug 22, 2012
 
@author: marat
'''
 
from scalarizr.storage2.volumes import base
 
from nose.tools import raises
import mock
 
 
class TestBase(object):
    def test_config_simple(self):
        vol = base.Base(device='/dev/sda1', mpoint='/', size=100, fscreated=False)
        config = vol.config()
        assert config.get('device') == '/dev/sda1'
        assert config.get('mpoint') == '/'
 
 
    def test_config_nested(self):
        vol = base.Base(device='/dev/mapper/lxc-host1',
                                pv_disks=['/dev/sdb', base.Base(device='/dev/sdc')],
                                vg='lxc', name='host1',
                                options={
                                        'extent_size': 4,
                                        'log_disk': base.Base(fstype='tmpfs')
                                })
        config = vol.config()
        assert 'pv_disks' in config
        assert len(config['pv_disks']) == 2
        assert type(config['pv_disks'][1]) == dict
        assert 'options' in config
        assert type(config['options']) == dict
        assert 'log_disk' in config['options']
        assert config['options']['log_disk']['fstype'] == 'tmpfs'
 
    def test_get_attr(self):
        vol = base.Base(custom=512)
        assert hasattr(vol, 'config'), 'Has config attribute'
        assert hasattr(vol, 'custom'), 'Has custom attribute'
        assert vol.custom == 512, 'Custom attribute value stored well'
        assert hasattr(vol, '_dictify'), 'Has function attribute'
 
    @raises(AttributeError)
    def test_get_attr_unknown(self):
        vol = base.Base()
        vol.unknown_attr
 
    def test_set_attr(self):
        vol = base.Base()
        vol.custom = 15
        assert vol.custom == 15
 
 
class TestVolume(object):
 
    @mock.patch('scalarizr.linux.mount.mount')
    def test_mount(self, m):
        vol = base.Volume(device='/dev/sdb', mpoint='/mnt')
        vol.mount()
        m.assert_called_once_with(vol.device, vol.mpoint)
 
 
    @mock.patch.multiple('scalarizr.linux.mount',
                                            mounts=mock.DEFAULT, mount=mock.DEFAULT)
    def test_mount_already_mounted(self, mounts, mount):
        mounts.return_value.__getitem__.return_value = mock.Mock(mpoint='/mnt')
        vol = base.Volume(device='/dev/sdb', mpoint='/mnt')
        vol.mount()
        mounts.return_value.__getitem__.assert_called_once_with('/dev/sdb')
        assert mount.call_count == 0, "Mount wasn't called"
 
 
    @mock.patch('scalarizr.linux.mount.mounts')
    def test_mounted_to(self, mounts):
        mounts.return_value.__getitem__.return_value = mock.Mock(mpoint='/mnt2')
        vol = base.Volume(device='/dev/sdc', mpoint='/mnt')
        assert vol.mounted_to() == '/mnt2'
        mounts.return_value.__getitem__.assert_called_once_with('/dev/sdc')
 
 
    def test_umount(self):
        pass
 
 
    @mock.patch.dict('scalarizr.storage2.filesystem_types', {'ext3': mock.Mock})
    def test_mkfs(self):
        vol = base.Volume(device='/dev/sdb')
        vol.mkfs()
        assert vol.fscreated, 'Filesystem created'
 
 
    @raises(KeyError)
    def test_mkfs_raises_unknown_filesystem(self):
        vol = base.Volume(device='/dev/sdb', fstype='unknown')
        vol.mkfs()
 
 
    def test_ensure(self):
        vol = base.Volume(device='/dev/sdb2')
        with mock.patch.multiple(vol, config=mock.DEFAULT, mkfs=mock.DEFAULT, mount=mock.DEFAULT):
            vol.ensure()
            vol.config.assert_called_once_with()
            assert vol.mkfs.call_count == 0, "mkfs wasn't called"
            assert vol.mount.call_count == 0, "mount wasn't called"
 
 
    def test_ensure_with_mount_and_mkfs(self):
        pass
 
 
class TestSnapshot(object):
    def test_restore(self):
        pass
 
