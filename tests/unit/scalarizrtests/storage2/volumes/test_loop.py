 
import os
 
import mock
 
from scalarizr import storage2
from nose.tools import raises
 
 
class TestLvmVolume(object):
 
    @mock.patch.multiple('scalarizr.linux.coreutils',
                                            dd=mock.DEFAULT,
                                            losetup=mock.DEFAULT,
                                            losetup_all=mock.DEFAULT)
    def test_ensure_new(self, dd, losetup, losetup_all):
        losetup_all.return_value.__getitem__.return_value = '/dev/loop0'
 
        vol = storage2.volume(type='loop', size=1, zerofill=True)
        vol.ensure()
 
        assert vol.device == '/dev/loop0'
        assert vol.file.startswith('/mnt/loopdev')
        dd.assert_called_once_with(**{
                        'if': '/dev/zero',
                        'of': vol.file,
                        'bs': '1M',
                        'count': 1024})
        losetup.assert_called_with(vol.file, find=True)
 
 
    @raises(storage2.StorageError)
    def test_ensure_new_not_enough_args(self):
        vol = storage2.volume(type='loop')
        vol.ensure()
 
    @mock.patch.multiple('scalarizr.linux.coreutils',
                                            dd=mock.DEFAULT,
                                            losetup=mock.DEFAULT,
                                            losetup_all=mock.DEFAULT)
    @mock.patch.object(os, 'statvfs')
    def test_ensure_new_with_parametrized_size(self, statvfs,
                                                                                    losetup_all, losetup, dd):
        statvfs.return_value=mock.Mock(
                        f_bsize=4096,
                        f_blocks=13092026,
                        f_bfree=10613528)
        losetup_all.return_value.__getitem__.return_value = '/dev/loop0'
 
        vol = storage2.volume(type='loop', size='25%root')
        vol.ensure()
 
        dd.assert_called_once_with(**{
                        'if': '/dev/zero',
                        'of': vol.file,
                        'bs': '1M',
                        'seek': 12784,
                        'count': 1})
 
 
    @mock.patch.multiple('scalarizr.linux.coreutils',
                                            losetup_all=mock.DEFAULT)
    @mock.patch('scalarizr.linux.coreutils.losetup')
    @mock.patch.object(os.path, 'exists')
    @mock.patch.object(os, 'stat')
    def test_ensure_existed(self, stat, exists, losetup, losetup_all):
        stat.return_value = mock.Mock(st_size=1073741931)
        exists.return_value=True
        losetup_all.return_value.__getitem__.return_value = '/mnt/loopdev0'
 
        vol = storage2.volume(
                type='loop',
                device='/dev/loop0',
                file='/mnt/loopdev0'
        )
        vol.ensure()
 
        losetup_all.assert_called_once_with()
 
    def test_restore(self):
        pass
 
