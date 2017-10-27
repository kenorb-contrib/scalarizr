 
from scalarizr import storage2
 
import mock
 
 
class TestXfsFileSystem(object):
 
    @mock.patch('scalarizr.linux.pkgmgr')
    @mock.patch('scalarizr.linux.coreutils.modprobe')
    @mock.patch('os.path.exists', return_value=False)
    def test_create(self, exists, modprobe, pkgmgr):
        fs = storage2.filesystem('xfs')
        assert fs.type == 'xfs'
        modprobe.assert_called_once_with('xfs')
        pkgmgr.installed.assert_called_with(fs.os_packages[0])
 
