 
import os
 
from scalarizr.linux import coreutils
 
import mock
 
def test_lsscsi():
    out_filename = os.path.abspath(__file__ + '/../../../fixtures/linux/lsscsi.out')
    m = mock.Mock(return_value=[open(out_filename).read()])
    with mock.patch('scalarizr.linux.system', m):
        ret = coreutils.lsscsi()
        assert set(ret.keys()) == set(['/dev/sr0', '/dev/sda', '/dev/sdb'])
        sdb = ret['/dev/sdb']
        assert sdb['host'] == '2'
        assert sdb['bus'] == '0'
        assert sdb['target'] == '0'
        assert sdb['lun'] == '0'
 
        sda = ret['/dev/sda']
        assert sda['target'] == '1'
        assert sda['host'] == '0'
 
