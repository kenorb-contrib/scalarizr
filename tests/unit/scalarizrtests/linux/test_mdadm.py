__author__ = 'Nick Demyanchuk'
 
import mock
import unittest
 
from scalarizr.linux import mdadm
from scalarizr.storage2 import StorageError
 
 
class TestMdadm(unittest.TestCase):
 
    @mock.patch('scalarizr.linux.mdadm.os')
    def test_findname(self, os_mock):
        os_mock.path.exists.side_effect = [True]*39 + [False]
        device = mdadm.findname()
        self.assertEqual(device, '/dev/md39')
        calls = [mock.call('/dev/md%s' % x) for x in range(40)]
        self.assertEqual(os_mock.path.exists.mock_calls, calls)
 
 
    @mock.patch('__builtin__.open')
    @mock.patch('scalarizr.linux.mdadm.detail')
    def test_mdfind(self, detail, open):
        mdstat = mock.MagicMock()
        open.return_value.__enter__.return_value = mdstat
 
        out = """Personalities : [linear] [raid0] [raid1] [raid10] [raid6] [raid5] [raid4] [multipath] [faulty]
                                md0 : active raid1 loop1[1] loop0[0]
                        1048000 blocks super 1.2 [2/2] [UU]
 
                                unused devices: <none>"""
        mdstat.readlines.return_value = out.splitlines()
 
        md0_details = dict(
                level='1',
                raid_devices=2,
                total_devices=2,
                state='clean',
                devices={
                '/dev/loop0': 'active sync',
                '/dev/loop1': 'active sync'
                }
        )
 
        def side_eff(device):
            if device == '/dev/md0':
                return md0_details
            else:
                raise StorageError('Unknown device')
 
        detail.side_effect = side_eff
 
 
        devices = ['/dev/loop0']
        self.assertRaises(StorageError, mdadm.mdfind, *devices)
        detail.assert_called_once_with('/dev/md0')
        detail.reset_mock()
        mdstat.readlines.assert_called_once_with()
 
        devices = ['/dev/loop0', '/dev/loop1']
        md_dev = mdadm.mdfind(*devices)
        detail.assert_called_once_with('/dev/md0')
        self.assertEqual(md_dev, '/dev/md0')
 
        devices = ['/dev/loop0', '/dev/loop1', '/dev/loop2']
        self.assertRaises(StorageError, mdadm.mdfind, *devices)
 
        devices = ['/dev/loop0', '/dev/loop2']
        self.assertRaises(StorageError, mdadm.mdfind, *devices)
 
    @mock.patch('scalarizr.linux.mdadm.mdadm')
    def test_detail(self, _mdadm):
        out1 = """
/dev/md0:
Version : 1.2
Creation Time : Tue Sep 18 09:20:25 2012
Raid Level : raid1
Array Size : 1048000 (1023.61 MiB 1073.15 MB)
Used Dev Size : 1048000 (1023.61 MiB 1073.15 MB)
Raid Devices : 2
Total Devices : 2
Persistence : Superblock is persistent
 
Update Time : Tue Sep 18 09:20:35 2012
  State : clean
Active Devices : 2
Working Devices : 2
Failed Devices : 0
Spare Devices : 0
 
   Name : 0
   UUID : 80988291:ca7de4e9:412e93dc:ea6f4c6b
 Events : 17
 
Number   Major   Minor   RaidDevice State
0       7        0        0      active sync   /dev/loop0
1       7        1        1      active sync   /dev/loop1
                        """
 
        _mdadm.return_value = (out1, '', 0)
        details = mdadm.detail('/dev/md0')
        _mdadm.assert_called_once_with('misc', None, '/dev/md0', detail=True)
        self.assertItemsEqual(details,
                                        {'devices': {
                                                '/dev/loop0': 'active sync',
                                                '/dev/loop1': 'active sync'
                                        },
                                   'level': '1',
                                   'raid_devices': 2,
                                   'rebuild_status': None,
                                   'state': 'clean ',
                                   'total_devices': 2})
 
        out2 = """
/dev/md0:
Version : 1.2
Creation Time : Tue Sep 18 09:20:25 2012
Raid Level : raid1
Array Size : 1048000 (1023.61 MiB 1073.15 MB)
Used Dev Size : 1048000 (1023.61 MiB 1073.15 MB)
Raid Devices : 2
Total Devices : 2
Persistence : Superblock is persistent
 
Update Time : Tue Sep 18 11:14:26 2012
  State : clean, degraded, recovering
Active Devices : 1
Working Devices : 2
Failed Devices : 0
Spare Devices : 1
 
Rebuild Status : 6% complete
 
   Name : 0
   UUID : 80988291:ca7de4e9:412e93dc:ea6f4c6b
 Events : 21
 
Number   Major   Minor   RaidDevice State
0       7        0        0      spare rebuilding   /dev/loop0
1       7        1        1      active sync   /dev/loop1
"""
 
        _mdadm.return_value = (out2, '', 0)
        _mdadm.reset_mock()
        details = mdadm.detail('/dev/md0')
        self.assertItemsEqual(details, {'level': '1',
                                                                        'total_devices': 2,
                                                                        'devices': {
                                                                                '/dev/loop1': 'active sync',
                                                                                '/dev/loop0': 'spare rebuilding'
                                                                        },
                                                                        'rebuild_status': 6,
                                                                        'state': 'clean, degraded, recovering',
                                                                        'raid_devices': 2})
 
