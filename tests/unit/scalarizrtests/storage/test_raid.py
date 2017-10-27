'''
Created on Nov 17, 2010
 
@author: spike
'''
import unittest
from scalarizr.storage.raid import Mdadm
from scalarizr.util import firstmatched, system
import os
import szr_unittest
 
class Test(unittest.TestCase):
    arrays  = None
    devices = None
 
    def __init__(self, methodName):
        self.raid    = Mdadm()
        return unittest.TestCase.__init__(self, methodName)
 
    def setUp(self):
        self.arrays  = []
        self.devices = []
 
    def tearDown(self):
        for array in self.arrays:
            system('mdadm -S -f %s' % array)
            system('mdadm --remove -f %s' % array)
        for device in self.devices:
            system('/sbin/losetup -d %s' % device)
        system('rm -f /tmp/device*')
 
    def testRaid0(self):
        for i in range(2):
            self.devices.append(self._get_loopback())
 
        array = self.raid.create(self.devices, 0)
        self.arrays.append(array)
        new_device = self._get_loopback()
        self.devices.append(new_device)
        self.assertRaises(Exception, self.raid.add_disk, (array, new_device))
        info = self.raid.get_array_info(array)
        self.assertEqual(info['level'], 'raid0')
        self.assertEqual(info['raid_devices'], 2)
        self.assertEqual(info['total_devices'], 2)
        self.assertRaises(Exception, self.raid.replace_disk, (self.devices[0], new_device))
        self.raid.delete(array)
        self.arrays.remove(array)
 
    def testRaid1(self):
        for i in range(2):
            self.devices.append(self._get_loopback())
 
        array = self.raid.create(self.devices, 1)
        self.arrays.append(array)
        info = self.raid.get_array_info(array)
        self.assertEqual(info['level'], 'raid1')
        self.assertEqual(info['raid_devices'], 2)
        self.assertEqual(info['total_devices'], 2)
        new_device = self._get_loopback()
        self.devices.append(new_device)
 
        self.raid.add_disk(array, new_device)
 
        info = self.raid.get_array_info(array)
        self.assertEqual(info['raid_devices'], 3)
        self.assertEqual(info['total_devices'], 3)
        self.assertTrue(os.path.basename(new_device) in self.raid.get_array_devices(array))
 
        another_device = self._get_loopback()
        self.devices.append(another_device)
        self.raid.replace_disk(new_device, another_device)
        array_devices = self.raid.get_array_devices(array)
        self.assertTrue(os.path.basename(another_device) in array_devices)
        self.assertFalse(os.path.basename(new_device) in array_devices)
 
        self.raid.remove_disk(another_device)
 
        info = self.raid.get_array_info(array)
        self.assertEqual(info['raid_devices'], 3)
        self.assertEqual(info['total_devices'], 2)
        self.raid.delete(array)
        self.arrays.remove(array)
 
    def testRaid5(self):
        for i in range(2):
            self.devices.append(self._get_loopback())
 
        array = self.raid.create(self.devices, 5)
        self.arrays.append(array)
        info = self.raid.get_array_info(array)
        self.assertEqual(info['level'], 'raid5')
        self.assertEqual(info['raid_devices'], 2)
        self.assertEqual(info['total_devices'], 2)
        new_device = self._get_loopback()
        self.devices.append(new_device)
 
        self.raid.add_disk(array, new_device)
 
        info = self.raid.get_array_info(array)
        self.assertEqual(info['raid_devices'], 3)
        self.assertEqual(info['total_devices'], 3)
        self.assertTrue(os.path.basename(new_device) in self.raid.get_array_devices(array))
 
        another_device = self._get_loopback()
        self.devices.append(another_device)
        self.raid.replace_disk(new_device, another_device)
        array_devices = self.raid.get_array_devices(array)
        self.assertTrue(os.path.basename(another_device) in array_devices)
        self.assertFalse(os.path.basename(new_device) in array_devices)
 
        self.raid.remove_disk(another_device)
 
        info = self.raid.get_array_info(array)
        self.assertEqual(info['raid_devices'], 3)
        self.assertEqual(info['total_devices'], 2)
        self.raid.delete(array)
        self.arrays.remove(array)
 
    def _get_loopback(self):
        image = '/tmp/device%s' % firstmatched(lambda x: not os.path.exists('/tmp/device%s' % x), range(100))
        system("dd if=/dev/zero of=%s bs=1M count=15" % image)
        loop_dev = system('/sbin/losetup -f --show %s' % image)[0].strip()
        return loop_dev
 
if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    szr_unittest.main()
    unittest.main()
 
