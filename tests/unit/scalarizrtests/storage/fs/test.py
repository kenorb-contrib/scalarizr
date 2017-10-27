'''
Created on Nov 14, 2010
 
@author: spike
'''
import unittest
from scalarizr.util import system2
from scalarizr.storage.fs.ext3 import Ext3FileSystem
from scalarizr.storage.fs.jfs import JfsFileSystem
from scalarizr.storage.fs.xfs import XfsFileSystem
import os
import re
import logging
from scalarizr.storage.util.loop import mkloop
 
 
class FileSystemTest(unittest.TestCase):
 
    device   = None
    img_file = None
    mpoint   = None
 
    def __init__(self, methodName):
        self.img_file = '/tmp/device'
        self.mpoint   = '/mnt/testdir'
        if not os.path.exists(self.mpoint):
            os.makedirs(self.mpoint)
        self._logger = logging.getLogger(__name__)
        return unittest.TestCase.__init__(self, methodName)
 
    def setUp(self):
        self.device  = mkloop(self.img_file, size=50)
 
    def tearDown(self):
        if self.device:
            system2('umount %s' % self.device, shell=True, raise_error=False)
            system2('/sbin/losetup -d %s' % self.device, shell=True)
            self.device = None
        system2('rm -f %s' % self.img_file, shell=True)
 
    def test_ext3(self):
 
        fs = Ext3FileSystem()
        fs.mkfs(self.device)
        self.assertFalse(fs.get_label(self.device))
        fs.set_label(self.device, 'testlabel')
        self.assertEqual('testlabel', fs.get_label(self.device))
        self._mount()
        self.assertEqual(self._get_size(), 49574)
        self._grow_partition()
        self._umount()
        fs.resize(self.device)
        self._mount()
        self.assertEqual(self._get_size(), 99384)
 
    def test_jfs(self):
        fs = JfsFileSystem()
        fs.mkfs(self.device)
        self.assertFalse(fs.get_label(self.device))
        fs.set_label(self.device, 'testlabel')
 
        self.assertEqual('testlabel', fs.get_label(self.device))
        self._mount()
        self.assertEqual(self._get_size(), 49968)
        self._grow_partition()
        fs.resize(self.device)
        self.assertEqual(self._get_size(), 101168)
 
    def test_xfs(self):
        fs = XfsFileSystem()
        fs.mkfs(self.device)
        self.assertFalse(fs.get_label(self.device))
        fs.set_label(self.device, 'testlabel')
        self.assertEqual('testlabel', fs.get_label(self.device))
        self._mount()
        self.assertEqual(self._get_size(), 46400)
        self._grow_partition()
        fs.resize(self.device)
        self.assertEqual(self._get_size(), 97600)
 
    def _grow_partition(self):
        system2('dd if=/dev/zero of=%s bs=1M count=50 seek=50' % self.img_file, shell=True)
        system2('/sbin/losetup -c %s' % self.device, shell=True)
 
    def _mount(self):
        out,err,rcode = system2('mount %s %s' % (self.device, self.mpoint), shell=True)
        if rcode:
            raise Exception('Error occured during mount operation.\n>>>Out:\n%s,\n>>>Err:\n%s' % (out, err))
 
    def _umount(self):
        out,err,rcode = system2('umount %s' % self.device, shell=True)
        if rcode:
            raise Exception('Error occured during umount operation.\n>>>Out:\n%s,\n>>>Err:\n%s' % (out, err))
 
    def _get_size(self):
        out = system2('df')[0]
        res = re.search('%s\s+(?P<size>\d+)' % self.device, out)
        if not res:
            raise Exception('Mount device before trying to get size of it.')
 
        return int(res.group('size'))
 
if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()
 
