'''
Created on Mar 15, 2010
 
@author: marat
'''
 
import unittest
import os
from scalarizr.handlers.ec2.rebundle import Ec2RebundleHandler, Manifest
from scalarizr.messaging.p2p import P2pMessage
from scalarizr.util import init_tests
 
 
class Test(unittest.TestCase):
 
    _resources_path = None
 
    def __init__(self, methodName='runTest'):
        unittest.TestCase.__init__(self, methodName)
        self._resources_path = os.path.realpath(os.path.dirname(__file__) + "/../../../resources")
 
    def _test_fileutil(self):
        filename = self._resources_path + "/fileutil.split"
        f = open(filename, "w")
        f.write("1234567")
        f.close()
 
        part_names = FileTool.split(filename, "fileutil.chunk", 3, self._resources_path)
        self.assertEqual(len(part_names), 3)
        self.assertEqual(os.path.getsize(self._resources_path + "/" + part_names[0]), 3)
        self.assertEqual(os.path.getsize(self._resources_path + "/" + part_names[2]), 1)
 
    def _test_manifest(self):
        m = Manifest(user="2121212245")
        self.assertTrue(m.user is not None)
        self.assertTrue(m.ec2_encrypted_iv is None)
 
    def test_empty_excludes(self):
        xml = """<?xml version="1.0"?><message id="ad851a7b-6512-45db-b0a5-77f915aada32" name="Rebundle"><meta/><body><role_name>scalarizr-centos5.2</role_name><bundle_task_id>45</bundle_task_id><excludes/></body></message>"""
        msg = P2pMessage()
        msg.fromxml(xml)
        self.assertTrue(msg.excludes is None)
 
 
if __name__ == "__main__":
    init_tests()
    unittest.main()
 
