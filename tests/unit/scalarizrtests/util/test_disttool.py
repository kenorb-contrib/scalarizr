'''
@author: Dmytro Korsakov
'''
import unittest
from scalarizr.util import disttool, init_tests
 
class Test(unittest.TestCase):
 
 
    def test_DistTool(self):
        self.assertTrue(disttool.is_linux())
        self.assertTrue(disttool.is_redhat_based())
        self.assertTrue(disttool.is_fedora())
 
        self.assertFalse(disttool.is_win())
        self.assertFalse(disttool.is_sun())
        self.assertFalse(disttool.is_debian_based())
 
 
if __name__ == "__main__":
    init_tests()
    unittest.main()
 
