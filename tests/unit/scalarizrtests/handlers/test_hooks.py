'''
Created on 04.03.2010
 
@author: Dmytro Korsakov
'''
import unittest
from scalarizr.handlers import hooks
from scalarizr.bus import bus
from ConfigParser import ConfigParser
import os
from scalarizr.util import init_tests
from scalarizr.util import configtool
import subprocess
 
class Test(unittest.TestCase):
 
    def tearDown(self):
        shutdown = os.path.realpath(os.path.dirname(__file__) + "/../../" + "resources/hooks/cleaner.sh")
        try:
            p = subprocess.Popen(
                     shutdown,
                     stdin=subprocess.PIPE,
                     stdout=subprocess.PIPE,
                     stderr=subprocess.PIPE)
            stdout, stderr = p.communicate()
            is_shutdown_failed = p.poll()
 
            if is_shutdown_failed:
                print "Failed removing temporary files"
 
        except OSError, e:
            print "Error at shutdown: %s", str(e.strerror)
 
    def test_create_hook(self):
        bus.base_path = os.path.realpath(os.path.dirname(__file__) + "/../../..")
        config = ConfigParser()
        config.read(bus.base_path + "/etc/config.ini")
        bus.config = config
        init_tests()
        resources_path = os.path.realpath(os.path.dirname(__file__) + "/../../" + "resources")
        bus.base_path = resources_path
        server_id = config.get(configtool.SECT_GENERAL, configtool.OPT_SERVER_ID)
        bus.define_events("init", "test")
        handler = hooks.HooksHandler()
        bus.fire('init')
        bus.fire("test", "test_2_done", aenv="test_3_done")
        #absolutely valid script created an empty file
        self.assertTrue(os.path.exists(resources_path + "/hooks/test_1_done"))
        #next script created a file named as 1st execution parameter
        self.assertTrue(os.path.exists(resources_path + "/hooks/test_2_done"))
        #3rd script touched file named $aenv
        self.assertTrue(os.path.exists(resources_path + "/hooks/test_3_done"))
        #test 4 touched file named server_id
        self.assertTrue(os.path.exists(resources_path + "/hooks/" + server_id))
        #test 5 doesn`t have an execution bit
        self.assertFalse(os.path.exists(resources_path + "/hooks/test_5_done"))
        #test 6 tried to execute script file with Exec format error
        self.assertFalse(os.path.exists(resources_path + "/hooks/test_6_done"))
        #test 7 consists an execution error
        self.assertTrue(os.path.exists(resources_path + "/hooks/test_7_done"))
        #test8 script has not a valid name , so supposed not to be executed
        self.assertFalse(os.path.exists(resources_path + "/hooks/test_8_done"))
 
 
if __name__ == "__main__":
    unittest.main()
 
