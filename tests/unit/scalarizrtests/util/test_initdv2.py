'''
Created on Oct 25, 2010
 
@author: spike
'''
import unittest
from scalarizr.handlers.apache import ApacheInitScript
from scalarizr.handlers.nginx import NginxInitScript
from scalarizr.handlers.mysql import MysqlInitScript
import time, os
from scalarizr.util.initdv2 import InitdError
 
class Test(unittest.TestCase):
    mysql  = None
    nginx  = None
    apache = None
 
    def __init__(self, methodName='runTest'):
        unittest.TestCase.__init__(self, methodName)
        self.mysql = MysqlInitScript()
        self.nginx = NginxInitScript()
        self.apache = ApacheInitScript()
 
 
    def setUp(self):
        print 'run setup'
        self.nginx.stop()
        self.apache.stop()
        print 'finished setup'
 
    def test_mysql(self):
        print 'run mysql test'
        # Stop
        self.mysql.stop()
        # Reload when stopped
        self.assertRaises(InitdError, self.mysql.reload)
        # Status when stopped
        start_time = time.time()
        status = self.mysql.status()
        run_time = time.time() - start_time
        self.assertEqual(status, 3)
        self.assertTrue(run_time < 1.2)
        # Start
        self.mysql.start()
        #self.assertEqual(self.mysql.status(), 0)
        # Restart
        self.mysql.restart()
        self.assertTrue(self.mysql.running)
        self.mysql.reload()
 
 
    def test_nginx(self):
        print 'run nginx test'
        # Stop
        self.nginx.stop()
        # Reload when stopped
        self.assertRaises(InitdError, self.nginx.reload)
        # Status when stopped
        start_time = time.time()
        status = self.nginx.status()
        run_time = time.time() - start_time
        self.assertEqual(status, 3)
        self.assertTrue(run_time < 1.1)
        # Start
        self.nginx.start()
        self.assertEqual(self.nginx.status(), 0)
        # Restart
        self.nginx.restart()
        self.assertTrue(self.nginx.running)
        self.nginx.reload()
 
    def test_apache(self):
        print 'run mysql test'
        self.apache.stop()
        # Reload when stopped
        self.assertRaises(InitdError, self.apache.reload)
        # Status when stopped
        start_time = time.time()
        status = self.apache.status()
        run_time = time.time() - start_time
        self.assertEqual(status, 3)
        self.assertTrue(run_time < 1.1)
        # Start
        self.apache.start()
        self.assertEqual(self.apache.status(), 0)
        # Restart
        self.apache.restart()
        self.assertTrue(self.apache.running)
        self.apache.reload()
 
 
class _Cnf:
 
    def __init__(self):
        class _rawini:
            def get(self, f, s):
                return '/usr/sbin/nginx'
        self.rawini = _rawini()
 
if __name__ == "__main__":
    import szr_unittest
    from scalarizr.bus import bus
    bus.cnf = _Cnf()
    unittest.main()
 
