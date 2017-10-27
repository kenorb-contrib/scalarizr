'''
Created on 16.02.2010
 
@author: Dmytro Korsakov
'''
 
import unittest
import os
from scalarizr.bus import bus
from scalarizr.handlers import apache
from szr_unittest import RESOURCE_PATH
from scalarizr.config import ScalarizrCnf
 
from szr_unittest_libs.mock import QueryEnvService
from scalarizr.queryenv import VirtualHost
 
 
class Test(unittest.TestCase):
 
    def setUp(self):
        bus.etc_path = os.path.join(RESOURCE_PATH, 'etc')
        cnf = ScalarizrCnf(bus.etc_path)
        cnf.load_ini('app')
        bus.cnf = cnf
        self._cnf = bus.cnf
 
        bus.base_path = os.path.realpath(RESOURCE_PATH + "/../../..")
        bus.share_path = os.path.join(bus.base_path, 'share')
 
        bus.queryenv_service = qe
        bus.define_events("before_host_down", "init")
 
        '''
        config = bus.config
        self.vhosts_path = config.get('behaviour_app','vhosts_path')
        self.httpd_conf_path = config.get('behaviour_app','httpd_conf_path')
        '''
 
    def _test_cleanup(self):
        old_vhost = self.vhosts_path + "/test.vhost"
        if not os.path.exists(self.vhosts_path):
            os.makedirs(self.vhosts_path)
        open(old_vhost,'w').close
        self.assertTrue(os.path.exists(old_vhost))
 
        test_vhost = self.vhosts_path + "/test-example.scalr.net-ssl.vhost.conf"
        if os.path.exists(test_vhost):
            os.remove(test_vhost)
        self.assertFalse(os.path.exists(test_vhost))
 
        bus.queryenv_service = qe
        a = apache.ApacheHandler()
        a.on_VhostReconfigure("")
 
        self.assertFalse(os.path.exists(old_vhost))
        self.assertTrue(os.path.exists(test_vhost))
        self.assertEqual(os.listdir(self.vhosts_path),['test-example.scalr.net-ssl.vhost.conf'])
 
        httpd_conf_file = open(self.httpd_conf_path, 'r')
        text = httpd_conf_file.read()
        index = text.find('Include ' + self.vhosts_path + '/*')
        self.assertNotEqual(index, -1)
 
vhost = VirtualHost(
                        hostname = "test-example.scalr.net",
                        type = "apache",
                        raw= """<VirtualHost *:80>
DocumentRoot /var/www/1/
ServerName test-example.scalr.net
CustomLog     /var/log/apache2/test-example.scalr.net-access.log1 combined
#  CustomLog     /var/log/apache2/test-example.scalr.net-access.log2 combined
ErrorLog      /var/log/apache2/test-example.scalr.net-error.log3
#ErrorLog      /var/log/apache2/test-example.scalr.net-error.log4#
# ErrorLog      /var/log/apache2/test-example.scalr.net-error.log_5#
#  ErrorLog      /var/log/apache2/test-example.scalr.net-error.log_6_#
# Other directives here
 
</VirtualHost> """,
                        https = True)
 
qe = QueryEnvService(
        list_virtual_hosts = list(vhost,),
        get_https_certificate = ("MIICWjCCAhigAwIBAgIESPX5.....1myoZSPFYXZ3AA9kwc4uOwhN","MIICWjCCAhigAwIBAgIESPX5.....1myoZSPFYXZ3AA9kwc4uOwhN")
)
 
bus.queryenv_service = qe
 
 
if __name__ == "__main__":
    unittest.main()
 
