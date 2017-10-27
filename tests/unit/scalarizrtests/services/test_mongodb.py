'''
Created on Nov 10, 2011
 
@author: Spike
'''
import os
import unittest
import shutil
 
from scalarizr.services import mongodb
from scalarizr.libs.metaconf import Configuration, NoPathError, ET
 
 
class ConfigTest(unittest.TestCase):
 
    config_class = None
 
    def _test_bool_option(self, option):
        self.assertRaises(ValueError, setattr, self.cnf, option, 'NotBoolValue')
 
        setattr(self.cnf, option, True)
        c = Configuration('mongodb')
        c.read(self.cnf_path)
        self.assertEqual('true', c.get(option))
        self.assertEqual(True, getattr(self.cnf, option))
 
        setattr(self.cnf, option, False)
        c = Configuration('mongodb')
        c.read(self.cnf_path)
        self.assertEqual('false', c.get(option))
        self.assertEqual(False, getattr(self.cnf, option))
 
        setattr(self.cnf, option, None)
        c = Configuration('mongodb')
        c.read(self.cnf_path)
        self.assertRaises(NoPathError, c.get, option)
 
        c.set(option, 'NotBool', force=True)
        c.write(self.cnf_path)
        self.assertRaises(ValueError, getattr, self.cnf, option)
 
 
    def _test_numeric_option(self, option):
        self.assertRaises(ValueError, setattr, self.cnf, option, 'NotNumericValue')
 
        setattr(self.cnf, option, 113)
        c = Configuration('mongodb')
        c.read(self.cnf_path)
        self.assertEqual('113', c.get(option))
        self.assertEqual(113, getattr(self.cnf, option))
 
        setattr(self.cnf, option, None)
        c = Configuration('mongodb')
        c.read(self.cnf_path)
        self.assertRaises(NoPathError, c.get, option)
 
 
 
    def _test_path_option(self, option):
        self.assertRaises(ValueError, setattr, self.cnf, option, '/not/exists')
        setattr(self.cnf, option, '/tmp')
        c = Configuration('mongodb')
        c.read(self.cnf_path)
        self.assertEqual('/tmp', c.get(option))
        self.assertEqual('/tmp', getattr(self.cnf, option))
 
 
    def _test_option(self, option):
        setattr(self.cnf, option, 'value')
        c = Configuration('mongodb')
        c.read(self.cnf_path)
        self.assertEqual('value', c.get(option))
 
        setattr(self.cnf, option, None)
        c = Configuration('mongodb')
        c.read(self.cnf_path)
        self.assertRaises(NoPathError, c.get, option)
 
 
    def tearDown(self):
        shutil.rmtree(self.cnf_dir)
 
 
    def setUp(self):
        self.cnf_dir = '/tmp/cfg'
        if not os.path.isdir(self.cnf_dir):
            os.makedirs(self.cnf_dir)
 
        self.cnf_path = os.path.join(self.cnf_dir, self.config_class.config_name)
        if os.path.isfile(self.cnf_path):
            os.remove(self.cnf_path)
 
        self.cnf = self.config_class.find(self.cnf_dir)
 
 
class TestMongoDBConfig(ConfigTest):
    config_class = mongodb.MongoDBConfig
 
 
    def test_dbpath(self):
        self._test_path_option('dbpath')
 
 
    def test_shardsvr(self):
        self._test_bool_option('shardsvr')
 
 
    def test_nojournal(self):
        self._test_bool_option('nojournal')
 
 
    def test_nohttpinterface(self):
        self._test_bool_option('nohttpinterface')
 
 
    def test_rest(self):
        self._test_bool_option('rest')
 
 
    def test_port(self):
        self._test_numeric_option('port')
 
 
    def test_replSet(self):
        self._test_option('replSet')
 
 
    def test_logpath(self):
        self._test_option('logpath')
 
 
    def test_logappend(self):
        self._test_bool_option('logappend')
 
 
class TestConfigServerConf(ConfigTest):
    config_class = mongodb.ConfigServerConf
 
    def test_configsvr(self):
        self._test_bool_option('configsvr')
 
 
 
 
 
if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()
 
