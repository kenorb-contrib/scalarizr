'''
Created on Nov 23, 2010
 
@author: shaitanich
'''
import unittest
from scalarizr.handlers.ssh_auth_keys import SSHKeys, UpdateSshAuthorizedKeysError
from scalarizr.linux.coreutils import touch
from szr_unittest import main as unit_main
import os
 
class _SSHKeys(SSHKeys):
    PATH = 'test_authorized_keys.txt'
 
 
 
class Test(unittest.TestCase):
 
    def setUp(self):
        self.ssh_keys = _SSHKeys()
 
        if not os.path.exists(self.ssh_keys.PATH):
            touch(self.ssh_keys.PATH)
 
 
    def tearDown(self):
        if os.path.exists(self.ssh_keys.PATH):
            os.remove(self.ssh_keys.PATH)
            pass
 
    def test_empty_data(self):
 
        class _Message:
            add = []
            remove = []
 
        self.ssh_keys.on_UpdateSshAuthorizedKeys(_Message)
        keys = None
        with open(self.ssh_keys.PATH, 'r') as fp:
            keys = fp.read()
        self.assertEquals('', keys)
 
    def test_add_new_keys(self):
        class _Message:
            add = ['new_key1', 'new_key2']
            remove = []
        self.ssh_keys.on_UpdateSshAuthorizedKeys(_Message)
        keys = None
        with open(self.ssh_keys.PATH, 'r') as fp:
            keys = fp.read()
        self.assertTrue(_Message.add[0] in keys)
        self.assertTrue(_Message.add[1] in keys)
 
    def test_add_existed_keys(self):
        pass
 
    def test_remove_unexisted_keys(self):
 
        class _Message:
            add = []
            remove = ['old_key1', 'old_key2']
 
        self.ssh_keys.on_UpdateSshAuthorizedKeys(_Message)
 
    def test_remove_existed_keys(self):
        old_keys = ['old_key3', 'old_key4']
        with open(self.ssh_keys.PATH, 'w') as fp:
            fp.write('\n'.join(old_keys))
        class _Message:
            add = []
            remove = old_keys
 
        self.ssh_keys.on_UpdateSshAuthorizedKeys(_Message)
        keys = None
        with open(self.ssh_keys.PATH, 'r') as fp:
            keys = fp.read()
        self.assertFalse(_Message.remove[0] in keys)
        self.assertFalse(_Message.remove[1] in keys)
 
    def test_unexisted_path(self):
        class __SSHKeys(SSHKeys):
            path = 'some.unexisted.path'
 
        class _Message:
            add = ['some_key']
            remove = []
 
        ssh_keys = __SSHKeys()
        self.assertRaises(UpdateSshAuthorizedKeysError, ssh_keys.on_UpdateSshAuthorizedKeys, _Message)
 
unit_main()
if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()
 
