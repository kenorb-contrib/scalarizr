'''
Created on Oct 6, 2011
 
'''
import unittest
import os
 
from scalarizr.util.dns import HostsFile
 
 
class Test(unittest.TestCase):
    FILENAME = '/tmp/test_hosts.txt'
    hosts = None
 
    def setUp(self):
 
        test_list = ['# comment  comment comment        comment',
                '127.0.0.1 localhost',
                '194.166.100.1 hostname.domain1 alias1 alias23',
                '\n\t',
                '#      trololo comment',
                '127.0.0.1        localhost     alias1  alias2']
 
        f = open(self.FILENAME, 'w')
        for l in test_list:
            f.write("%s\n" % l)
        f.close()
 
        self.hosts = HostsFile(self.FILENAME)
 
    def tearDown(self):
        if os.path.exists(self.FILENAME):
            #os.remove(self.FILENAME)
            pass
 
 
    def test_get(self):
        host = self.hosts.get('localhost')
        self.assertEqual('127.0.0.1', host.ipaddr)
        self.assertEqual('localhost', host.hostname)
        self.assertEqual(set(('alias1', 'alias2')), host.aliases)
 
    def test_get_nonexisted(self):
        self.assertRaises(KeyError, lambda: self.hosts['non-existed.com'])
 
    def test_resolve(self):
        self.assertEqual('127.0.0.1', self.hosts.resolve('localhost'))
 
    def test_alias(self):
        self.hosts.alias('localhost', 'test_alias1', 'test_alias2')
        host = self.hosts.get('localhost')
        self.assertEqual(set(['test_alias1', 'test_alias2', 'alias1', 'alias2']), host.aliases)
 
    def test_unalias(self):
        self.hosts.unalias('localhost', 'alias1', 'alias2')
        self.assertEqual(set([]), self.hosts.get('localhost').aliases)
 
 
    def test_remove(self):
        self.hosts.remove('localhost')
        self.hosts._reload()
        self.assertFalse(self.hosts.resolve('localhost'))
 
 
    def test_map(self):
        self.hosts.map('192.168.100.38', 'localhost', 'ALIASS')
        host = self.hosts.get('localhost')
        self.assertEqual(host.ipaddr, '192.168.100.38')
        self.assertEqual(host.aliases, set(['ALIASS']))
 
 
if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testInit']
    unittest.main()
 
