'''
Created on 26.01.2012
 
@author: sam
'''
import unittest
import os
import logging
import shutil
 
from scalarizr.api import haproxy
from scalarizr.services import haproxy as hap_serv
from scalarizr.util import iptables
 
LOG = logging.getLogger(__name__)
 
TEMP_PATH = '/tmp/haproxy_api.cfg'
 
class TestHAProxyAPI(unittest.TestCase):
 
    def __init__(self, methodName='runTest'):
        unittest.TestCase.__init__(self, methodName=methodName)
 
        self.protocol='http'
        self.server_protocol='http'
        self.port='1154'
        self.server_port=2254
        self.backend='role:1234'
        self.ipaddr='248.64.125.158'
 
    def setUp(self):
        shutil.copy(os.path.dirname(__file__) + '/../../../resources/etc/haproxy_api.cfg.backup',
                TEMP_PATH)
        self.api = haproxy.HAProxyAPI(TEMP_PATH)
        self.api.svs.start()
 
    def tearDown(self):
        if os.path.exists(TEMP_PATH):
            self.api.svs.stop()
        try:
            iptables.remove_rule_once('ACCEPT', self.port, 'tcp')
        except:
            pass
 
    def test_create_listener(self):
        self.api.create_listener(protocol=self.protocol, port=self.port,
                                server_protocol=self.server_protocol, server_port=self.server_port,
                                backend=self.backend)
 
        self.assertEqual(self.api.svs.status(), 0)
 
        ln = hap_serv.naming('listen', self.protocol, self.port)
        bnd = hap_serv.naming('backend', self.server_protocol, self.server_port, backend=self.backend)
 
        self.assertEqual(self.api.cfg.backends[bnd]['timeout']['check'], '3s')
        self.assertEqual(self.api.cfg.listener[ln]['mode'], self.protocol)
 
        self.api.cfg.reload()
 
        self.assertEqual(self.api.cfg.backends[bnd]['timeout']['check'], '3s')
        self.assertEqual(self.api.cfg.listener[ln]['mode'], self.protocol)
        self.assertEqual(self.api.cfg.backends[bnd]['mode'], self.server_protocol)
        #TODO: add some server with another default-server and wait for exception
 
    def test_delete_listener(self):
 
        self.api.create_listener(protocol=self.protocol, port=self.port,
                        server_protocol=self.server_protocol,server_port=self.server_port, backend=self.backend)
        self.assertEqual(self.api.cfg.listener['scalr:listen:%s:%s' %\
                        (self.protocol, self.port)]['mode'], self.protocol)
        self.assertEqual(self.api.cfg.sections('scalr:listen:%s:%s' % (self.protocol,
                         self.port)), ['scalr:listen:%s:%s' % (self.protocol, self.port)])
        self.assertEqual(self.api.cfg.listener['scalr:listen:%s:%s' % (self.protocol, self.port)]\
                        ['default_backend'], 'scalr:backend:%s:%s:%s' % \
                        (self.backend, self.server_protocol, self.server_port))
 
        self.api.delete_listener(port=self.port, protocol=self.protocol)
 
        self.assertEqual(self.api.cfg.sections('scalr:listen:%s:%s' % (self.protocol, self.port)), [])
        self.assertEqual(self.api.cfg.sections('scalr:backend:%s:%s:%s' %\
                                        (self.backend, self.server_protocol, self.server_port)), [])
 
    def test_add_server(self):
        self.api.create_listener(protocol=self.protocol, port=self.port, server_port=self.server_port,
                backend=self.backend)
 
        self.api.add_server(ipaddr=self.ipaddr, backend=self.backend)
 
        self.api.cfg.reload()
        self.assertEqual(self.api.cfg.backends['scalr:backend:%s:%s:%s' % (self.backend,
                self.server_protocol, self.server_port)]['server'][self.ipaddr.replace('.', '-')],
                {'address': self.ipaddr, 'port': str(self.server_port), 'check':True})
 
    def test_remove_server(self):
        self.api.create_listener(protocol=self.protocol, port=self.port, server_port=self.server_port,
                server_protocol=self.server_protocol, backend=self.backend)
        self.api.add_server(ipaddr=self.ipaddr, backend=self.backend)
 
        self.assertEqual(self.api.cfg.backends['scalr:backend:%s:%s:%s' % (self.backend,
                self.server_protocol, self.server_port)]['server'][self.ipaddr.replace('.', '-')],
                {'address': self.ipaddr, 'port': str(self.server_port), 'check':True})
 
        self.api.remove_server(ipaddr=self.ipaddr, backend=self.backend)
 
        try:
            server = self.api.cfg.backends['scalr:backend:%s:%s:%s' % (self.backend,
            self.protocol, self.port)]['server'][self.ipaddr.replace('.', '-')]
        except:
            server = None
        self.assertIsNone(server)
 
 
        self.api.add_server(ipaddr=self.ipaddr, backend=self.backend)
        self.api.add_server(ipaddr='10.168.58.46', backend=self.backend)
 
 
        self.assertEqual(self.api.cfg.backends['scalr:backend:%s:%s:%s' % (self.backend,
                self.server_protocol, self.server_port)]['server'][self.ipaddr.replace('.', '-')],
                {'address': self.ipaddr, 'port': str(self.server_port), 'check':True})
 
        self.assertEqual(self.api.cfg.backends['scalr:backend:%s:%s:%s' % (self.backend,
                self.server_protocol, self.server_port)]['server']['10-168-58-46'],
                {'address': '10.168.58.46', 'port': str(self.server_port), 'check':True})
 
        for srv in self.api.list_servers(backend=self.backend):
            self.api.remove_server(ipaddr=srv, backend=self.backend)
 
        try:
            server = self.api.cfg.backends['scalr:backend:%s:%s:%s' % (self.backend,
                    self.protocol, self.port)]['server'][self.ipaddr.replace('.', '-')]
 
            server = self.api.cfg.backends['scalr:backend:%s:%s:%s' % (
                    self.backend, self.protocol, self.port)]['server']['10-168-58-46']
        except:
            server = None
        self.assertIsNone(server)
 
        self.api.create_listener(protocol=self.protocol, port=2288, server_port=self.server_port,
                        server_protocol=self.server_protocol, backend='role:4321')
 
        self.api.add_server(ipaddr=self.ipaddr)
        self.api.add_server(ipaddr='10.168.58.46')
 
        self.assertEqual(self.api.cfg.backends['scalr:backend:%s:%s:%s' % (self.backend,
                self.server_protocol, self.server_port)]['server']['10-168-58-46'],
                {'address': '10.168.58.46', 'port': str(self.server_port), 'check':True})
 
        self.assertEqual(self.api.cfg.backends['scalr:backend:%s:%s:%s' % ('role:4321',
                self.server_protocol, self.server_port)]['server']['10-168-58-46'],
                {'address': '10.168.58.46', 'port': str(self.server_port), 'check':True})
 
 
        for srv in self.api.list_servers():
            self.api.remove_server(ipaddr=srv)
 
        try:
            server = self.api.cfg.backends['scalr:backend:%s:%s:%s' % (self.backend,
                    self.protocol, self.port)]['server'][self.ipaddr.replace('.', '-')]
 
            server = self.api.cfg.backends['scalr:backend:%s:%s:%s' % ('role:4321',
                    self.protocol, self.port)]['server']['10-168-58-46']
        except:
            server = None
        self.assertIsNone(server)
 
        iptables.remove_rule_once('ACCEPT', 2288, 'tcp')
 
    def test_configure_healthcheck(self):
        flag = True
        try:
            self.api.configure_healthcheck(target=None,
                                                                    interval=5,
                                                                    timeout=3,
                                                                    unhealthy_threshold=2,
                                                            healthy_threshold=10)
            flag = False
        except Exception, e:
            LOG.debug('Backend not exist. Details: %s', e)
            self.assertTrue(flag)
 
        self.api.create_listener(protocol=self.protocol, port=self.port, server_port=self.server_port,
                server_protocol=self.server_protocol, backend=self.backend)
 
        self.api.add_server(ipaddr=self.ipaddr, backend=self.backend)
        self.api.add_server(ipaddr='218.124.68.210', backend=self.backend)
 
        self.api.configure_healthcheck(target='%s:%s' % (self.server_protocol, self.server_port),
                                                                        interval='5m',
                                                                        timeout=5,
                                                                        unhealthy_threshold=20,
                                                                healthy_threshold=100)
 
        self.assertEqual(self.api.cfg.backends['scalr:backend:%s:%s:%s' % (self.backend,
                self.server_protocol, self.server_port)]['default-server'], {'inter': '5m', 'rise': '100', 'fall': '20'})
        self.assertTrue(self.api.cfg.backends['scalr:backend:%s:%s:%s' % (self.backend,
                self.server_protocol, self.server_port)]['server'][self.ipaddr.replace('.','-')]['check'])
 
        try:
            self.api.configure_healthcheck(target='http:14080',
                                                                    interval=5,
                                                                    timeout=3,
                                                                    unhealthy_threshold=2,
                                                            healthy_threshold=10)
            flag = False
        except Exception, e:
            LOG.debug('Backend not exist. Details: %s', e)
            self.assertTrue(flag)
 
    def test_reset_healthcheck(self):
        self.api.create_listener(protocol=self.protocol, port=self.port, server_port=self.server_port,
                server_protocol=self.server_protocol, backend=self.backend)
 
        self.api.add_server(ipaddr=self.ipaddr, backend=self.backend)
        self.api.add_server(ipaddr='218.124.68.210', backend=self.backend)
 
        LOG.debug('-----------------------------------------')
        target='%s:%s' % (self.server_protocol, self.server_port)
        LOG.debug('target = `%s`', target)
        LOG.debug('-----------------------------------------')
        self.api.configure_healthcheck(target=target,
                                                                        interval='29m',
                                                                        timeout='3m',
                                                                        unhealthy_threshold=20,
                                                                healthy_threshold=100)
 
 
        self.api.reset_healthcheck(target)
 
        self.assertEqual(self.api.cfg.backends['scalr:backend:%s:%s:%s' % (self.backend,
                self.server_protocol, self.server_port)]['default-server'], {'inter': '30s', 'rise': '10', 'fall': '2'})
        self.assertEqual(self.api.cfg.backends['scalr:backend:%s:%s:%s' % (self.backend,
                self.server_protocol, self.server_port)]['timeout']['check'], '3s')
        #self.assertTrue(self.api.cfg.backends['scalr:backend:%s:%s:%s' % (self.backend,
        #       self.protocol, self.port)]['server'][self.ipaddr.replace('.','-')]['check'])
 
    def test_list_servers(self):
        self.api.create_listener(protocol=self.protocol, port=self.port,
                server_protocol=self.server_protocol, server_port=self.server_port, backend=self.backend)
        self.api.add_server(ipaddr=self.ipaddr, backend=self.backend)
        self.api.add_server(ipaddr='218.124.68.210', backend=self.backend)
 
        self.api.create_listener(protocol='tcp', port=46852, backend='role:468513')
        self.api.add_server(ipaddr='18.24.6.10', backend='role:468513')
        self.api.add_server(ipaddr='218.45.86.41', backend='role:468513')
 
        servers = self.api.list_servers()
        self.assertEqual(servers, ['248.64.125.158', '218.124.68.210', '18.24.6.10', '218.45.86.41'])
        servers = self.api.list_servers('role:468513')
        self.assertEqual(servers, ['18.24.6.10', '218.45.86.41'])
        self.api.delete_listener(protocol='tcp', port=46852)
 
    def test_list_listeners(self):
        self.api.create_listener(protocol=self.protocol, port=self.port,
                server_port=self.server_port, backend=self.backend)
        self.api.create_listener(protocol=self.protocol, port=int('1%s' % self.port),
                server_port=self.server_port, backend='%s5' % self.backend)
 
        listens = self.api.list_listeners()
 
        self.assertIsNotNone(listens[0])
        self.assertIsNotNone(listens[1])
        self.api.delete_listener(protocol=self.protocol, port=int('1%s' % self.port))
 
    def test_get_servers_health(self):
        try:
            stats = self.api.get_servers_health(self.ipaddr)
        except Exception, e:
            import sys
            raise AttributeError, 'Error recived servers health, details: %s' % e, sys.exc_info()[2]
        LOG.debug('%s', stats)
        self.assertIsNotNone(stats)
 
def tearDownModule():
    #os.remove(TEMP_PATH)
    pass
if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()
 
