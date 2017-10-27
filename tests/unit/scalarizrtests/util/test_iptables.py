'''
Created on july 21, 2010
 
@author: shaitanich
'''
import unittest
from scalarizr.util import iptables
from scalarizr.util.iptables import RuleSpec, IpTables
from scalarizr.util import system
import time
 
class Test(unittest.TestCase):
 
 
    def setUp(self):
        self.ip_tables = IpTables()
        self.ip_tables.flush()
 
 
    def tearDown(self):
        self.ip_tables.flush()
 
 
    def test_valid_rule(self):
        rule = RuleSpec(protocol=iptables.P_TCP, source='127.0.0.0/8', dport = '90', jump='ACCEPT')
        print"\ntest_valid_rule\n"
        print str(rule)
 
        self.ip_tables.append_rule(rule)
 
        list_system_rules = self.ip_tables.list_rules()
 
        for system_rule in list_system_rules:
            print str(system_rule[0])
 
        self.assertEquals(rule, list_system_rules[0][0])
        print "Objects are equal."
 
 
    def test_delete_rule(self):
        rule = RuleSpec(protocol=iptables.P_TCP, destination='127.0.0.0/8', jump='ACCEPT')
 
        print "\ntest_delete_rule\n"
        print "added:",str(rule)
 
        self.ip_tables.append_rule(rule)
        print "received:", str(self.ip_tables.list_rules()[0][0])
        self.ip_tables.delete_rule(rule)
        print "deleted."
        self.ip_tables.flush()
 
        self.assertEquals(self.ip_tables.list_rules(), [])
 
 
    def test_insert_rule(self):
        rule = RuleSpec(protocol=iptables.P_TCP, source='127.0.0.0/8', jump='DROP')
 
        print "\ntest_insert_rule\n"
        print "Filling table with rules"
 
        self.ip_tables.append_rule(rule)
        print str(self.ip_tables.list_rules()[0][0])
        print "Number of rules in table:", len(self.ip_tables.list_rules())
        print "Inserting rule to the top"
 
        new_rule = RuleSpec(protocol=iptables.P_TCP, destination='127.0.0.0/8', jump='DROP')
        self.ip_tables.insert_rule('1', new_rule)
 
        print "Number of rules in table changed to:", len(self.ip_tables.list_rules())
        print "New rule:\n", str(new_rule)
 
        self.assertEquals(len(self.ip_tables.list_rules()), 2)
 
        self.assertEquals(new_rule, self.ip_tables.list_rules()[0][0])
        self.assertEquals(self.ip_tables.list_rules()[1][0], rule)
 
 
if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()
 
