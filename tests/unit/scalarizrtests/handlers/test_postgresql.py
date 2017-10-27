'''
Created on April 21th 2011
 
@author: shaitanich
'''
import unittest
from scalarizr.handlers                 import postgresql
from scalarizr.util import initdv2
 
class Test(unittest.TestCase):
 
 
    def setUp(self):
        pass
 
 
    def tearDown(self):
        pass
 
 
    def _testPgHbaRecord(self):
        record = postgresql.PgHbaRecord()
        print "'%s'" % record
        s = str(record)
        self.assertEquals(str(record), str(postgresql.PgHbaRecord.from_string(s)))
        self.assertEquals(record, postgresql.PgHbaRecord.from_string(s))
 
        raw = 'hostnossl  database  scalr  127.0.0.1  10.0.0.0  password  auth=options'
        r2 = postgresql.PgHbaRecord.from_string(raw)
        s2 = str(r2)
        print raw, '\n', s2
        self.assertEquals(str(r2), str(postgresql.PgHbaRecord.from_string(s2)))
        self.assertEquals(r2, postgresql.PgHbaRecord.from_string(s2))
 
    def test_psql(self):
        self.psql = postgresql.PSQL()
        print self.psql.list_pg_databases()
        print self.psql.list_pg_roles()
        print self.psql.test_connection()
        pg = postgresql.PostgreSql()
        pg.service.start()
 
if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()
 
