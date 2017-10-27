 
import os
import mock
import unittest
 
from scalarizr.services import mysql as mysql_svc
 
class MysqlInitScriptTest(unittest.TestCase):
 
    @mock.patch('scalarizr.services.mysql.MysqlInitScript._get_mysql_logerror_path')
    def test_get_mysql_error(self, mock_get_mysql_logerror_path):
        mock_get_mysql_logerror_path.return_value = '/tmp/test_error.log'
        content = "\n\
130518 15:29:39 mysqld_safe Starting mysqld daemon with databases from /var/lib/mysql\n\
121221 10:32:04 InnoDB: Fatal error: cannot allocate memory for the buffer pool\n\
121221 10:32:04 ERROR Plugin 'InnoDB' init function returned error.\n\
121221 10:32:04 ERROR Plugin 'InnoDB' registration as a STORAGE ENGINE failed.\n\
121221 10:32:04 ERROR Unknown/unsupported storage engine: InnoDB\n\
121221 10:32:04 ERROR Aborting\n\
130518 15:29:39 InnoDB: Mutexes and rw_locks use GCC atomic builtins\n"
        with open('/tmp/test_error.log', 'w') as f:
            f.write(content)
 
        right_content = "\
121221 10:32:04 InnoDB: Fatal error: cannot allocate memory for the buffer pool\n\
121221 10:32:04 ERROR Plugin 'InnoDB' init function returned error.\n\
121221 10:32:04 ERROR Plugin 'InnoDB' registration as a STORAGE ENGINE failed.\n\
121221 10:32:04 ERROR Unknown/unsupported storage engine: InnoDB\n\
121221 10:32:04 ERROR Aborting\n"
        obj = mysql_svc.MysqlInitScript()
        assert obj._get_mysql_error() == right_content
 
        os.remove('/tmp/test_error.log')
 
 
if __name__ == "__main__":
    unittest.main()
 
