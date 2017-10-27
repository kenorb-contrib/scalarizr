 
from nose.tools import eq_, ok_
import mock
 
@mock.patch.dict('scalarizr.node.__node__', {
	'messaging': mock.MagicMock(),
	'platform': mock.Mock(name='ec2'),
	'behavior': 'percona',
	'role_name': 'percona-centos6',
	'farm_id': 123,
	'mysql': {
		'behavior': 'percona'
	}
})
@mock.patch('scalarizr.services.backup.backup')
class TestMySQLAPI(object):
 
	def test_create_backup_mysqldump(self, backup, *args):
		backup_result =  {'chunk.1' : 'in clouds'}
		backup.return_value.run.return_value = backup_result
		backup.return_value.type = 'mysqldump'
 
		from scalarizr.api.mysql import MySQLAPI
		api = MySQLAPI()
		api.create_backup(async=False)
 
		from scalarizr.node import __node__
		eq_(__node__['messaging'].send.call_args_list[0], 
			mock.call('DbMsr_CreateBackupResult', {'status': 'ok', 'backup_parts': backup_result, 'db_type': 'percona'}))
		args, kwds = __node__['messaging'].send.call_args_list[1]
		eq_(kwds['body']['result'], backup_result)
 
	def test_create_backup_snap(self, backup, *args):
		assert 0
 
