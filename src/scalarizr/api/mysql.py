'''
Created on Dec 04, 2011
 
@author: marat
'''
import sys
import string
 
from scalarizr.node import __node__
from scalarizr.services.mysql2 import __mysql__
 
from scalarizr import rpc, storage2
from scalarizr.api import operation
from scalarizr.services import mysql as mysql_svc
from scalarizr.services import backup as backup_module
from scalarizr.services import ServiceError
from scalarizr.util.cryptotool import pwgen
from scalarizr.handlers import build_tags
 
 
class MySQLAPI(object):
    """
    @xxx: reporting is a pain
    """
 
    error_messages = {
        'empty': "'%s' can't be blank",
        'invalid': "'%s' is invalid, '%s' expected"
    }
 
    def __init__(self):
        self._mysql_init = mysql_svc.MysqlInitScript()
        self._op_api = operation.OperationAPI()
 
    @rpc.command_method
    def grow_volume(self, volume, growth, async=False):
        self._check_invalid(volume, 'volume', dict)
        self._check_empty(volume.get('id'), 'volume.id')
 
        def do_grow(op):
            vol = storage2.volume(volume)
            self._mysql_init.stop('Growing data volume')
            try:
                growed_vol = vol.grow(**growth)
                __mysql__['volume'] = dict(growed_vol)
                return dict(growed_vol)
            finally:
                self._mysql_init.start()
 
        return self._op_api.run('mysql.grow-volume', do_grow, exclusive=True, async=async)
 
 
    def _check_invalid(self, param, name, type_):
        assert isinstance(param, type_), \
            self.error_messages['invalid'] % (name, type_)
 
    def _check_empty(self, param, name):
        assert param, self.error_messages['empty'] % name
 
    @rpc.command_method
    def reset_password(self, new_password=None):
        """
        Reset password for MySQL user 'scalr_master'. Return new password
        """
        if not new_password:
            new_password = pwgen(20)
        mysql_cli = mysql_svc.MySQLClient(__mysql__['root_user'],
                                          __mysql__['root_password'])
        master_user = __mysql__['master_user']
 
        if mysql_cli.user_exists(master_user, 'localhost'):
            mysql_cli.set_user_password(master_user, 'localhost', new_password)
        else:
            mysql_cli.create_user(master_user, 'localhost', new_password)
 
        if mysql_cli.user_exists(master_user, '%'):
            mysql_cli.set_user_password(master_user, '%', new_password)
        else:
            mysql_cli.create_user(master_user, '%', new_password)
 
        mysql_cli.flush_privileges()
 
        return new_password
 
    @rpc.query_method
    def replication_status(self):
        mysql_cli = mysql_svc.MySQLClient(__mysql__['root_user'],
                                          __mysql__['root_password'])
        if int(__mysql__['replication_master']):
            master_status = mysql_cli.master_status()
            result = {'master': {'status': 'up',
                                 'log_file': master_status[0],
                                 'log_pos': master_status[1]}}
            return result
        else:
            try:
                slave_status = mysql_cli.slave_status()
                slave_status = dict(zip(map(string.lower, slave_status.keys()),
                                        slave_status.values()))
                slave_running = slave_status['slave_io_running'] == 'Yes' and \
                    slave_status['slave_sql_running'] == 'Yes'
                slave_status['status'] = 'up' if slave_running else 'down'
                return {'slave': slave_status}
            except ServiceError:
                return {'slave': {'status': 'down'}}
 
 
    @rpc.command_method
    def create_backup(self, backup=None, async=True):
 
        def do_backup(op, backup_conf=None):
            try:
                purpose = '{0}-{1}'.format(
                        __mysql__.behavior, 
                        'master' if int(__mysql__.replication_master) == 1 else 'slave')
                backup = {
                    'type': 'mysqldump',
                    'cloudfs_dir': __node__.platform.scalrfs.backups('mysql'),
                    'description': 'MySQL backup (farm: {0} role: {1})'.format(
                            __node__.farm_id, __node__.role_name),
                    'tags': build_tags(purpose, 'active')
                }
                backup.update(backup_conf or {})
 
                if backup['type'] == 'snap_mysql':
                    backup['description'].replace('backup', 'data bundle')
                    backup['volume'] = __mysql__.volume
 
                bak = op.data['bak'] = backup_module.backup(**backup)
                try:
                    restore = bak.run()
                finally:
                    del op.data['bak']
 
                # For Scalr < 4.5.0
                if bak.type == 'mysqldump':
                    __node__.messaging.send('DbMsr_CreateBackupResult', {
                        'db_type': __mysql__.behavior,
                        'status': 'ok',
                        'backup_parts': restore.parts
                    })
                else:
                    data = {
                        'restore': dict(restore)
                    }
                    if backup["type"] == 'snap_mysql':
                        data.update({
                            'snapshot_config': dict(restore.snapshot),
                            'log_file': restore.log_file,
                            'log_pos': restore.log_pos,
                        })
                    __node__.messaging.send('DbMsr_CreateDataBundleResult', {
                        'db_type': __mysql__.behavior,
                        'status': 'ok',
                        __mysql__.behavior: data
                    })
 
                return dict(restore)
            except:
                # For Scalr < 4.5.0
                c, e, t = sys.exc_info()
                msg_name = 'DbMsr_CreateBackupResult' \
                            if backup['type'] == 'mysqldump' else \
                            'DbMsr_CreateDataBundleResult'
                __node__.messaging.send(msg_name, {
                    'db_type': __mysql__.behavior,
                    'status': 'error',
                    'last_error': str(e)
                })
                raise c, e, t
 
        def cancel_backup(op):
            bak = op.data.get('bak')
            if bak:
                bak.kill()
 
        return self._op_api.run('mysql.create-backup', 
                func=do_backup, cancel_func=cancel_backup, 
                func_kwds={'backup_conf': backup},
                async=async, exclusive=True)
 
 
