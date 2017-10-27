'''
Created on Feb 25, 2013
 
@author: uty
'''
 
import os
import re
import logging
import time
import tarfile
import tempfile
import shutil
 
from scalarizr.bus import bus
from scalarizr.node import __node__
from scalarizr.util import PopenError, system2
from scalarizr.util.cryptotool import pwgen
from scalarizr.services import postgresql as postgresql_svc
from scalarizr import rpc
from scalarizr.services import backup
from scalarizr.config import BuiltinBehaviours
from scalarizr.handlers import DbMsrMessages, HandlerError
from scalarizr.api import operation
from scalarizr.linux.coreutils import chown_r
from scalarizr.services.postgresql import PSQL, PG_DUMP, SU_EXEC
from scalarizr.storage2.cloudfs import LargeTransfer
 
 
LOG = logging.getLogger(__name__)
 
 
BEHAVIOUR = SERVICE_NAME = BuiltinBehaviours.POSTGRESQL
STORAGE_PATH = "/mnt/pgstorage"
OPT_SNAPSHOT_CNF = 'snapshot_config'
OPT_REPLICATION_MASTER = postgresql_svc.OPT_REPLICATION_MASTER
__postgresql__ = postgresql_svc.__postgresql__
 
 
class PostgreSQLAPI(object):
 
    replication_status_query = '''SELECT
    CASE WHEN pg_last_xlog_receive_location() = pg_last_xlog_replay_location()
    THEN 0
    ELSE EXTRACT (EPOCH FROM now() - pg_last_xact_replay_timestamp()) END
    AS xlog_delay;
    '''
 
    def __init__(self):
        self._op_api = operation.OperationAPI()
        self.postgresql = postgresql_svc.PostgreSql()  #?
 
    @rpc.command_method
    def reset_password(self, new_password=None):
        """ Reset password for PostgreSQL user 'scalr_master'. Return new password """
        if not new_password:
            new_password = pwgen(10)
        pg = postgresql_svc.PostgreSql()
        if pg.master_user.exists():
            pg.master_user.change_role_password(new_password)
            pg.master_user.change_system_password(new_password)
        else:
            pg.create_linux_user(pg.master_user.name, new_password)
            pg.create_pg_role(pg.master_user.name,
                                new_password,
                                super=True,
                                force=False)
        return new_password
 
    def _parse_query_out(self, out):
        '''
        Parses xlog_delay or error string from strings like:
         log_delay
        -----------
                 034
        (1 row)
 
        and:
        ERROR:  function pg_last_xact_replay_timesxtamp() does not exist
        LINE 1: select pg_last_xact_replay_timesxtamp() as not_modified_sinc...
                       ^
        HINT:  No function matches the given name and argument...
 
        '''
        result = {'error': None, 'xlog_delay': None}
        error_match = re.search(r'ERROR:.*?\n', out)
        if error_match:
            result['error'] = error_match.group()
            return result
 
        diff_match = re.search(r'xlog_delay.+-\n *\d+', out, re.DOTALL)
        if not diff_match:
            #if no error and query returns nothing
            return result
 
        result['xlog_delay'] = diff_match.group().splitlines()[-1].strip()
        return result
 
    @rpc.query_method
    def replication_status(self):
        psql = postgresql_svc.PSQL()
        try:
            query_out = psql.execute(self.replication_status_query)
        except PopenError, e:
            if 'function pg_last_xact_replay_timestamp() does not exist' in str(e):
                raise BaseException('This version of PostgreSQL server does not support replication status')
            else:
                raise e
        query_result = self._parse_query_out(query_out)
 
        is_master = int(__postgresql__[OPT_REPLICATION_MASTER])
 
        if not query_result['xlog_delay']:
            if is_master:
                return {'master': {'status': 'up'}}
            return {'slave': {'status': 'down',
                              'error': query_result['error']}}
        return {'slave': {'status': 'up',
                          'xlog_delay': query_result['xlog_delay']}}
 
 
    @rpc.command_method
    def create_databundle(self, async=True):
 
        def do_databundle(op):
            try:
                bus.fire('before_postgresql_data_bundle')
                LOG.info("Creating PostgreSQL data bundle")
                backup_obj = backup.backup(type='snap_postgresql',
                                           volume=__postgresql__['volume'],
                                           tags=__postgresql__['volume'].tags)
                restore = backup_obj.run()
                snap = restore.snapshot
 
 
                used_size = int(system2(('df', '-P', '--block-size=M', STORAGE_PATH))[0].split('\n')[1].split()[2][:-1])
                bus.fire('postgresql_data_bundle', snapshot_id=snap.id)
 
                # Notify scalr
                msg_data = {
                    'db_type': BEHAVIOUR,
                    'status': 'ok',
                    'used_size' : '%.3f' % (float(used_size) / 1000,),
                    BEHAVIOUR: {OPT_SNAPSHOT_CNF: dict(snap)}
                }
 
                __node__.messaging.send(DbMsrMessages.DBMSR_CREATE_DATA_BUNDLE_RESULT,
                                        msg_data)
 
                return restore
 
            except (Exception, BaseException), e:
                LOG.exception(e)
 
                # Notify Scalr about error
                __node__.messaging.send(DbMsrMessages.DBMSR_CREATE_DATA_BUNDLE_RESULT, 
                                        dict(db_type=BEHAVIOUR,
                                             status='error',
                                             last_error=str(e)))
 
        return self._op_api.run('postgresql.create-databundle', 
                                func=do_databundle,
                                func_kwds={},
                                async=async,
                                exclusive=True)
 
 
    @rpc.command_method
    def create_backup(self, async=True):
 
        def do_backup(op):
            tmpdir = backup_path = None
            tmp_path = os.path.join(__postgresql__['storage_dir'], 'tmp')
            try:
                # Get databases list
                psql = PSQL(user=self.postgresql.root_user.name)
                databases = psql.list_pg_databases()
                if 'template0' in databases:
                    databases.remove('template0')
 
                if not os.path.exists(tmp_path):
                    os.makedirs(tmp_path)
 
                # Defining archive name and path
                backup_filename = time.strftime('%Y-%m-%d-%H:%M:%S')+'.tar.gz'
                backup_path = os.path.join(tmp_path, backup_filename)
 
                # Creating archive 
                backup_obj = tarfile.open(backup_path, 'w:gz')
 
                # Dump all databases
                LOG.info("Dumping all databases")
                tmpdir = tempfile.mkdtemp(dir=tmp_path)       
                chown_r(tmpdir, self.postgresql.root_user.name)
 
                def _single_backup(db_name):
                    dump_path = tmpdir + os.sep + db_name + '.sql'
                    pg_args = '%s %s --no-privileges -f %s' % (PG_DUMP, db_name, dump_path)
                    su_args = [SU_EXEC, '-', self.postgresql.root_user.name, '-c', pg_args]
                    err = system2(su_args)[1]
                    if err:
                        raise HandlerError('Error while dumping database %s: %s' % (db_name, err))  #?
                    backup_obj.add(dump_path, os.path.basename(dump_path))  
 
                for db_name in databases:
                    _single_backup(db_name)
 
                backup_obj.close()
 
                # Creating list of full paths to archive chunks
                #if os.path.getsize(backup_path) > __postgresql__['pgdump_chunk_size']:
                #    parts = [os.path.join(tmpdir, file) for file in split(backup_path, backup_filename, __postgresql__['pgdump_chunk_size'], tmpdir)]
                #else:
                #    parts = [backup_path]
                #sizes = [os.path.getsize(file) for file in parts]
 
                cloud_storage_path = __node__.platform.scalrfs.backups(BEHAVIOUR)
 
                suffix = 'master' if int(__postgresql__[OPT_REPLICATION_MASTER]) else 'slave'
                backup_tags = {'scalr-purpose': 'postgresql-%s' % suffix}
 
                LOG.info("Uploading backup to %s with tags %s" % (cloud_storage_path, backup_tags))
                trn = LargeTransfer(backup_path, cloud_storage_path, tags=backup_tags)
                manifest = trn.run()
                LOG.info("Postgresql backup uploaded to cloud storage under %s/%s",
                                cloud_storage_path, backup_filename)
 
                result = list(dict(path=os.path.join(os.path.dirname(manifest.cloudfs_path), c[0]), size=c[2]) for c in
                                manifest['files'][0]['chunks'])
 
                # Notify Scalr
                __node__.messaging.send(DbMsrMessages.DBMSR_CREATE_BACKUP_RESULT,
                                        dict(db_type=BEHAVIOUR,
                                             status='ok',
                                             backup_parts=result))
 
                return result  #?
 
            except (Exception, BaseException), e:
                LOG.exception(e)
 
                # Notify Scalr about error
                __node__.messaging.send(DbMsrMessages.DBMSR_CREATE_BACKUP_RESULT,
                                        dict(db_type=BEHAVIOUR,
                                             status='error',
                                             last_error=str(e)))
 
            finally:
                if tmpdir:
                    shutil.rmtree(tmpdir, ignore_errors=True)
                if backup_path and os.path.exists(backup_path):
                    os.remove(backup_path)
 
        return self._op_api.run('postgresql.create-backup', 
                                func=do_backup,
                                func_kwds={},
                                async=async,
                                exclusive=True)
 
 
