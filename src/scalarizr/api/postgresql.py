'''
Created on Feb 25, 2013

@author: uty
'''

import os
import re
import sys
import logging
import tempfile
import shutil

from scalarizr.bus import bus
from scalarizr.node import __node__
from scalarizr.util import PopenError, system2
from scalarizr.util.cryptotool import pwgen
from scalarizr.services import postgresql as postgresql_svc
from scalarizr import rpc, storage2
from scalarizr import linux
from scalarizr.services import backup
from scalarizr.config import BuiltinBehaviours
from scalarizr.handlers import DbMsrMessages, HandlerError
from scalarizr.handlers import transfer_result_to_backup_result
from scalarizr.api import operation
from scalarizr.linux.coreutils import chown_r
from scalarizr.services.postgresql import PSQL, PG_DUMP, SU_EXEC
from scalarizr.storage2 import largetransfer
from scalarizr.util import Singleton
from scalarizr.linux import pkgmgr
from scalarizr import exceptions
from scalarizr.api import BehaviorAPI
from scalarizr.api import SoftwareDependencyError


LOG = logging.getLogger(__name__)


BEHAVIOUR = SERVICE_NAME = BuiltinBehaviours.POSTGRESQL
STORAGE_PATH = "/mnt/pgstorage"
OPT_SNAPSHOT_CNF = 'snapshot_config'
OPT_REPLICATION_MASTER = postgresql_svc.OPT_REPLICATION_MASTER
__postgresql__ = postgresql_svc.__postgresql__


class PostgreSQLAPI(BehaviorAPI):
    """
    Basic API for managing PostgreSQL 9.x service.

    Namespace::

        postgresql
    """
    __metaclass__ = Singleton

    behavior = 'postgresql'

    replication_status_query = '''SELECT
    CASE WHEN pg_last_xlog_receive_location() = pg_last_xlog_replay_location()
    THEN 0
    ELSE EXTRACT (EPOCH FROM now() - pg_last_xact_replay_timestamp()) END
    AS xlog_delay;
    '''

    def __init__(self):
        self._op_api = operation.OperationAPI()
        self.postgresql = postgresql_svc.PostgreSql()  #?
        self.service = postgresql_svc.PgSQLInitScript()

    @rpc.command_method
    def start_service(self):
        """
        Starts PostgreSQL service.

        Example::

            api.postgresql.start_service()
        """
        self.service.start()

    @rpc.command_method
    def stop_service(self, reason=None):
        """
        Stops PostgreSQL service.

        Example::

            api.postgresql.stop_service()
        """
        self.service.stop(reason)

    @rpc.command_method
    def reload_service(self):
        """
        Reloads PostgreSQL configuration.

        Example::

            api.postgresql.reload_service()
        """
        self.service.reload()

    @rpc.command_method
    def restart_service(self):
        """
        Restarts PostgreSQL service.

        Example::

            api.postgresql.restart_service()
        """
        self.service.restart()

    @rpc.command_method
    def get_service_status(self):
        """
        Checks PostgreSQL service status.

        RUNNING = 0
        DEAD_PID_FILE_EXISTS = 1
        DEAD_VAR_LOCK_EXISTS = 2
        NOT_RUNNING = 3
        UNKNOWN = 4

        :return: Status num.
        :rtype: int


        Example::

            >>> api.postgresql.get_service_status()
            0
        """
        return self.service.status()

    @rpc.command_method
    def reset_password(self, new_password=None):
        """
        Resets password for PostgreSQL user 'scalr_main'.

        :returns: New password
        :rtype: str
        """
        if not new_password:
            new_password = pwgen(10)
        pg = postgresql_svc.PostgreSql()
        if pg.main_user.exists():
            pg.main_user.change_role_password(new_password)
            pg.main_user.change_system_password(new_password)
        else:
            pg.create_linux_user(pg.main_user.name, new_password)
            pg.create_pg_role(pg.main_user.name,
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

        split = out.split()  # [SCALARIZR-1642]
        if split and "xlog_delay" in split[0] and len(split) > 2:
            lag = split[2]
            try:
                result['xlog_delay'] = int(float(lag))
            except ValueError:
                pass

        return result

    @rpc.query_method
    def replication_status(self):
        """
        Checks current replication status.

        :return: Postgresql replication status.
        :rtype: dict

        Examples::

            On main:

            {'main': {'status': 'up'}}

            On broken subordinate:

            {'subordinate': {'status': 'down','error': <errmsg>}}

            On normal subordinate:

            {'subordinate': {'status': 'up', 'xlog_delay': <xlog_delay>}}

        """
        psql = postgresql_svc.PSQL()
        try:
            query_out = psql.execute(self.replication_status_query)
        except PopenError, e:
            if 'function pg_last_xact_replay_timestamp() does not exist' in str(e):
                raise BaseException('This version of PostgreSQL server does not support replication status')
            else:
                raise e
        query_result = self._parse_query_out(query_out)

        is_main = int(__postgresql__[OPT_REPLICATION_MASTER])

        if query_result['xlog_delay'] is None:
            if is_main:
                return {'main': {'status': 'up'}}
            return {'subordinate': {'status': 'down',
                              'error': query_result['error']}}
        return {'subordinate': {'status': 'up',
                          'xlog_delay': query_result['xlog_delay']}}


    @rpc.command_method
    def create_databundle(self, async=True):
        """
        Creates a new data bundle of /mnt/pgstrage.
        """

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
                    'used_size': '%.3f' % (float(used_size) / 1000,),
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
        """
        Creates a new backup of every available database and uploads gzipped data to the cloud storage.

        .. Warning::
            System database 'template0' is not included in backup.
        """

        def do_backup(op):
            tmpdir = None
            dumps = []
            tmp_path = os.path.join(__postgresql__['storage_dir'], 'tmp')
            try:
                # Get databases list
                psql = PSQL(user=self.postgresql.root_user.name)
                databases = psql.list_pg_databases()
                if 'template0' in databases:
                    databases.remove('template0')

                if not os.path.exists(tmp_path):
                    os.makedirs(tmp_path)

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
                    dumps.append(dump_path)

                for db_name in databases:
                    _single_backup(db_name)

                cloud_storage_path = __node__.platform.scalrfs.backups(BEHAVIOUR)

                suffix = 'main' if int(__postgresql__[OPT_REPLICATION_MASTER]) else 'subordinate'
                backup_tags = {'scalr-purpose': 'postgresql-%s' % suffix}

                LOG.info("Uploading backup to %s with tags %s" % (cloud_storage_path, backup_tags))

                def progress_cb(progress):
                    LOG.debug('Uploading %s bytes' % progress)

                uploader = largetransfer.Upload(dumps, cloud_storage_path, progress_cb=progress_cb)
                uploader.apply_async()
                uploader.join()
                manifest = uploader.manifest

                LOG.info("Postgresql backup uploaded to cloud storage under %s", cloud_storage_path)
                LOG.debug(manifest.data)

                # Notify Scalr
                result = transfer_result_to_backup_result(manifest)
                __node__.messaging.send(DbMsrMessages.DBMSR_CREATE_BACKUP_RESULT,
                                        dict(db_type=BEHAVIOUR,
                                             status='ok',
                                             backup_parts=result))

                return result

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

        return self._op_api.run('postgresql.create-backup',
                                func=do_backup,
                                func_kwds={},
                                async=async,
                                exclusive=True)


    @classmethod
    def do_check_software(cls, system_packages=None):
        os_name = linux.os['name'].lower()
        os_vers = linux.os['version']
        requirements = None
        if os_name == 'ubuntu':
            if os_vers >= '12':
                requirements = [
                    ['postgresql-9.1', 'postgresql-client-9.1'],
                    ['postgresql-9.2', 'postgresql-client-9.2'],
                    ['postgresql-9.3', 'postgresql-client-9.3'],
                    ['postgresql>=9.1,<9.4', 'postgresql-client>=9.1,<9.4'],
                ]
            elif os_vers >= '10':
                requirements = [
                    ['postgresql-9.0', 'postgresql-client-9.0'],
                    ['postgresql-9.1', 'postgresql-client-9.1'],
                    ['postgresql-9.2', 'postgresql-client-9.2'],
                    ['postgresql-9.3', 'postgresql-client-9.3'],
                    ['postgresql>=9.0,<9.4', 'postgresql-client>=9.0,<9.4'],
                ]
        elif os_name == 'debian':
            requirements = [
                ['postgresql-9.2', 'postgresql-client-9.2'],
                ['postgresql-9.3', 'postgresql-client-9.3'],
                ['postgresql>=9.2,<9.4', 'postgresql-client>=9.2,<9.4'],
            ]
        elif linux.os.redhat_family:
            if os_vers >= '6':
                requirements = [
                    ['postgresql91-server', 'postgresql91', 'postgresql91-devel'],
                    ['postgresql92-server', 'postgresql92', 'postgresql92-devel'],
                    ['postgresql93-server', 'postgresql93', 'postgresql93-devel'],
                    ['postgresql-server>=9.1,<9.4', 'postgresql>=9.1,<9.4', 'postgresql-devel>=9.1,<9.4'],
                ]
            elif os_vers >= '5':
                requirements = [
                    ['postgresql90-server', 'postgresql90', 'postgresql90-devel'],
                    ['postgresql91-server', 'postgresql91', 'postgresql91-devel'],
                    ['postgresql92-server', 'postgresql92', 'postgresql92-devel'],
                    ['postgresql93-server', 'postgresql93', 'postgresql93-devel'],
                    ['postgresql-server>=9.0,<9.4', 'postgresql>=9.0,<9.4', 'postgresql-devel>=9.0,<9.4'],
                ]
        elif linux.os.oracle_family:
            requirements = [
                ['postgresql92-server', 'postgresql92', 'postgresql92-devel'],
                ['postgresql-server>=9.2,<9.3', 'postgresql>=9.2,<9.3', 'postgresql-devel>=9.2,<9.3'],
            ]
        if requirements is None:
            raise exceptions.UnsupportedBehavior(
                    cls.behavior,
                    "Not supported on {0} os family".format(linux.os['family']))
        errors = list()
        for requirement in requirements:
            try:
                installed = pkgmgr.check_software(requirement[0], system_packages)[0]
                try:
                    pkgmgr.check_software(requirement[1:], system_packages)
                    return installed
                except pkgmgr.NotInstalledError:
                    e = sys.exc_info()[1]
                    raise SoftwareDependencyError(e.args[0])
            except:
                e = sys.exc_info()[1]
                errors.append(e)
        for cls in [pkgmgr.VersionMismatchError, SoftwareDependencyError, pkgmgr.NotInstalledError]:
            for error in errors:
                if isinstance(error, cls):
                    raise error

    @rpc.command_method
    def grow_volume(self, volume, growth, async=False):
        """
        Stops PostgreSQL service, Extends volume capacity and starts PostgreSQL service again.
        Depending on volume type growth parameter can be size in GB or number of disks (e.g. for RAID volumes)

        :type volume: dict
        :param volume: Volume configuration object

        :type growth: dict
        :param growth: size in GB for regular disks or number of volumes for RAID configuration.

        Growth keys:

            - size (Type: int, Availability: ebs, csvol, cinder, gce_persistent) -- A new size for persistent volume.
            - iops (Type: int, Availability: ebs) -- A new IOPS value for EBS volume.
            - volume_type (Type: string, Availability: ebs) -- A new volume type for EBS volume. Values: "standard" | "io1".
            - disks (Type: Growth, Availability: raid) -- A growth dict for underlying RAID volumes.
            - disks_count (Type: int, Availability: raid) - number of disks.

        :type async: bool
        :param async: Execute method in a separate thread and report status
                        with Operation/Steps mechanism.

        Example:

        Grow EBS volume to 50Gb::

            new_vol = api.postgresql.grow_volume(
                volume={
                    'id': 'vol-e13aa63ef',
                },
                growth={
                    'size': 50
                }
            )
        """

        assert isinstance(volume, dict), "volume configuration is invalid, 'dict' type expected"
        assert volume.get('id'), "volume.id can't be blank"

        def do_grow(op):
            vol = storage2.volume(volume)
            self.stop_service(reason='Growing data volume')
            try:
                grown_vol = vol.grow(**growth)
                postgresql_svc.__postgresql__['volume'] = dict(grown_vol)
                return dict(grown_vol)
            finally:
                self.start_service()

        return self._op_api.run('postgresql.grow-volume', do_grow, exclusive=True, async=async)

