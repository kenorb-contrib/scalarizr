
'''
Created on Dec 04, 2011

@author: marat
'''
import sys
import string
import os

from scalarizr.node import __node__
from scalarizr.services.mysql2 import __mysql__

from scalarizr import rpc, storage2
from scalarizr.api import operation
from scalarizr.services import mysql as mysql_svc
from scalarizr.services import backup as backup_module
from scalarizr.services import ServiceError
from scalarizr.util.cryptotool import pwgen
from scalarizr.handlers import build_tags
from scalarizr.util import Singleton
from scalarizr import linux
from scalarizr.linux import pkgmgr
from scalarizr import exceptions
from scalarizr.api import BehaviorAPI
from scalarizr.api import SoftwareDependencyError


class MySQLAPI(BehaviorAPI):
    """
    Basic API for replacing data volume, changing mysql passwords,
    creating backups, monitoring replication and controlling service status.

    Namespace::

        mysql
    """
    __metaclass__ = Singleton

    behavior = ['mysql', 'mysql2']

    error_messages = {
        'empty': "'%s' can't be blank",
        'invalid': "'%s' is invalid, '%s' expected"
    }

    def __init__(self):
        self._mysql_init = mysql_svc.MysqlInitScript()
        self._op_api = operation.OperationAPI()

    @rpc.command_method
    def start_service(self):
        """
        Starts MySQL service.

        Example::

            api.mysql.start_service()
        """
        self._mysql_init.start()

    @rpc.command_method
    def stop_service(self, reason=None):
        """
        Stops MySQL service.

        :param reason: Message to appear in log before service is stopped.
        :type reason: str

        Example::

            api.mysql.stop_service("Configuring MySQL service.")
        """
        self._mysql_init.stop(reason)

    @rpc.command_method
    def reload_service(self):
        """
        Reloads MySQL service.

        :param reason: Message to appear in log before service is reloaded.
        :type reason: str

        Example::

            api.mysql.reload_service("Applying new settings in my.cnf")
        """
        self._mysql_init.reload()

    @rpc.command_method
    def restart_service(self):
        """
        Restarts MySQL service.

        :param reason: Message to appear in log before service is restarted.
        :type reason: str

        Example::

            api.mysql.restart_service("Applying new service configuration preset.")
        """
        self._mysql_init.restart()

    @rpc.command_method
    def get_service_status(self):
        """
        Checks Apache service status.

        RUNNING = 0
        DEAD_PID_FILE_EXISTS = 1
        DEAD_VAR_LOCK_EXISTS = 2
        NOT_RUNNING = 3
        UNKNOWN = 4

        :return: Status num.
        :rtype: int
        """
        return self._mysql_init.status()

    @rpc.command_method
    def grow_volume(self, volume, growth, async=False):
        """
        Stops MySQL service, Extends volume capacity and starts MySQL service again.
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

            new_vol = api.mysql.grow_volume(
                volume={
                    'id': 'vol-e13aa63ef',
                },
                growth={
                    'size': 50
                }
            )
        """
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
        """
        Checks current replication status.

        :return: MySQL replication status.
        :rtype: dict
        """
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
        """
        Creates a new backup of every available database and uploads gzipped data to the cloud storage.
        """
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

    @classmethod
    def do_check_software(cls, system_packages=None):
        requirements = None
        if linux.os.debian_family:
            requirements = [
                ['mysql-server>=5.0,<5.7', 'mysql-client>=5.0,<5.7'],
                ['mysql-server-5.1', 'mysql-client-5.1'],
                ['mysql-server-5.5', 'mysql-client-5.5'],
                ['mysql-server-5.6', 'mysql-client-5.6'],
                ['percona-server-server-5.1', 'percona-server-client-5.1']
            ]
        elif linux.os.redhat_family or linux.os.oracle_family:
            requirements = [
                ['mysql-server>=5.0,<5.6', 'mysql>=5.0,<5.6'],
                ['mysql-server>=5.0,<5.6', 'mysql55'],
                ['Percona-Server-server-51', 'Percona-Server-client-51']
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

