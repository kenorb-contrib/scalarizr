'''
Created on Nov 15, 2011
 
@author: dmitry
'''
 
import os
import sys
import time
import shutil
import logging
import glob
import threading
 
# Core
from scalarizr.bus import bus
from scalarizr.messaging import Messages
from scalarizr.handlers import ServiceCtlHandler, DbMsrMessages, HandlerError, \
        build_tags
import scalarizr.services.mysql as mysql_svc
from scalarizr.service import CnfController, _CnfManifest
from scalarizr.services import ServiceError
from scalarizr.platform import UserDataOptions
from scalarizr.libs import metaconf
from scalarizr.util import system2, disttool, firstmatched, initdv2, software, cryptotool
 
 
from scalarizr import storage2, linux
from scalarizr.linux import iptables, coreutils, pkgmgr
from scalarizr.services import backup
from scalarizr.services import mysql2 as mysql2_svc  # backup/restore providers
from scalarizr.node import __node__
from scalarizr.api import service as preset_service
from scalarizr.api import mysql as mysql_api
from scalarizr.api import operation as operation_api
 
# Libs
from scalarizr.libs.metaconf import Configuration, NoPathError
 
 
LOG = logging.getLogger(__name__)
 
SU_EXEC = '/bin/su'
BASH = '/bin/bash'
 
__mysql__ = mysql2_svc.__mysql__
 
 
 
PRIVILEGES = {
        __mysql__['repl_user']: ('Repl_slave_priv', ),
        __mysql__['stat_user']: ('Repl_client_priv', )
}
 
 
class MysqlMessages:
 
    CREATE_PMA_USER = "Mysql_CreatePmaUser"
    """
    @ivar pma_server_ip: User host
    @ivar farm_role_id
    @ivar root_password
    """
 
    CREATE_PMA_USER_RESULT = "Mysql_CreatePmaUserResult"
    """
    @ivar status: ok|error
    @ivar last_error
    @ivar pma_user
    @ivar pma_password
    @ivar farm_role_id
    """
 
    CONVERT_VOLUME = "ConvertVolume"
    """
    @ivar volume: volume configuration
    """
 
    CONVERT_VOLUME_RESULT = "ConvertVolumeResult"
    """
    @ivar status: ok|error
    @ivar last_error
    @ivar volume: converted volume configuration
    """
 
 
 
def get_handlers():
    return [MysqlHandler()]
 
 
class DBMSRHandler(ServiceCtlHandler):
    pass
 
initdv2.explore(__mysql__['behavior'], mysql_svc.MysqlInitScript)
 
class MysqlCnfController(CnfController):
    _mysql_version = None
    _merged_manifest = None
    _cli = None
 
    def __init__(self):
        self._init_script = initdv2.lookup(__mysql__['behavior'])
        self.sendline = ''
        definitions = {'ON': '1', 'TRUE': '1', 'OFF' :'0', 'FALSE': '0'}
        CnfController.__init__(self,
                        __mysql__['behavior'],
                        mysql_svc.MYCNF_PATH,
                        'mysql',
                        definitions) #TRUE,FALSE
 
    @property
    def root_client(self):
        if not self._cli:
            self._cli = mysql_svc.MySQLClient(
                                    __mysql__['root_user'],
                                    __mysql__['root_password'])
        return self._cli
 
 
    @property
    def _manifest(self):
        f_manifest = CnfController._manifest
        base_manifest = f_manifest.fget(self)
        path = self._manifest_path
 
        s = {}
        out = None
 
        if not self._merged_manifest:
            cmd = '%s --no-defaults --verbose SU_EXEC' % mysql_svc.MYSQLD_PATH
            out = system2('%s - mysql -s %s -c "%s"' % (SU_EXEC, BASH, cmd),
                                    shell=True, raise_exc=False, silent=True)[0]
 
        if out:
            raw = out.split(49*'-'+' '+24*'-')
            if raw:
                a = raw[-1].split('\n')
                if len(a) > 5:
                    b = a[1:-5]
                    for item in b:
                        c = item.split()
                        if len(c) > 1:
                            key = c[0]
                            val = ' '.join(c[1:])
                            s[key.strip()] = val.strip()
 
        if s:
            m_config = Configuration('ini')
            if os.path.exists(path):
                m_config.read(path)
 
            for variable in base_manifest:
                name = variable.name
                dv_path = './%s/default-value' % name
 
                try:
                    old_value =  m_config.get(dv_path)
                    if name in s:
                        new_value = s[name]
                    else:
                        name = name.replace('_','-')
                        if name in s:
                            new_value = self.definitions[s[name]] if s[name] in self.definitions else s[name]
                            if old_value != new_value and new_value != '(No default value)':
                                LOG.debug('Replacing %s default value %s with precompiled value %s',
                                                name, old_value, new_value)
                                m_config.set(path=dv_path, value=new_value, force=True)
                except NoPathError:
                    pass
            m_config.write(path)
 
        self._merged_manifest = _CnfManifest(path)
        return self._merged_manifest
 
 
    def get_system_variables(self):
        vars_ = CnfController.get_system_variables(self)
        LOG.debug('Variables from config: %s' % str(vars_))
        if self._init_script.running:
            cli_vars = self.root_client.show_global_variables()
            vars_.update(cli_vars)
        return vars_
 
    def apply_preset(self, preset):
 
        CnfController.apply_preset(self, preset)
 
    def _before_apply_preset(self):
        self.sendline = ''
 
    def _after_set_option(self, option_spec, value):
        LOG.debug('callback "_after_set_option": %s %s (Need restart: %s)'
                        % (option_spec, value, option_spec.need_restart))
 
        if value != option_spec.default_value and not option_spec.need_restart:
            LOG.debug('Preparing to set run-time variable %s to %s' % (option_spec.name, value))
            self.sendline += 'SET GLOBAL %s = %s; ' % (option_spec.name, value)
 
 
    def _after_remove_option(self, option_spec):
        if option_spec.default_value and not option_spec.need_restart:
            LOG.debug('Preparing to set run-time variable %s to default [%s]'
                                    % (option_spec.name,option_spec.default_value))
            self.sendline += 'SET GLOBAL %s = DEFAULT; ' % (option_spec.name)
 
    def _after_apply_preset(self):
        if not self._init_script.running:
            LOG.info('MySQL isn`t running, skipping process of applying run-time variables')
            return
 
        if self.sendline and self.root_client.test_connection():
            LOG.debug(self.sendline)
            try:
                self.root_client.fetchone(self.sendline)
            except BaseException, e:
                LOG.error('Cannot set global variables: %s' % e)
            else:
                LOG.debug('All global variables has been set.')
            finally:
                #temporary fix for percona55 backup issue (SCALARIZR-435)
                self.root_client.reconnect()
        elif not self.sendline:
            LOG.debug('No global variables changed. Nothing to set.')
        elif not self.root_client.test_connection():
            LOG.debug('No connection to MySQL. Skipping SETs.')
 
 
    def _get_version(self):
        if not self._mysql_version:
            info = software.software_info('mysql')
            self._mysql_version = info.version
        return self._mysql_version
 
 
class MysqlHandler(DBMSRHandler):
 
 
    def __init__(self):
        self.mysql = mysql_svc.MySQL()
        cnf_ctl = MysqlCnfController() if __mysql__['behavior'] in ('mysql2', 'percona') else None  # mariadb dont do old presets 
        ServiceCtlHandler.__init__(self,
                        __mysql__['behavior'],
                        self.mysql.service,
                        cnf_ctl)
 
        self.preset_provider = mysql_svc.MySQLPresetProvider()
        preset_service.services[__mysql__['behavior']] = self.preset_provider
 
        bus.on(init=self.on_init, reload=self.on_reload)
        bus.define_events(
                'before_mysql_data_bundle',
                'mysql_data_bundle',
                # @param host: New master hostname
                'before_mysql_change_master',
                # @param host: New master hostname
                # @param log_file: log file to start from
                # @param log_pos: log pos to start from
                'mysql_change_master'
                'before_slave_promote_to_master',
                'slave_promote_to_master'
        )
 
        self._mysql_api = mysql_api.MySQLAPI()
        self._op_api = operation_api.OperationAPI()
        self._backup_id = None
        self._data_bundle_id = None
        self.on_reload()
 
 
    def accept(self, message, queue, behaviour=None, platform=None, os=None, dist=None):
        return __mysql__['behavior'] in behaviour and (
                                message.name == DbMsrMessages.DBMSR_NEW_MASTER_UP
                        or      message.name == DbMsrMessages.DBMSR_PROMOTE_TO_MASTER
                        or      message.name == DbMsrMessages.DBMSR_CREATE_DATA_BUNDLE
                        or  message.name == DbMsrMessages.DBMSR_CANCEL_DATA_BUNDLE
                        or      message.name == DbMsrMessages.DBMSR_CREATE_BACKUP
                        or      message.name == DbMsrMessages.DBMSR_CANCEL_BACKUP
                        or  message.name == Messages.UPDATE_SERVICE_CONFIGURATION
                        or  message.name == Messages.BEFORE_HOST_TERMINATE
                        or  message.name == MysqlMessages.CREATE_PMA_USER
                        or      message.name == MysqlMessages.CONVERT_VOLUME)
 
 
    def on_reload(self):
        LOG.debug("on_reload")
        self._queryenv = bus.queryenv_service
        self._platform = bus.platform
 
 
    def on_init(self):
        LOG.debug("on_init")
        bus.on("host_init_response", self.on_host_init_response)
        bus.on("before_host_up", self.on_before_host_up)
        bus.on("before_reboot_start", self.on_before_reboot_start)
 
        self._insert_iptables_rules()
 
        if __node__['state'] == 'running':
            vol = storage2.volume(__mysql__['volume'])
            vol.ensure(mount=True)
            __mysql__['volume'] = vol
            if int(__mysql__['replication_master']):
                LOG.debug("Checking Scalr's %s system users presence",
                                __mysql__['behavior'])
                creds = self.get_user_creds()
                self.create_users(**creds)
 
 
    def on_host_init_response(self, message):
        """
        Check mysql data in host init response
        @type message: scalarizr.messaging.Message
        @param message: HostInitResponse
        """
        LOG.debug("on_host_init_response")
 
        log = bus.init_op.logger
        log.info('Accept Scalr configuration')
        if not message.body.has_key(__mysql__['behavior']):
            msg = "HostInitResponse message for MySQL behavior " \
                            "must have '%s' property" % __mysql__['behavior']
            raise HandlerError(msg)
 
 
        # Apply MySQL data from HIR
        md = getattr(message, __mysql__['behavior']).copy()
 
        if 'preset' in md:
            self.initial_preset = md['preset']
            del md['preset']
            LOG.debug('Scalr sent current preset: %s' % self.initial_preset)
 
        md['compat_prior_backup_restore'] = False
        if md.get('volume'):
            # New format
            md['volume'] = storage2.volume(md['volume'])
            if 'backup' in md:
                md['backup'] = backup.backup(md['backup'])
            if 'restore' in md:
                md['restore'] = backup.restore(md['restore'])
 
        else:
 
            # Compatibility transformation
            # - volume_config -> volume
            # - master n'th start, type=ebs - del snapshot_config
            # - snapshot_config + log_file + log_pos -> restore
            # - create backup on master 1'st start
 
            md['compat_prior_backup_restore'] = True
            if md.get('volume_config'):
                md['volume'] = storage2.volume(
                                md.pop('volume_config'))
            else:
                md['volume'] = storage2.volume(
                                type=md['snapshot_config']['type'])
 
            # Initialized persistent disk have latest data.
            # Next statement prevents restore from snapshot
            if md['volume'].device and \
                                    md['volume'].type in ('ebs', 'csvol', 'cinder', 'raid'):
                md.pop('snapshot_config', None)
 
            if md.get('snapshot_config'):
                md['restore'] = backup.restore(
                                type='snap_mysql',
                                snapshot=md.pop('snapshot_config'),
                                volume=md['volume'],
                                log_file=md.pop('log_file'),
                                log_pos=md.pop('log_pos'))
            elif int(md['replication_master']) and \
                                    not md['volume'].device:
                md['backup'] = backup.backup(
                                type='snap_mysql',
                                volume=md['volume'])
 
        __mysql__.update(md)
 
        LOG.debug('__mysql__: %s', md)
        LOG.debug('volume in __mysql__: %s', 'volume' in __mysql__)
        LOG.debug('restore in __mysql__: %s', 'restore' in __mysql__)
        LOG.debug('backup in __mysql__: %s', 'backup' in __mysql__)
 
        __mysql__['volume'].mpoint = __mysql__['storage_dir']
        __mysql__['volume'].tags = self.resource_tags()
        if 'backup' in __mysql__:
            __mysql__['backup'].tags = self.resource_tags()
            __mysql__['backup'].description = self._data_bundle_description()
 
 
    def on_before_host_up(self, message):
        LOG.debug("on_before_host_up")
        """
        Configure MySQL __mysql__['behavior']
        @type message: scalarizr.messaging.Message
        @param message: HostUp message
        """
 
        self.generate_datadir()
        self.mysql.service.stop('Configuring MySQL')
 
        # On Debian/GCE we've got 'Another MySQL daemon already running with the same unix socket.'
        socket_file = mysql2_svc.my_print_defaults('mysqld').get('socket')
        if socket_file:
            coreutils.remove(socket_file)
 
        if 'Amazon' == linux.os['name']:
            self.mysql.my_cnf.pid_file = os.path.join(__mysql__['data_dir'], 'mysqld.pid')
 
        repl = 'master' if int(__mysql__['replication_master']) else 'slave'
        bus.fire('before_mysql_configure', replication=repl)
        if repl == 'master':
            self._init_master(message)
        else:
            self._init_slave(message)
        # Force to resave volume settings
        __mysql__['volume'] = storage2.volume(__mysql__['volume'])
        bus.fire('service_configured', service_name=__mysql__['behavior'],
                        replication=repl, preset=self.initial_preset)
 
 
    def on_BeforeHostTerminate(self, message):
        LOG.debug('Handling BeforeHostTerminate message from %s' % message.local_ip)
 
        if message.local_ip == __node__['private_ip']:
            self.mysql.service.stop(reason='Server will be terminated')
            LOG.info('Detaching MySQL storage')
            vol = storage2.volume(__mysql__['volume'])
            vol.detach()
            if not int(__mysql__['replication_master']):
                LOG.info('Destroying volume %s', vol.id)
                vol.destroy(remove_disks=True)
                LOG.info('Volume %s has been destroyed.' % vol.id)
            else:
                vol.umount()
 
 
    def on_Mysql_CreatePmaUser(self, message):
        LOG.debug("on_Mysql_CreatePmaUser")
        assert message.pma_server_ip
        assert message.farm_role_id
 
        try:
            # Operation allowed only on Master server
            if not int(__mysql__['replication_master']):
                msg = 'Cannot add pma user on slave. ' \
                                'It should be a Master server'
                raise HandlerError(msg)
 
            pma_server_ip = message.pma_server_ip
            farm_role_id  = message.farm_role_id
            pma_password = cryptotool.pwgen(20)
            LOG.info("Adding phpMyAdmin system user")
 
            if  self.root_client.user_exists(__mysql__['pma_user'], pma_server_ip):
                LOG.info('PhpMyAdmin system user already exists. Removing user.')
                self.root_client.remove_user(__mysql__['pma_user'], pma_server_ip)
 
            self.root_client.create_user(__mysql__['pma_user'], pma_server_ip,
                                                                    pma_password, privileges=None)
            LOG.info('PhpMyAdmin system user successfully added')
 
            # Notify Scalr
            self.send_message(MysqlMessages.CREATE_PMA_USER_RESULT, dict(
                    status       = 'ok',
                    pma_user         = __mysql__['pma_user'],
                    pma_password = pma_password,
                    farm_role_id = farm_role_id,
            ))
 
        except (Exception, BaseException), e:
            LOG.exception(e)
 
            # Notify Scalr about error
            self.send_message(MysqlMessages.CREATE_PMA_USER_RESULT, dict(
                    status          = 'error',
                    last_error      =  str(e).strip(),
                    farm_role_id = farm_role_id
            ))
 
    def on_DbMsr_CreateBackup(self, message):
        LOG.debug("on_DbMsr_CreateBackup")
        self._backup_id = self._mysql_api.create_backup(
                backup={'type': 'mysqldump'}, 
                async=True)
 
 
    def on_DbMsr_CancelBackup(self, message):
        LOG.debug("on_DbMsr_CancelBackup")
        self._op_api.cancel(self._backup_id)
 
 
    def on_DbMsr_CreateDataBundle(self, message):
        LOG.debug("on_DbMsr_CreateDataBundle")
        backup = message.body.get(__mysql__.behavior, {}).get('backup', {})
        if not backup:
            backup = {"type": "snap_mysql"}
        self._data_bundle_id = self._mysql_api.create_backup(
                backup=backup, 
                async=True)
 
 
    def on_DbMsr_CancelDataBundle(self, message):
        LOG.debug("on_DbMsr_CancelDataBundle")
        self._op_api.cancel(self._data_bundle_id)
 
 
    def on_DbMsr_PromoteToMaster(self, message):
        """
        Promote slave to master
        """
        LOG.debug("on_DbMsr_PromoteToMaster")
        mysql2 = message.body[__mysql__['behavior']]
 
        if int(__mysql__['replication_master']):
            LOG.warning('Cannot promote to master. Already master')
            return
        LOG.info('Starting Slave -> Master promotion')
 
        bus.fire('before_slave_promote_to_master')
 
        __mysql__['compat_prior_backup_restore'] = mysql2.get('volume_config') or \
                                                    mysql2.get('snapshot_config') or \
                                                    message.body.get('volume_config') and \
                                                    not mysql2.get('volume')
        new_vol = None
        if __node__['platform'].name == 'idcf':
            new_vol = None
        elif mysql2.get('volume_config'):
            new_vol = storage2.volume(mysql2.get('volume_config'))
 
 
        try:
            if new_vol and new_vol.type not in ('eph', 'lvm'):
                if self.mysql.service.running:
                    self.root_client.stop_slave()
 
                    self.mysql.service.stop('Swapping storages to promote slave to master')
 
                # Unplug slave storage and plug master one
                old_vol = storage2.volume(__mysql__['volume'])
                try:
                    if old_vol.type == 'raid':
                        old_vol.detach()
                    else:
                        old_vol.umount()
                    new_vol.mpoint = __mysql__['storage_dir']
                    new_vol.ensure(mount=True)
                    # Continue if master storage is a valid MySQL storage
                    if self._storage_valid():
                        # Patch configuration files
                        self.mysql.move_mysqldir_to(__mysql__['storage_dir'])
                        self.mysql._init_replication(master=True)
                        # Set read_only option
                        #self.mysql.my_cnf.read_only = False
                        self.mysql.my_cnf.set('mysqld/sync_binlog', '1')
                        self.mysql.my_cnf.set('mysqld/innodb_flush_log_at_trx_commit', '1')
                        self.mysql.my_cnf.delete_options(['mysqld/read_only'])
                        self.mysql.service.start()
                        # Update __mysql__['behavior'] configuration
                        __mysql__.update({
                                'replication_master': 1,
                                'root_password': mysql2['root_password'],
                                'repl_password': mysql2['repl_password'],
                                'stat_password': mysql2['stat_password'],
                                'volume': new_vol
                        })
 
                        try:
                            old_vol.destroy(remove_disks=True)
                        except:
                            LOG.warn('Failed to destroy old MySQL volume %s: %s',
                                                    old_vol.id, sys.exc_info()[1])
 
                        # Send message to Scalr
                        msg_data = {
                                'status': 'ok',
                                'db_type': __mysql__['behavior'],
                                __mysql__['behavior']: {}
                        }
                        if __mysql__['compat_prior_backup_restore']:
                            msg_data[__mysql__['behavior']].update({
                                    'volume_config': dict(__mysql__['volume'])
                            })
                        else:
                            msg_data[__mysql__['behavior']].update({
                                    'volume': dict(__mysql__['volume'])
                            })
 
                        self.send_message(
                                        DbMsrMessages.DBMSR_PROMOTE_TO_MASTER_RESULT,
                                        msg_data)
                    else:
                        msg = "%s is not a valid MySQL storage" % __mysql__['data_dir']
                        raise HandlerError(msg)
                except:
                    self.mysql.service.stop('Detaching new volume')
                    new_vol.detach()
                    if old_vol.type == 'raid':
                        old_vol.ensure(mount=True)
                    else:
                        old_vol.mount()
                    raise
            else:
                #self.mysql.my_cnf.read_only = False
                self.mysql.my_cnf.delete_options(['mysqld/read_only'])
                #self.mysql.service.restart()
                self.mysql.service.stop()
                self.mysql.service.start()
 
                self.root_client.stop_slave()
                self.root_client.reset_master()
                self.mysql.flush_logs(__mysql__['data_dir'])
 
                __mysql__.update({
                        'replication_master': 1,
                        'root_password': mysql2['root_password'],
                        'repl_password': mysql2['repl_password'],
                        'stat_password': mysql2['stat_password'],
                })
 
                restore = None
                no_data_bundle = mysql2.get('no_data_bundle', False)
                if not no_data_bundle:
                    if mysql2.get('backup'):
                        bak = backup.backup(**mysql2.get('backup'))
                    else:
                        bak = backup.backup(
                                        type='snap_mysql',
                                        volume=__mysql__['volume'] ,
                                        description=self._data_bundle_description(),
                                        tags=self.resource_tags())
                    restore = bak.run()
 
                # Send message to Scalr
                msg_data = dict(
                        status="ok",
                        db_type = __mysql__['behavior']
                )
                if __mysql__['compat_prior_backup_restore']:
                    result = {
                            'volume_config': dict(__mysql__['volume'])
                    }
                    if restore:
                        result.update({
                                'snapshot_config': dict(restore.snapshot),
                                'log_file': restore.log_file,
                                'log_pos': restore.log_pos
                        })
                else:
                    result = {
                            'volume': dict(__mysql__['volume'])
                    }
                    if restore:
                        result['restore'] = dict(restore)
 
                msg_data[__mysql__['behavior']] = result
 
 
                self.send_message(DbMsrMessages.DBMSR_PROMOTE_TO_MASTER_RESULT, msg_data)
                LOG.info('Promotion completed')
 
            bus.fire('slave_promote_to_master')
 
        except (Exception, BaseException), e:
            LOG.exception(e)
 
            msg_data = dict(
                    db_type = __mysql__['behavior'],
                    status="error",
                    last_error=str(e))
            self.send_message(DbMsrMessages.DBMSR_PROMOTE_TO_MASTER_RESULT, msg_data)
 
            # Change back read_only option
            self.mysql.my_cnf.read_only = True
 
            # Start MySQL
            self.mysql.service.start()
 
 
 
 
    def on_DbMsr_NewMasterUp(self, message):
        try:
            assert message.body.has_key("db_type")
            assert message.body.has_key("local_ip")
            assert message.body.has_key("remote_ip")
            assert message.body.has_key(__mysql__['behavior'])
 
            mysql2 = message.body[__mysql__['behavior']]
 
            if int(__mysql__['replication_master']):
                LOG.debug('Skip NewMasterUp. My replication role is master')
                return
 
            host = message.local_ip or message.remote_ip
            LOG.info("Switching replication to a new MySQL master %s", host)
            bus.fire('before_mysql_change_master', host=host)
 
            LOG.debug("__mysql__['volume']: %s", __mysql__['volume'])
 
            if __mysql__['volume'].type in ('eph', 'lvm') or __node__['platform'].name == 'idcf':
                if 'restore' in mysql2:
                    restore = backup.restore(**mysql2['restore'])
                else:
                    # snap_mysql restore should update MySQL volume, and delete old one
                    restore = backup.restore(
                                            type='snap_mysql',
                                            log_file=mysql2['log_file'],
                                            log_pos=mysql2['log_pos'],
                                            volume=__mysql__['volume'],
                                            snapshot=mysql2['snapshot_config'])
                # XXX: ugly
                old_vol = None
                if __mysql__['volume'].type == 'eph':
                    self.mysql.service.stop('Swapping storages to reinitialize slave')
 
                    LOG.info('Reinitializing Slave from the new snapshot %s (log_file: %s log_pos: %s)',
                                    restore.snapshot['id'], restore.log_file, restore.log_pos)
                    new_vol = restore.run()
                else:
                    if __node__['platform'].name == 'idcf':
                        self.mysql.service.stop('Detaching old Slave volume')
                        old_vol = dict(__mysql__['volume'])
                        old_vol = storage2.volume(old_vol)
                        old_vol.umount()
 
                    restore.run()
 
                log_file = restore.log_file
                log_pos = restore.log_pos
 
                self.mysql.service.start()
 
                if __node__['platform'].name == 'idcf' and old_vol:
                    LOG.info('Destroying old Slave volume')
                    old_vol.destroy(remove_disks=True)
            else:
                LOG.debug("Stopping slave i/o thread")
                self.root_client.stop_slave_io_thread()
                LOG.debug("Slave i/o thread stopped")
 
                LOG.debug("Retrieving current log_file and log_pos")
                status = self.root_client.slave_status()
                log_file = status['Master_Log_File']
                log_pos = status['Read_Master_Log_Pos']
                LOG.debug("Retrieved log_file=%s, log_pos=%s", log_file, log_pos)
 
 
            self._change_master(
                    host=host,
                    user=__mysql__['repl_user'],
                    password=mysql2['repl_password'],
                    log_file=log_file,
                    log_pos=log_pos,
                    timeout=120
            )
 
            LOG.debug("Replication switched")
            bus.fire('mysql_change_master', host=host, log_file=log_file, log_pos=log_pos)
 
            msg_data = dict(
                    db_type = __mysql__['behavior'],
                    status = 'ok'
            )
            self.send_message(DbMsrMessages.DBMSR_NEW_MASTER_UP_RESULT, msg_data)
 
        except (Exception, BaseException), e:
            LOG.exception(e)
 
            msg_data = dict(
                    db_type = __mysql__['behavior'],
                    status="error",
                    last_error=str(e))
            self.send_message(DbMsrMessages.DBMSR_NEW_MASTER_UP_RESULT, msg_data)
 
 
    def on_ConvertVolume(self, message):
        try:
            if __node__['state'] != 'running':
                raise HandlerError('scalarizr is not in "running" state')
 
            old_volume = storage2.volume(__mysql__['volume'])
            new_volume = storage2.volume(message.volume)
 
            if old_volume.type != 'eph' or new_volume.type != 'lvm':
                raise HandlerError('%s to %s convertation unsupported.' %
                                                   (old_volume.type, new_volume.type))
 
            new_volume.ensure()
            __mysql__.update({'volume': new_volume})
        except:
            e = sys.exc_info()[1]
            LOG.error('Volume convertation failed: %s' % e)
            self.send_message(MysqlMessages.CONVERT_VOLUME_RESULT,
                            dict(status='error', last_error=str(e)))
 
 
 
    def on_before_reboot_start(self, *args, **kwargs):
        self.mysql.service.stop('Instance is going to reboot')
 
 
    def generate_datadir(self):
        try:
            datadir = mysql2_svc.my_print_defaults('mysqld').get('datadir')
            if datadir and \
                    os.path.isdir(datadir) and \
                    not os.path.isdir(os.path.join(datadir, 'mysql')):
                self.mysql.service.start()
                self.mysql.service.stop('Autogenerating datadir')
        except:
            #TODO: better error handling
            pass
 
 
    def _storage_valid(self):
        binlog_base = os.path.join(__mysql__['storage_dir'], mysql_svc.STORAGE_BINLOG)
        return os.path.exists(__mysql__['data_dir']) and glob.glob(binlog_base + '*')
 
 
    def _change_selinux_ctx(self):
        try:
            chcon = software.which('chcon')
        except LookupError:
            return
        if disttool.is_redhat_based():
            LOG.debug('Changing SELinux file security context for new mysql datadir')
            system2((chcon, '-R', '-u', 'system_u', '-r',
                     'object_r', '-t', 'mysqld_db_t', os.path.dirname(__mysql__['storage_dir'])), raise_exc=False)
 
 
    def _fix_percona_debian_cnf(self):
        if __mysql__['behavior'] == 'percona' and \
                                                os.path.exists(__mysql__['debian.cnf']):
            LOG.info('Fixing socket options in %s', __mysql__['debian.cnf'])
            debian_cnf = metaconf.Configuration('mysql')
            debian_cnf.read(__mysql__['debian.cnf'])
 
            sock = mysql2_svc.my_print_defaults('mysqld')['socket']
            debian_cnf.set('client/socket', sock)
            debian_cnf.set('mysql_upgrade/socket', sock)
            debian_cnf.write(__mysql__['debian.cnf'])
 
 
    def _init_master(self, message):
        """
        Initialize MySQL master
        @type message: scalarizr.messaging.Message
        @param message: HostUp message
        """
        LOG.info("Initializing MySQL master")
        log = bus.init_op.logger
 
        log.info('Create storage')
        if 'restore' in __mysql__ and \
                        __mysql__['restore'].type == 'snap_mysql':
            __mysql__['restore'].run()
        else:
            if __node__['platform'].name == 'idcf':
                if __mysql__['volume'].id:
                    LOG.info('Cloning volume to workaround reattachment limitations of IDCF')
                    __mysql__['volume'].snap = __mysql__['volume'].snapshot()
 
            __mysql__['volume'].ensure(mount=True, mkfs=True)
            LOG.debug('MySQL volume config after ensure: %s', dict(__mysql__['volume']))
 
        coreutils.clean_dir(__mysql__['defaults']['datadir'])
        self.mysql.flush_logs(__mysql__['data_dir'])
        self.mysql.move_mysqldir_to(__mysql__['storage_dir'])
        self._change_selinux_ctx()
 
        storage_valid = self._storage_valid()
        user_creds = self.get_user_creds()
        self._fix_percona_debian_cnf()
        #datadir = mysql2_svc.my_print_defaults('mysqld').get('datadir', __mysql__['defaults']['datadir'])
        #if not storage_valid and datadir.find(__mysql__['data_dir']) == 0:
        #    # When role was created from another mysql role it contains modified my.cnf settings
        #    #self.mysql.my_cnf.datadir = '/var/lib/mysql'
        #    self.mysql.my_cnf.delete_options(['mysqld/log_bin'])
 
 
 
        if not storage_valid:
            '''
            if linux.os['family'] == 'RedHat':
                try:
                    # Check if selinux enabled
                    selinuxenabled_bin = software.which('selinuxenabled')
                    if selinuxenabled_bin:
                        se_enabled = not system2((selinuxenabled_bin, ), raise_exc=False)[2]
                        if se_enabled:
                            # Set selinux context for new mysql datadir
                            semanage = mysql_svc.get_semanage()
                            linux.system('%s fcontext -a -t mysqld_db_t "%s(/.*)?"'
                                         % (semanage, __mysql__['storage_dir']), shell=True)
                            # Restore selinux context
                            restorecon = software.which('restorecon')
                            linux.system('%s -R -v %s' % (restorecon, __mysql__['storage_dir']), shell=True)
                except:
                    LOG.debug('Selinux context setup failed', exc_info=sys.exc_info())
                '''
 
            self.mysql.my_cnf.delete_options(['mysqld/log_bin'])
            linux.system(['mysql_install_db', '--user=mysql', '--datadir=%s' % __mysql__['data_dir']])
 
        # Patch configuration
        options = {
            'bind-address': '0.0.0.0',
            'datadir': __mysql__['data_dir'],
            'log_bin': os.path.join(__mysql__['binlog_dir'], 'binlog'),
            'log-bin-index': os.path.join(__mysql__['binlog_dir'], 'binlog.index'),  # MariaDB
            'sync_binlog': '1',
            'innodb_flush_log_at_trx_commit': '1',
            'expire_logs_days': '10'
        }
        for key, value in options.items():
            self.mysql.my_cnf.set('mysqld/' + key, value)
 
        if not storage_valid:
            if linux.os.debian_family and os.path.exists(__mysql__['debian.cnf']):
                self.mysql.service.start()
                debian_cnf = metaconf.Configuration('mysql')
                debian_cnf.read(__mysql__['debian.cnf'])
                sql = ("GRANT ALL PRIVILEGES ON *.* "
                        "TO 'debian-sys-maint'@'localhost' "
                        "IDENTIFIED BY '{0}'").format(debian_cnf.get('client/password'))
                linux.system(['mysql', '-u', 'root', '-e', sql])
                self.mysql.service.stop()
 
            coreutils.chown_r(__mysql__['data_dir'], 'mysql', 'mysql')
        if 'restore' in __mysql__ and \
                        __mysql__['restore'].type == 'xtrabackup':
            # XXX: when restoring data bundle on ephemeral storage, data dir should by empty
            # but move_mysqldir_to call required to set several options in my.cnf
            coreutils.clean_dir(__mysql__['data_dir'])
 
        #self._change_selinux_ctx()
 
        log.info('Patch my.cnf configuration file')
        # Init replication
        self.mysql._init_replication(master=True)
 
        if 'restore' in __mysql__ and \
                        __mysql__['restore'].type == 'xtrabackup':
            __mysql__['restore'].run()
 
 
        # If It's 1st init of mysql master storage
        if not storage_valid:
            if os.path.exists(__mysql__['debian.cnf']):
                log.info("Copying debian.cnf file to mysql storage")
                shutil.copy(__mysql__['debian.cnf'], __mysql__['storage_dir'])
 
        # If volume has mysql storage directory structure (N-th init)
        else:
            log.info('InnoDB recovery')
            self._copy_debian_cnf_back()
            if 'restore' in __mysql__ and  __mysql__['restore'].type != 'xtrabackup':
                self._innodb_recovery()
                self.mysql.service.start()
 
        log.info('Create Scalr users')
        # Check and create mysql system users
        self.create_users(**user_creds)
 
        log.info('Create data bundle')
        if 'backup' in __mysql__:
            __mysql__['restore'] = __mysql__['backup'].run()
 
        # Update HostUp message
        log.info('Collect HostUp data')
        md = dict(
                replication_master=__mysql__['replication_master'],
                root_password=__mysql__['root_password'],
                repl_password=__mysql__['repl_password'],
                stat_password=__mysql__['stat_password'],
                master_password=__mysql__['master_password']
        )
        if __mysql__['compat_prior_backup_restore']:
            if 'restore' in __mysql__:
                md.update(dict(
                                log_file=__mysql__['restore'].log_file,
                                log_pos=__mysql__['restore'].log_pos,
                                snapshot_config=dict(__mysql__['restore'].snapshot)))
            elif 'log_file' in __mysql__:
                md.update(dict(
                                log_file=__mysql__['log_file'],
                                log_pos=__mysql__['log_pos']))
            md.update(dict(
                                    volume_config=dict(__mysql__['volume'])))
        else:
            md.update(dict(
                    volume=dict(__mysql__['volume'])
            ))
            for key in ('backup', 'restore'):
                if key in __mysql__:
                    md[key] = dict(__mysql__[key])
 
 
        message.db_type = __mysql__['behavior']
        setattr(message, __mysql__['behavior'], md)
 
 
 
 
    def _init_slave(self, message):
        """
        Initialize MySQL slave
        @type message: scalarizr.messaging.Message
        @param message: HostUp message
        """
        LOG.info("Initializing MySQL slave")
        log = bus.init_op.logger
 
        log.info('Create storage')
        if 'restore' in __mysql__ and \
                        __mysql__['restore'].type == 'snap_mysql':
            __mysql__['restore'].run()
        else:
            __mysql__['volume'].ensure(mount=True, mkfs=True)
 
        log.info('Patch my.cnf configuration file')
        self.mysql.service.stop('Required by Slave initialization process')
        self.mysql.flush_logs(__mysql__['data_dir'])
 
        # Change configuration files
        LOG.info("Changing configuration files")
        self.mysql.my_cnf.datadir = __mysql__['data_dir']
        self.mysql.my_cnf.expire_logs_days = 10
        LOG.debug('bind-address pre: %s', self.mysql.my_cnf.bind_address)
        self.mysql.my_cnf.bind_address = '0.0.0.0'
        LOG.debug('bind-address post: %s', self.mysql.my_cnf.bind_address)
        self.mysql.my_cnf.read_only = True
        self.mysql.my_cnf.set('mysqld/log-bin-index', __mysql__['binlog_dir'] + '/binlog.index')  # MariaDB
        self._fix_percona_debian_cnf()
 
        log.info('Move data directory to storage')
        self.mysql.move_mysqldir_to(__mysql__['storage_dir'])
        self._change_selinux_ctx()
        self.mysql._init_replication(master=False)
        self._copy_debian_cnf_back()
 
        if 'restore' in __mysql__ and \
                        __mysql__['restore'].type == 'xtrabackup':
            __mysql__['restore'].run()
 
        log.info('InnoDB recovery')
        if 'restore' in __mysql__ \
                        and __mysql__['restore'].type != 'xtrabackup':
            self._innodb_recovery()
 
        log.info('Change replication Master')
        # Change replication master
        LOG.info("Requesting master server")
        master_host = self.get_master_host()
        self.mysql.service.start()
        self._change_master(
                        host=master_host,
                        user=__mysql__['repl_user'],
                        password=__mysql__['repl_password'],
                        log_file=__mysql__['restore'].log_file,
                        log_pos=__mysql__['restore'].log_pos,
                        timeout=240)
 
        # Update HostUp message
        log.info('Collect HostUp data')
        message.db_type = __mysql__['behavior']
 
 
    def get_master_host(self):
        master_host = None
        while not master_host:
            try:
                master_host = list(host
                        for host in self._queryenv.list_roles(behaviour=__mysql__['behavior'])[0].hosts
                        if host.replication_master)[0]
            except IndexError:
                LOG.debug("QueryEnv respond with no mysql master. " +
                                "Waiting %d seconds before the next attempt", 5)
            time.sleep(5)
        LOG.debug("Master server obtained (local_ip: %s, public_ip: %s)",
                        master_host.internal_ip, master_host.external_ip)
        return master_host.internal_ip or master_host.external_ip
 
 
    def _copy_debian_cnf_back(self):
        debian_cnf = os.path.join(__mysql__['storage_dir'], 'debian.cnf')
        if disttool.is_debian_based() and os.path.exists(debian_cnf):
            LOG.debug("Copying debian.cnf from storage to mysql configuration directory")
            shutil.copy(debian_cnf, '/etc/mysql/')
 
 
    @property
    def root_client(self):
        return mysql_svc.MySQLClient(
                                __mysql__['root_user'],
                                __mysql__['root_password'])
 
 
    def _innodb_recovery(self, storage_path=None):
        storage_path = storage_path or __mysql__['storage_dir']
        binlog_path     = os.path.join(storage_path, mysql_svc.STORAGE_BINLOG)
        data_dir = os.path.join(storage_path, mysql_svc.STORAGE_DATA_DIR),
        pid_file = os.path.join(storage_path, 'mysql.pid')
        socket_file = os.path.join(storage_path, 'mysql.sock')
        mysqld_safe_bin = software.which('mysqld_safe')
 
        LOG.info('Performing InnoDB recovery')
        mysqld_safe_cmd = (mysqld_safe_bin,
                '--socket=%s' % socket_file,
                '--pid-file=%s' % pid_file,
                '--datadir=%s' % data_dir,
                '--log-bin=%s' % binlog_path,
                '--skip-networking',
                '--skip-grant',
                '--bootstrap',
                '--skip-slave-start')
        system2(mysqld_safe_cmd, stdin="select 1;")
 
 
    def _insert_iptables_rules(self):
        if iptables.enabled():
            iptables.FIREWALL.ensure([
                    {"jump": "ACCEPT", "protocol": "tcp", "match": "tcp", "dport": "3306"},
            ])
 
 
    def get_user_creds(self):
        options = {
                __mysql__['root_user']: 'root_password',
                __mysql__['repl_user']: 'repl_password',
                __mysql__['stat_user']: 'stat_password',
                # __mysql__['master_user']: 'master_password'
                # TODO: disabled scalr_master user until scalr will send/recv it in communication messages
                }
        creds = {}
        for login, opt_pwd in options.items():
            password = __mysql__[opt_pwd]
            if not password:
                password = cryptotool.pwgen(20)
                __mysql__[opt_pwd] = password
            creds[login] = password
        return creds
 
 
    def create_users(self, **creds):
        users = {}
        root_cli = mysql_svc.MySQLClient(__mysql__['root_user'], creds[__mysql__['root_user']])
 
        local_root = mysql_svc.MySQLUser(root_cli, __mysql__['root_user'],
                                        creds[__mysql__['root_user']], host='localhost')
 
        #local_master = mysql_svc.MySQLUser(root_cli, __mysql__['master_user'], 
        #                                creds[__mysql__['master_user']], host='localhost', 
        #                                privileges=PRIVILEGES.get(__mysql__['master_user'], None))
        #users['master@localhost'] = local_master
 
        if not self.mysql.service.running:
            self.mysql.service.start()
 
        try:
            if not local_root.exists() or not local_root.check_password():
                users.update({'root@localhost': local_root})
                self.mysql.service.stop('creating users')
                self.mysql.service.start_skip_grant_tables()
            else:
                LOG.debug('User %s exists and has correct password' % __mysql__['root_user'])
        except ServiceError, e:
            if 'Access denied for user' in str(e):
                users.update({'root@localhost': local_root})
                self.mysql.service.stop('creating users')
                self.mysql.service.start_skip_grant_tables()
            else:
                raise
 
        for login, password in creds.items():
            user = mysql_svc.MySQLUser(root_cli, login, password,
                                    host='%', privileges=PRIVILEGES.get(login, None))
            users[login] = user
 
        for login, user in users.items():
            if not user.exists():
                LOG.debug('User %s not found. Recreating.' % login)
                user.create()
            elif not user.check_password():
                LOG.warning('Password for user %s was changed. Recreating.' %  login)
                user.remove()
                user.create()
            users[login] = user
 
        self.mysql.service.stop_skip_grant_tables()
        self.mysql.service.start()
        return users
 
    def _data_bundle_description(self):
        pl = bus.platform
        return 'MySQL data bundle (farm: %s role: %s)' % (
                                pl.get_user_data(UserDataOptions.FARM_ID),
                                pl.get_user_data(UserDataOptions.ROLE_NAME))
 
 
    def _datadir_size(self):
        stat = os.statvfs(__mysql__['storage_dir'])
        return stat.f_bsize * stat.f_blocks / 1024 / 1024 / 1024 + 1
 
 
    def _change_master(self, host, user, password, log_file, log_pos, timeout=None):
 
        LOG.info("Changing replication Master to server %s (log_file: %s, log_pos: %s)",
                        host, log_file, log_pos)
 
        timeout = timeout or int(__mysql__['change_master_timeout'])
 
        # Changing replication master
        self.root_client.stop_slave()
        self.root_client.change_master_to(host, user, password, log_file, log_pos)
 
        # Starting slave
        result = self.root_client.start_slave()
        LOG.debug('Start slave returned: %s' % result)
        if result and 'ERROR' in result:
            raise HandlerError('Cannot start mysql slave: %s' % result)
 
        time_until = time.time() + timeout
        status = None
        while time.time() <= time_until:
            status = self.root_client.slave_status()
            if status['Slave_IO_Running'] == 'Yes' and \
                    status['Slave_SQL_Running'] == 'Yes':
                break
            time.sleep(5)
        else:
            if status:
                if not status['Last_Error']:
                    logfile = firstmatched(lambda p: os.path.exists(p),
                                                            ('/var/log/mysqld.log', '/var/log/mysql.log'))
                    if logfile:
                        gotcha = '[ERROR] Slave I/O thread: '
                        size = os.path.getsize(logfile)
                        fp = open(logfile, 'r')
                        try:
                            fp.seek(max((0, size - 8192)))
                            lines = fp.read().split('\n')
                            for line in lines:
                                if gotcha in line:
                                    status['Last_Error'] = line.split(gotcha)[-1]
                        finally:
                            fp.close()
 
                msg = "Cannot change replication Master server to '%s'. "  \
                                "Slave_IO_Running: %s, Slave_SQL_Running: %s, " \
                                "Last_Errno: %s, Last_Error: '%s'" % (
                                host, status['Slave_IO_Running'], status['Slave_SQL_Running'],
                                status['Last_Errno'], status['Last_Error'])
                raise HandlerError(msg)
            else:
                raise HandlerError('Cannot change replication master to %s' % (host))
 
 
        LOG.debug('Replication master is changed to host %s', host)
 
 
    def resource_tags(self):
        purpose = '%s-' % __mysql__['behavior'] + ('master' if int(__mysql__['replication_master']) == 1 else 'slave')
        return build_tags(purpose, 'active')
 
