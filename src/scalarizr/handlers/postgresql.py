from __future__ import with_statement

'''
Created on April 18th, 2011

@author: Dmytro Korsakov
'''
from __future__ import with_statement

import os
import time
import logging

from scalarizr import config
from scalarizr.bus import bus
from scalarizr.messaging import Messages
from scalarizr.config import ScalarizrState
from scalarizr.handlers import ServiceCtlHandler, HandlerError, DbMsrMessages
from scalarizr.linux.coreutils import chown_r
from scalarizr import linux
from scalarizr.util import system2, software, cryptotool, initdv2
from scalarizr.linux import iptables
from scalarizr.services.postgresql import PostgreSql, ROOT_USER, \
    PgUser, PgSQLPresetProvider, __postgresql__
from scalarizr.services import postgresql as postgresql_svc
from scalarizr.node import __node__
from scalarizr import storage2
from scalarizr.services import backup
from scalarizr.api import postgresql as postgresql_api


BEHAVIOUR = SERVICE_NAME = postgresql_api.BEHAVIOUR
LOG = logging.getLogger(__name__)

PG_SOCKET_DIR = '/var/run/postgresql/'
STORAGE_PATH = postgresql_api.STORAGE_PATH
STORAGE_VOLUME_CNF = 'postgresql.json'
STORAGE_SNAPSHOT_CNF = 'postgresql-snap.json'

OPT_VOLUME_CNF = 'volume_config'
OPT_SNAPSHOT_CNF = postgresql_api.OPT_SNAPSHOT_CNF
OPT_ROOT_USER = 'root_user'
OPT_ROOT_PASSWORD = "root_password"
OPT_ROOT_SSH_PUBLIC_KEY = "root_ssh_public_key"
OPT_ROOT_SSH_PRIVATE_KEY = "root_ssh_private_key"
OPT_CURRENT_XLOG_LOCATION = 'current_xlog_location'
OPT_REPLICATION_MASTER = postgresql_svc.OPT_REPLICATION_MASTER


__postgresql__.update({
    'port': 5432,
    'storage_dir': '/mnt/pgstorage',
    'root_user': 'scalr',
    'pgdump_chunk_size': 200 * 1024 * 1024,
})


def get_handlers():
    return [PostgreSqlHander()]


SSH_KEYGEN_SELINUX_MODULE = """
module local 1.0;

require {
    type initrc_tmp_t;
    type ssh_keygen_t;
    type initrc_t;
    type etc_runtime_t;
    class tcp_socket { read write };
    class file { read write getattr };
}

#============= ssh_keygen_t ==============
allow ssh_keygen_t etc_runtime_t:file { read write getattr };
allow ssh_keygen_t initrc_t:tcp_socket { read write };
allow ssh_keygen_t initrc_tmp_t:file { read write };
"""


class PostgreSqlHander(ServiceCtlHandler):
    _logger = None

    _queryenv = None
    """ @type _queryenv: scalarizr.queryenv.QueryEnvService """

    _platform = None
    """ @type _platform: scalarizr.platform.Ec2Platform """

    _cnf = None
    ''' @type _cnf: scalarizr.config.ScalarizrCnf '''

    preset_provider = None

    def accept(self, message, queue, behaviour=None, platform=None, os=None, dist=None):
        return BEHAVIOUR in behaviour and (
            message.name == DbMsrMessages.DBMSR_NEW_MASTER_UP
            or message.name == DbMsrMessages.DBMSR_PROMOTE_TO_MASTER
            or message.name == DbMsrMessages.DBMSR_CREATE_DATA_BUNDLE
            or message.name == DbMsrMessages.DBMSR_CREATE_BACKUP
            or message.name == Messages.UPDATE_SERVICE_CONFIGURATION
            or message.name == Messages.HOST_INIT
            or message.name == Messages.BEFORE_HOST_TERMINATE
            or message.name == Messages.HOST_UP
            or message.name == Messages.HOST_DOWN)


    def __init__(self):
        self._service_name = SERVICE_NAME
        ServiceCtlHandler.__init__(self, SERVICE_NAME, initdv2.lookup(SERVICE_NAME))
        bus.on("init", self.on_init)
        bus.define_events(
            'before_postgresql_data_bundle',

            'postgresql_data_bundle',

            # @param host: New master hostname
            'before_postgresql_change_master',

            # @param host: New master hostname
            'postgresql_change_master',

            'before_slave_promote_to_master',

            'slave_promote_to_master'
        )

        self._hir_volume_growth = None
        self._postgresql_api = postgresql_api.PostgreSQLAPI()

        self.on_reload()


    def on_init(self):
        #temporary fix for starting-after-rebundle issue
        if not os.path.exists(PG_SOCKET_DIR):
            os.makedirs(PG_SOCKET_DIR)
            chown_r(PG_SOCKET_DIR, 'postgres')

        bus.on("host_init_response", self.on_host_init_response)
        bus.on("before_host_up", self.on_before_host_up)
        bus.on("before_reboot_start", self.on_before_reboot_start)

        self._insert_iptables_rules()

        if __node__['state'] == ScalarizrState.BOOTSTRAPPING:

            if linux.os.redhat_family:
                def selinux_enabled():
                    selinuxenabled = software.which('selinuxenabled')
                    if selinuxenabled:
                        _, _, ret_code = system2((selinuxenabled, ), raise_exc=False)
                        return 0 == ret_code
                    # Consider it enabled by default
                    return True

                checkmodule_path = software.which('checkmodule')
                semodule_package_path = software.which('semodule_package')
                semodule_path = software.which('semodule')

                if all((checkmodule_path, semodule_package_path, semodule_path)):
                    if selinux_enabled():
                        with open('/tmp/sshkeygen.te', 'w') as fp:
                            fp.write(SSH_KEYGEN_SELINUX_MODULE)

                        self._logger.debug('Compiling SELinux policy for ssh-keygen')
                        system2((checkmodule_path, '-M', '-m', '-o',
                                 '/tmp/sshkeygen.mod', '/tmp/sshkeygen.te'), logger=self._logger)

                        self._logger.debug('Building SELinux package for ssh-keygen')
                        system2((semodule_package_path, '-o', '/tmp/sshkeygen.pp',
                                 '-m', '/tmp/sshkeygen.mod'), logger=self._logger)

                        self._logger.debug('Loading ssh-keygen SELinux package')
                        system2((semodule_path, '-i', '/tmp/sshkeygen.pp'), logger=self._logger)


        if __node__['state'] == 'running':

            vol = storage2.volume(__postgresql__['volume'])
            vol.ensure(mount=True)

            self.postgresql.service.start()
            self.accept_all_clients()

            self._logger.debug("Checking presence of Scalr's PostgreSQL root user.")
            root_password = self.root_password

            if not self.postgresql.root_user.exists():
                self._logger.debug("Scalr's PostgreSQL root user does not exist. Recreating")
                self.postgresql.root_user = self.postgresql.create_linux_user(ROOT_USER, root_password)
            else:
                try:
                    self.postgresql.root_user.check_system_password(root_password)
                    self._logger.debug("Scalr's root PgSQL user is present. Password is correct.")
                except ValueError:
                    self._logger.warning("Scalr's root PgSQL user was changed. Recreating.")
                    self.postgresql.root_user.change_system_password(root_password)

            if self.is_replication_master:
                #ALTER ROLE cannot be executed in a read-only transaction
                self._logger.debug("Checking password for pg_role scalr.")
                if not self.postgresql.root_user.check_role_password(root_password):
                    LOG.warning("Scalr's root PgSQL role was changed. Recreating.")
                    self.postgresql.root_user.change_role_password(root_password)


    def on_reload(self):
        self._queryenv = bus.queryenv_service
        self._platform = bus.platform
        self.postgresql = PostgreSql()
        self.preset_provider = PgSQLPresetProvider()


    def on_HostInit(self, message):
        if message.local_ip != self._platform.get_private_ip() and message.local_ip in self.pg_hosts:
            LOG.debug('Got new slave IP: %s. Registering in pg_hba.conf' % message.local_ip)
            self.postgresql.register_slave(message.local_ip)


    def on_HostUp(self, message):
        if message.local_ip == self._platform.get_private_ip():
            self.accept_all_clients()
        elif message.local_ip in self.farm_hosts:
            self.postgresql.register_client(message.local_ip)



    def on_HostDown(self, message):
        if  message.local_ip != self._platform.get_private_ip():
            self.postgresql.unregister_client(message.local_ip)
            if self.is_replication_master and self.farmrole_id == message.farm_role_id:
                self.postgresql.unregister_slave(message.local_ip)

    @property
    def farm_hosts(self):
        list_roles = self._queryenv.list_roles()
        servers = []
        for serv in list_roles:
            for host in serv.hosts :
                servers.append(host.internal_ip or host.external_ip)
        LOG.debug("QueryEnv returned list of servers within farm: %s" % servers)
        return servers


    @property
    def pg_hosts(self):
        '''
        All pg instances including those in Initializing state
        '''
        list_roles = self._queryenv.list_roles(behaviour=BEHAVIOUR, with_init=True)
        servers = []
        for pg_serv in list_roles:
            for pg_host in pg_serv.hosts:
                servers.append(pg_host.internal_ip or pg_host.external_ip)
        LOG.debug("QueryEnv returned list of %s servers: %s" % (BEHAVIOUR, servers))
        return servers


    def accept_all_clients(self):
        farm_hosts = self.farm_hosts
        for ip in farm_hosts:
                self.postgresql.register_client(ip, force=False)
        if farm_hosts:
            self.postgresql.service.reload('Granting access to all servers within farm.', force=True)


    @property
    def root_password(self):
        return __postgresql__['%s_password' % ROOT_USER]


    @property
    def farmrole_id(self):
        return __node__[config.OPT_FARMROLE_ID]


    def store_password(self, name, password):
        __postgresql__['%s_password' % name] = password


    @property
    def _tmp_path(self):
        return os.path.join(__postgresql__['storage_dir'], 'tmp')


    @property
    def is_replication_master(self):
        return True if int(__postgresql__[OPT_REPLICATION_MASTER]) else False


    def on_host_init_response(self, message):
        """
        Check postgresql data in host init response
        @type message: scalarizr.messaging.Message
        @param message: HostInitResponse
        """
        log = bus.init_op.logger
        log.info('Accept Scalr configuration')

        if not message.body.has_key(BEHAVIOUR) or message.db_type != BEHAVIOUR:
            raise HandlerError("HostInitResponse message for PostgreSQL behaviour must have 'postgresql' property and db_type 'postgresql'")

        postgresql_data = message.postgresql.copy()

        #Extracting service configuration preset from message
        if 'preset' in postgresql_data:
            self.initial_preset = postgresql_data['preset']
            LOG.debug('Scalr sent current preset: %s' % self.initial_preset)
            del postgresql_data['preset']

        #Extracting or generating postgresql root password
        postgresql_data['%s_password' % ROOT_USER] = postgresql_data.get(OPT_ROOT_PASSWORD) or cryptotool.pwgen(10)
        del postgresql_data[OPT_ROOT_PASSWORD]

        #Extracting replication ssh keys from message
        root = PgUser(ROOT_USER, self.postgresql.pg_keys_dir)
        root.store_keys(postgresql_data[OPT_ROOT_SSH_PUBLIC_KEY], postgresql_data[OPT_ROOT_SSH_PRIVATE_KEY])
        del postgresql_data[OPT_ROOT_SSH_PUBLIC_KEY]
        del postgresql_data[OPT_ROOT_SSH_PRIVATE_KEY]


        if postgresql_data.get('volume'):
            # New format
            postgresql_data['compat_prior_backup_restore'] = False
            postgresql_data['volume'] = storage2.volume(postgresql_data['volume'])

            LOG.debug("message.pg['volume']: %s", postgresql_data['volume'])
            if 'backup' in postgresql_data:
                postgresql_data['backup'] = backup.backup(postgresql_data['backup'])
                LOG.debug("message.pg['backup']: %s", postgresql_data['backup'])
            if 'restore' in postgresql_data:
                postgresql_data['restore'] = backup.restore(postgresql_data['restore'])
                LOG.debug("message.pg['restore']: %s", postgresql_data['restore'])
        else:

            # Compatibility transformation
            # - volume_config -> volume
            # - master n'th start, type=ebs - del snapshot_config
            # - snapshot_config -> restore
            # - create backup object on master 1'st start

            postgresql_data['compat_prior_backup_restore'] = True
            if postgresql_data.get(OPT_VOLUME_CNF):
                postgresql_data['volume'] = storage2.volume(
                    postgresql_data.pop(OPT_VOLUME_CNF))

            elif postgresql_data.get(OPT_SNAPSHOT_CNF):
                postgresql_data['volume'] = storage2.volume(
                    type=postgresql_data[OPT_SNAPSHOT_CNF]['type'])

            else:
                raise HandlerError('No volume config or snapshot config provided')

            if postgresql_data['volume'].device and \
                            postgresql_data['volume'].type in ('ebs', 'csvol', 'cinder', 'raid', 'gce_persistent'):
                LOG.debug("Master n'th start detected. Removing snapshot config from message")
                postgresql_data.pop(OPT_SNAPSHOT_CNF, None)

            if postgresql_data.get(OPT_SNAPSHOT_CNF):
                postgresql_data['restore'] = backup.restore(
                    type='snap_postgresql',
                    snapshot=postgresql_data.pop(OPT_SNAPSHOT_CNF),
                    volume=postgresql_data['volume'])

            if int(postgresql_data['replication_master']):
                postgresql_data['backup'] = backup.backup(
                    type='snap_postgresql',
                    volume=postgresql_data['volume'])

        self._hir_volume_growth = postgresql_data.pop('volume_growth', None)

        LOG.debug("Update postgresql config with %s", postgresql_data)
        __postgresql__.update(postgresql_data)
        __postgresql__['volume'].mpoint = __postgresql__['storage_dir']


    def on_before_host_up(self, message):
        """
        Configure PostgreSQL behaviour
        @type message: scalarizr.messaging.Message
        @param message: HostUp message
        """

        repl = 'master' if self.is_replication_master else 'slave'
        #bus.fire('before_postgresql_configure', replication=repl)

        if self.is_replication_master:
            self._init_master(message)
        else:
            self._init_slave(message)
        # Force to resave volume settings
        __postgresql__['volume'] = storage2.volume(__postgresql__['volume'])
        bus.fire('service_configured', service_name=SERVICE_NAME, replication=repl, preset=self.initial_preset)


    def on_before_reboot_start(self, *args, **kwargs):
        """
        Stop PostgreSQL and unplug storage
        """
        self.postgresql.service.stop('rebooting')


    def on_BeforeHostTerminate(self, message):
        LOG.info('Handling BeforeHostTerminate message from %s' % message.local_ip)
        if message.local_ip == self._platform.get_private_ip():
            LOG.info('Stopping %s service' % BEHAVIOUR)
            self.postgresql.service.stop('Server will be terminated')
            if not self.is_replication_master:
                LOG.info('Destroying volume %s' % __postgresql__['volume'].id)
                __postgresql__['volume'].destroy(remove_disks=True)
                LOG.info('Volume %s has been destroyed.' % __postgresql__['volume'].id)
            else:
                __postgresql__['volume'].umount()


    def on_DbMsr_CreateDataBundle(self, message):
        LOG.debug("on_DbMsr_CreateDataBundle")
        self._postgresql_api.create_databundle(async=True)


    def on_DbMsr_PromoteToMaster(self, message):
        """
        Promote slave to master
        @type message: scalarizr.messaging.Message
        @param message: postgresql_PromoteToMaster
        """
        LOG.debug("on_DbMsr_PromoteToMaster")
        postgresql = message.body[BEHAVIOUR]

        if int(__postgresql__['replication_master']):
            LOG.warning('Cannot promote to master. Already master')
            return

        LOG.info('Starting Slave -> Master promotion')
        bus.fire('before_slave_promote_to_master')

        msg_data = {
            'db_type' : BEHAVIOUR,
            'status' : 'ok',
            BEHAVIOUR : {}
        }

        tx_complete = False

        new_vol = None
        if postgresql.get('volume_config'):
            new_vol = storage2.volume(postgresql.get('volume_config'))

        try:
            self.postgresql.stop_replication()

            if new_vol and new_vol.type not in ('eph', 'lvm'):
                self.postgresql.service.stop('Unplugging slave storage and then plugging master one')

                old_vol = storage2.volume(__postgresql__['volume'])
                old_vol.detach(force=True)

                new_vol.mpoint = __postgresql__['storage_dir']
                new_vol.ensure(mount=True)

                if not self.postgresql.cluster_dir.is_initialized(STORAGE_PATH):
                    raise HandlerError("%s is not a valid postgresql storage" % STORAGE_PATH)

                __postgresql__['volume'] = new_vol
                msg_data[BEHAVIOUR] = {'volume_config': dict(new_vol)}

            slaves = [host.internal_ip for host in self._get_slave_hosts()]
            self.postgresql.init_master(STORAGE_PATH, self.root_password, slaves)
            self.postgresql.start_replication()
            __postgresql__[OPT_REPLICATION_MASTER] = 1

            if not new_vol or new_vol.type in ('eph', 'lvm'):
                snap = self._create_snapshot()
                __postgresql__['snapshot'] = snap
                msg_data[BEHAVIOUR].update({OPT_SNAPSHOT_CNF : dict(snap)})

            msg_data[OPT_CURRENT_XLOG_LOCATION] = None # useless but required by Scalr

            self.send_message(DbMsrMessages.DBMSR_PROMOTE_TO_MASTER_RESULT, msg_data)

            tx_complete = True
            bus.fire('slave_promote_to_master')

        except (Exception, BaseException), e:
            LOG.exception(e)

            self.send_message(DbMsrMessages.DBMSR_PROMOTE_TO_MASTER_RESULT, dict(
                db_type=BEHAVIOUR,
                status="error",
                last_error=str(e)
            ))

            self.postgresql.service.stop('Unplugging broken storage and then plugging the old one')
            if new_vol:
                new_vol.detach()
            # Get back slave storage
            if old_vol:
                old_vol.ensure(mount=True)
            self.postgresql.service.start()

        if tx_complete and new_vol and new_vol.type not in ('eph', 'lvm'):
            # Delete slave EBS
            old_vol.destroy(remove_disks=True)


    def on_DbMsr_NewMasterUp(self, message):
        """
        Switch replication to a new master server
        @type message: scalarizr.messaging.Message
        @param message:  DbMsr_NewMasterUp
        """
        try:
            assert message.body.has_key("db_type")
            assert message.body.has_key("local_ip")
            assert message.body.has_key("remote_ip")
            assert message.body.has_key(BEHAVIOUR)

            postgresql_data = message.body[BEHAVIOUR]

            if int(__postgresql__['replication_master']):
                LOG.debug('Skip NewMasterUp. My replication role is master')
                return

            host = message.local_ip or message.remote_ip
            LOG.info("Switching replication to a new PostgreSQL master %s", host)
            bus.fire('before_postgresql_change_master', host=host)

            LOG.debug("__postgresql__['volume']: %s", __postgresql__['volume'])

            if __postgresql__['volume'].type in ('eph', 'lvm'):
                if 'restore' in postgresql_data:
                    restore = backup.restore(**postgresql_data['restore'])
                else:
                    restore = backup.restore(
                        type='snap_postgresql',
                        volume=__postgresql__['volume'],
                        snapshot=postgresql_data[OPT_SNAPSHOT_CNF])

                if __postgresql__['volume'].type == 'eph':
                    self.postgresql.service.stop('Swapping storages to reinitialize slave')

                    LOG.info('Reinitializing Slave from the new snapshot %s',
                        restore.snapshot['id'])
                    new_vol = restore.run()

                #self.postgresql.service.start()

            self.postgresql.init_slave(STORAGE_PATH, host, __postgresql__['port'], self.root_password)
            LOG.debug("Replication switched")
            bus.fire('postgresql_change_master', host=host)

            msg_data = dict(
                db_type = BEHAVIOUR,
                status = 'ok'
            )
            self.send_message(DbMsrMessages.DBMSR_NEW_MASTER_UP_RESULT, msg_data)

        except (Exception, BaseException), e:
            LOG.exception(e)

            msg_data = dict(
                db_type = BEHAVIOUR,
                status="error",
                last_error=str(e))
            self.send_message(DbMsrMessages.DBMSR_NEW_MASTER_UP_RESULT, msg_data)


    def on_DbMsr_CreateBackup(self, message):
        #TODO: Think how to move the most part of it into Postgresql class
        # Retrieve password for scalr pg user
        LOG.debug("on_DbMsr_CreateBackup")
        self._postgresql_api.create_backup(async=True)


    def _init_master(self, message):
        """
        Initialize postgresql master
        @type message: scalarizr.messaging.Message
        @param message: HostUp message
        """
        log = bus.init_op.logger
        log.info("Initializing PostgreSQL master")

        log.info('Create storage')

        # Plug storage
        if 'restore' in __postgresql__ and\
           __postgresql__['restore'].type == 'snap_postgresql':
            __postgresql__['restore'].run()
        else:
            if __node__['platform'].name == 'idcf':
                if __postgresql__['volume'].id:
                    LOG.info('Cloning volume to workaround reattachment limitations of IDCF')
                    __postgresql__['volume'].snap = __postgresql__['volume'].snapshot()

            if self._hir_volume_growth:
                #Growing maser storage if HIR message contained "growth" data
                LOG.info("Attempting to grow data volume according to new data: %s" % str(self._hir_volume_growth))
                grown_volume = __postgresql__['volume'].grow(**self._hir_volume_growth)
                grown_volume.mount()
                __postgresql__['volume'] = grown_volume
            else:
                __postgresql__['volume'].ensure(mount=True, mkfs=True)
            LOG.debug('Postgres volume config after ensure: %s', dict(__postgresql__['volume']))

        log.info('Initialize Master')
        self.postgresql.init_master(mpoint=STORAGE_PATH, password=self.root_password)

        if self.postgresql.first_start and 'backup' in __postgresql__:
            log.info('Create data bundle')
            __postgresql__['restore'] = __postgresql__['backup'].run()
        else:
            log.debug('No need to create data bundle')

        log.info('Collect HostUp data')
        # Update HostUp message
        msg_data = dict({OPT_REPLICATION_MASTER: str(int(self.is_replication_master)),
                        OPT_ROOT_USER: self.postgresql.root_user.name,
                        OPT_ROOT_PASSWORD: self.root_password,
                        OPT_ROOT_SSH_PRIVATE_KEY: self.postgresql.root_user.private_key,
                        OPT_ROOT_SSH_PUBLIC_KEY: self.postgresql.root_user.public_key,
                        OPT_CURRENT_XLOG_LOCATION: None})

        if self._hir_volume_growth:
            msg_data['volume_template'] = dict(__postgresql__['volume'].clone())

        if __postgresql__['compat_prior_backup_restore']:
            if 'restore' in __postgresql__:
                msg_data.update(dict(
                    snapshot_config=dict(__postgresql__['restore'].snapshot)))
            msg_data.update(dict(
                volume_config=dict(__postgresql__['volume'])))
        else:
            msg_data.update(dict(
                volume=dict(__postgresql__['volume'])
            ))
            for key in ('backup', 'restore'):
                if key in __postgresql__:
                    msg_data[key] = dict(__postgresql__[key])

        message.db_type = BEHAVIOUR
        message.postgresql = msg_data.copy()

        try:
            del msg_data[OPT_SNAPSHOT_CNF], msg_data[OPT_VOLUME_CNF]
        except KeyError:
            pass

        __postgresql__.update(msg_data)


    def _get_master_host(self):
        master_host = None
        LOG.info("Requesting master server")
        while not master_host:
            try:
                master_host = list(host
                    for host in self._queryenv.list_roles(behaviour=BEHAVIOUR)[0].hosts
                    if host.replication_master)[0]
            except IndexError:
                LOG.debug("QueryEnv respond with no postgresql master. " +
                        "Waiting %d seconds before the next attempt", 5)
                time.sleep(5)
        return master_host

    def _get_slave_hosts(self):
        LOG.info("Requesting standby servers")
        return list(host for host in self._queryenv.list_roles(behaviour=BEHAVIOUR)[0].hosts
                if not host.replication_master)

    def _init_slave(self, message):
        """
        Initialize postgresql slave
        @type message: scalarizr.messaging.Message
        @param message: HostUp message
        """
        log = bus.init_op.logger
        log.info("Initializing PostgreSQL slave")

        log.info('Create storage')
        LOG.debug("Initialize slave storage")
        if 'restore' in __postgresql__ and\
           __postgresql__['restore'].type == 'snap_postgresql':
            __postgresql__['restore'].run()
        else:
            __postgresql__['volume'].ensure(mount=True, mkfs=True)

        log.info('Initialize Slave')
        # Change replication master
        master_host = self._get_master_host()

        LOG.debug("Master server obtained (local_ip: %s, public_ip: %s)",
                master_host.internal_ip, master_host.external_ip)

        host = master_host.internal_ip or master_host.external_ip
        self.postgresql.init_slave(STORAGE_PATH, host, __postgresql__['port'], self.root_password)

        log.info('Collect HostUp data')
        # Update HostUp message
        message.db_type = BEHAVIOUR


    def _create_snapshot(self):
        LOG.info("Creating PostgreSQL data bundle")
        backup_obj = backup.backup(type='snap_postgresql',
            volume=__postgresql__['volume'])
        restore = backup_obj.run()
        return restore.snapshot


    def _insert_iptables_rules(self):
        if iptables.enabled():
            iptables.FIREWALL.ensure([
                {"jump": "ACCEPT", "protocol": "tcp", "match": "tcp", "dport": str(__postgresql__['port'])},
            ])
