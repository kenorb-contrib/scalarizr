from __future__ import with_statement
'''
Created on Aug 12, 2011

@author: Dmytro Korsakov
'''
from __future__ import with_statement

import os
import re
import sys
import time
import logging

from scalarizr import handlers
from scalarizr.api import redis as redis_api

from scalarizr import config, storage2
from scalarizr.node import __node__
from scalarizr.bus import bus
from scalarizr.messaging import Messages
from scalarizr.util import system2, cryptotool, software, initdv2
from scalarizr.linux import iptables, which
from scalarizr.services import redis, backup
from scalarizr.service import CnfController
from scalarizr.handlers import ServiceCtlHandler, HandlerError, DbMsrMessages
from scalarizr import node


BEHAVIOUR = SERVICE_NAME = CNF_SECTION = redis_api.BEHAVIOUR

__redis__ = redis.__redis__

STORAGE_PATH                            = redis_api.STORAGE_PATH
STORAGE_VOLUME_CNF                      = 'redis.json'
STORAGE_SNAPSHOT_CNF                    = 'redis-snap.json'

OPT_VOLUME_CNF                          = 'volume_config'
OPT_SNAPSHOT_CNF                        = 'snapshot_config'

REDIS_CNF_PATH                          = 'cnf_path'
UBUNTU_CONFIG_PATH                      = '/etc/redis/redis.conf'
CENTOS_CONFIG_PATH                      = '/etc/redis.conf'

BACKUP_CHUNK_SIZE                       = 200*1024*1024


LOG = logging.getLogger(__name__)


initdv2.explore(SERVICE_NAME, redis.RedisInitScript)


def get_handlers():
    return [RedisHandler()]


class RedisHandler(ServiceCtlHandler, handlers.FarmSecurityMixin):

    _queryenv = None
    """ @type _queryenv: scalarizr.queryenv.QueryEnvService """

    _platform = None
    """ @type _platform: scalarizr.platform.Ec2Platform """

    _cnf = None
    ''' @type _cnf: scalarizr.config.ScalarizrCnf '''

    default_service = None
    redis_instances = None

    @property
    def is_replication_master(self):
        return __redis__["replication_master"]


    @property
    def persistence_type(self):
        try:
            value = __redis__["persistence_type"]
        except KeyError:
            value = None

        if not value:
            value = 'snapshotting'
            __redis__["persistence_type"] = value
        LOG.debug('Got persistence_type : %s' % value)

        return value


    def accept(self, message, queue, behaviour=None, platform=None, os=None, dist=None):
        return BEHAVIOUR in behaviour and (
        message.name == DbMsrMessages.DBMSR_NEW_MASTER_UP
        or      message.name == DbMsrMessages.DBMSR_PROMOTE_TO_MASTER
        or      message.name == DbMsrMessages.DBMSR_CREATE_DATA_BUNDLE
        or      message.name == DbMsrMessages.DBMSR_CREATE_BACKUP
        or  message.name == Messages.UPDATE_SERVICE_CONFIGURATION
        or  message.name == Messages.BEFORE_HOST_TERMINATE
        or  message.name == Messages.HOST_INIT)


    def get_initialization_phases(self, hir_message):
        if BEHAVIOUR in hir_message.body:

            steps = [self._step_accept_scalr_conf, self._step_create_storage]
            if hir_message.body[BEHAVIOUR]['replication_master'] == '1':
                steps += [self._step_init_master]
            else:
                steps += [self._step_init_slave]
            steps += [self._step_collect_host_up_data]

            return {'before_host_up': [{
                                       'name': self._phase_redis,
                                       'steps': steps
                                       }]}


    def __init__(self):
        self._hir_volume_growth = None
        self._redis_api = redis_api.RedisAPI()
        self.preset_provider = redis.RedisPresetProvider()

        from_port = __redis__['ports_range'][0]
        to_port = __redis__['ports_range'][-1]
        handlers.FarmSecurityMixin.__init__(self)
        ServiceCtlHandler.__init__(self, SERVICE_NAME, cnf_ctl=RedisCnfController())

        bus.on("init", self.on_init)
        bus.define_events(
                'before_%s_data_bundle' % BEHAVIOUR,

                '%s_data_bundle' % BEHAVIOUR,

                # @param host: New master hostname
                'before_%s_change_master' % BEHAVIOUR,

                # @param host: New master hostname
                '%s_change_master' % BEHAVIOUR,

                'before_slave_promote_to_master',

                'slave_promote_to_master'
        )

        self._phase_redis = 'Configure Redis'
        self._phase_data_bundle = self._op_data_bundle = 'Redis data bundle'
        self._phase_backup = self._op_backup = 'Redis backup'
        self._step_copy_database_file = 'Copy database file'
        self._step_upload_to_cloud_storage = 'Upload data to cloud storage'
        self._step_accept_scalr_conf = 'Accept Scalr configuration'
        self._step_patch_conf = 'Patch configuration files'
        self._step_create_storage = 'Create storage'
        self._step_init_master = 'Initialize Master'
        self._step_init_slave = 'Initialize Slave'
        self._step_create_data_bundle = 'Create data bundle'
        self._step_change_replication_master = 'Change replication Master'
        self._step_collect_host_up_data = 'Collect HostUp data'

        self.on_reload()

    def _set_overcommit_option(self):
        try:
            with open('/proc/sys/vm/overcommit_memory', 'r') as f:
                proc = f.read().strip()

            with open('/etc/sysctl.conf', 'r') as f:
                match = re.search(r'^\s*vm.overcommit_memory\s*=\s*(\d+)', f.read(), re.M)
                sysctl = match.group(1) if match else None

            if (proc == '2') or (proc == sysctl == '0'):
                LOG.info('Kernel option vm.overcommit_memory is set to %s by user. '
                         'Consider changing it to 1 for optimal Redis functioning. '
                         'More information here: http://redis.io/topics/admin', proc)
            else:
                LOG.debug('Setting vm.overcommit_memory to 1')
                system2((which('sysctl'), 'vm.overcommit_memory=1'))
        except:
            LOG.debug("Failed to set vm.overcommit_memory option", exc_info=sys.exc_info())


    def on_init(self):

        bus.on("host_init_response", self.on_host_init_response)
        bus.on("before_host_up", self.on_before_host_up)
        bus.on("before_reboot_start", self.on_before_reboot_start)
        bus.on("before_reboot_finish", self.on_before_reboot_finish)

        self._set_overcommit_option()

        if __node__['state'] == 'running':
            self._ensure_security()

            vol = storage2.volume(__redis__['volume'])
            vol.ensure(mount=True)
            __redis__['volume'] = vol

            ports = [__redis__['defaults']['port'],]
            passwords = [self.get_main_password(),]
            num_processes = 1
            farm_role_id = self._cnf.rawini.get(config.SECT_GENERAL, config.OPT_FARMROLE_ID)
            params = self._queryenv.list_farm_role_params(farm_role_id).get('params', {})
            if 'redis' in params:
                redis_data = params['redis']
                for param in ('ports', 'passwords', 'num_processes'):
                    if param not in redis_data:
                        break
                    else:
                        ports = map(int, redis_data['ports'])
                        passwords = redis_data['passwords']
                        num_processes = int(redis_data['num_processes'])

            self.redis_instances = redis.RedisInstances()

            self.redis_instances.init_processes(num_processes, ports, passwords)
            self.redis_instances.start()

            self._init_script = self.redis_instances.get_default_process()


    def on_reload(self):
        self._queryenv = bus.queryenv_service
        self._platform = bus.platform
        self._cnf = bus.cnf
        ini = self._cnf.rawini
        self._role_name = ini.get(config.SECT_GENERAL, config.OPT_ROLE_NAME)

        self._storage_path = STORAGE_PATH

        self._volume_config_path  = self._cnf.private_path(os.path.join('storage', STORAGE_VOLUME_CNF))
        self._snapshot_config_path = self._cnf.private_path(os.path.join('storage', STORAGE_SNAPSHOT_CNF))

        self.default_service = initdv2.lookup(SERVICE_NAME)


    def _ensure_security(self):
        ports = "{0}:{1}".format(
                    __redis__['ports_range'][0], 
                    __redis__['ports_range'][-1])
        if self.use_passwords and iptables.enabled():
            if __node__['state'] == 'running':
                # TODO: deprecate and remove it in 2015
                # Fix to enable access outside farm when use_passwords=True
                try:
                    iptables.FIREWALL.remove({
                        "protocol": "tcp", 
                        "match": "tcp", 
                        "dport": ports,
                        "jump": "DROP"
                    })
                except:
                    # silently ignore non existed rule error
                    pass
            iptables.FIREWALL.ensure([{
                "jump": "ACCEPT", 
                "protocol": "tcp", 
                "match": "tcp", 
                "dport": ports
            }])
        else:
            self.init_farm_security([ports]) 


    def on_host_init_response(self, message):
        """
        Check redis data in host init response
        @type message: scalarizr.messaging.Message
        @param message: HostInitResponse
        """
        log = bus.init_op.logger
        log.info('Accept Scalr configuration')

        if not message.body.has_key(BEHAVIOUR) or message.db_type != BEHAVIOUR:
            raise HandlerError("HostInitResponse message for %s behaviour must have '%s' property and db_type '%s'"
                               % (BEHAVIOUR, BEHAVIOUR, BEHAVIOUR))

        config_dir = os.path.dirname(self._volume_config_path)
        if not os.path.exists(config_dir):
            os.makedirs(config_dir)

        redis_data = message.redis.copy()
        LOG.info('Got Redis part of HostInitResponse: %s' % redis_data)

        if 'preset' in redis_data:
            self.initial_preset = redis_data['preset']
            del redis_data['preset']
            LOG.debug('Scalr sent current preset: %s' % self.initial_preset)


        '''
        XXX: following line enables support for old scalr installations
        use_password shoud be set by postinstall script for old servers
        '''
        redis_data["use_password"] = redis_data.get("use_password", '1')

        ports = []
        passwords = []
        num_processes = 1

        if 'ports' in redis_data and redis_data['ports']:
            ports = map(int, redis_data['ports'])
            del redis_data['ports']

        if 'passwords' in redis_data and redis_data['passwords']:
            passwords = redis_data['passwords']
            del redis_data['passwords']

        if 'num_processes' in redis_data and redis_data['num_processes']:
            num_processes = int(redis_data['num_processes'])
            del redis_data['num_processes']

        redis_data['volume'] = storage2.volume(
                        redis_data.pop('volume_config'))

        if redis_data['volume'].device and \
                                redis_data['volume'].type in ('ebs', 'csvol', 'cinder', 'raid'):
            redis_data.pop('snapshot_config', None)

        if redis_data.get('snapshot_config'):
            redis_data['restore'] = backup.restore(
                    type='snap_redis',
                    snapshot=redis_data.pop('snapshot_config'),
                    volume=redis_data['volume'])

        self._hir_volume_growth = redis_data.pop('volume_growth', None)


        # Update configs
        __redis__.update(redis_data)
        __redis__['volume'].mpoint = __redis__['storage_dir']
        if self.default_service.running:
            self.default_service.stop('Terminating default redis instance')

        self.redis_instances = redis.RedisInstances()
        ports = ports or [__redis__['defaults']['port'],]
        passwords = passwords or [self.get_main_password(),]
        self.redis_instances.init_processes(num_processes, ports=ports, passwords=passwords)

        self._ensure_security()


    def on_before_host_up(self, message):
        """
        Configure redis behaviour
        @type message: scalarizr.messaging.Message
        @param message: HostUp message
        """

        repl = 'master' if self.is_replication_master else 'slave'
        message.redis = {}

        if self.is_replication_master:
            self._init_master(message)
        else:
            self._init_slave(message)

        __redis__['volume'] = storage2.volume(__redis__['volume'])

        self._init_script = self.redis_instances.get_default_process()
        message.redis['ports'] = self.redis_instances.ports
        message.redis['passwords'] = self.redis_instances.passwords
        message.redis['num_processes'] = len(self.redis_instances.instances)
        message.redis['volume_config'] = dict(__redis__['volume'])
        bus.fire('service_configured', service_name=SERVICE_NAME, replication=repl, preset=self.initial_preset)


    def on_before_reboot_start(self, *args, **kwargs):
        self.redis_instances.save_all()


    def on_before_reboot_finish(self, *args, **kwargs):
        """terminating old redis instance managed by init scrit"""
        if self.default_service.running:
            self.default_service.stop('Treminating default redis instance')


    def on_BeforeHostTerminate(self, message):
        LOG.info('Handling BeforeHostTerminate message from %s' % message.local_ip)
        if message.local_ip == self._platform.get_private_ip():
            LOG.info('Dumping redis data on disk')
            self.redis_instances.save_all()
            LOG.info('Stopping %s service' % BEHAVIOUR)
            self.redis_instances.stop('Server will be terminated')
            if not self.is_replication_master:
                LOG.info('Destroying volume %s' % __redis__['volume'].id)
                __redis__['volume'].destroy(remove_disks=True)
                LOG.info('Volume %s was destroyed.' % __redis__['volume'].id)
            else:
                __redis__['volume'].umount()


    def on_DbMsr_CreateDataBundle(self, message):
        self._redis_api.create_databundle()


    def on_DbMsr_PromoteToMaster(self, message):
        """
        Promote slave to master
        @type message: scalarizr.messaging.Message
        @param message: redis_PromoteToMaster
        """

        if message.db_type != BEHAVIOUR:
            LOG.error('Wrong db_type in DbMsr_PromoteToMaster message: %s' % message.db_type)
            return

        if self.is_replication_master:
            LOG.warning('Cannot promote to master. Already master')
            return
        bus.fire('before_slave_promote_to_master')

        master_storage_conf = message.body.get('volume_config')
        tx_complete = False
        old_vol                 = None
        new_storage_vol = None

        msg_data = dict(
                db_type=BEHAVIOUR,
                status="ok",
                )

        try:
            if master_storage_conf and master_storage_conf['type'] != 'eph':

                self.redis_instances.stop('Unplugging slave storage and then plugging master one')

                old_vol = storage2.volume(__redis__['volume'])
                old_vol.detach(force=True)
                new_storage_vol = storage2.volume(master_storage_conf)
                new_storage_vol.ensure(mount=True)
                __redis__['volume'] = new_storage_vol

            self.redis_instances.init_as_masters(self._storage_path)
            __redis__['replication_master'] = 1
            msg_data[BEHAVIOUR] = {'volume_config': dict(__redis__['volume'])}
            self.send_message(DbMsrMessages.DBMSR_PROMOTE_TO_MASTER_RESULT, msg_data)

            tx_complete = True
            bus.fire('slave_promote_to_master')

        except (Exception, BaseException), e:
            LOG.exception(e)
            if new_storage_vol and not new_storage_vol.detached:
                new_storage_vol.detach(force=True)
            # Get back slave storage
            if old_vol:
                old_vol.ensure(mount=True)
                __redis__['volume'] = old_vol

            self.send_message(DbMsrMessages.DBMSR_PROMOTE_TO_MASTER_RESULT, dict(
                    db_type=BEHAVIOUR,
                    status="error",
                    last_error=str(e)
            ))

            # Start redis
            self.redis_instances.start()

        if tx_complete and old_vol is not None:
            # Delete slave EBS
            old_vol.destroy(remove_disks=True)


    def on_DbMsr_NewMasterUp(self, message):
        """
        Switch replication to a new master server
        @type message: scalarizr.messaging.Message
        @param message:  DbMsr__NewMasterUp
        """
        if not message.body.has_key(BEHAVIOUR) or message.db_type != BEHAVIOUR:
            raise HandlerError("DbMsr_NewMasterUp message for %s behaviour must have '%s' property and db_type '%s'" %
                               BEHAVIOUR, BEHAVIOUR, BEHAVIOUR)

        if self.is_replication_master:
            LOG.debug('Skipping NewMasterUp. My replication role is master')
            return

        host = message.local_ip or message.remote_ip
        LOG.info("Switching replication to a new %s master %s"% (BEHAVIOUR, host))
        bus.fire('before_%s_change_master' % BEHAVIOUR, host=host)

        self.redis_instances.init_as_slaves(self._storage_path, host)
        self.redis_instances.wait_for_sync()

        LOG.debug("Replication switched")
        bus.fire('%s_change_master' % BEHAVIOUR, host=host)


    def on_DbMsr_CreateBackup(self, message):
        self._redis_api.create_backup()


    def _init_master(self, message):
        """
        Initialize redis master
        @type message: scalarizr.messaging.Message
        @param message: HostUp message
        """
        log = bus.init_op.logger

        log.info("Initializing %s master" % BEHAVIOUR)
        log.info('Create storage')

        # Plug storage
        if 'restore' in __redis__ and __redis__['restore'].type == 'snap_redis':
            __redis__['restore'].run()
        else:
            if node.__node__['platform'].name == 'idcf':
                if __redis__['volume'].id:
                    LOG.info('Cloning volume to workaround reattachment limitations of IDCF')
                    __redis__['volume'].snap = __redis__['volume'].snapshot()

            if self._hir_volume_growth:
                #Growing maser storage if HIR message contained "growth" data
                LOG.info("Attempting to grow data volume according to new data: %s" % str(self._hir_volume_growth))
                grown_volume = __redis__['volume'].grow(**self._hir_volume_growth)
                grown_volume.mount()
                __redis__['volume'] = grown_volume
            else:
                __redis__['volume'].ensure(mount=True, mkfs=True)
                LOG.debug('Redis volume config after ensure: %s', dict(__redis__['volume']))

        log.info('Initialize Master')
        password = self.get_main_password()

        self.redis_instances.init_as_masters(mpoint=self._storage_path)

        msg_data = dict()
        msg_data.update({
            "replication_master": '1',
            "master_password": password,
        })

        log.info('Collect HostUp data')
        # Update HostUp message

        if self._hir_volume_growth:
            msg_data['volume_template'] = dict(__redis__['volume'].clone())

        if msg_data:
            message.db_type = BEHAVIOUR
            message.redis = msg_data.copy()


    @property
    def use_passwords(self):
        return __redis__["use_password"]


    def get_main_password(self):
        password = __redis__["master_password"]

        if self.use_passwords and not password:
            password = cryptotool.pwgen(20)
            __redis__["master_password"] = password

        return password

    def _get_master_host(self):
        master_host = None
        LOG.info("Requesting master server")
        while not master_host:
            try:
                master_host = list(host
                        for host in self._queryenv.list_roles(behaviour=BEHAVIOUR)[0].hosts
                        if host.replication_master)[0]
            except IndexError:
                LOG.debug("QueryEnv respond with no %s master. " % BEHAVIOUR +
                          "Waiting %d seconds before the next attempt" % 5)
                time.sleep(5)
        return master_host


    def _init_slave(self, message):
        """
        Initialize redis slave
        @type message: scalarizr.messaging.Message
        @param message: HostUp message
        """
        log = bus.init_op.logger
        log.info("Initializing %s slave" % BEHAVIOUR)

        log.info('Create storage')

        LOG.debug("Initializing slave storage")
        __redis__['volume'].ensure(mount=True, mkfs=True)

        log.info('Initialize Slave')
        # Change replication master
        master_host = self._get_master_host()

        LOG.debug("Master server obtained (local_ip: %s, public_ip: %s)",
                master_host.internal_ip, master_host.external_ip)

        host = master_host.internal_ip or master_host.external_ip
        self.redis_instances.init_as_slaves(self._storage_path, host)
        self.redis_instances.wait_for_sync()

        log.info('Collect HostUp data')
        # Update HostUp message
        message.db_type = BEHAVIOUR


    def _create_snapshot(self):
        LOG.info("Creating Redis data bundle")
        backup_obj = backup.backup(type='snap_redis', volume=__redis__['volume'])
        restore = backup_obj.run()
        return restore.snapshot


class RedisCnfController(CnfController):

    def __init__(self):
        cnf_path = redis.get_redis_conf_path()
        CnfController.__init__(self, BEHAVIOUR, cnf_path, 'redis', {'1':'yes', '0':'no'})


    @property
    def _software_version(self):
        return software.software_info('redis').version


    def get_main_password(self):
        return


    def _after_apply_preset(self):
        cli = redis.RedisCLI(__redis__["master_password"])
        cli.bgsave()
