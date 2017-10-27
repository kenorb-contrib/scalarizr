"""
Created on Aug 1, 2012
 
@author: Dmytro Korsakov
"""
 
from __future__ import with_statement
 
import os
import time
import logging
from scalarizr import config
from scalarizr.bus import bus
from scalarizr import rpc
from scalarizr.linux import iptables
from scalarizr.api import operation
from scalarizr.util import system2, PopenError
from scalarizr.services import redis as redis_service
from scalarizr.services import backup
from scalarizr.handlers import transfer_result_to_backup_result, DbMsrMessages
from scalarizr.services.redis import __redis__
from scalarizr.util.cryptotool import pwgen
from scalarizr.storage2.cloudfs import LargeTransfer
from scalarizr import node
 
 
BEHAVIOUR = config.BuiltinBehaviours.REDIS
STORAGE_PATH = '/mnt/redisstorage'
 
 
LOG = logging.getLogger(__name__)
 
 
class RedisAPI(object):
 
    _cnf = None
    _queryenv = None
 
    def __init__(self):
        self._cnf = bus.cnf
        self._op_api = operation.OperationAPI()
        self._queryenv = bus.queryenv_service
        ini = self._cnf.rawini
        self._role_name = ini.get(config.SECT_GENERAL, config.OPT_ROLE_NAME)
        self.redis_instances = redis_service.RedisInstances()
 
    @rpc.command_method
    def launch_processes(self, num=None, ports=None, passwords=None, async=False):
        if ports and passwords and len(ports) != len(passwords):
            raise AssertionError('Number of ports must be equal to number of passwords')
        if num and ports and num != len(ports):
            raise AssertionError('When ports range is passed its length must be equal to num parameter')
        if not __redis__["replication_master"]:
            if not passwords or not ports:
                raise AssertionError('ports and passwords are required to launch processes on redis slave')
        available_ports = self.available_ports
        if num > len(available_ports):
            raise AssertionError('Cannot launch %s new processes: Ports available: %s' % (num, str(available_ports)))
        if ports:
            for port in ports:
                if port not in available_ports:
                    raise AssertionError('Cannot launch Redis process on port %s: Already running' % port)
        else:
            ports = available_ports[:num]
 
        def do_launch_processes(op):
            result = self._launch(ports, passwords, op)
            return dict(ports=result[0], passwords=result[1])
 
        return self._op_api.run('Launch Redis processes', do_launch_processes, async=async)
 
    @rpc.command_method
    def shutdown_processes(self, ports, remove_data=False, async=False):
        def do_shutdown_processes(op):
            return self._shutdown(ports, remove_data)
        return self._op_api.run('Shutdown Redis processes', do_shutdown_processes, async=async)
 
    @rpc.query_method
    def list_processes(self):
        return self.get_running_processes()
 
    def _launch(self, ports=None, passwords=None, op=None):
        log = op.logger if op else LOG
        ports = ports or []
        passwords = passwords or []
        log.debug('Launching redis processes on ports %s with passwords %s', ports, passwords)
 
        primary_ip = self.get_primary_ip()
        assert primary_ip is not None
 
        new_passwords = []
        new_ports = []
 
        for port, password in zip(ports, passwords or [None for port in ports]):
            log.info('Launch Redis %s on port %s', 
                'Master' if __redis__["replication_master"] else 'Slave', port)
 
            if iptables.enabled():
                iptables.FIREWALL.ensure({
                    "jump": "ACCEPT", "protocol": "tcp", "match": "tcp", "dport": port
                })
 
            redis_service.create_redis_conf_copy(port)
            redis_process = redis_service.Redis(port, password)
 
            if not redis_process.service.running:
                if __redis__["replication_master"]:
                    current_password = redis_process.init_master(STORAGE_PATH)
                else:
                    current_password = redis_process.init_slave(STORAGE_PATH, primary_ip, port)
                new_passwords.append(current_password)
                new_ports.append(port)
                log.debug('Redis process has been launched on port %s with password %s' % (port, current_password))
 
            else:
                raise BaseException('Cannot launch redis on port %s: the process is already running' % port)
 
        return new_ports, new_passwords
 
    def _shutdown(self, ports, remove_data=False, op=None):
        log = op.logger if op else LOG
        freed_ports = []
        for port in ports:
            log.info('Shutdown Redis %s on port %s' % (
                'Master' if __redis__["replication_master"] else 'Slave', port))
 
            instance = redis_service.Redis(port=port)
            if instance.service.running:
                password = instance.redis_conf.requirepass
                instance.password = password
                log.debug('Dumping redis data on disk using password %s from config file %s' % (
                    password, instance.redis_conf.path))
                instance.redis_cli.save()
                log.debug('Stopping the process')
                instance.service.stop()
                freed_ports.append(port)
            if remove_data and instance.db_path and os.path.exists(instance.db_path):
                os.remove(instance.db_path)
 
        return dict(ports=freed_ports)
 
    @property
    def busy_ports(self):
        return redis_service.get_busy_ports()
 
    @property
    def available_ports(self):
        return redis_service.get_available_ports()
 
    def get_running_processes(self):
        processes = {}
        ports = []
        passwords = []
        for port in self.busy_ports:
            conf_path = redis_service.get_redis_conf_path(port)
 
            if port == redis_service.__redis__['defaults']['port']:
                args = ('ps', '-G', 'redis', '-o', 'command', '--no-headers')
                out = system2(args, silent=True)[0].split('\n')
                default_path = __redis__['defaults']['redis.conf']
                try:
                    p = [x for x in out if x and __redis__['redis-server'] in x and default_path in x]
                except PopenError:
                    p = []
                if p:
                    conf_path = __redis__['defaults']['redis.conf']
 
            LOG.debug('Got config path %s for port %s', conf_path, port)
            redis_conf = redis_service.RedisConf(conf_path)
            password = redis_conf.requirepass
            processes[port] = password
            ports.append(port)
            passwords.append(password)
            LOG.debug('Redis config %s has password %s', conf_path, password)
        return dict(ports=ports, passwords=passwords)
 
    @property
    def persistence_type(self):
        return __redis__["persistence_type"]
 
    def get_primary_ip(self):
        master_host = None
        LOG.info("Requesting master server")
        while not master_host:
            try:
                master_host = list(
                    host for host in self._queryenv.list_roles(behaviour=BEHAVIOUR)[0].hosts
                    if host.replication_master)[0]
            except IndexError:
                LOG.debug("QueryEnv responded with no %s master. " % BEHAVIOUR +
                    "Waiting %d seconds before the next attempt" % 5)
                time.sleep(5)
        host = master_host.internal_ip or master_host.external_ip
        LOG.debug('primary IP: %s', host)
        return host
 
    @rpc.command_method
    def reset_password(self, port=__redis__['defaults']['port'], new_password=None):
        """ Reset auth for Redis process on port `port`. Return new password """
        if not new_password:
            new_password = pwgen(20)
 
        redis_conf = redis_service.RedisConf.find(port=port)
        redis_conf.requirepass = new_password
 
        if redis_conf.slaveof:
            redis_conf.masterauth = new_password
 
        redis_wrapper = redis_service.Redis(port=port, password=new_password)
        redis_wrapper.service.reload()
 
        if int(port) == __redis__['defaults']['port']:
            __redis__["master_password"] = new_password
 
        return new_password
 
    @rpc.query_method
    def replication_status(self):
        ri = redis_service.RedisInstances()
 
        if __redis__["replication_master"]:
            masters = {}
            for port in ri.ports:
                masters[port] = {'status': 'up'}
            return {'masters': masters}
 
        slaves = {}
        for redis_process in ri.instances:
            repl_data = {}
            for key, val in redis_process.redis_cli.info.items():
                if key.startswith('master'):
                    repl_data[key] = val
            if 'master_link_status' in repl_data:
                repl_data['status'] = repl_data['master_link_status']
            slaves[redis_process.port] = repl_data
 
        return {'slaves': slaves}
 
    @rpc.command_method
    def create_databundle(self, async=True):
 
        def do_databundle(op):
            try:
                bus.fire('before_%s_data_bundle' % BEHAVIOUR)
                # Creating snapshot
                LOG.info("Creating Redis data bundle")
                backup_obj = backup.backup(type='snap_redis',
                                           volume=__redis__['volume'],
                                           tags=__redis__['volume'].tags)  # TODO: generate the same way as in
                                                                           # mysql api or use __node__
                restore = backup_obj.run()
                snap = restore.snapshot
 
                used_size = int(system2(('df', '-P', '--block-size=M', STORAGE_PATH))[0].split('\n')[1].split()[2][:-1])
                bus.fire('%s_data_bundle' % BEHAVIOUR, snapshot_id=snap.id)
 
                # Notify scalr
                msg_data = dict(
                    db_type=BEHAVIOUR,
                    used_size='%.3f' % (float(used_size) / 1000,),
                    status='ok'
                )
                msg_data[BEHAVIOUR] = {'snapshot_config': dict(snap)}
 
                node.__node__.messaging.send(DbMsrMessages.DBMSR_CREATE_DATA_BUNDLE_RESULT, msg_data)
 
                return restore
 
            except (Exception, BaseException), e:
                LOG.exception(e)
 
                # Notify Scalr about error
                node.__node__.messaging.send(DbMsrMessages.DBMSR_CREATE_DATA_BUNDLE_RESULT, dict(
                    db_type=BEHAVIOUR,
                    status='error',
                    last_error=str(e)))
 
        return self._op_api.run('redis.create-databundle', 
                                func=do_databundle,
                                func_kwds={},
                                async=async,
                                exclusive=True)  #?
 
    @rpc.command_method
    def create_backup(self, async=True):
 
        def do_backup(op):
            try:
                self.redis_instances.save_all()
                dbs = [r.db_path for r in self.redis_instances if r.db_path]
 
                cloud_storage_path = bus.platform.scalrfs.backups(BEHAVIOUR)  #? __node__.platform
                LOG.info("Uploading backup to cloud storage (%s)", cloud_storage_path)
                transfer = LargeTransfer(dbs, cloud_storage_path)
                result = transfer.run()
                result = transfer_result_to_backup_result(result)
 
                node.__node__.messaging.send(DbMsrMessages.DBMSR_CREATE_BACKUP_RESULT, dict(
                    db_type=BEHAVIOUR,
                    status='ok',
                    backup_parts=result))
 
                return result  #?
 
            except (Exception, BaseException), e:
                LOG.exception(e)
 
                # Notify Scalr about error
                node.__node__.messaging.send(DbMsrMessages.DBMSR_CREATE_BACKUP_RESULT, dict(
                    db_type=BEHAVIOUR,
                    status='error',
                    last_error=str(e)))
 
        return self._op_api.run('redis.create-backup', 
                                func=do_backup,
                                func_kwds={},
                                async=async,
                                exclusive=True)  #?
 
