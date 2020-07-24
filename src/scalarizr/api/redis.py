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
from scalarizr import rpc, storage2
from scalarizr import linux
from scalarizr.linux import iptables
from scalarizr.api import operation
from scalarizr.util import system2, PopenError
from scalarizr.util import initdv2
from scalarizr.services import redis as redis_service
from scalarizr.services import backup
from scalarizr.handlers import transfer_result_to_backup_result, DbMsrMessages
from scalarizr.services.redis import __redis__
from scalarizr.util.cryptotool import pwgen
from scalarizr.storage2 import largetransfer
from scalarizr import node
from scalarizr.util import Singleton
from scalarizr.linux import pkgmgr
from scalarizr import exceptions
from scalarizr.api import BehaviorAPI


BEHAVIOUR = config.BuiltinBehaviours.REDIS
STORAGE_PATH = '/mnt/redisstorage'


LOG = logging.getLogger(__name__)


class RedisAPI(BehaviorAPI):
    """
    Basic API for managing Redis 2.x service.

    Namespace::

        redis
    """
    __metaclass__ = Singleton

    behavior = 'redis'

    _cnf = None
    _queryenv = None

    def __init__(self):
        self._op_api = operation.OperationAPI()
        self._queryenv = bus.queryenv_service
        self.redis_instances = redis_service.RedisInstances()

    @property
    def _role_name(self):
        self._cnf = bus.cnf
        ini = self._cnf.rawini
        return ini.get(config.SECT_GENERAL, config.OPT_ROLE_NAME)

    def _reinit_instances(self):
        proc = self.get_all_processes()
        ports, passwords = proc['ports'], proc['passwords']

        self.redis_instances.instances = []
        self.redis_instances.init_processes(len(ports), ports, passwords)

    def _get_redis_instances(self, ports=None, indexes=None):
        assert not (ports is not None and indexes is not None)
        self._reinit_instances()

        if ports is None and indexes is None:
            return self.redis_instances.instances

        if ports is not None:
            if not isinstance(ports, list):
                ports = [ports]
            return [self.redis_instances.get_instance(str(port)) for port in ports]

        if not isinstance(indexes, list):
            indexes = [indexes]
        return [self.redis_instances.instances[int(index)] for index in indexes]

    @rpc.command_method
    def start_service(self, ports=None, indexes=None):
        """
        ports and indexes parameters can be int or list of ints.
        """
        assert not (ports is not None and indexes is not None)
        instances = self._get_redis_instances(ports, indexes)
        for inst in instances:
            inst.service.start()

    @rpc.command_method
    def stop_service(self, ports=None, indexes=None, reason=None):
        """
        ports and indexes parameters can be int or list of ints.
        """
        assert not (ports is not None and indexes is not None)
        instances = self._get_redis_instances(ports, indexes)
        for inst in instances:
            inst.service.stop(reason)

    @rpc.command_method
    def reload_service(self, ports=None, indexes=None):
        """
        ports and indexes parameters can be int or list of ints.
        """
        assert not (ports is not None and indexes is not None)
        instances = self._get_redis_instances(ports, indexes)
        for inst in instances:
            inst.service.reload()

    @rpc.command_method
    def restart_service(self, ports=None, indexes=None):
        """
        ports and indexes parameters can be int or list of ints.
        """
        assert not (ports is not None and indexes is not None)
        instances = self._get_redis_instances(ports, indexes)
        for inst in instances:
            inst.service.restart()

    @rpc.command_method
    def get_service_status(self, ports=None, indexes=None):
        """Returns dict of processes ports as keys and their statuses as values"""
        assert not (ports is not None and indexes is not None)
        statuses = {}
        instances = self._get_redis_instances(ports, indexes)
        for inst in instances:
            status = initdv2.Status.NOT_RUNNING
            if inst.service.running:
                status = initdv2.Status.RUNNING
            statuses[inst.port] = status
        return statuses

    @rpc.command_method
    def launch_processes(self, num=None, ports=None, passwords=None, async=False):
        """
        Launches multiple redis processes
        """
        if ports and passwords and len(ports) != len(passwords):
            raise AssertionError('Number of ports must be equal to number of passwords')
        if num and ports and num != len(ports):
            raise AssertionError('When ports range is passed its length must be equal to num parameter')
        if not __redis__["replication_main"]:
            if not passwords or not ports:
                raise AssertionError('ports and passwords are required to launch processes on redis subordinate')
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
        """
        Stops multiple redis processes.
        """
        def do_shutdown_processes(op):
            return self._shutdown(ports, remove_data)
        return self._op_api.run('Shutdown Redis processes', do_shutdown_processes, async=async)

    @rpc.query_method
    def list_processes(self):
        """
        Returns information about all running redis processes.
        """
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
                'Main' if __redis__["replication_main"] else 'Subordinate', port)

            if iptables.enabled():
                iptables.FIREWALL.ensure({
                    "jump": "ACCEPT", "protocol": "tcp", "match": "tcp", "dport": port
                })

            redis_service.create_redis_conf_copy(port)
            redis_process = redis_service.Redis(port, password)

            if not redis_process.service.running:
                if __redis__["replication_main"]:
                    current_password = redis_process.init_main(STORAGE_PATH)
                else:
                    current_password = redis_process.init_subordinate(STORAGE_PATH, primary_ip, port)
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
                'Main' if __redis__["replication_main"] else 'Subordinate', port))

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

    def _get_conf(self, port):
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
        return redis_conf

    def _get_processes(self, ports):
        processes = {'ports': [], 'passwords': []}
        for port in ports:
            redis_conf = self._get_conf(port)
            password = redis_conf.requirepass
            processes['ports'].append(port)
            processes['passwords'].append(password)
            LOG.debug('Redis config %s has password %s', redis_conf, password)
        return processes

    def _get_redis_ports(self):
        conf_paths = os.listdir(os.path.dirname(__redis__['defaults']['redis.conf']))
        ports = [conf.split('.')[1] for conf in conf_paths if conf.startswith("redis") and conf.endswith(".conf")]
        return filter(lambda x: x.isdigit(), ports)

    def get_running_processes(self):
        return self._get_processes(self.busy_ports)

    def get_stopped_processes(self):
        running_ports = self.busy_ports
        all_ports = self._get_redis_ports()
        stopped_ports = [port for port in all_ports if port not in running_ports]
        return self._get_processes(stopped_ports)

    def get_all_processes(self):
        all_ports = self._get_redis_ports()
        return self._get_processes(all_ports)

    @property
    def persistence_type(self):
        return __redis__["persistence_type"]

    def get_primary_ip(self):
        main_host = None
        LOG.info("Requesting main server")
        while not main_host:
            try:
                main_host = list(
                    host for host in self._queryenv.list_roles(behaviour=BEHAVIOUR)[0].hosts
                    if host.replication_main)[0]
            except IndexError:
                LOG.debug("QueryEnv responded with no %s main. " % BEHAVIOUR +
                    "Waiting %d seconds before the next attempt" % 5)
                time.sleep(5)
        host = main_host.internal_ip or main_host.external_ip
        LOG.debug('primary IP: %s', host)
        return host

    @rpc.command_method
    def reset_password(self, port=__redis__['defaults']['port'], new_password=None):
        """ Reset auth for Redis process on port `port`. Return new password """
        if not new_password:
            new_password = pwgen(20)

        redis_conf = redis_service.RedisConf.find(port=port)
        redis_conf.requirepass = new_password

        if redis_conf.subordinateof:
            redis_conf.mainauth = new_password

        redis_wrapper = redis_service.Redis(port=port, password=new_password)
        redis_wrapper.service.reload()

        if int(port) == __redis__['defaults']['port']:
            __redis__["main_password"] = new_password

        return new_password

    @rpc.query_method
    def replication_status(self):
        """
        Checks current replication status.

        :return: Redis replication status.
        :rtype: dict
        """
        ri = redis_service.RedisInstances()

        if __redis__["replication_main"]:
            mains = {}
            for port in ri.ports:
                mains[port] = {'status': 'up'}
            return {'mains': mains}

        subordinates = {}
        for redis_process in ri.instances:
            repl_data = {}
            for key, val in redis_process.redis_cli.info.items():
                if key.startswith('main'):
                    repl_data[key] = val
            if 'main_link_status' in repl_data:
                repl_data['status'] = repl_data['main_link_status']
            subordinates[redis_process.port] = repl_data

        return {'subordinates': subordinates}

    @rpc.command_method
    def create_databundle(self, async=True):
        """
        Creates a new data bundle of /mnt/redis-storage.
        """

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
        """
        Creates a new backup of db files of all currently running redis processes
        and uploads gzipped data to the cloud storage.
        """
        def do_backup(op):
            try:
                self.redis_instances.save_all()
                dbs = [r.db_path for r in self.redis_instances if r.db_path]

                cloud_storage_path = bus.platform.scalrfs.backups(BEHAVIOUR)  #? __node__.platform
                LOG.info("Uploading backup to cloud storage (%s)", cloud_storage_path)

                def progress_cb(progress):
                    LOG.debug('Uploading %s bytes' % progress)

                uploader = largetransfer.Upload(dbs, cloud_storage_path,
                                                progress_cb=progress_cb)
                uploader.apply_async()
                uploader.join()
                manifest = uploader.manifest

                result = transfer_result_to_backup_result(manifest)

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

    @classmethod
    def do_check_software(cls, system_packages=None):
        os_name = linux.os['name'].lower()
        os_vers = linux.os['version']
        requirements = None
        if os_name == 'ubuntu':
            if os_vers >= '14':
                requirements = ['redis-server>=2.6,<2.9']
            elif os_vers >= '12':
                requirements = ['redis-server>=2.2,<2.9']
            elif os_vers >= '10':
                requirements = ['redis-server>=2.2,<2.3']
        elif os_name == 'debian':
            if os_vers >= '7':
                requirements = ['redis-server>=2.6,<2.9']
            elif os_vers >= '6':
                requirements = ['redis-server>=2.6,<2.7']
        elif linux.os.oracle_family or os_name == 'redhat' or os_name == 'centos':
            requirements = ['redis>=2.4,<2.9']
        elif os_name == 'amazon':
            if os_vers >= '2014':
                requirements = ['redis>=2.8,<2.9']
        if requirements is None:
            raise exceptions.UnsupportedBehavior(
                    cls.behavior,
                    "Not supported on {0} os family".format(linux.os['family']))
        return pkgmgr.check_software(requirements, system_packages)[0]

    @rpc.command_method
    def grow_volume(self, volume, growth, async=False):
        """
        Stops Redis service, Extends volume capacity and starts Redis service again.
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

            new_vol = api.redis.grow_volume(
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

        LOG.info("Attempting to grow volume '%s'. New size: %s" % (volume.get('id'), growth))

        def do_grow(op):
            vol = storage2.volume(volume)
            ports = self.busy_ports
            LOG.debug("Stopping Redis processes on ports %s before growing data volume." % str(ports))
            self.stop_service(ports=ports, reason='Growing data volume')
            LOG.debug("All redis processes stopped. Attempting to grow data volume.")
            try:
                growed_vol = vol.grow(**growth)
                redis_service.__redis__['volume'] = dict(growed_vol)
                return dict(growed_vol)
            finally:
                self.start_service(ports)
                LOG.info("Grow process: Redis service has been started on ports %s." % str(ports))

        return self._op_api.run('redis.grow-volume', do_grow, exclusive=True, async=async)

