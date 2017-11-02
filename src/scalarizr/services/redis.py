from __future__ import with_statement
'''
Created on Aug 10, 2011

@author: Dmytro Korsakov
'''

import os
import signal
import logging
import shutil

from scalarizr import storage2, node
from scalarizr.util import initdv2, system2, PopenError, wait_until, Singleton
from scalarizr.services import backup
from scalarizr.services import lazy, BaseConfig, BaseService, ServiceError, PresetProvider
from scalarizr.util import cryptotool, firstmatched
from scalarizr import linux
from scalarizr.linux.coreutils import chown_r
from scalarizr.libs.metaconf import NoPathError


__redis__ = node.__node__['redis']
__redis__.update({
    'storage_dir': '/mnt/redisstorage',
    'redis-cli': '/usr/bin/redis-cli',
    'pid_dir': '/var/run/redis' if os.path.isdir('/var/run/redis') else '/var/run',
    'defaults': {
        'dir': '/var/lib/redis',
        'port': 6379,
        'user': 'redis'
    },
    'su': '/bin/su',
    'bash': '/bin/bash',
    'db_filename': 'dump.rdb',
    'aof_filename': 'appendonly.aof',
    'preset_filename': 'redis.conf'
})
if linux.os.debian_family:
    __redis__.update({
        'redis-server': '/usr/bin/redis-server',
        'pid_file': os.path.join(__redis__['pid_dir'], 'redis-server.pid')
    })
    __redis__['defaults'].update({
        'redis.conf': '/etc/redis/redis.conf',
    })
else:
    # [SCALARIZR-1776]
    redis_bin_path = firstmatched(os.path.exists, ('/usr/sbin/redis-server', '/usr/bin/redis-server'))

    __redis__.update({
        'redis-server': redis_bin_path,
        'pid_file': os.path.join(__redis__['pid_dir'], 'redis.pid')
    })
    __redis__['defaults'].update({
        'redis.conf': '/etc/redis.conf',
    })

__redis__.update({
    'config_dir': os.path.dirname(__redis__['defaults']['redis.conf']),
    'ports_range': range(__redis__['defaults']['port'], 
                         __redis__['defaults']['port'] + 16)
})


SERVICE_NAME = 'redis'
LOG = logging.getLogger(__name__)


class RedisInitScript(initdv2.ParametrizedInitScript):
    socket_file = None

    @lazy
    def __new__(cls, *args, **kws):
        obj = super(RedisInitScript, cls).__new__(cls, *args, **kws)
        cls.__init__(obj)
        return obj

    def __init__(self):
        if linux.os.ubuntu and linux.os['version'] >= (10, 4):
            initd_script = ('/usr/sbin/service', 'redis-server')
        else:
            initd_script = firstmatched(os.path.exists, ('/etc/init.d/redis', '/etc/init.d/redis-server'))
        initdv2.ParametrizedInitScript.__init__(self, name=SERVICE_NAME,
                initd_script=initd_script)

    @property
    def _processes(self):
        return [p for p in get_redis_processes() if p == __redis__['defaults']['redis.conf']]

    def status(self):
        return initdv2.Status.RUNNING if self._processes else initdv2.Status.NOT_RUNNING

    def stop(self, reason=None):
        initdv2.ParametrizedInitScript.stop(self)
        wait_until(lambda: not self._processes, timeout=10, sleep=1)

    def restart(self, reason=None):
        self.stop()
        self.start()

    def reload(self, reason=None):
        initdv2.ParametrizedInitScript.restart(self)

    def start(self):
        initdv2.ParametrizedInitScript.start(self)
        wait_until(lambda: self._processes, timeout=10, sleep=1)
        """
        redis_conf = RedisConf.find()
        password = redis_conf.requirepass
        cli = RedisCLI(password)
        wait_until(lambda: cli.test_connection(), timeout=10, sleep=1)
        """


class Redisd(object):

    config_path = None
    redis_conf = None
    port = None
    cli = None

    name = 'redis-server'

    def __init__(self, config_path=None, port=None):
        self.config_path = config_path
        self.port = port or self.redis_conf.port

    @property
    def redis_conf(self):
        return RedisConf(self.config_path)

    @property
    def cli(self):
        return RedisCLI(self.redis_conf.requirepass, self.port)

    @classmethod
    def find(cls, config_obj=None, port=None):
        return cls(config_obj.path, port)

    def start(self):
        try:
            if not self.running:

                #TODO: think about moving this code elsewhere
                if self.port == __redis__['defaults']['port']:
                    base_dir = self.redis_conf.dir
                    snap_src = os.path.join(base_dir, __redis__['db_filename'])
                    snap_dst = os.path.join(base_dir, get_snap_db_filename(__redis__['defaults']['port']))
                    if os.path.exists(snap_src) and not os.path.exists(snap_dst):
                        shutil.move(snap_src, snap_dst)
                        if 'snapshotting' == __redis__["persistence_type"]:
                            self.redis_conf.dbfilename = snap_dst
                    aof_src = os.path.join(base_dir, __redis__['aof_filename'])
                    aof_dst = os.path.join(base_dir, get_aof_db_filename(__redis__['defaults']['port']))
                    if os.path.exists(aof_src) and not os.path.exists(aof_dst):
                        shutil.move(aof_src, aof_dst)
                        if 'aof' == __redis__["persistence_type"]:
                            self.redis_conf.appendfilename = aof_dst

                LOG.debug('Starting %s on port %s' % (__redis__['redis-server'], self.port))
                system2('%s %s -s %s -c "%s %s"' % (
                    __redis__['su'], 
                    __redis__['defaults']['user'], 
                    __redis__['bash'], 
                    __redis__['redis-server'], 
                    self.config_path), shell=True, close_fds=True, preexec_fn=os.setsid)
                wait_until(lambda: self.running)
                #wait_until(lambda: self.cli.test_connection())
                LOG.debug('%s process has been started.' % SERVICE_NAME)

        except PopenError, e:
            LOG.error('Unable to start redis process: %s' % e)
            raise initdv2.InitdError(e)

    def stop(self, reason=None):
        if self.running:
            if self.pid:
                LOG.debug("Waiting until redis service is ready to shut down")
                wait_until(lambda: not self._is_aof_rewrite_running)  # http://redis.io/commands/shutdown
                LOG.info('Stopping redis server on port %s (pid %s). Reason: %s' % (self.port, self.pid, reason))
                os.kill(int(self.pid), signal.SIGTERM)
                #self.cli.execute("SHUTDOWN SAVE")
                wait_until(lambda: not self.running)
                LOG.debug("Redis process terminated.")
            else:
                #XXX: rare case when process is alive but scalarizr is unable to get PID
                raise ServiceError("Cannot stop redis process: PID file not found.")

    def restart(self, reason=None, force=True):
        #force parameter is needed
        #for compatibility with lazyInitScript
        if self.running:
            self.stop()
        self.start()

    def reload(self, reason=None):
        self.restart()

    @property
    def running(self):
        for config_path in get_redis_processes():
            is_default_conf = config_path == __redis__['defaults']['redis.conf']
            is_default_port = int(self.port) == __redis__['defaults']['port']
            if (config_path == self.config_path) or (is_default_conf and is_default_port):
                return True
        return False

    @property
    def _is_aof_rewrite_running(self):
        """
        Redis 2.8 seems to be unable to shutdown when it has child processes running
        """
        try:
            out = system2(('ps', '-G', 'redis', '-o', 'command', '--no-headers'))[0]
        except:
            out = ''
        if out:
            for line in out.split('\n'):
                if "redis-aof-rewrite" in line and str(self.port) in line:
                    return True
        return False

    @property
    def pid(self):
        pid = None
        pidfile = self.redis_conf.pidfile
        LOG.debug('Got pidfile %s from redis config %s' % (pidfile, self.redis_conf.path))

        if pidfile == get_pidfile(__redis__['defaults']['port']):
            if not os.path.exists(pidfile) or not open(pidfile).read().strip():
                LOG.debug('Pidfile is empty. Trying default pidfile %s' % __redis__['pid_file'])
                pidfile = __redis__['pid_file']

        if os.path.exists(pidfile):
            pid = open(pidfile).read().strip()
        else:
            LOG.debug('No pid found in pidfile %s' % pidfile)
        LOG.debug('Redis process on port %s has pidfile %s and pid %s' % (self.port, pidfile, pid))
        return pid


class RedisInstances(object):

    __metaclass__ = Singleton

    instances = None

    def __init__(self):
        self.instances = []

    @property
    def ports(self):
        return [instance.port for instance in self.instances]

    @property
    def passwords(self):
        return [instance.password for instance in self.instances]

    def __iter__(self):
        return iter(self.instances)

    def get_processes(self):
        return [instance.service for instance in self.instances]

    def get_config_files(self):
        return [instance.redis_conf.path for instance in self.instances]

    def get_default_process(self):
        return self.get_instance(port=__redis__['defaults']['port']).service

    def get_instance(self, port=None):
        for instance in self.instances:
            if instance.port == port:
                return instance
        raise ServiceError('Redis instance with port %s not found' % port)

    def init_processes(self, num, ports=None, passwords=None):
        ports = ports or []
        passwords = passwords or []
        if not __redis__["use_password"]:
            # Ignoring passwords from HostInitResponse if use_password=0
            passwords = [None for password in passwords]
        if len(ports) < num:
            diff = num-len(ports)
            LOG.debug("Passed ports: %s. Need to find %s more." % (str(ports), diff))
            additional_ports = [port for port in get_available_ports() if port not in ports]
            if len(additional_ports) < diff:
                raise ServiceError('Not enough free ports')

            LOG.debug("Found available ports: %s" % str(additional_ports))
            ports += additional_ports[:diff]

        if len(passwords) < len(ports):
            diff = len(ports) - len(passwords)
            if __redis__["use_password"]:
                LOG.debug("Generating %s additional passwords for ports %s" % (diff, ports[-diff:]))
                additional_passwords= [cryptotool.pwgen(20) for port in ports[-diff:]]
                LOG.debug("Generated passwords: %s" % str(additional_passwords))
                passwords += additional_passwords
            else:
                LOG.debug("Setting  %s additional empty passwords for ports %s" % (diff, ports[-diff:]))
                passwords += [None for port in ports[-diff:]]

        assert len(ports) == len(passwords)

        creds = dict(zip(ports, passwords))
        LOG.debug("Initializing redis processes: %s" % str(creds))
        for port, password in creds.items():
            if port not in self.ports:
                create_redis_conf_copy(port)
                redis_process = Redis(port, password)
                self.instances.append(redis_process)
        LOG.debug('Total of redis processes: %d' % len(self.instances))

    def kill_processes(self, ports=[], remove_data=False):
        for instance in self.instances:
            if instance.port in ports:
                instance.service.stop()
                if remove_data and instance.db_path and os.path.exists(instance.db_path):
                    os.remove(instance.db_path)
                self.instances.remove(instance)

    def start(self):
        for redis in self.instances:
            redis.service.start()

    def stop(self, reason = None):
        for redis in self.instances:
            redis.service.stop(reason)

    def restart(self, reason = None):
        for redis in self.instances:
            redis.service.restart(reason)

    def reload(self, reason = None):
        for redis in self.instances:
            redis.service.reload(reason)

    def save_all(self):
        for redis in self.instances:
            if redis.service.running:
                redis.redis_cli.save()

    def init_as_masters(self, mpoint):
        passwords = []
        ports = []
        for redis in self.instances:
            redis.init_master(mpoint)
            passwords.append(redis.password)
            ports.append(redis.port)
        return ports, passwords

    def init_as_slaves(self, mpoint, primary_ip):
        passwords = []
        ports = []
        for redis in self.instances:
            passwords.append(redis.password)
            ports.append(redis.port)
            redis.init_slave(mpoint, primary_ip, redis.port)
        return ports, passwords

    def wait_for_sync(self, link_timeout=None, sync_timeout=None):
        #consider using threads
        for redis in self.instances:
            redis.wait_for_sync(link_timeout, sync_timeout)


class Redis(BaseService):

    _instance = None
    port = None
    password = None

    def __init__(self, port=__redis__['defaults']['port'], password=None):
        self._objects = {}
        self.port = port
        self.password = password

    def init_master(self, mpoint):
        self.service.stop('Configuring master. Moving Redis db files')
        self.init_service(mpoint)
        self.redis_conf.masterauth = None
        self.redis_conf.slaveof = None
        self.service.start()
        return self.current_password

    def init_slave(self, mpoint, primary_ip, primary_port):
        self.service.stop('Configuring slave')
        self.init_service(mpoint)
        self.change_primary(primary_ip, primary_port)
        self.service.start()
        return self.current_password

    def wait_for_sync(self,link_timeout=None,sync_timeout=None):
        LOG.info('Waiting for link with master')
        wait_until(lambda: self.redis_cli.master_link_status == 'up', sleep=3, timeout=link_timeout)
        LOG.info('Waiting for sync with master to complete')
        wait_until(lambda: not self.redis_cli.master_sync_in_progress, sleep=10, timeout=sync_timeout)
        LOG.info('Sync with master completed')

    def change_primary(self, primary_ip, primary_port):
        """
        Currently redis slaves cannot use existing data to catch up with master
        Instead they create another db file while performing full sync
        Wchich may potentially cause free space problem on redis storage
        And broke whole initializing process.
        So scalarizr removes all existing data on initializing slave
        to free as much storage space as possible.
        """
        aof_fname = self.redis_conf.appendfilename
        rdb_fname = self.redis_conf.dbfilename
        for fname in os.listdir(__redis__['storage_dir']):
            if fname in (aof_fname, rdb_fname):
                path = os.path.join(__redis__['storage_dir'], fname)
                os.remove(path)
                LOG.info("Old db file removed: %s" % path)

        self.redis_conf.masterauth = self.password
        self.redis_conf.slaveof = (primary_ip, primary_port)

    def init_service(self, mpoint):
        if not os.path.exists(mpoint):
            os.makedirs(mpoint)
            LOG.debug('Created directory structure for redis db files: %s' % mpoint)

        chown_r(mpoint, __redis__['defaults']['user'])

        self.redis_conf.requirepass = self.password
        self.redis_conf.daemonize = True
        self.redis_conf.dir = mpoint
        self.redis_conf.bind = None
        self.redis_conf.port = self.port
        self.redis_conf.pidfile = get_pidfile(self.port)

        persistence_type = __redis__["persistence_type"]
        if persistence_type == 'snapshotting':
            self.redis_conf.appendonly = False
            self.redis_conf.dbfilename = get_snap_db_filename(self.port)
            self.redis_conf.appendfilename = None
        elif persistence_type == 'aof':
            aof_path = get_aof_db_filename(self.port)
            self.redis_conf.appendonly = True
            self.redis_conf.appendfilename = aof_path
            self.redis_conf.dbfilename = None
            self.redis_conf.save = {}
        elif persistence_type == 'nopersistence':
            self.redis_conf.dbfilename = get_snap_db_filename(self.port)
            self.redis_conf.appendonly = False
            self.redis_conf.appendfsync = 'no'
            self.redis_conf.save = {}
            assert not self.redis_conf.save
        LOG.debug('Persistence type is set to %s' % persistence_type)

    @property
    def current_password(self):
        return self.redis_conf.requirepass

    @property
    def db_path(self):
        if __redis__["persistence_type"] in ('snapshotting', 'nopersistence'):
            return os.path.join(self.redis_conf.dir, self.redis_conf.dbfilename)
        elif 'aof' == __redis__["persistence_type"]:
            return os.path.join(self.redis_conf.dir, self.redis_conf.appendfilename)

    def _get_redis_conf(self):
        return self._get('redis_conf', RedisConf.find, __redis__['config_dir'], self.port)

    def _set_redis_conf(self, obj):
        self._set('redis_conf', obj)

    def _get_redis_cli(self):
        return self._get('redis_cli', RedisCLI.find, self.redis_conf)

    def _set_redis_cli(self, obj):
        self._set('redis_cli', obj)

    def _get_service(self):
        return self._get('service', Redisd.find, self.redis_conf, self.port)

    def _set_service(self, obj):
        self._set('service', obj)

    service = property(_get_service, _set_service)
    redis_conf = property(_get_redis_conf, _set_redis_conf)
    redis_cli = property(_get_redis_cli, _set_redis_cli)


class BaseRedisConfig(BaseConfig):

    config_type = 'redis'

    def set(self, option, value, append=False):
        self._init_configuration()
        if value:
            if append:
                self.data.add(option, str(value))
                LOG.debug('Option "%s" added to %s with value "%s"' % (option, self.path, str(value)))
            else:
                self.data.set(option, str(value), force=True)
                LOG.debug('Option "%s" set to "%s" in %s' % (option, str(value), self.path))
        else:
            self.data.comment(option)
            LOG.debug('Option "%s" commented in %s' % (option, self.path))
        self._cleanup(True)


    def set_sequential_option(self, option, seq):
        is_typle = type(seq) is tuple
        try:
            assert seq is None or is_typle
        except AssertionError:
            raise ValueError('%s must be a sequence (got %s instead)' % (option, seq))
        self.set(option, ' '.join(map(str,seq)) if is_typle else None)


    def get_sequential_option(self, option):
        raw = self.get(option)
        return raw.split() if raw else ()


    def get_list(self, option):
        self._init_configuration()
        try:
            value = self.data.get_list(option)
        except NoPathError:
            try:
                value = getattr(self, option+'_default')
            except AttributeError:
                value = ()
        self._cleanup(False)
        return value


    def get_dict_option(self, option):
        raw = self.get_list(option)
        d = {}
        for raw_value in raw:
            k,v = raw_value.split()
            if k and v:
                d[k] = v
        return d


    def set_dict_option(self, option, d):
        try:
            assert d is None or type(d)==dict
            #cleaning up
            #TODO: make clean process smarter using indexes
            for i in self.get_list(option):
                self.set(option+'[0]', None)

            #adding multiple entries
            for k,v in d.items():
                val = ' '.join(map(str,'%s %s'%(k,v)))
                self.set(option, val, append=True)
        except ValueError:
            raise ValueError('%s must be a sequence (got %s instead)' % (option, d))


class RedisConf(BaseRedisConfig):

    config_name = 'redis.conf'

    @classmethod
    def find(cls, config_dir=None, port=__redis__['defaults']['port']):
        conf_name = get_redis_conf_basename(port)
        conf_path = os.path.join(__redis__['config_dir'], conf_name)
        return cls(os.path.join(config_dir, conf_name) if config_dir else conf_path)


    def _get_dir(self):
        return self.get('dir')


    def _set_dir(self, path):
        self.set_path_type_option('dir', path)


    def _get_bind(self):
        return self.get_sequential_option('bind')


    def _set_bind(self, list_ips):
        self.set_sequential_option('bind', list_ips)


    def _get_slaveof(self):
        return self.get_sequential_option('slaveof')


    def _set_slaveof(self, conn_data):
        '''
        @tuple conndata: (ip,) or (ip, port)
        '''
        self.set_sequential_option('slaveof', conn_data)


    def _get_masterauth(self):
        return self.get('masterauth')


    def _set_masterauth(self, passwd):
        self.set('masterauth', passwd)


    def _get_requirepass(self):
        return self.get('requirepass')


    def _set_requirepass(self, passwd):
        self.set('requirepass', passwd)


    def _get_appendonly(self):
        return True if self.get('appendonly') == 'yes' else False


    def _set_appendonly(self, on):
        assert on == True or on == False
        self.set('appendonly', 'yes' if on else 'no')


    def _get_dbfilename(self):
        return self.get('dbfilename')


    def _set_dbfilename(self, fname):
        self.set('dbfilename', fname)


    def _get_save(self):
        return self.get_dict_option('save')


    def _set_save(self, save_dict):
        self.set_dict_option('save', save_dict)


    def _get_pidfile(self):
        return self.get('pidfile')


    def _set_pidfile(self, pid):
        self.set('pidfile', pid)


    def _get_port(self):
        return self.get('port')


    def _set_port(self, number):
        self.set('port', number)


    def _get_logfile(self):
        return self.get('logfile')


    def _set_logfile(self, path):
        self.set('logfile', path)


    def _get_appendfilename(self):
        return self.get('appendfilename')


    def _set_appendfilename(self, path):
        self.set('appendfilename', path)


    def _get_daemonize(self):
        return self.get('daemonize')


    def _set_daemonize(self, yes=True):
        self.set('daemonize', 'yes' if yes else 'no')


    def _get_appendfsync(self):
        return self.get('appendfsync')


    def _set_appendfsync(self, value):
        self.set('appendfsync', value)


    daemonize = property(_get_daemonize, _set_daemonize)
    appendfilename = property(_get_appendfilename, _set_appendfilename)
    pidfile = property(_get_pidfile, _set_pidfile)
    port = property(_get_port, _set_port)
    logfile = property(_get_logfile, _set_logfile)
    dir = property(_get_dir, _set_dir)
    save = property(_get_save, _set_save)
    bind = property(_get_bind, _set_bind)
    slaveof = property(_get_slaveof, _set_slaveof)
    masterauth = property(_get_masterauth, _set_masterauth)
    requirepass = property(_get_requirepass, _set_requirepass)
    appendonly = property(_get_appendonly, _set_appendonly)
    dbfilename = property(_get_dbfilename, _set_dbfilename)
    appendfsync = property(_get_appendfsync, _set_appendfsync)
    #dbfilename_default = __redis__['db_filename']
    #appendfilename_default = __redis__['aof_filename']
    port_default = __redis__['defaults']['port']


class RedisCLI(object):

    port = None
    password = None
    path = __redis__['redis-cli']


    class no_keyerror_dict(dict):
        def __getitem__(self, key):
            try:
                return dict.__getitem__(self, key)
            except KeyError:
                return None


    def __init__(self, password=None, port=__redis__['defaults']['port']):
        self.port = port
        self.password = password

        if not os.path.exists(self.path):
            raise OSError('redis-cli not found')


    @classmethod
    def find(cls, redis_conf):
        return cls(redis_conf.requirepass, port=redis_conf.port)


    def execute(self, query, silent=False):
        if not self.password:
            full_query = query
        else:
            full_query = 'AUTH %s\n%s' % (self.password, query)

        execute_query = lambda: system2([self.path, '-p', self.port], stdin=full_query,silent=True, warn_stderr=False)
        try:
            out = execute_query()[0]

            #fix for redis 2.4 AUTH
            if 'Client sent AUTH, but no password is set' in out:
                execute_query = lambda: system2([self.path], stdin=query, silent=True)
                out = execute_query()[0]

            if "Redis is loading the dataset in memory" in out:
                #[SCALARIZR-1604]
                #test until service becomes available:
                wait_until(lambda: "LOADING" not in system2([self.path], stdin='ping', silent=True)[0])
                #run query again:
                out = execute_query()[0]

            elif out.startswith('ERR'):
                raise PopenError(out)

            elif out.startswith('OK\n'):
                out = out[3:]
            if out.endswith('\n'):
                out = out[:-1]
            return out
        except PopenError, e:
            if not silent:
                LOG.error('Unable to execute query %s with redis-cli: %s' % (query, e))
            raise


    @property
    def info(self):
        info = self.execute('info')
        LOG.debug('Redis INFO: %s' % info)
        d = self.no_keyerror_dict()
        if info:
            for i in info.strip().split('\n'):
                raw = i[:-1] if i.endswith('\r') else i
                if raw:
                    kv = raw.split(':')
                    if len(kv)==2:
                        key, val = kv
                        if key:
                            d[key] = val
        return d


    @property
    def aof_enabled(self):
        return True if self.info['aof_enabled']=='1' else False


    @property
    def bgrewriteaof_in_progress(self):
        return True if self.info['bgrewriteaof_in_progress']=='1' else False


    @property
    def bgsave_in_progress(self):
        return True if self.info['bgsave_in_progress']=='1' else False


    @property
    def changes_since_last_save(self):
        return int(self.info['changes_since_last_save'])


    @property
    def connected_slaves(self):
        return int(self.info['connected_slaves'])


    @property
    def last_save_time(self):
        return int(self.info['last_save_time'])


    @property
    def redis_version(self):
        return self.info['redis_version']


    @property
    def role(self):
        return self.info['role']


    @property
    def master_host(self):
        info = self.info
        if info['role']=='slave':
            return info['master_host']
        return None


    @property
    def master_port(self):
        info = self.info
        if info['role']=='slave':
            return int(info['master_port'])
        return None


    @property
    def master_link_status(self):
        info = self.info
        if info['role'] == 'slave':
            return info['master_link_status']
        return None


    def bgsave(self, wait_until_complete=True):
        if not self.bgsave_in_progress:
            self.execute('bgsave')
        if wait_until_complete:
            wait_until(lambda: not self.bgsave_in_progress, sleep=5, timeout=900)


    def bgrewriteaof(self, wait_until_complete=True):
        if not self.bgrewriteaof_in_progress:
            self.execute('bgrewriteaof')
        if wait_until_complete:
            wait_until(lambda: not self.bgrewriteaof_in_progress, sleep=5, timeout=900)


    def save(self):
        LOG.info('Flushing redis data to disk (cli on port %s)', self.port)
        self.bgrewriteaof() if self.aof_enabled else self.bgsave()


    @property
    def master_last_io_seconds_ago(self):
        info = self.info
        if info['role'] == 'slave':
            return int(info['master_last_io_seconds_ago'])
        return None


    @property
    def master_sync_in_progress(self):
        info = self.info
        if info['role'] == 'slave':
            return True if info['master_sync_in_progress']=='1' else False
        return False


class RedisPresetProvider(PresetProvider):

    __metaclass__ = Singleton

    def __init__(self):
        pass


    def get_preset(self, manifest):
        for provider in self.providers:
            if provider.service.port == __redis__['defaults']['port']:
                return provider.get_preset(manifest)


    def set_preset(self, settings, manifest):
        for provider in self.providers:
            for fname in settings:
                if fname == __redis__['preset_filename']:
                    provider.set_preset(settings, manifest)


    @property
    def providers(self):
        providers = []
        LOG.debug('Getting list of redis preset providers')
        for port in get_busy_ports():
            service = Redisd(get_redis_conf_path(port), int(port))
            config_mapping = {__redis__['preset_filename']: service.redis_conf}
            providers.append(PresetProvider(service, config_mapping))
        return providers


class RedisSnapBackup(backup.SnapBackup):
    def __init__(self, **kwds):
        super(RedisSnapBackup, self).__init__(**kwds)
        self.on(freeze=self.freeze)
        self._redis_instances = RedisInstances()


    def freeze(self, volume, state):
        system2('sync', shell=True)
        self._redis_instances.save_all()
        system2('sync', shell=True)


class RedisSnapRestore(backup.SnapRestore):
    def __init__(self, **kwds):
        super(RedisSnapRestore, self).__init__(**kwds)
        self.on(complete=self.complete)


    def complete(self, volume):
        vol = storage2.volume(volume)
        vol.mpoint = __redis__['storage_dir']
        vol.mount()

backup.backup_types['snap_redis'] = RedisSnapBackup
backup.restore_types['snap_redis'] = RedisSnapRestore


def get_snap_db_filename(port=__redis__['defaults']['port']):
    return 'dump.%s.rdb' % port

def get_aof_db_filename(port=__redis__['defaults']['port']):
    return 'appendonly.%s.aof' % port

def get_redis_conf_basename(port=__redis__['defaults']['port']):
    return 'redis.%s.conf' % port


def get_port(conf_path=__redis__['defaults']['redis.conf']):
    '''
    returns number from config filename
    e.g. 6380 from redis.6380.conf
    '''
    if conf_path == __redis__['defaults']['redis.conf']:
        return __redis__['defaults']['port']
    raw = conf_path.split('.')
    return int(raw[-2]) if len(raw) > 2 else None


def get_redis_conf_path(port=__redis__['defaults']['port']):
    return os.path.join(__redis__['config_dir'], get_redis_conf_basename(port))


def get_log_path(port=__redis__['defaults']['port']):
    return '/var/log/redis/redis-server.%s.log' % port


def get_pidfile(port=__redis__['defaults']['port']):

    pid_file = os.path.join(__redis__['pid_dir'], 'redis-server.%s.pid' % port)
    if not os.path.exists(pid_file):  # fix for ubuntu1004
        open(pid_file, 'w').close()
        chown_r(pid_file, 'redis')
    return pid_file


def create_redis_conf_copy(port=__redis__['defaults']['port']):
    if not os.path.exists(__redis__['defaults']['redis.conf']):
        raise ServiceError('Default redis config %s does not exist' % __redis__['defaults']['redis.conf'])
    dst = get_redis_conf_path(port)
    if not os.path.exists(dst):
        LOG.debug('Copying %s to %s.' % (__redis__['defaults']['redis.conf'],dst))
        shutil.copy(__redis__['defaults']['redis.conf'], dst)
        chown_r(dst, 'redis')
    else:
        LOG.debug('%s already exists.' % dst)


def get_redis_processes():
    config_files = list()
    try:
        out = system2(('ps', '-G', 'redis', '-o', 'command', '--no-headers'))[0]
    except:
        out = ''
    if out:
        for line in out.split('\n'):
            words = line.split()
            if len(words) == 2 and words[0] == __redis__['redis-server']:
                if words[1].startswith("*:"):  # XXX: 2.8 support
                    config_files.append(get_redis_conf_path(words[1][2:]))
                else:
                    config_files.append(words[1])
    return config_files


def get_busy_ports():
    busy_ports = []
    args = ('ps', '-G', 'redis', '-o', 'command', '--no-headers')
    try:
        out = system2(args, silent=True)[0].split('\n')
        p = [x for x in out if x and __redis__['redis-server'] in x]
    except PopenError,e:
        p = []
    LOG.debug('Running redis processes: %s' % p)
    for redis_process in p:
        for port in __redis__['ports_range']:
            conf_name = get_redis_conf_basename(port)
            if conf_name in redis_process or str(port) in redis_process:
                busy_ports.append(port)
                break
            elif __redis__['defaults']['port'] == port and __redis__['defaults']['redis.conf'] in redis_process:
                busy_ports.append(port)
    LOG.debug('busy_ports: %s' % busy_ports)
    return busy_ports


def get_available_ports():
    busy_ports = get_busy_ports()
    available = [port for port in __redis__['ports_range'] if port not in busy_ports]
    LOG.debug("Available ports: %s" % available)
    return available
