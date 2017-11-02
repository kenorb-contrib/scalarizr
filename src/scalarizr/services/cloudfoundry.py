from __future__ import with_statement
'''
Created on Aug 29, 2011

@author: marat
'''

import logging
import os
import subprocess
import time

from scalarizr import util


import yaml


LOG = logging.getLogger(__name__)


class CloudFoundryError(Exception):
    pass

class Base(object):
    def __init__(self, cf, name):
        self.cf = cf
        self.name = name
        self._ip_route = None

    def start(self):
        self.exec_vcap('start')


    def stop(self):
        self.exec_vcap('stop')


    def restart(self):
        self.exec_vcap('restart')


    def exec_vcap(self, action):
        cmd = ' '.join((self.cf.home + '/bin/vcap', action, self.name, '--no-color'))
        LOG.debug('Executing %s', cmd)
        proc = subprocess.Popen(('/bin/bash', '-c', 'source /usr/local/rvm/scripts/rvm; ' + cmd),
                                                close_fds=True,
                                                stdout=open('/dev/null', 'w'),
                                                stderr=subprocess.STDOUT)
        if proc.wait():
            LOG.warn('%s %s failed. returncode: %s', action, self.name, proc.returncode)
        else:
            LOG.debug('Finished')


    @property
    def running(self):
        if os.path.exists(self.pid_file):
            stat_file = '/proc/%s/stat' % self.pid
            if os.path.exists(stat_file):
                stat = open(stat_file).read()
                LOG.debug('Contents of %s:\n%s', stat_file, stat)
                return stat.split(' ')[2] != 'Z'
            else:
                LOG.debug('Component %s not running. File %s', self.name, stat_file)
        return False


    @property
    def pid_file(self):
        return self.config['pid']


    @property
    def pid(self):
        return open(self.pid_file).read().strip()


    @property
    def log_file(self):
        try:
            return self.config['log_file']
        except KeyError:
            return '/tmp/vcap-run/%s.log' % self.name


class Component(Base):

    def __init__(self, cf, name, config_file=None):
        Base.__init__(self, cf, name)
        self.config_file = config_file or os.path.join(self.cf.home,
                                                                                                name, 'config', name + '.yml')
        self.config = yaml.load(open(self.config_file))


    def _get_ip_route(self):
        return self._ip_route


    def _set_ip_route(self, ip):
        LOG.debug('Setting %s ip route: %s', self.name, ip)
        sed('local_route.*', 'local_route: ' + ip, self.config_file)
        self._ip_route = ip


    ip_route = property(_get_ip_route, _set_ip_route)


    def _get_allow_external_app_uris(self):
        return self.config['app_uris']['allow_external']

    def _set_allow_external_app_uris(self, b):
        LOG.debug('Setting %s app_uris/allow_external: %s', self.name, b)
        sed('  allow_external.*', '  allow_external: ' + str(b).lower(), self.config_file)


    allow_external_app_uris = property(_get_allow_external_app_uris, _set_allow_external_app_uris)


    @property
    def home(self):
        return os.path.join(self.cf.home, self.name)


class Service(Base):
    def __init__(self, cf, name, config_dir=None):
        Base.__init__(self, cf, name)
        self.config_dir = config_dir or os.path.join(self.cf.home, 'services', name, 'config')
        self.node_config_file = os.path.join(self.config_dir, '%s_node.yml' % name)
        self.gateway_config_file = os.path.join(self.config_dir, '%s_gateway.yml' % name)

        self.node_config = yaml.load(open(self.node_config_file))
        self.gateway_config = yaml.load(open(self.gateway_config_file))
        self.config_files = [self.node_config_file, self.gateway_config_file]


    def _get_ip_route(self):
        return self._ip_route


    def _set_ip_route(self, ip):
        LOG.debug('Setting %s ip route: %s', self.name, ip)
        for file in self.config_files:
            sed('ip_route.*', 'ip_route: ' + ip, file)
        self._ip_route = ip


    ip_route = property(_get_ip_route, _set_ip_route)


    def flush_node_config(self):
        fp = open(self.node_config_file)
        yaml.safe_dump(self.node_config, fp, default_flow_style=False)
        fp.close()



class CloudFoundry(object):

    def __init__(self, home):
        self.home = home
        self.components = {}
        for name in ('cloud_controller', 'router', 'health_manager', 'dea'):
            self.components[name] = Component(self, name)
        self.services = {}
        for name in ('mysql', 'postgresql', 'mongodb', 'redis', 'rabbit', 'neo4j'):
            self.services[name] = Service(self, name)

        self._mbus_url = None
        self._cloud_controller = None
        self._db_file = None


    def _set_mbus(self, url):
        LOG.debug('Changing mbus server: %s', url)
        find = subprocess.Popen(('find', self.home, '-name', '*.yml'), stdout=subprocess.PIPE)
        grep = subprocess.Popen(('xargs', 'grep', '--files-with-matches', 'mbus'), stdin=find.stdout, stdout=subprocess.PIPE)
        sed = subprocess.Popen(('xargs', 'sed', '--in-place', 's/mbus.*/mbus: %s/1' % url.replace('/', '\/')), stdin=grep.stdout)
        out, err = sed.communicate()
        if sed.returncode:
            raise util.PopenError('Failed to update mbus for all VCAP components', out, err, sed.returncode, None)
        self._mbus_url = url


    def _get_mbus(self, mbus):
        return self._mbus_url


    mbus = property(_get_mbus, _set_mbus)


    def _set_cloud_controller(self, host):
        LOG.debug('Setting cloud controller host: %s', host)
        self._cloud_controller = host
        self.mbus = 'nats://%s:4222/' % host


    def _get_cloud_controller(self):
        return self._cloud_controller


    cloud_controller = property(_get_cloud_controller, _set_cloud_controller)


    def start(self, *cmps):
        started = []
        for name in cmps:
            cmp = self.components[name]
            if not cmp.running:
                LOG.info('Starting %s', name)
                cmp.start()
                started.append(cmp)

        # Check 6 times that all requred services were started
        i, ntimes, sleep = 0, 6, 5
        logs = {}
        while i < ntimes:
            failed = []
            for cmp in started:
                if not cmp.running:
                    failed.append(cmp)
                    if os.path.exists(cmp.log_file):
                        logs[cmp] = open(cmp.log_file).read()
            i += 1
            if i < ntimes:
                time.sleep(sleep)

        if failed:
            for cmp in logs:
                LOG.warn('%s:\n%s', cmp.log_file, logs[cmp])
            raise CloudFoundryError('%d component(s) failed to start (%s)' % (
                                                            len(failed), ', '.join(cmp.name for cmp in failed)))
        LOG.debug('Started %d component(s)', len(started))


    def stop(self, *cmps):
        for name in cmps:
            cmp = self.components[name]
            LOG.info('Stopping %s', name)
            cmp.stop()


    def init_db(self):
        cmp = self.components['cloud_controller']
        dbenv = cmp.config['database_environment'][cmp.config['rails_environment']]
        if dbenv['adapter'] == 'sqlite3':
            if not os.path.exists(dbenv['database']):
                LOG.info("Cloud controller database doesn't exists. Creating")
                system('cd %s; rake db:migrate' % cmp.home)


    def valid_datadir(self, datadir):
        return os.path.exists(os.path.join(datadir, 'data'))


    def init_datadir(self, datadir):
        for name in ('dea', 'cloud_controller/db', 'cloud_controller/tmp'):
            dir = os.path.join(datadir, name)
            if not os.path.exists(dir):
                os.makedirs(dir)


def sed(search, replace, filename):
    util.system2(('sed', '--in-place',
                            's/%s/%s/1' % (search.replace('/', '\\/'), replace.replace('/', '\\/')),
                            filename))


def system(*args, **kwds):
    cmd = args[0]
    if not isinstance(cmd, basestring):
        cmd = ' '.join(cmd)
    return util.system2(('/bin/bash', '-c', 'source /usr/local/rvm/scripts/rvm; ' + cmd), **kwds)
