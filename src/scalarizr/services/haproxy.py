
from augeas import Augeas
import cStringIO
import csv
import logging
import os
import re
import shutil
import signal
import socket
import string
import sys
from threading import local
import time
from textwrap import dedent

from scalarizr import util
from scalarizr.libs import metaconf
from scalarizr.node import __node__
from scalarizr.util import initdv2


BEHAVIOUR = 'haproxy'
SERVICE_NAME = 'haproxy'
HAPROXY_EXEC = '/usr/sbin/haproxy'
HAPROXY_CFG_PATH = '/etc/haproxy/haproxy.cfg'
HAPROXY_LENS_DIR = os.path.join(__node__['share_dir'], 'haproxy_lens')


LOG = logging.getLogger(__name__)


class HAProxyError(BaseException):
    pass


class HAProxyConfManager(object):
    conf_path = 'etc/haproxy/haproxy.cfg'

    def __init__(self, root_path=None):
        if root_path is None:
            root_path = '/'
        self.conf_path = root_path + self.conf_path
        self._conf_xpath = '/files' + self.conf_path + '/'
        self._augeas = Augeas(root=root_path, loadpath=HAPROXY_LENS_DIR)

    def load(self):
        LOG.debug('Loading haproxy.conf')
        self._augeas.load()

    def save(self):
        LOG.debug('Saving haproxy.conf')
        self._augeas.save()

    def get(self, xpath):
        """
        Returns values of label at given xpath. If label is presented but has no value,
        returns True
        xpath is relative to haproxy.cfg file
        """
        if self._augeas.match(self._conf_xpath + xpath) == []:
            return None
        value = self._augeas.get(self._conf_xpath + xpath)
        return value if value is not None else True

    def _find_xpath_gen(self, base_xpath, sublabel, value_pattern):
        section_xpaths = self._augeas.match(self._conf_xpath+base_xpath)
        for xpath in section_xpaths:
            match = re.search(value_pattern, self._augeas.get(xpath+'/'+sublabel))
            if match != None:
                yield xpath.replace(self._conf_xpath, '')

    def find_all_xpaths(self, base_xpath, sublabel, value_pattern):
        """
        Returns list of all labels from given base_xpath which sublabel
        matches `value_pattern`

        `value_pattern` is regexp

        This metod is useful when you need to find certain section from config
        that contains number of such sections (listen[1], listen[2]...)

        ..Example:
            cnf.get_matched_xpaths('listen', 'name', 'test*')
            this will find every listen whose name begins with 'test'
        """
        return list(self._find_xpath_gen(base_xpath, sublabel, value_pattern))

    def find_one_xpath(self, base_xpath, sublabel, value_pattern):
        """
        Returns label xpath by given value_pattern of sublabel. Returns None if no label is found.

        This metod is useful when you need to find certain section from config
        that contains number of such sections (listen[1], listen[2]...)
        """
        try:
            return self._find_xpath_gen(base_xpath, sublabel, value_pattern).next()
        except StopIteration:
            return None

    def get_all_xpaths(self, base_xpath):
        return [x.replace(self._conf_xpath, '')
            for x in self._augeas.match(self._conf_xpath+base_xpath)]

    def set(self, xpath, value=None, save_conf=True):
        """
        Sets label at given xpath with given value. If there is no label - creates one.
        If there is - updates its value.

        `value` can be None/True to set label without actual value
        or string
        or iterable
        or dict to set number of sublabels at once.

        `xpath` is relative to haproxy.cfg file.
        """
        LOG.debug('Setting %s with value: %s' % (xpath, value))
        if isinstance(value, dict):
            self._augeas.set(self._conf_xpath+xpath, None)
            for k, v in value.items():
                self.set(xpath+'/'+k, v, save_conf=False)
        elif hasattr(value, '__iter__'):
            for v in value:
                self.add(xpath, v, save_conf=False)
        else:
            if value == True:
                value = None
            elif value == False:
                return self.remove(xpath)
            elif value is not None:
                value = str(value)
            self._augeas.set(self._conf_xpath+xpath, value)
        if save_conf:
            self.save()

    def add(self, xpath, value=None, save_conf=True):
        """
        Adds node at given xpath. New nodes are appended
        xpath is relative to haproxy.cfg file
        Returns xpath of created node
        """
        nodes_qty = len(self._augeas.match(self._conf_xpath+xpath))

        if nodes_qty != 0:
            xpath += '[%s]' % (nodes_qty+1)
        self.set(xpath, value, save_conf)
        return xpath

    def insert(self, xpath, label, value=None, before=True):
        """
        Inserts label before or after given in xpath
        xpath is relative to haproxy.cfg file
        """
        xpath = self._conf_xpath + xpath
        self._augeas.insert(xpath, label, before)
        if value is not None:
            labels = self._augeas.match(os.path.dirname(xpath))
            base_label = os.path.basename(xpath)
            inserted_xpath = None
            if label == base_label:
                if xpath.endswith(']'):
                    inserted_xpath = xpath if before else labels[labels.index(xpath)+1]
                else:
                    inserted_xpath += '[1]' if before else '[2]'
            else:
                index = labels.index(xpath) + (-1 if before else 1)
                inserted_xpath = labels[index]

            self.set(inserted_xpath.replace(self._conf_xpath, ''), value)

    def add_conf(self, conf, append=False):
        """
        Append raw conf to the end of haproxy.conf file
        or insert at the beginning before everything
        """
        LOG.debug('Adding raw conf part to haproxy.conf:\n%s' % conf)
        raw = None
        with open(self.conf_path, 'r') as conf_file:
            raw = conf_file.read()
        if not conf.endswith('\n'):
            conf += '\n'
        raw = '\n'.join((raw, conf) if append else (conf, raw))
        with open(self.conf_path, 'w') as conf_file:
            conf_file.write(raw)
        self.load()

    def extend_section(self, conf, section_name):
        """
        Appends raw conf to the section with given name
        """
        LOG.debug('Adding raw conf part to section of haproxy.conf with name %s :\n%s' % 
            (section_name, conf))
        raw = None
        with open(self.conf_path, 'r') as conf_file:
            raw = conf_file.read()
        if conf.endswith('\n'):
            conf = conf[:-1]
        # reindenting conf
        conf = dedent(conf)
        conf = '    ' + conf.replace('\n', '\n    ')
        raw = re.sub(section_name+r'\s*\n', '%s\n%s\n'%(section_name, conf), raw)
        with open(self.conf_path, 'w') as conf_file:
            conf_file.write(raw)
        self.load()

    def remove(self, xpath):
        LOG.debug('Removing %s' % xpath)
        return self._augeas.remove(self._conf_xpath + xpath)


class StatSocket(object):
    '''
    haproxy unix socket API
    - one-to-one naming
    - connect -> request -> disconnect

    Create object:
    >> ss = StatSocket('/var/run/haproxy-stats.sock')

    Show stat:
    >> ss.show_stat()
    [{'status': 'UP', 'lastchg': '68', 'weight': '1', 'slim': '', 'pid': '1', 'rate_lim': '',
    'check_duration': '0', 'rate': '0', 'req_rate': '', 'check_status': 'L4OK', 'econ': '0',
    ...'''

    def __init__(self, address='/var/run/haproxy-stats.sock'):
        try:
            self.sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            self.sock.connect(address)
            self.adress = address
        except:
            raise Exception, "Couldn't connect to socket on address: %s%s" % (address, sys.exc_info()[1]), sys.exc_info()[2]


    def show_stat(self):
        '''
        @rtype: list[dict]
        '''
        try:
            self.sock.send('show stat\n')
            stat = self.sock.makefile('r').read()

            fieldnames = filter(None, stat[2:stat.index('\n')].split(','))
            reader = csv.DictReader(cStringIO.StringIO(stat[stat.index('\n'):]), fieldnames)
            res=[]
            for row in reader:
                res.append(row)
            return res
        except:
            raise Exception, "Error working with sockets. Details: %s" % sys.exc_info()[1],\
                    sys.exc_info()[2]


class HAProxyInitScript(initdv2.InitScript):
    '''
    haproxy init script
    - start
    - stop
    - restart
    - reload
    - status
    '''

    def __init__(self, path=None):
        self.pid_file = '/var/run/haproxy.pid'
        self.config_path = path or HAPROXY_CFG_PATH
        self.haproxy_exec = '/usr/sbin/haproxy'
        self.socks = None
        self.timeout = 30


    def start(self):
        if self.status() == 0:
            raise initdv2.InitdError("Cannot start HAProxy. It already running.")

        util.system2([self.haproxy_exec, '-f', self.config_path, '-p', self.pid_file, '-D'],)
        if self.pid_file:
            try:
                util.wait_until(lambda: os.path.exists(self.pid_file), timeout=self.timeout,
                                sleep=0.2, error_text="HAProxy pid file %s does'not exist"%
                                self.pid_file)
            except:
                err = "Cannot start HAProxy: pid file %s hasn't been created. " \
                        "Details: %s" % (self.pid_file, sys.exc_info()[1])
                raise initdv2.InitdError, err, sys.exc_info()[2]


    def stop(self):
        if os.path.exists(self.pid_file):
            try:
                pid = self.pid()
                if pid:
                    os.kill(pid, signal.SIGTERM)
                    util.wait_until(lambda: not os.path.exists('/proc/%s' % pid),
                            timeout=self.timeout, sleep=0.2, error_text="Can't stop HAProxy")
                    if os.path.exists('/proc/%s' % pid):
                        os.kill(pid, signal.SIGKILL)
            except:
                err = "Error stopping service. Details: %s" % sys.exc_info()[1]
                raise initdv2.InitdError, err, sys.exc_info()[2]
            finally:
                os.remove(self.pid_file)


    def restart(self):
        try:
            self.stop()
        except:
            LOG.debug('Error stopping HAProxy. Details: %s%s'% (sys.exc_info()[1], sys.exc_info()[2]))
        self.start()


    def reload(self):
        try:
            if os.path.exists(self.pid_file):
                pid = self.pid()
                if pid:
                    args = [self.haproxy_exec, '-f', self.config_path, '-p', self.pid_file, '-D', '-sf', pid]
                    util.system2(args, close_fds=True, logger=LOG, preexec_fn=os.setsid)
                    util.wait_until(lambda: self.pid() and self.pid() != pid,
                            timeout=self.timeout, sleep=0.5, error_text="Error reloading HAProxy service process.")
                    if self.status() != 0:
                        raise initdv2.InitdError("HAProxy service not running.")
            else:
                raise LookupError('File %s not exist'%self.pid_file)
        except:
            raise initdv2.InitdError, "HAProxy service not running can't reload it."\
                    " Details: %s" % sys.exc_info()[1], sys.exc_info()[2]


    def pid(self):
        '''Read #pid of the process from pid_file'''
        if os.path.isfile(self.pid_file):
            with open(self.pid_file, 'r') as fp:
                return long(fp.read())

initdv2.explore(SERVICE_NAME, HAProxyInitScript)
