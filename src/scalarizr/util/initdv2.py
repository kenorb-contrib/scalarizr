from __future__ import with_statement
'''
Created on Aug 29, 2010

@author: marat
@author: spike
'''
import socket
import string
import os
import sys
import time
import re
from threading import local
import logging

from scalarizr import linux
from scalarizr.util import system2, PopenError


LOG = logging.getLogger(__name__)

_services  = dict()
_instances = dict()


# TODO: error codes not used
class InitdError(BaseException):
    GENERIC_ERR = 1
    INVALID_ARG = 2
    UNIMPLEMENTED = 3
    INSUFFICIENT_PRIVILEGE = 4
    NOT_INSTALLED = 5
    NOT_CONFIGURED = 6
    NOT_RUNNING = 7

    @property
    def code(self):
        return len(self.args) > 1 and self.args[1] or None

    @property
    def message(self):
        return self.args[0]

class Status:
    RUNNING = 0
    DEAD_PID_FILE_EXISTS = 1
    DEAD_VAR_LOCK_EXISTS = 2
    NOT_RUNNING = 3
    UNKNOWN = 4

class InitScript(object):
    name = None
    pid_file = None
    lock_file = None

    def start(self):
        '''
        @raise InitdError:
        '''
        pass

    def stop(self):
        '''
        @raise InitdError:
        '''
        pass

    def restart(self):
        '''
        @raise InitdError:
        '''
        pass

    def reload(self):
        '''
        @raise InitdError:
        '''
        pass

    def status(self):
        '''
        @return: Service status
        @rtype: scalarizr.util.initdv2.Status
        '''
        if self.pid_file:
            if not os.path.exists(self.pid_file):
                return Status.NOT_RUNNING
            pid = None
            with open(self.pid_file, 'r') as fp:
                pid = fp.read().strip()
            if os.path.isfile('/proc/%s/status' % pid):
                try:
                    fp = open('/proc/%s/status' % pid)
                    status = fp.read()
                except:
                    return Status.NOT_RUNNING
                finally:
                    fp.close()

                if status:
                    pid_state = re.search('State:\s+(?P<state>\w)', status).group('state')
                    if pid_state in ('T', 'Z'):
                        return Status.NOT_RUNNING
            else:
                return Status.NOT_RUNNING
        if self.socks:
            try:
                for sock in self.socks:
                    timeout = sock.timeout
                    sock.timeout = 1
                    try:
                        wait_sock(sock)
                    finally:
                        sock.timeout = timeout
            except InitdError:
                return Status.NOT_RUNNING

        return Status.RUNNING

    def configtest(self, path=None):
        """
        @raise InitdError:
        """
        pass

    def trans(self, enter=None, exit=None):
        return self

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return None

class SockParam:
    def __init__(self, port=None, family=socket.AF_INET, type=socket.SOCK_STREAM, conn_address=None, timeout=5):

        self.family = family
        self.type = type
        self.conn_address = (conn_address or '127.0.0.1', int(port))
        self.timeout = timeout

class ParametrizedInitScript(InitScript):
    name = None

    def __init__(self, name, initd_script, pid_file=None, lock_file=None, socks=None):
        '''
        if isinstance(initd_script, basestring):
            if not os.path.exists(initd_script):
                raise InitdError("Can't find %s init script at %s. Make sure that %s is installed" % (
                        name, initd_script, name))
            if not os.access(initd_script, os.X_OK):
                raise InitdError("Permission denied to execute %s" % (initd_script))
        '''

        self.name = name
        self.initd_script = initd_script
        self.pid_file = pid_file
        self.lock_file = lock_file
        self.socks = socks
        self.local = local()

        '''
        @param socks: list(SockParam)
        '''

    def _start_stop_reload(self, action):
        try:

            args = [self.initd_script] \
                            if isinstance(self.initd_script, basestring) \
                            else list(self.initd_script)
            args.append(action)
            out, err, returncode = system2(args, close_fds=True, preexec_fn=os.setsid)
        except PopenError, e:
            raise InitdError("Popen failed with error %s" % (e,))

        if returncode:
            raise InitdError("Cannot %s %s. output= %s. %s" % (action, self.name, out, err), returncode)

        if self.socks and (action != "stop" and not (action == 'reload' and not self.running)):
            for sock in self.socks:
                wait_sock(sock)

#               if self.pid_file:
#                       if (action == "start" or action == "restart") and not os.path.exists(self.pid_file):
#                               raise InitdError("Cannot start %s. pid file %s doesn't exists" % (self.name, self.pid_file))
#                       if action == "stop" and os.path.exists(self.pid_file):
#                               raise InitdError("Cannot stop %s. pid file %s still exists" % (self.name, self.pid_file))

        return True

    def start(self):
        return self._start_stop_reload('start')

    def stop(self):
        return self._start_stop_reload('stop')

    def restart(self):
        return self._start_stop_reload('restart')

    def reload(self):
        if not self.running:
            raise InitdError('Service "%s" is not running' % self.name, InitdError.NOT_RUNNING)
        return self._start_stop_reload('reload')

    @property
    def running(self):
        return self.status() == Status.RUNNING

    def running_on_exit(self):
        self.local.on_exit = Status.RUNNING
        return self

    def running_on_enter(self):
        self.local.on_enter = Status.RUNNING
        return self

    def __enter__(self):
        self._ctxmgr_ensure_status('on_enter')
        return self

    def __exit__(self, *args):
        self._ctxmgr_ensure_status('on_exit')

    def _ctxmgr_ensure_status(self, status_attr, reason_attr=None):
        if hasattr(self.local, status_attr):
            cur_status = self.status()
            status = getattr(self.local, status_attr)
            if status != cur_status:
                if status == Status.RUNNING:
                    if cur_status == Status.NOT_RUNNING:
                        self.start()
                    else:
                        self.restart()
                else:
                    self.stop(getattr(self.local, reason_attr))

            delattr(self.local, status_attr)
            if reason_attr:
                delattr(self.local, reason_attr)


class Daemon(object):
    '''
    Alternate implementation (from updclient project)
    TODO: we should merge Daemon and ParametrizedInitScript classes and update clients code
    '''
    def __init__(self, name):
        self.name = name
        if linux.os.name == 'Ubuntu' and linux.os.release >= (10, 4):
            self.init_script = ['service', self.name]
        else:
            self.init_script = ['/etc/init.d/' + self.name]
    
    if linux.os.windows_family:
        def ctl(self, command, raise_exc=True):
            return linux.system(('sc', command, self.name), raise_exc=raise_exc)
    else:
        def ctl(self, command, raise_exc=True):
            return linux.system(self.init_script + [command], 
                    raise_exc=raise_exc, close_fds=True, preexec_fn=os.setsid)
    
    def restart(self):
        LOG.info('Restarting %s', self.name)
        if linux.os.windows_family:
            self.ctl('stop')
            time.sleep(1)
            self.ctl('start')
        else:
            self.ctl('restart')
    
    def forcerestart(self):
        LOG.info('Forcefully restarting %s', self.name)
        self.ctl('stop')
        try:
            out = linux.system('ps -C %s --noheaders -o pid' % self.name)[0]
            for pid in out.strip().splitlines():
                LOG.debug('Killing process %s', pid)
                os.kill(pid, 9)
        finally:
            self.ctl('start')
    
    def condrestart(self):
        LOG.info('Conditional restarting %s', self.name)
        self.ctl('condrestart')
    
    def start(self):
        LOG.info('Starting %s', self.name)
        self.ctl('start')
    
    def stop(self):
        LOG.info('Stopping %s', self.name)
        self.ctl('stop')
    
    @property
    def running(self):
        if linux.os.windows_family:
            out = self.ctl('query')[0]
            lines = filter(None, map(string.strip, out.splitlines()))
            for line in lines:
                name, value = map(string.strip, line.split(':', 1))
                if name.lower() == 'state':
                    return value.lower().endswith('running')
        else:
            return not self.ctl('status', raise_exc=False)[2] 



def explore(name, init_script_cls):
    _services[name] = init_script_cls

def lookup(name):
    '''
    Lookup init script object by service name
    '''
    if not _services.has_key(name):
        raise InitdError('No service has been explored with name %s ' % name)

    if not _instances.has_key(name):
        _instances[name] = _services[name]()

    return _instances[name]

def wait_sock(sock = None):
    if not isinstance(sock, SockParam):
        raise InitdError('Socks parameter must be instance of SockParam class')

    time_start = time.time()
    while time.time() - time_start < sock.timeout:
        try:
            s = socket.socket(sock.family, sock.type)
            s.connect(sock.conn_address)
            s.shutdown(2)
            del s
            return
        except:
            time.sleep(1)
            pass
    raise InitdError ("Service unavailable after %d seconds of waiting" % sock.timeout)
