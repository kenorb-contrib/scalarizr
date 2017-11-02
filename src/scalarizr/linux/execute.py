__author__ = 'Nick'

import re
import os
import sys
import time
import errno
import signal
import locale
import logging
import subprocess

if sys.version_info[0:2] >= (2, 7):
    from collections import OrderedDict
else:
    from scalarizr.externals.collections import OrderedDict

from scalarizr import linux
from scalarizr.linux import pkgmgr
from scalarizr.util import software


class parameter_handler(object):
    """
    decorator for executor methods, to handle options, specified by regexp
    """
    def __init__(self, regexp):
        self.regexp = re.compile(regexp)

    def __call__(self, fn):
        fn.regexp = self.regexp
        return fn


class ProcessTimeout(Exception):
    pass


class ProcessError(Exception):
    pass


class Process(object):


    result = (None, None, None)

    def __init__(self, executable, popen_obj):
        self.popen_obj = popen_obj
        self.executable = executable


    @property
    def stdin(self): return self.popen_obj.stdin

    @property
    def stdout(self): return self.popen_obj.stdout

    @property
    def stderr(self): return self.popen_obj.stderr


    def terminate(self):
        eradicate(self.popen_obj)


    def _get_stream(self, name):
        stream_obj = getattr(self.popen_obj, name, None)
        try:
            if stream_obj is not None and not stream_obj.closed:
                stream_str = stream_obj.read()
                return stream_str
        except:
            pass


    def wait(self, timeout=None):
        if timeout is None:
            stdout, stderr = self.popen_obj.communicate()
            returncode = self.popen_obj.returncode
            self.result = returncode, stdout, stderr
            return self.result

        wait_start = time.time()
        while True:
            returncode = self.popen_obj.poll()
            if returncode is not None:
                stdout = self._get_stream('stdout')
                stderr = self._get_stream('stderr')
                self.result = returncode, stdout, stderr
                return self.result
            else:
                if timeout:
                    now = time.time()
                    if now - wait_start > timeout:
                        raise ProcessTimeout('Process %s reached timeout %s sec' % (self.executable, timeout))


class BaseExec(object):
    """
    Base class for software-specific executors.
    It's main goal is to provide simple and convinient way to process command line arguments for
    different software.

    Command line options processing flow:
        __call__, start and start_nowait methods accept arbitrary number of arguments and keyword arguments.
        Keyword args (kwargs) are treated as command line keys ant their values. Arguments (args) are treated as
        command line parameters.

            pvcreate --force -u mypv-32 --zero y  /dev/md0 /dev/loop1
            |        |__________________________| |__________________|
            |           keys and values               parameters
            executable

        1. Kwargs and args are passed to _before_all_handlers. This method should return list of parameters
           and iterator of (key, value) pairs for kwargs (it could set kwargs processing order, or alter/change
           args and kwargs, following some logic).

        2. For each key-value pair, trying to find suitable handler among "parameter-handler"-decorated methods.
           If decorator's regexp matches the key, decorated method handles key-value pair. If no methods matches
           the key, "_default_handler" will be used to handle key-value pair.

           Default handler should accept key-value pair, and list of command line arguments,
           which handler should complement. "parameter_handler"-decorated handler additionaly accepts regexp-search
           result (see iptables implementation for details)

        3. Final list of command line arguments is passed to "_after_all_handlers" method, and result is considered
           to be final list of command line arguments


    If started witn "start_nowait" method, executor will return corresponding Process object. It has self-explanatory
    "wait" method, it will wait for process to finish, and will set stdout, stderr and returncode attributes of Process
    instance. It also has "terminate" method, which will annihilate entire process tree starting from your process.

    You can easily redefine standart stdin, stdout and stderr (subprocess.PIPE by default) by assigning them to
    corresponding attributes of executor object, or by passing them to constructor:

        dd = dd_executor(stdin=my_stdin, stdout=my_stdout)

        or

        firewall = iptables_exec()
        firewall.stdin = MY_FILE_DESCRIPTOR

    Remember that after each call, all three standart streams will be reset to default (subprocess.PIPE).


    """
    _handlers = dict()
    executable = None
    _checked = False
    package = None


    def __init__(self, lazy_check=True, raise_exc=True, timeout=None,
                        acceptable_codes=None, logger=None, to_log=True, **kwds):
        """
        :param lazy_check: if True, executable check will be perform right before first run,
                            otherwise, check will be performed immediately
        :param raise_exc: if True, bad return code will raise ProcessError exception
        :param timeout: process timeout in seconds. If called with start() or __call__(),
                        will raise ProcessTimeout, if reached
        :param acceptable_codes: List of return codes, that are good (will not raise ProcessError, if
                                    raise_exc == True). By default - (0,)
        :param logger: You can pass your logger here, all produced log messages will use it, instead of default.
        :param to_log: If True, cmd args, stdout and stderr will be logged. Log level - debug.
        :param kwds: kwargs to pass to subprocess.
        """

        self.lazy_check = lazy_check
        self.raise_exc=raise_exc
        self.timeout = timeout
        self.acceptable_codes = acceptable_codes or (0,)
        self.logger = logger or logging.getLogger(__name__)
        self.to_log = to_log

        kwds['close_fds'] = True
        self.subprocess_kwds = kwds

        if not lazy_check:
            self.check()
        self._collect_handlers()


    def check(self):
        if not self.executable.startswith('/'):
            exec_paths = software.which(self.executable)
            exec_path = exec_paths[0] if exec_paths else None
        else:
            exec_path = self.executable

        if not exec_path or not os.access(exec_path, os.X_OK):
            if self.package:
                pkgmgr.installed(self.package)

            else:
                msg = 'Executable %s is not found, you should either ' \
                      'specify `package` attribute or install the software ' \
                      'manually' % self.executable
                raise linux.LinuxError(msg)


    def _get_stdin(self):
        return self.subprocess_kwds.get('stdin')
    def _set_stdin(self, stdin):
        self.subprocess_kwds['stdin'] = stdin
    stdin = property(_get_stdin, _set_stdin)

    def _get_stdout(self):
        return self.subprocess_kwds.get('stdout')
    def _set_stdout(self, stdout):
        self.subprocess_kwds['stdout'] = stdout
    stdout = property(_get_stdout, _set_stdout)

    def _get_stderr(self):
        return self.subprocess_kwds.get('stderr')
    def _set_stderr(self, stderr):
        self.subprocess_kwds['stderr'] = stderr
    stderr = property(_get_stderr, _set_stderr)


    def _collect_handlers(self):
        # Collects self methods wrapped in handler decorator
        for attr in dir(self):
            attr = getattr(self, attr)
            if callable(attr) and hasattr(attr, 'regexp'):
                self._handlers[attr.regexp] = attr


    def _default_handler(self, key, value, cmd_args):
        # Default parameter handler, very straightforward by default
        if len(key) == 1:
            cmd_args.append('-%s' % key)
        else:
            cmd_args.append('--%s' % key.replace('_', '-'))

        if value is True:
            return
        else:
            cmd_args.append(str(value))


    def _before_all_handlers(self, *params, **keys):
        # By default, return same params and key-value pairs iterator
        return params, keys.iteritems()


    def _after_all_handlers(self, cmd_args):
        # By default, do nothing to final cmd args
        return cmd_args


    def prepare_args(self, *params, **keys):
        cmd_args = []
        # 1st step, before handlers
        params, key_value_pairs = self._before_all_handlers(*params, **keys)

        # 2nd step, trying to find appropriate handler, otherwise using default
        for key, value in key_value_pairs:
            for validator_re, handler in self._handlers.iteritems():
                re_result = validator_re.match(key)
                if re_result:
                    handler(re_result, key, value, cmd_args)
                    break
            else:
                self._default_handler(key, value, cmd_args)

        # 3rd step, after all
        cmd_args.extend(params)
        cmd_args = self._after_all_handlers(cmd_args)
        return cmd_args


    def start(self, *params, **keys):
        proc = self.start_nowait(*params, **keys)
        rcode, out, err = proc.wait(self.timeout)
        if self.to_log:
            self.logger.debug('stdout: %s' %  out)
            self.logger.debug('stderr: %s' %  err)

        if rcode not in self.acceptable_codes and self.raise_exc:
            raise ProcessError('Process %s finished with code %s.' % (self.executable, rcode))

        return rcode, out, err


    def start_nowait(self, *params, **keys):
        try:
            if not self._checked:
                self.check()
            if len(keys) == 1 and 'kwargs' in keys:
                keys = keys['kwargs']
                # Set locale
            if not 'env' in self.subprocess_kwds:
                self.subprocess_kwds['env'] = os.environ
                # Set en_US locale or C
            if not self.subprocess_kwds['env'].get('LANG'):
                default_locale = locale.getdefaultlocale()
                if default_locale == ('en_US', 'UTF-8'):
                    self.subprocess_kwds['env']['LANG'] = 'en_US'
                else:
                    self.subprocess_kwds['env']['LANG'] = 'C'

            cmd_args = self.prepare_args(*params, **keys)

            if not self.subprocess_kwds.get('shell') and not self.executable.startswith('/'):
                self.executable = software.which(self.executable)

            final_args = (self.executable,) + tuple(cmd_args)
            self._check_streams()
            if self.to_log:
                self.logger.debug('Starting subprocess. Args: %s' % ' '.join(final_args))

            popen = subprocess.Popen(final_args, **self.subprocess_kwds)
            process = Process(self.executable, popen)
            return process
        finally:
            for stream in ('stderr, stdout, stdin'):
                self.subprocess_kwds.pop(stream, None)

    __call__ = start

    def _check_streams(self):
        if not 'stdin' in self.subprocess_kwds:
            self.subprocess_kwds['stdin'] = subprocess.PIPE
        if not 'stdout' in self.subprocess_kwds:
            self.subprocess_kwds['stdout'] = subprocess.PIPE
        if not 'stderr' in self.subprocess_kwds:
            self.subprocess_kwds['stderr'] = subprocess.PIPE


#### Implementations ######


class dd_exec(BaseExec):
    executable = '/bin/dd'

    # dd uses -- before parameter only for --version and --help,
    # which we can ignore
    def _default_handler(self, key, value, cmd_args):
        cmd_args.append('%s=%s' % (key, value))

    @parameter_handler('^conv$')
    def conv(self, re_result, key, value, cmd_args):
        if isinstance(value, list) or isinstance(value, tuple):
            value = ','.join(map(str, value))
        cmd_args.append('%s=%s' % (key, value))


class iptables_exec(BaseExec):
    executable = '/sbin/iptables'
    package = 'iptables'

    @parameter_handler('^(not_)?(.+)')
    def params_with_not(self, re_result, key, value, cmd_args):
        if re_result.group(1):
            cmd_args.append('!')
        self._default_handler(re_result.group(2), value, cmd_args)


    def _before_all_handlers(self, *params, **keys):
        # in iptables, protocol and match should preceed other flags
        ordered_keys = OrderedDict()
        for key in ("t", "protocol", "p", "not_protocol", "not_p", "match"):
            if key in keys:
                ordered_keys[key] = keys.pop(key)
        ordered_keys.update(keys)
        return params, ordered_keys.iteritems()


class rsync(BaseExec):
    executable = 'rsync'
    package='rsync'

    @parameter_handler('^exclude$')
    def _handle_exclude(self, re_result, key, value, cmd_args):
        if not isinstance(value, (tuple, list)):
            value = (value, )
        for val in value:
            cmd_args.extend(['--exclude', val])


def eradicate(process):
    """
    Kill process tree.
    :param process: pid (int) or subprocess.Popen instance

    """

    class Victim(object):

        def __init__(self, process):
            self._obj = process

        @property
        def pid(self):
            return self._obj if isinstance(self._obj, int) else \
                       self._obj.pid

        def get_children(self):
            try:
                pgrep = linux.system(linux.build_cmd_args(
                        executable="pgrep",
                        short=["-P"],
                        params=[str(self.pid)]))
            except linux.LinuxError:
                children = []
            else:
                children = map(int, pgrep[0].splitlines())
            return children

        def die(self, grace=2):
            if isinstance(self._obj, subprocess.Popen):
                self._obj.terminate()
                time.sleep(grace)
                self._obj.kill()
                time.sleep(0.1)
                self._obj.poll()  # avoid leaving defunct processes
            else:
                try:
                    os.kill(self.pid, signal.SIGTERM)
                    time.sleep(grace)
                    os.kill(self.pid, signal.SIGKILL)
                except OSError, e:
                    if e.errno == errno.ESRCH:
                        pass  # no such process
                    else:
                       raise Exception("Failed to stop pid %s" % self.pid)

    victim = Victim(process)
    children = victim.get_children()
    victim.die()
    map(eradicate, children)
