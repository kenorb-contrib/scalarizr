from __future__ import with_statement

import socket
import os
import re
import logging
import locale
import threading
import weakref
import time
import sys
import signal
import string
import pkgutil
import traceback
import platform
import functools

if sys.platform == 'win32':
    import win32com.client


from scalarizr.bus import bus
from scalarizr import exceptions


LOG = logging.getLogger(__name__)


class UtilError(BaseException):
    pass


class NullPool(object):

    def __init__(self, creator):
        self._creator = creator
        self._object = None

    def get(self):
        return self._object or self._creator()

    def dispose_local(self):
        self._object = None

    def dispose_all(self):
        self._object = None


class LocalPool(object):

    def __init__(self, creator, pool_size=50):
        self._logger = logging.getLogger(__name__)
        self._creator = creator         
        self._object = threading.local()
        
        self._all_conns = []
        self.size = pool_size

    def do_create(self):
        return self._creator()

    def get(self):
        try:
            o = self._object.current
            if o():
                return o()
            else:
                self._logger.debug("Current weakref is empty")
        except AttributeError, e:
            self._logger.debug("Caught: %s", e)
    
        self._logger.debug("Creating new object...")
        o = self.do_create()
        self._logger.debug("Created %s", o)
        self._object.current = weakref.ref(o)
        self._logger.debug("Added weakref %s", self._object.current)
        self._all_conns.append(o)
        if len(self._all_conns) > self.size:
            self.cleanup()
        return o

    def cleanup(self):
        if len(self._all_conns) > self.size:
            self._logger.debug("Pool has exceeded the amount of maximum connections (%s). Starting cleaning process.", self.size)
            l = len(self._all_conns) - self.size
            self._logger.debug("Removing %s from connection pool", self._all_conns[:l])
            self._all_conns = self._all_conns[l:]

    def dispose_local(self):
        if hasattr(self._object, 'current'):
            del self._object.current

    def dispose_all(self):
        self._object = threading.local()


class SqliteLocalObject(LocalPool):
    def do_create(self):
        return _SqliteConnection(self, self._creator)

class _SqliteConnection(object):
    _conn = None
    #_lo = None
    _creator = None
    
    def __init__(self, lo, creator):
        #self._lo = lo
        self._creator = creator

    def get_connection(self):
        if not self._conn:
            self._conn = self._creator()
        return self._conn

class dicts:
        
    @staticmethod
    def merge(a, b):
        res = {}
        for key in a.keys():
            if not key in b:
                res[key] = a[key]
                continue
        
            if type(a[key]) != type(b[key]):
                res[key] = b[key]
            elif dict == type(a[key]):
                res[key] = dicts.merge(a[key], b[key])
            elif list == type(a[key]):
                res[key] = a[key] + b[key]
            else:
                res[key] = b[key]
            del(b[key])
    
        res.update(b)
        return res

    @staticmethod
    def encode(a, encoding='ascii'):
        if not isinstance(a, dict):
            raise ValueError('dict type expected, but %s passed' % type(a))
        ret = {}
        for key, value in a.items():
            ret[key.encode(encoding)] = dicts.encode(value, encoding) \
                            if isinstance(value, dict) else value.encode(encoding) \
                            if isinstance(value, basestring) else value 
        return ret

    @staticmethod
    def keys2ascii(a):
        if not isinstance(a, dict):
            raise ValueError('dict type expected, but %s passed' % type(a))
        ret = {}
        for key, value in a.items():
            ret[key.encode('ascii')] = dicts.keys2ascii(value) if isinstance(value, dict) else value
        return ret


def cached(f, cache={}):
    '''
    Decorator
    '''
    def g(*args, **kwargs):
        key = (f, tuple(args), frozenset(kwargs.items()))
        if key not in cache:
            cache[key] = f(*args, **kwargs)
        return cache[key]
    return g        

def firstmatched(function, sequence, default=None):
    for s in sequence:
        if function(s):
            return s
    else:
        return default  

def daemonize():
    # First fork
    pid = os.fork()
    if pid > 0:
        sys.exit(0)     

    os.chdir("/")
    os.setsid()
    os.umask(0)
    
    # Second fork
    pid = os.fork()
    if pid > 0:
        sys.exit(0)
        
    # Redirect standard file descriptors
    sys.stdout.flush()
    sys.stderr.flush()
    si = file(os.devnull, 'r')
    so = file(os.devnull, 'a+')
    se = file(os.devnull, 'a+', 0)
    os.dup2(si.fileno(), sys.stdin.fileno())
    os.dup2(so.fileno(), sys.stdout.fileno())
    os.dup2(se.fileno(), sys.stderr.fileno())
    
    
def system(args, shell=True):
    import subprocess
    logger = logging.getLogger(__name__)
    logger.debug("system: %s", hasattr(args, '__iter__') and ' '.join(args) or args)
    p = subprocess.Popen(args, shell=shell, env={'LANG' : 'en_US'}, 
                    stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    out, err = p.communicate()
    if out:
        logger.debug("stdout: " + out)
    if err:
        logger.debug("stderr: " + err)
    return out, err, p.returncode

class PopenError(BaseException):
        
    def __str__(self):
        if len(self.args) >= 5:
            args = [self.error_text + '. ' if self.error_text else '']
            args += [self.proc_args[0] if hasattr(self.proc_args, '__iter__') else self.proc_args.split(' ')[0]]
            args += [self.returncode, self.out, self.err, self.proc_args]

            ret = '%s %s (code: %s) <out>: %s <err>: %s <args>: %s' % tuple(args)
            return ret.strip()
        else:
            return self.error_text

    @property
    def error_text(self):
        return len(self.args) and self.args[0] or ''

    @property
    def out(self):
        return self.args[1]

    @property
    def err(self):
        return self.args[2]

    @property
    def returncode(self):
        return self.args[3]

    @property
    def proc_args(self):
        return self.args[4]


def apply_english_env(env):
    """
    Forces LANG and/or LANGUAGE to be English.
    Forces encoding to UTF-8 for subprocesses.
    """
    for name in ('LANG', 'LANGUAGE'):
        if not env.get(name, '').startswith('en'):
            env[name] = 'en_US.UTF-8'


def system2(*popenargs, **kwargs):
    import subprocess, cStringIO
    
    silent = kwargs.pop('silent', False)
    logger = kwargs.pop('logger', logging.getLogger(__name__))
    log_level = kwargs.pop('log_level', logging.DEBUG)
    warn_stderr = kwargs.pop('warn_stderr', False)
    raise_exc = kwargs.pop('raise_exc', kwargs.pop('raise_error',  True))
    ExcClass = kwargs.pop('exc_class', PopenError)
    error_text = kwargs.pop('error_text', '')
    input = None
    
    if kwargs.get('err2out'):
        # Redirect stderr -> stdout
        kwargs['stderr'] = subprocess.STDOUT
        
    if not 'stdout' in kwargs:
        # Capture stdout
        kwargs['stdout'] = subprocess.PIPE
        
    if not 'stderr' in kwargs:
        # Capture stderr
        kwargs['stderr'] = subprocess.PIPE
        
    if isinstance(kwargs.get('stdin'),  basestring):
        # Pass string into stdin
        input = kwargs['stdin']
        kwargs['stdin'] = subprocess.PIPE
        
    if len(popenargs) > 0 and hasattr(popenargs[0], '__iter__'):
        # Cast arguments to str
        popenargs = list(popenargs)
        popenargs[0] = tuple('%s' % arg for arg in popenargs[0])
        
    env = kwargs.setdefault('env', dict(os.environ.items()))
    apply_english_env(env)
   
    logger.debug('system: %s' % (popenargs[0],))
    p = subprocess.Popen(*popenargs, **kwargs)
    out, err = p.communicate(input=input)
    if not silent:
        if out:
            logging.log(log_level, 'stdout: ' + out)
        if err:
            logger.log(logging.WARN if warn_stderr else log_level, 'stderr: ' + err)

    if p.returncode and raise_exc:
        raise ExcClass(error_text, out and out.strip() or '', err and err.strip() or '', p.returncode, popenargs[0])
    return out, err, p.returncode



def wait_until(target, args=None, kwargs=None, sleep=5, logger=None, timeout=None, start_text=None, error_text=None, raise_exc=True):
    args = args or ()
    kwargs = kwargs or {}
    time_until = None
    if timeout:
        time_until = time.time() + timeout
    if start_text and logger:
        text = start_text
        if isinstance(timeout, int):
            text += '(timeout: %d seconds)' % timeout
        logger.info(text)
    while not target(*args, **kwargs):
        if time_until and time.time() >= time_until:
            msg = error_text + '. ' if error_text else ''
            msg += 'Timeout: %d seconds reached' % (timeout, )
            if raise_exc:
                raise BaseException(msg)
            else:
                return False
        if logger:
            logger.debug("Wait %.2f seconds before the next attempt", sleep)
        time.sleep(sleep)
    return True


def xml_strip(el):
    for child in list(el.childNodes):
        if child.nodeType==child.TEXT_NODE and child.nodeValue.strip() == '':
            el.removeChild(child)
        else:
            xml_strip(child)
    return el       


def url_replace_hostname(url, newhostname):
    import urlparse 
    r = url if isinstance(url, tuple) else urlparse.urlparse(url)
    r2 = list(r)
    r2[1] = newhostname
    if r.port:
        r2[1] += ":" + str(r.port)
    return urlparse.urlunparse(r2)
    


def read_shebang(path=None, script=None):
    if path:
        file = first_line = None
        try:
            file = open(path, 'r')
            first_line = file.readline()
        finally:
            if file:
                file.close()
    elif script:
        if not isinstance(script, basestring):
            raise ValueError('argument `script` should be a basestring subclass')
        eol_index = script.find(os.linesep)
        first_line = eol_index != -1 and script[0:eol_index] or script
    else:
        raise ValueError('one of arguments `path` or `script` should be passed')

    shebang = re.search(re.compile('^#!(\S+.+)'), first_line)
    if shebang:
        return split_strip(shebang.group(1))[0]
    return None

def split_strip(value, separator=' '):
    return map(string.strip, value.split(separator))


def parse_size(size):
    """
    Read string like 10K, 12M, 1014B and return size in bytes
    """
    ret = str(size)
    dim = ret[-1]           
    ret = float(ret[0:-1])
    if dim.lower() == "b":
        pass            
    elif dim.lower() == "k":
        ret *= 1024
    elif dim.lower() == "m":
        ret *= 1048576  

    return ret
    
def format_size(size, precision=2):
    """
    Format size in Bytes, KBytes and MBytes
    """
    ret = float(size)
    dim = "B"
    if ret > 1000:
        ret = ret/1000
        dim = "K"
    if ret > 1000:
        ret = ret/1000
        dim = "M"
        
    s = "%."+str(precision)+"f%s"
    return s % (ret, dim)   

def parse_bool(value):
    return value in ('1', 1, True)

def backup_file(filename):
    import shutil
    logger = logging.getLogger(__name__)
    max_backups = 50
    
    for i in range(0, max_backups):
        bkname = '%s.bak.%s' % (filename, i)            
        if not os.path.exists(bkname):
            logger.debug('Backuping %s to %s', filename, bkname)
            shutil.copy(filename, bkname)
            return bkname
    raise UtilError("Max backups limit %d exceed for file %s" % (max_backups, filename))


def timethis(what):
    try:
        import time
    except ImportError:
        import timemodule as time
    from contextlib import contextmanager   
    
    @contextmanager
    def benchmark():
        start = time.time()
        yield
        end = time.time()
        print("%s : %0.3f seconds" % (what, end-start))
    if hasattr(what,"__call__"):
        def timed(*args,**kwargs):
            with benchmark():
                return what(*args,**kwargs)
        return timed
    else:
        return benchmark()


def split_ex(value, separator=",", allow_empty=False, ct=list):
    return ct(v.strip() 
                    for v in value.split(separator) 
                    if allow_empty or (not allow_empty and v)) if value else ct()


def get_free_devname():
    avail_letters = set(string.ascii_lowercase[5:16])
    try:
        pl = bus.platform
        conn = pl.new_ec2_conn()
        volumes = conn.get_all_volumes(filters={'attachment.instance-id': pl.get_instance_id()})
        for volume in volumes:
            try:
                avail_letters.remove(volume.attach_data.device[-1])
            except KeyError:
                pass
    except:
        pass
        
    dev_list = os.listdir('/dev')
    for letter in avail_letters:
        device = 'sd'+letter
        if not device in dev_list:
            return '/dev/'+device
    
def kill_childs(pid):
    ppid_re = re.compile('^PPid:\s*(?P<pid>\d+)\s*$', re.M)
    for process in os.listdir('/proc'):
        if not re.match('\d+', process):
            continue
        try:
            fp = open('/proc/' + process + '/status')
            process_info = fp.read()
            fp.close()
        except:
            pass
    
        Ppid_result = re.search(ppid_re, process_info)
        if not Ppid_result:
            continue
        ppid = Ppid_result.group('pid')
        if int(ppid) == pid:
            try:
                os.kill(int(process), signal.SIGKILL)
            except:
                pass


def get_children(ppid, recursive=False):
    children = []
    tmp = {}
    if sys.platform == 'win32':
        wmi = win32com.client.GetObject('winmgmts:')
        processes = wmi.InstancesOf('Win32_Process')
        for process in processes:
            tmp.setdefault(int(process.Properties_('ParentProcessId').Value), []).append(
                           int(process.Properties_('ProcessID').Value))
    else:
        ppid_re = re.compile('^PPid:\s*(?P<pid>\d+)\s*$', re.M)
        for process in os.listdir('/proc'):
            if not re.match('\d+', process):
                continue
            try:
                fp = open('/proc/' + process + '/status')
                process_info = fp.read()
                fp.close()
            except:
                pass

            Ppid_result = re.search(ppid_re, process_info)
            if not Ppid_result:
                continue
            _ppid = Ppid_result.group('pid')
            tmp.setdefault(int(_ppid), []).append(int(process))

    def append(ppid, children):
        for pid in tmp.get(ppid, []):
            append(pid, children)
        children += tmp.get(ppid, [])

    if recursive:
        append(ppid, children)
    else:
        children = tmp.get(ppid, [])

    return children


def ping_socket(host, port, exc_str=None):
    s = socket.socket()
    try:
        s.connect((host, port))
    except:
        raise Exception(exc_str or 'Service is not running: Port %s on %s closed.' % (port, host))

def port_in_use(port):
    s = socket.socket()
    try:
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)         
        s.bind(('0.0.0.0', port))
        return False
    except socket.error:
        return True
    finally:
        s.close()

        
class PeriodicalExecutor:
    _logger = None
    _tasks = None
    _lock = None
    _ex_thread = None
    _shutdown = None
    
    def __init__(self):
        self._logger = logging.getLogger(__name__ + '.PeriodicalExecutor')
        self._tasks = dict()
        self._ex_thread = threading.Thread(target=self._executor, name='PeriodicalExecutor')
        self._ex_thread.setDaemon(True)
        self._lock = threading.Lock()

    def start(self):
        self._shutdown = False          
        self._ex_thread.start()
        
    def shutdown(self):
        self._shutdown = True
        self._ex_thread.join(1)

    def add_task(self, fn, interval, title=None):
        self._lock.acquire()
        try:
            if fn in self._tasks:
                raise BaseException('Task %s already registered in executor with an interval %s minutes', 
                        fn, self._tasks[fn])
            if interval <= 0:
                raise ValueError('interval should be > 0')
            self._tasks[fn] = dict(fn=fn, interval=interval, title=title, last_exec_time=0)
        finally:
            self._lock.release()

    def remove_task(self, fn):
        self._lock.acquire()
        try:
            if fn in self._tasks:
                del self._tasks[fn]
        finally:
            self._lock.release()
    
    def _tasks_to_execute(self):
        self._lock.acquire()            
        try:
            now = time.time()                       
            return list(task for task in self._tasks.values()
                            if now - task['last_exec_time'] > task['interval'])
        finally:
            self._lock.release()
    
    def _executor(self):
        while not self._shutdown:
            for task in self._tasks_to_execute():
                self._logger.debug('Executing task %s', task['title'] or task['fn'])
                try:
                    task['last_exec_time'] = time.time()
                    task['fn']()
                except (BaseException, Exception), e:
                    self._logger.exception(e)
                if self._shutdown:
                    break
            if not self._shutdown:
                time.sleep(1)
        
                
                
def run_detached(binary, args=[], env=None):
    if not os.path.exists(binary):
        from . import software
        binary_base = os.path.basename(binary)
        try:
            binary = software.which(binary_base)
        except LookupError:
            raise Exception('Cannot find %s executable' % binary_base)


    pid = os.fork()
    if pid == 0:
        os.setsid()
        pid = os.fork()
        if pid != 0:
            os._exit(0)

        os.chdir('/')
        os.umask(0)
        
        import resource         # Resource usage information.
        maxfd = resource.getrlimit(resource.RLIMIT_NOFILE)[1]
        if (maxfd == resource.RLIM_INFINITY):
            maxfd = 1024
            
        for fd in range(0, maxfd):
            try:
                os.close(fd)
            except OSError:
                pass

        os.open('/dev/null', os.O_RDWR)

        os.dup2(0, 1)
        os.dup2(0, 2)   
        
        try:
            if env:
                args.append(env)
                os.execle(binary, binary, *args)
            else:
                os.execl(binary, binary, *args)
        except Exception:
            os._exit(255)
            
            
def which(arg):
    return system2(['/bin/which', arg], raise_exc=False)[0].strip()


def import_class(import_str):
    """Returns a class from a string including module and class"""
    module, cls_name = import_str.rsplit('.', 1)
    try:
        if module not in sys.modules:
            __import__(module)
        return getattr(sys.modules[module], cls_name)
    except:
        raise exceptions.NotFound('Class %s cannot be found' % import_str)


def import_object(import_str, *args, **kwds):
    """Returns an object including a module or module and class"""
    try:
        __import__(import_str)
        return sys.modules[import_str]
    except ImportError:
        cls = import_class(import_str)
        return cls(*args, **kwds)



def linux_package(name):
    # @todo install package with apt or yum. raise beautiful errors
    raise NotImplementedError()
                            

class Hosts:    
    @classmethod
    def set(cls, addr, hostname):
        hosts = cls.hosts()
        hosts[hostname] = addr
        cls._write(hosts)
        
    @classmethod
    def delete(cls, addr=None, hostname=None):
        hosts = cls.hosts()
        if hostname:
            if hosts.has_key(hostname):
                del hosts[hostname]
        if addr:
            hostnames = hosts.keys()
            for host in hostnames:
                if addr == hosts[host]: 
                    del hosts[host]
        cls._write(hosts)               

    @classmethod
    def hosts(cls):
        ret = {}
        with open('/etc/hosts') as f:
            hosts = f.readlines()
            for host in hosts:
                host_line = host.strip()
                if not host_line or host_line.startswith('#'):
                    continue
                addr, hostname = host.split(None, 1)
                ret[hostname.strip()] = addr
        return ret

    @classmethod
    def _write(cls, hosts):
        with open('/etc/hosts', 'w') as f:
            for hostname, addr in hosts.iteritems():
                f.write('%s\t%s\n' % (addr, hostname))


class capture_exception:
    def __init__(self, logger=None):
        self.logger = logger or logging.getLogger(__name__)


    def __enter__(self):
        self.exc_info = sys.exc_info()
        self.orig_exc_catched = not all(map(lambda x: x is None, self.exc_info))


    def __exit__(self, *exc_info):
        exc_catched = not all(map(lambda x: x is None, exc_info))
        e_type, e_val, tb = exc_info
        if self.orig_exc_catched:
            if exc_catched:
                log_msg = ''.join(traceback.format_exception(e_type, e_val, tb))
                self.logger.debug(log_msg)
            orig_type, orig_val, orig_tb = self.exc_info
            raise orig_type, orig_val, orig_tb

        else:
            if exc_catched:
                e_type, e_val, tb = exc_info
                raise e_type, e_val, tb


class Singleton(type):
    _instances = {}
    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super(Singleton, cls).__call__(*args, **kwargs)
        return cls._instances[cls]


def add_authorized_key(ssh_public_key):
    ssh_home_dir = '/root/.ssh'
    authorized_keys_path = os.path.join(ssh_home_dir, 'authorized_keys')

    if not os.path.exists(ssh_home_dir):
        os.makedirs(ssh_home_dir, 0700)
    if not os.path.exists(authorized_keys_path):
        open(authorized_keys_path, 'w+').close()

    c = None
    with open(authorized_keys_path, 'r') as fp:
        c = fp.read()
    idx = c.find(ssh_public_key)
    if idx == -1:
        if c and c[-1] != '\n':
            c += '\n'
        c += ssh_public_key + "\n"
        LOG.debug("Add server ssh public key to authorized_keys")
    elif idx > 0 and c[idx-1] != '\n':
        c = c[0:idx] + '\n' + c[idx:]
        LOG.warn('Adding new-line character before server SSH key in authorized_keys file')

    os.chmod(authorized_keys_path, 0600)
    try:
        with open(authorized_keys_path, 'w') as fp:
            fp.write(c)
    finally:
        os.chmod(authorized_keys_path, 0400)


class Server(object):
    def __init__(self):
        self.running = False
        self.stop_event = threading.Event()
    
    def serve_forever(self):
        self.start()
        self.stop_event.clear()
        try:
            # do polling here to enable signal handling
            while not self.stop_event.isSet():
                self.stop_event.wait(0.2)
        except:
            self.stop()
            raise
    
    def stop(self):
        try:
            if self.running:
                self.do_stop()
        finally:
            self.running = False
            self.stop_event.set()
    
    def start(self):
        if not self.running:
            self.do_start()
        self.running = True
        
    def do_start(self):
        pass
    
    def do_stop(self):
        pass


class InfoToDebugFilter(object):
    def filter(self, record):
        if record.levelno == logging.INFO:
            record.levelno = logging.DEBUG
            record.levelname = logging.getLevelName(record.levelno)
            return True
            

def init_logging(filename, verbose):
    kwds = {}
    if hasattr(filename, 'write'):
        kwds['stream'] = filename
    else:
        kwds['filename'] = filename
    if verbose:
        kwds['level'] = logging.DEBUG
    else:
        kwds['level'] = logging.INFO
    kwds['format'] = '%(asctime)s - %(levelname)s - %(name)s - %(message)s'
    logging.basicConfig(**kwds) 

    logger = logging.getLogger('requests.packages.urllib3.connectionpool')
    logger.addFilter(InfoToDebugFilter())


if platform.uname()[0] == 'Windows':
    import _winreg as winreg
    import pythoncom
    
    REG_KEY = 'Software\\Microsoft\\Windows\\CurrentVersion\\Uninstall\\Scalarizr'

    def reg_value(value_name):
        # Yes I know that win32api includes Reg* functions, 
        # but KEY_WOW64_64KEY flag to access 64bit registry from 32bit app doesn't works

        hkey = winreg.OpenKeyEx(winreg.HKEY_LOCAL_MACHINE, REG_KEY, 0, winreg.KEY_READ)
        try:
            return winreg.QueryValueEx(hkey, value_name)[0]
        finally:
            winreg.CloseKey(hkey)


    def coinitialized(fn):
        @functools.wraps(fn)
        def decorator(*args, **kwargs):
            pythoncom.CoInitializeEx(pythoncom.COINIT_MULTITHREADED)
            try:
                return fn(*args, **kwargs)
            finally:
                pythoncom.CoUninitialize()
        return decorator


def write_file_to_stream(path, stream):
    with open(path, 'rb') as f:
        while True:
            data = f.read(4096)
            if not data:
                break
            stream.write(data)

