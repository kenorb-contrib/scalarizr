import os
import re
import sys
import time
import uuid
import json
import copy
import types
import Queue
import socket
import signal
import sqlite3
import logging
import datetime
import threading
import traceback
import multiprocessing
import multiprocessing.connection
import multiprocessing.synchronize

from abc import ABCMeta, abstractmethod

from scalarizr import util
from scalarizr.bus import bus
from scalarizr.util import sqlite_server

if sys.platform == 'win32':
    import win32api
    import win32con
    import win32job
    import win32process

    push_server_base_address = r'\\.\pipe'
    pull_server_base_address = r'\\.\pipe'
    SIGKILL = signal.SIGTERM
    SIGINT = signal.SIGTERM
else:
    from ctypes import cdll

    PR_SET_PDEATHSIG = 1
    push_server_base_address = '/tmp'
    pull_server_base_address = '/tmp'
    SIGKILL = signal.SIGKILL
    SIGINT = signal.SIGINT

LOG = logging.getLogger(__name__)

MAX_WORKERS = 4
SOFT_TIMEOUT = 60
HARD_TIMEOUT = 120
ERROR_SLEEP = 5
EXECUTOR_POLL_SLEEP = 1
WORKER_SUPERVISOR_SLEEP = 1


__tasks__ = {}


class TimeoutError(Exception):
    pass


class SoftTimeLimitExceeded(TimeoutError):
    pass


class HardTimeLimitExceeded(TimeoutError):
    pass


class ParentProcessError(Exception):
    pass


class TaskKilledError(Exception):
    pass


def task(name=None):
    """Task decorator"""

    def wrapper(f, name=name):
        assert isinstance(f, types.FunctionType)
        if name is None:
            name = '%s.%s' % (f.__module__, f.__name__)
        assert name not in __tasks__, 'Task with name %s already exists' % name
        __tasks__.update({name: f})
        return f

    return wrapper


class Event(multiprocessing.synchronize.Event):

    """Custom Event class to avoid lock in multiprocessing.Event.wait() method"""

    def wait(self, timeout=None):
        if timeout:
            result = super(Event, self).wait(timeout=timeout)
        else:
            while True:
                result = super(Event, self).wait(timeout=1)
                if result:
                    break
        return result


def get_connection():
    return bus.db


class Task(dict):

    """Task class"""

    schema = [
        'task_id',
        'name',
        'args',
        'kwds',
        'state',
        'result',
        'traceback',
        'start_date',
        'end_date',
        'worker_id',
        'soft_timeout',
        'hard_timeout',
    ]

    @classmethod
    def _load(cls, *args, **kwds):
        """
        :param args: field names to select
        :type args: tuple

        :param kwds: condition for select statement
        :type kwds: dict

        :returns: row as dict from database
        :rtype: list
        """

        args = args or tuple(cls.schema)
        names = ', '.join(args)
        query = "SELECT {} FROM tasks".format(names)
        if kwds:
            where = True
            for k, v in kwds.iteritems():
                if where:
                    query += " WHERE {}=\"{}\"".format(k, v)
                    where = False
                else:
                    query += " AND {}=\"{}\"".format(k, v)
        conn = get_connection()
        curs = conn.cursor()
        curs.execute(query)
        return curs.fetchall()

    @classmethod
    def load_by_id(cls, task_id):
        """
        Load task from database by task id

        :returns: Task object
        :rtype: Task
        """

        for data in cls._load(task_id=task_id):
            return cls(**data)

    @classmethod
    def load_tasks(cls, *args, **kwds):
        """
        Load task fields listed in args according to condition in kwds from database

        :param args: field names to select
        :type args: tuple

        :param kwds: condition for select statement
        :type kwds: dict

        :returns: Task object
        :rtype: generator
        """

        for data in cls._load(*args, **kwds):
            yield cls(**data)

    @classmethod
    def create(cls, name, args=None, kwds=None, soft_timeout=None, hard_timeout=None):
        """
        Create Task object

        :returns: Task object
        :rtype: Task
        """

        args = args or ()
        kwds = kwds or {}
        soft_timeout = soft_timeout or SOFT_TIMEOUT
        hard_timeout = hard_timeout or HARD_TIMEOUT
        data = {
            'task_id': uuid.uuid4().hex,
            'name': name,
            'args': args,
            'kwds': kwds,
            'state': 'pending',
            'soft_timeout': soft_timeout,
            'hard_timeout': hard_timeout,
        }
        return cls(**data)

    @classmethod
    def delete(cls, task_id):
        """Delete task from database"""

        query = "DELETE FROM tasks WHERE task_id=?"
        conn = get_connection()
        curs = conn.cursor()
        curs.execute(query, (task_id,))

    def __init__(self, **kwds):
        super(Task, self).__init__()
        for k in self.schema:
            if k in kwds:
                v = kwds[k]
            else:
                v = None
            if k in ['args', 'kwds'] and not isinstance(v, basestring):
                v = json.dumps(v)
            super(Task, self).__setitem__(k, v)

    def in_db(self):
        """
        :returns: is task in database
        :rtype: bool
        """

        for data in self._load('task_id', task_id=self['task_id']):
            return True
        return False

    def reset(self):
        """
        Reset task to pending state and save into database
        Use tmp object for thread safety
        """

        tmp = copy.copy(self)
        tmp['state'] = 'pending'
        tmp['result'] = None
        tmp['traceback'] = None
        tmp['start_date'] = None
        tmp['end_date'] = None
        tmp['worker_id'] = None
        self.update(tmp)

    def acquire(self, worker_id):
        """
        Acquire task
        Use tmp object for thread safety
        """

        self.load()
        tmp = copy.copy(self)
        if tmp['state'] != 'pending':
            return False
        tmp['state'] = 'running'
        tmp['worker_id'] = worker_id
        tmp['start_date'] = datetime.datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
        tmp.save()
        self.update(tmp)
        return True

    def insert(self):
        """Insert task into database"""

        query = "INSERT INTO tasks VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
        conn = get_connection()
        curs = conn.cursor()
        curs.execute(query, (self['task_id'], self['name'], self['args'],
                             self['kwds'], self['state'], self['result'],
                             self['traceback'], self['start_date'], self['end_date'],
                             self['worker_id'], self['soft_timeout'], self['hard_timeout']))

    def load(self, *args):
        """Update local task object from database"""

        for data in self._load(*args, task_id=self['task_id']):
            self.update(data)
            break

    def save(self, *args):
        """
        Update database from local object

        :param args: field names to save
        :type args: tuple
        """

        args = args or tuple(self.schema)
        query = "UPDATE tasks SET "
        for name in args:
            query += '%s=?, ' % name
        query = query[:-2] + ' WHERE task_id=?'
        values = tuple(self[name] for name in args) + (self['task_id'],)
        conn = get_connection()
        curs = conn.cursor()
        curs.execute(query, values)

    def set_result(self, result):
        self['result'] = json.dumps(result)
        self['state'] = 'completed'

    def set_exception(self, exc, trace=None):
        self['result'] = json.dumps({
            'exc_type': '%s.%s' % (exc.__class__.__module__, exc.__class__.__name__),
            'exc_message': str(exc),
            'exc_data': exc.__dict__,
        })
        self['end_date'] = datetime.datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
        if trace:
            self['traceback'] = ''.join(traceback.format_tb(trace))
        self['state'] = 'failed'


class IPCServer(threading.Thread):

    __metaclass__ = ABCMeta

    def __init__(self, address, authkey):
        self.address = address
        self.authkey = authkey
        self.listener = None
        self.cls_name = '{}.{}'.format(self.__module__, self.__class__.__name__)
        self._terminate = threading.Event()
        super(IPCServer, self).__init__()
        self.daemon = True

    def run(self):
        LOG.debug('{} started'.format(self.cls_name))

        while not self._terminate.is_set():
            try:
                if sys.platform != 'win32' and os.path.exists(self.address):
                    LOG.debug('Removing {}'.format(self.address))
                    os.remove(self.address)

                self.listener = multiprocessing.connection.Listener(self.address,
                                                                    authkey=self.authkey)
                try:
                    while not self._terminate.is_set():
                        conn = self.listener.accept()
                        try:
                            LOG.debug('{} new connection'.format(self.cls_name))
                            self.handle(conn)
                        finally:
                            conn.close()
                finally:
                    self.listener.close()
            except:
                if isinstance(sys.exc_info()[1], KeyboardInterrupt) or self._terminate.is_set():
                    break
                else:
                    LOG.exception('{} error: {}'.format(self.cls_name, sys.exc_info()[:2]))
                    time.sleep(ERROR_SLEEP)

        if sys.platform != 'win32' and os.path.exists(self.address):
            os.remove(self.address)
        LOG.debug('{} exited'.format(self.cls_name))

    def terminate(self):
        """Naming same as multiprocessing.Process.terminate"""

        try:
            self._terminate.set()
            if sys.platform == 'win32':
                try:
                    conn = multiprocessing.connection.Client(self.address, authkey=self.authkey)
                except WindowsError:
                    pass
            else:
                self.listener._listener._socket.shutdown(socket.SHUT_RDWR)
        except:
            LOG.exception('{} terminate error: {}'.format(self.cls_name, sys.exc_info()[:2]))

    @abstractmethod
    def handle(self, conn):
        return


class PushServer(IPCServer):

    def __init__(self, queue, *args, **kwds):
        super(PushServer, self).__init__(*args, **kwds)
        self.queue = queue

    def handle(self, conn):
        worker_id = conn.recv()
        while not self._terminate.is_set():
            try:
                task = self.queue.get(timeout=1)
            except Queue.Empty:
                continue
            if task.acquire(worker_id):
                LOG.debug('{} send {}'.format(self.cls_name, task))
                conn.send(task)
            break


class PullServer(IPCServer):

    def handle(self, conn):
        task = conn.recv()
        if task['state'] in ('completed', 'failed'):
            task['end_date'] = datetime.datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
        LOG.debug('{} received {}'.format(self.cls_name, task))
        task.save()
        try:
            Executor._ready_events[task['task_id']].set()
            del Executor._ready_events[task['task_id']]
        except KeyError:
            pass


class Executor(object):

    _workers = {}
    _ready_events = {}

    def __init__(self, max_workers=None, soft_timeout=None, hard_timeout=None, task_modules=None):
        self._push_queue = Queue.Queue()
        self._push_server_address = os.path.join(push_server_base_address,
                                                 'szr_%s' % uuid.uuid4().hex)
        self._pull_server_address = os.path.join(pull_server_base_address,
                                                 'szr_%s' % uuid.uuid4().hex)
        self._ipc_authkey = str(uuid.uuid4().hex)
        self._push_server = None
        self._pull_server = None
        self._max_workers = max_workers or MAX_WORKERS
        self._soft_timeout = soft_timeout or SOFT_TIMEOUT
        self._hard_timeout = hard_timeout or HARD_TIMEOUT
        self._state = 'stopped'
        self._state_lock = threading.Lock()
        self._poll_thread = None

        task_modules = task_modules or ()
        for module in task_modules:
            __import__(module)

        # windows only
        self._job_obj = None

    @property
    def workers(self):
        return self.__class__._workers.get(self, [])

    @workers.setter
    def workers(self, value):
        self.__class__._workers[self] = value

    def _launch_worker(self, wait=False):
        worker = Worker(self._push_server_address, self._pull_server_address, self._ipc_authkey)
        worker.start()

        if sys.platform == 'win32':
            permissions = win32con.PROCESS_TERMINATE | win32con.PROCESS_SET_QUOTA
            handle = win32api.OpenProcess(permissions, False, worker.pid)
            try:
                win32job.AssignProcessToJobObject(self._job_obj, handle)
            finally:
                win32api.CloseHandle(handle)

        if wait:
            worker.wait_start()
        self.workers.append(worker)
        return worker

    def _check_workers(self):
        self.workers = [worker for worker in self.workers if worker.is_alive()]
        for i in range((self._max_workers - len(self.workers)) or 0):
            self._launch_worker()

    def _validate_running_tasks(self):
        workers_ids = [worker.worker_id for worker in self.workers if worker.is_alive()]
        for task in Task.load_tasks(state='running'):
            if str(task['worker_id']) in workers_ids:
                continue
            LOG.debug('Found invalid task {}, reset it'.format(task))
            task.reset()
            task.save()
            if self._state == 'started':
                self._push_queue.put(task)

    def _poll(self):
        """Separate thread"""

        while True:
            try:
                with self._state_lock:
                    # use lock to avoid simultaneously starting and stopping workers from different
                    # threads
                    if self._state == 'stopped':
                        return
                    self._check_workers()

                self._validate_running_tasks()

                time.sleep(EXECUTOR_POLL_SLEEP)
            except:
                LOG.exception('Executor poll error: {}'.format(sys.exc_info()[:2]))
                time.sleep(ERROR_SLEEP)

    def start(self):
        if self._state != 'stopped':
            return

        LOG.debug('Starting Executor')

        self._state = 'starting'

        self._push_server = PushServer(self._push_queue, self._push_server_address,
                                       self._ipc_authkey)
        self._pull_server = PullServer(self._pull_server_address, self._ipc_authkey)
        self._push_server.start()
        self._pull_server.start()

        if sys.platform == 'win32':
            self._job_obj = win32job.CreateJobObject(None, 'Bollard')
            ex_info = win32job.QueryInformationJobObject(self._job_obj,
                                                         win32job.JobObjectExtendedLimitInformation)
            ex_info['BasicLimitInformation'][
                'LimitFlags'] = win32job.JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE
            win32job.SetInformationJobObject(
                self._job_obj, win32job.JobObjectExtendedLimitInformation, ex_info)

        self._validate_running_tasks()

        for task in Task.load_tasks(state='pending'):
            self._push_queue.put(task)

        # wait for push and pull server before start workers
        while not self._push_server.is_alive() or not self._pull_server.is_alive():
            time.sleep(0.1)

        self._poll_thread = threading.Thread(target=self._poll)
        self._poll_thread.daemon = True
        self._poll_thread.start()

        self._state = 'started'
        LOG.debug('Executor started')

    def stop(self):
        if self._state != 'started':
            return

        LOG.debug('Stopping Executor')

        with self._state_lock:
            # use lock to avoid simultaneously starting and stopping workers from different
            # threads
            self._state = 'stopped'
            for worker in self.workers:
                worker.stop()

        self.workers = []

        self._push_server.terminate()
        self._pull_server.terminate()

        while self._push_server.is_alive() or self._pull_server.is_alive():
            time.sleep(0.1)
        while self._poll_thread.is_alive():
            time.sleep(0.1)

        LOG.debug('Executor stopped')

    def apply_async(self, task_name, args=None, kwds=None, soft_timeout=None, hard_timeout=None):
        if isinstance(task_name, types.FunctionType):
            m, n = task_name.__module__, task_name.__name__
            task_name = '%s.%s' % (m, n)
        soft_timeout = soft_timeout or self._soft_timeout
        hard_timeout = hard_timeout or self._hard_timeout
        task = Task.create(task_name, args=args, kwds=kwds, soft_timeout=soft_timeout,
                           hard_timeout=hard_timeout)
        LOG.debug('Apply task {}'.format(task))
        task.insert()

        if self._state == 'started':
            self._push_queue.put(task)

        ready_event = threading.Event()
        self.__class__._ready_events[task['task_id']] = ready_event
        async_result = AsyncResult(task)
        return async_result

    @classmethod
    def revoke(cls, task_id):
        """
        Only running tasks can be revoked
        """

        for task in Task.load_tasks(task_id=task_id):
            if task['state'] != 'running':
                continue
            try:
                LOG.debug('Killing task {}'.format(task_id))
                task.set_exception(TaskKilledError())
                task.save()
            finally:
                for executor, workers in Executor._workers.iteritems():
                    for worker in workers:
                        worker.lock()
                        try:
                            if str(task['worker_id']) == worker.worker_id:
                                worker.terminate()
                                break
                        finally:
                            worker.unlock()


prog = re.compile(r'.*\nState:\t*(.) *\((.*)\)\n.*')


def is_alive(pid):
    if sys.platform == 'win32':
        if pid not in win32process.EnumProcesses():
            return False
    else:
        try:
            with open('/proc/%d/status' % pid, 'r') as f:
                text = f.read()
                match = prog.match(text)
                assert match.groups()[0] != 'Z'
        except (IOError, AttributeError, AssertionError):
            return False
    return True


class Worker(multiprocessing.Process):

    def __init__(self, push_server_addr, pull_server_addr, authkey):
        self._push_server_address = push_server_addr
        self._pull_server_address = pull_server_addr
        self._ipc_authkey = authkey
        self._started_ev = Event()
        self._lock_ev = Event()
        self._task_lock = None
        self._worker_id = None
        self._ppid = None
        self._soft_timeout_called = False

        self.task = None
        super(Worker, self).__init__()

    @property
    def worker_id(self):
        return str(self._worker_id)

    @property
    def ppid(self):
        return self._ppid

    def lock(self):
        """Lock worker to prevent getting new task"""

        self._lock_ev.clear()

    def unlock(self):
        """Unlock worker to allow getting new task"""

        self._lock_ev.set()

    def _signal_handler(self, signum, frame):
        if signum == signal.SIGHUP:
            raise ParentProcessError()
        if signum == signal.SIGUSR1:
            raise SoftTimeLimitExceeded()

    def _get_task(self):
        conn = multiprocessing.connection.Client(self._push_server_address,
                                                 authkey=self._ipc_authkey)
        try:
            conn.send(self.worker_id)
            self.task = conn.recv()
        finally:
            conn.close()

        self._soft_timeout_called = False

    def _return_result(self):
        conn = multiprocessing.connection.Client(self._pull_server_address,
                                                 authkey=self._ipc_authkey)
        try:
            conn.send(self.task)
        finally:
            conn.close()

    def _supervisor(self):
        while self._started_ev.is_set():
            # 1. check parent process
            if not is_alive(self.ppid):
                msg = 'Worker {} supervisor error: parent process is not alive'.format(
                    self.worker_id)
                LOG.error(msg)
                self.terminate()

            # 2. check task timeouts
            self._task_lock.acquire()
            try:
                if self.task:
                    dt_format = '%Y-%m-%d %H:%M:%S'
                    dt_start = datetime.datetime.strptime(self.task['start_date'], dt_format)
                    # 2.1 hard timeout
                    hard_delta = datetime.timedelta(seconds=float(self.task['hard_timeout']))
                    if datetime.datetime.utcnow() > dt_start + hard_delta:
                        msg = 'Hard timeout has been occurred for task {}'.format(self.task)
                        LOG.warning(msg)
                        self.task.set_exception(HardTimeLimitExceeded())
                        self._return_result()
                        self.terminate()
                    # 2.2 soft timeout
                    # only for linux
                    if sys.platform == 'win32':
                        continue
                    soft_delta = datetime.timedelta(seconds=float(self.task['soft_timeout']))
                    if datetime.datetime.utcnow() > dt_start + soft_delta:
                        if not self._soft_timeout_called:
                            msg = 'Soft timeout has been occurred for task {}'.format(self.task)
                            LOG.warning(msg)
                            os.kill(self.pid, signal.SIGUSR1)
                            self._soft_timeout_called = True
            except:
                msg = 'Worker {} supervisor error: {}'.format(self.worker_id, sys.exc_info()[:2])
                LOG.exception(msg)
                self.terminate()
            finally:
                self._task_lock.release()
                time.sleep(WORKER_SUPERVISOR_SLEEP)

    def _start_supervisor(self):
        supervisor = threading.Thread(target=self._supervisor)
        supervisor.daemon = True
        supervisor.start()
        LOG.debug('Worker {} supervisor started'.format(self.worker_id))

    def run(self):
        try:
            LOG.debug('Worker {} started'.format(self.worker_id))
            if sys.platform != 'win32':
                signal.signal(signal.SIGHUP, self._signal_handler)
                signal.signal(signal.SIGUSR1, self._signal_handler)
                cdll['libc.so.6'].prctl(PR_SET_PDEATHSIG, signal.SIGHUP)

            self._task_lock = threading.Lock()

            self._started_ev.set()
            self._start_supervisor()

            self.unlock()

            while True:
                try:
                    self._lock_ev.wait()
                    self._get_task()

                    msg = 'Worker {} get task: {}'.format(self.worker_id, self.task)
                    LOG.debug(msg)

                    result = self._execute_task()
                    with self._task_lock:
                        self.task.set_result(result)
                except ParentProcessError:
                    raise
                except KeyboardInterrupt:
                    with self._task_lock:
                        if self.task:
                            self.task.reset()
                    return
                except:
                    with self._task_lock:
                        if self.task:
                            if isinstance(sys.exc_info()[0], KeyboardInterrupt):
                                self.task.reset()
                                LOG.debug('Task {} has been interrupted'.format(self.task))
                                return
                            else:
                                self.task.set_exception(sys.exc_info()[1], trace=sys.exc_info()[2])
                                msg = 'Task {} error: {}'.format(self.task, sys.exc_info()[:2])
                                LOG.exception(msg)
                        else:
                            raise
                finally:
                    try:
                        if sys.exc_info() and isinstance(sys.exc_info()[0], ParentProcessError):
                            # don't return result for ParentProcessError because PullServer has
                            # already died
                            sys.exc_clear()
                        else:
                            with self._task_lock:
                                if self.task:
                                    self._return_result()
                    finally:
                        with self._task_lock:
                            self.task = None
        except ParentProcessError:
            LOG.error('Worker {} error: {}'.format(self.worker_id, sys.exc_info()[:2]))
        except:
            LOG.exception('Worker {} error: {}'.format(self.worker_id, sys.exc_info()[:2]))
        finally:
            LOG.debug('Worker {} exited'.format(self.worker_id))
            self._worker_id = None
            self._started_ev.clear()

    def _execute_task(self):
        msg = 'Worker {} executing task with task_id {}'.format(
            self.worker_id, self.task['task_id'])
        LOG.debug(msg)
        f = __tasks__[self.task['name']]
        args = json.loads(str(self.task['args']))
        kwds = json.loads(str(self.task['kwds']))
        return f(*args, **kwds)

    def start(self):
        assert not self.is_alive(), 'Worker already running'
        self._worker_id = uuid.uuid4().hex
        LOG.debug('Starting worker {}'.format(self.worker_id))
        self._ppid = os.getpid()
        super(Worker, self).start()

    def stop(self):
        LOG.debug('Stoping worker {}'.format(self.worker_id))
        if self.is_alive():
            try:
                stop_timeout = 5
                self.wait_start(timeout=stop_timeout)
                os.kill(self.pid, SIGINT)
                self.join(timeout=stop_timeout)
            except TimeoutError:
                LOG.warning('Stopping worker {} error: timeout'.format(self.worker_id))
                self.terminate()

    def terminate(self):
        LOG.debug('Terminating worker {}'.format(self.worker_id))
        if self.is_alive():
            for pid in util.get_children(self.pid, recursive=True):
                try:
                    os.kill(pid, SIGKILL)
                except OSError as e:
                    if e.errno != 3:  #  No such process
                        raise
            os.kill(self.pid, SIGKILL)
            self.join()

    def wait_start(self, timeout=None):
        if not self._started_ev.wait(timeout=timeout):
            raise TimeoutError()


class AsyncResult(object):

    result_poll_timeout = 1
    ready_event_timeout = 10

    def __init__(self, task):
        """
        :param task: Task instance or task_id
        :type task: Task or str
        """

        if isinstance(task, Task):
            assert task.in_db()
            self._task = task
        elif isinstance(task, basestring):
            self._task = Task(task_id=task)
            self._task.load()
        else:
            raise TypeError('Argument task must be instance of Task class or task_id')
        self._ready_event = Executor._ready_events.get(self._task['task_id'], None)

    def __getattr__(self, name):
        assert name in self._task.schema, "'%s' not in Task schema" % name
        if name not in ['task_id', 'name', 'args', 'kwds']:
            self._task.load()
        return self._task[name]

    def get(self, timeout=None):
        self._task.load()
        remaining_timeout = timeout

        # lool until timeout will occurred or task state will be changed to completed or failed
        while self._task['state'] in ['pending', 'running']:
            if timeout and not remaining_timeout:
                # Timeout has occurred
                break
            if self._ready_event:
                if timeout:
                    curr_timeout = min(self.ready_event_timeout, remaining_timeout)
                else:
                    curr_timeout = self.ready_event_timeout
                self._ready_event.wait(timeout=curr_timeout)
            else:
                if timeout:
                    curr_timeout = min(self.result_poll_timeout, remaining_timeout)
                else:
                    curr_timeout = self.result_poll_timeout
                time.sleep(curr_timeout)
            if timeout:
                remaining_timeout -= curr_timeout
            self._task.load()

        if self._task['state'] not in ['completed', 'failed']:
            raise TimeoutError("Task '%s' get timeout" % self._task['task_id'])

        result = json.loads(str(self._task['result']))

        if self._task['state'] == 'failed':
            cls = util.import_class(result['exc_type'])
            exc = cls(result['exc_message'], **result['exc_data'])
            raise exc

        return result

    def revoke(self):
        Executor.revoke(self._task['task_id'])
