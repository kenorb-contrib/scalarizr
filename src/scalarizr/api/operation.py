
import uuid
import sys
import threading
import time
import logging
import traceback
import datetime

from scalarizr import rpc
from scalarizr.node import __node__
from scalarizr.util import Singleton


LOG = logging.getLogger(__name__)

class OperationError(Exception):
    pass

class AlreadyInProgressError(OperationError):
    pass

class OperationNotFoundError(OperationError):
    pass

class OperationAPI(object):
    """
    Basic API to control operations.

    Namespace::

        operation
    """

    __metaclass__ = Singleton

    def __init__(self):
        self._ops = {}
        self.rotate_thread = threading.Thread(
            name='Rotate finished operations', 
            target=OperationAPI.rotate_runnable
        )
        self.rotate_thread.setDaemon(True)
        self.rotate_thread.start()

    @rpc.query_method
    def list(self):
        """
        Returns list of all operations currently executing 
        and executed since Scalarizr start

        :rtype: list
        """
        keys = ('id', 'name', 'status', 'start_date', 'finish_date')
        retval = []
        for op in self._ops.values():
            op = op.serialize()
            retval.append(dict(zip(keys, [op[k] for k in keys])))
        return retval


    @rpc.query_method
    def result(self, operation_id=None):
        """
        Returns result of an operation by operation ID.

        :param operation_id: Operation ID
        :type operation_id: str
        """
        return self.get(operation_id).serialize()

    @rpc.command_method
    def cancel(self, operation_id=None):
        """
        Cancels operation.

        :param operation_id: Operation ID
        :type operation_id: str
        """
        self.get(operation_id).cancel()

    @rpc.query_method
    def has_in_progress(self):
        return bool(self.find(status='in-progress'))

    def create(self, name, func, **kwds):
        op = Operation(name, func, **kwds)
        self._ops[op.operation_id] = op
        return op

    def get(self, operation_id):
        try:
            return self._ops[operation_id]
        except KeyError:
            msg = "'{0}' not found".format(operation_id)
            raise OperationNotFoundError(msg)

    def remove(self, operation_id):
        del self._ops[operation_id]

    def find(self, name=None, finished_before=None, status=None, exclusive=None):
        if name:
            ret = [op for op in self._ops.values() if op.name == name]
        else:
            ret = self._ops.values()
        if finished_before:
            now = time.time()
            ret = [op for op in ret 
                    if op.finished_at and now - op.finished_at > finished_before]
        if status:
            if status == 'finished':
                ret = [op for op in ret if op.finished]
            else:
                ret = [op for op in ret if op.status == status]
        if exclusive:
            ret = [op for op in ret if op.exclusive]
        return ret

    def run(self, name, func, async=True, **kwds):
        op = self.create(name, func, **kwds)
        if async:
            return op.run_async()
        else:
            return op.run()

    @classmethod
    def rotate_runnable(cls):
        api = cls()
        one_day = 86400
        two_days = one_day * 2
        while True:
            time.sleep(one_day)
            LOG.debug('Rotating operations finished older then 2 days')
            for op in api.find(finished_before=two_days):
                api.remove(op.operation_id)



class _LogHandler(logging.Handler):
    def __init__(self, op):
        self.op = op
        logging.Handler.__init__(self, logging.INFO)

    def emit(self, record):
        trace_marker = 'Traceback (most recent call last)'
        msg = self.format(record)
        if trace_marker in msg:
            msg = msg[0:msg.index(trace_marker)].strip()
        self.op.logs.append(msg)


class Operation(object):

    def __init__(self, name, func, func_args=None, func_kwds=None, 
                cancel_func=None, exclusive=False, notifies=True):
        self.operation_id = str(uuid.uuid4())
        self.name = name
        ops = OperationAPI().find(name, status='in-progress', exclusive=True)
        if ops:
            msg = "'{0}' already in progress".format(name)
            raise AlreadyInProgressError(msg, ops[0].operation_id)
        self.func = func
        self.func_args = list(func_args or [])
        self.func_kwds = dict(func_kwds or {})
        self.cancel_func = cancel_func
        self.notifies = notifies
        self.status = 'new'
        self.result = None
        self.logs = []
        self.data = {}
        self.error = None
        self.started_at = None
        self.finished_at = None
        self.exclusive = exclusive
        self.async = False
        self.canceled = False
        self.thread = None
        self.logger = None
        self._init_log()

    def _init_log(self):
        self.logger = logging.getLogger('scalarizr.ops.{0}'.format(self.name))
        hdlr = _LogHandler(self)
        hdlr.setLevel(logging.INFO)
        self.logger.addHandler(hdlr)    

    def _in_progress(self):
        self.status = 'in-progress'
        self.started_at = time.time() 
        try: 
            self._completed(self.func(self, *self.func_args, **self.func_kwds))
            if self.canceled:
                raise Exception('User canceled')
        except:
            self._failed()
        finally:
            self.finished_at = time.time()
            if self.notifies:
                __node__['messaging'].send('OperationResult', body=self.serialize())

    def run(self):
        self._in_progress()
        return self.result

    @property
    def finished(self):
        return self.status in ('failed', 'completed') 

    def run_async(self):
        print 'run_async'
        self.thread = threading.Thread(
            name='Task {0}'.format(self.name), 
            target=self._in_progress
        )
        self.async = True
        self.thread.start()
        return self.operation_id

    def cancel(self):
        self.canceled = True
        if self.cancel_func:
            try:
                self.cancel_func(self)
            except:
                msg = ('Cancelation function failed for '
                        'operation {0}').format(self.operation_id)
                self.logger.exception(msg)

    def _failed(self, *exc_info):
        self.error = exc_info or sys.exc_info()
        self.status = 'failed' if not self.canceled else 'canceled'
        if self.canceled:
            self.logger.warn('Operation "%s" (id: %s) canceled', self.name, self.operation_id)
        else:
            self.logger.error('Operation "%s" (id: %s) failed. Reason: %s', 
                    self.name, self.operation_id, self.error[1], exc_info=self.error)

    def _completed(self, result=None):
        self.result = result
        self.status = 'completed'

    def serialize(self):
        ret = {
            'id': self.operation_id,
            'name': self.name,
            'status': self.status,
            'result': self.result,
            'error': None,
            'trace': None,
            'logs': self.logs,
            'start_date': self.started_at and \
                    datetime.datetime.fromtimestamp(self.started_at).isoformat() or \
                    None,
            'finish_date': self.finished_at and \
                    datetime.datetime.fromtimestamp(self.finished_at).isoformat() or \
                    None
        }
        if self.error:
            ret['error'] = str(self.error[1])
            ret['trace'] = '\n'.join(traceback.format_tb(self.error[2]))
        return ret
