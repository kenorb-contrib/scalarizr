__author__ = 'spike'
 
import sys
import logging
import threading
import Queue
 
LOG = logging.getLogger(__name__)
 
 
 
class EventMgr(object):
 
    events = dict()
    waitlist = dict()
 
    def add_listener(self, event, fn):
        if not event in self.events:
            self.events[event] = list()
        self.events[event].append(fn)
 
    def wait(self, event, timeout=None, fn=None):
        """
        wait for specific event to happen.
 
        If fn argument is passed, it must be callable, which accept 1 argument of Event type. It will be
        used as callback for event, function's result will be returned as a result:
 
            is_first = wait(Event(event='queryenv', method='list-roles'), timeout=120,
                                fn=lambda ev: ev.server.index == 1)
        """
        t_event = threading.Event()
        queue = fn and Queue.Queue() or None
        if not isinstance(event, Event):
            event = Event(**event)
        LOG.debug('Wait for event: %s' % event.cond)
 
        # TODO: Multiple threads could wait for single event ?
        self.waitlist[event] = (t_event, queue, fn)
        try:
            t_event.wait(timeout)
            if not t_event.isSet():
                raise Exception('Timeout occured while waiting for breakpoint')
 
            if fn:
                res = queue.get()
                if res['status'] == 'error':
                    e_t, e_v, e_tb = res['error']
                    raise e_t, e_v, e_tb
                else:
                    return res['result']
        finally:
            del self.waitlist[event]
 
    def notify(self, event_to_apply):
        """
        Notifies listeners that specified event has happened. First, it notifies those listeners who wait
        for event with timeouts (using `wait` method). After that, notify all other listeners.
        """
 
        # Find matching listeners before any notifications, since event can be changed during notifications
        to_notify = list()
        for event, fn in self.events.iteritems():
            if event in event_to_apply:
                to_notify.append(fn)
 
        # TODO: first - set all thread_events, second - run callbacks
        # Notify listeners who uses `wait` method, run callbacks
        for event, tevent_and_queue_and_maybe_fn in self.waitlist.iteritems():
            if event in event_to_apply:
                tevent, queue, fn = tevent_and_queue_and_maybe_fn
                tevent.set()
                if fn is not None:
                    try:
                        queue.put(dict(status='ok', result=fn(event_to_apply)))
                    except:
                        queue.put(dict(status='error', error=sys.exc_info()))
 
        # Notify `listener` wrapped functions in spies
        for fn_list in to_notify:
            for fn in fn_list:
                fn(event_to_apply)
 
 
class Event(object):
 
    def __init__(self, **cond):
        bp = cond.copy()
        for key in ('source', 'target'):
            if bp.get(key):
                if '.' in bp[key]:
                    bp[key + '_behavior'], bp[key + '_index'] = bp[key].split('.')
                else:
                    bp[key + '_behavior'] = bp[key]
                del bp[key]
        self.cond = bp
 
    def __getitem__(self, item):
        return self.cond[item]
 
    def __contains__(self, test_cond):
        test_event = test_cond if isinstance(test_cond, Event) else Event(test_cond)
        for k, v in test_event.cond.iteritems():
            k, attr = k.split('.', 1) if '.' in k else (k, None)
            # TODO: use wildcards for strings, add lambdas support
            if not k in self.cond:
                return False
            self_value = self.cond[k] if not attr else getattr(self.cond[k], attr)
            if callable(v):
                if not v(self_value):
                    return False
            else:
                if self_value != v:
                    return False
        return True
 
    def __getattr__(self, item):
        try:
            return self.cond[item]
        except KeyError:
            raise AttributeError(item)
 
 
def listener(*args, **kwds):
    if len(args) == 1 and not kwds:
        kwds = args[0]
    def wrapper(fn):
        bp = Event(**kwds)
        fn._events = bp
        return fn
    return wrapper
 
