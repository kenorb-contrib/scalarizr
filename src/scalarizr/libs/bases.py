from __future__ import with_statement

import sys
import types
import inspect


class Observable(object):

    def __init__(self, *args):
        self._listeners = {}
        self._events_suspended = False
        self.define_events(*args)


    def define_events(self, *args):
        for event in args:
            # we can't allow user to define the same event twice and lose subscribers
            if not event in self._listeners:
                self._listeners[event] = list()

    def event_defined(self, event):
        return event in self._listeners
        

    def list_events(self):
        return self._listeners.keys()

    def fire(self, event, *args, **kwargs):
        if not self._events_suspended:
            if self._listeners.has_key(event):
                for ln in self._listeners[event]:
                    ln(*args, **kwargs)


    def on(self, *args, **kwargs):
        """
        Add listener

        1) Add listeners to one event
        obj.on("add", func1, func2, ...)
        2) Add listeners to many events
        obj.on(add=func1, remove=func2, apply=func3, ...)
        """
        if len(args) >= 2:
            event = args[0]
            if not self._listeners.has_key(event):
                raise BaseException("Event '%s' is not defined" % event)
            for listener in args[1:]:
                if not listener in self._listeners[event]:
                    self._listeners[event].append(listener)
        elif kwargs:
            for event in kwargs.keys():
                self.on(event, kwargs[event])

    def un(self, event, listener):
        """
        Remove listener
        """
        if self._listeners.has_key(event):
            if listener in self._listeners[event]:
                self._listeners[event].remove(listener)

    def suspend_events(self):
        self._events_suspended = True

    def resume_events(self):
        self._events_suspended = False


class ConfigDriven(object):

    _config = None

    error_messages = {
            'empty_attr': 'Attribute should be specified: %s',
            'empty_param': 'Parameter should be specified: %s'
    }

    features = {}

    def __init__(self, **kwds):
        arginfo = inspect.getargvalues(inspect.currentframe())
        self._config = dict((arg, arginfo.locals[arg]) for arg in arginfo.args[1:])
        self._config.update(arginfo.locals[arginfo.keywords])


    def config(self):
        return self._dictify(self._config.copy())


    def _dictify(self, data=None):
        if isinstance(data, dict):
            ret = {}
            for key in data:
                ret[key] = self._dictify(data[key])
            return ret
        elif isinstance(data, list) or isinstance(data, tuple):
            ret = [self._dictify(item) for item in data]
        elif type(data) in (str, unicode, bool, int, long, float, types.NoneType):
            ret = data
        elif isinstance(data, ConfigDriven):
            ret = data.config()
        else:
            ret = repr(data)
        return ret


    def __iter__(self):
        for key, value in self.config().items():
            yield (key, value)


    def __setattr__(self, name, value):
        data = self.__dict__ \
                        if name in dir(self) or name[0] == '_' \
                        else self.__dict__['_config']
        data[name] = value


    def __getattr__(self, name):
        if name in self.__dict__['_config']:
            return self.__dict__['_config'][name]
        raise AttributeError(name)


    def __delattr__(self, name):
        if name in self.__dict__['_config']:
            del self.__dict__['_config'][name]
        else:
            raise AttributeError(name)


    def __hasattr__(self, name):
        return name in self.__dict__['_config']

    def _check_attr(self, name):
        assert hasattr(self, name) and getattr(self, name),  \
                        self.error_messages['empty_attr'] % name



class Task(Observable, ConfigDriven):

    def __init__(self, **kwds):
        ConfigDriven.__init__(self, **kwds)
        Observable.__init__(self,
                'start',    # When job is started
                'complete', # When job is finished with success
                'error'     # When job is finished with error
        )
        self.__running = False
        self.__result  = None


    def kill(self):
        if self.__running:
            self._kill()
            self._cleanup()


    def _kill(self):
        pass


    def _cleanup(self):
        pass


    def run(self):
        if self.__running:
            raise Exception('Another operation is running')
        try:
            self.__running = True
            self.fire('start')
            self.__result = self._run()
            self.fire('complete', self.__result)
            return self.__result
        except:
            exc_info = sys.exc_info()
            self.fire('error', exc_info)
            self._cleanup()
            raise exc_info[0], exc_info[1], exc_info[2]
        finally:
            self.__running = False


    def _run(self):
        pass


    @property
    def running(self):
        return self.__running

    def result(self):
        return self.__result
