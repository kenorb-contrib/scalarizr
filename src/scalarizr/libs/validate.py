from __future__ import with_statement
'''
Simple validation decorators

@author: marat

Sample usage:
>> @validate.param('port', type=int)
>> @validate.param('ipaddr', type='ipv4')
>> @validate.param('backend', re=r'^role:\d+$')
>> def foo(port=None, ipaddr=None, backend=None):
...    pass
>>

IMPORTANT!
The current limitation: all function args should have default values
'''

import re
import inspect

import logging
LOG = logging.getLogger(__name__)

MESSAGES = {
        'type': 'Type error(%s expected): %s',
        'empty': 'Empty: %s',
        're': "Doesn't match expression %s: %s",
        'choises': 'Allowed values are %s: %s',
        'unknown_user_type': 'Unknown user type: %s'
}

USER_TYPES = {
        'ipv4': re.compile(r'^(25[0-5]|2[0-4]\d|[0-1]?\d?\d)(\.(25[0-5]|2[0-4]\d|[0-1]?\d?\d)){3}$'),
        'ipv6': re.compile(r'^(?:[0-9a-fA-F]{1,4}:){7}[0-9a-fA-F]{1,4}$')
}

class rule(object):
    re = None
    choises = None
    type = None
    user_type = None

    def __init__(self, type=None, re=None, choises=None):
        #LOG.debug('Class rule.__init__:, type =%s, re=%s, choises=%s', type, re, choises)
        if isinstance(type, basestring):
            if type in USER_TYPES:
                re = USER_TYPES[type]
                self.user_type = type
                type = None
            else:
                raise ValueError(MESSAGES['unknown_user_type'] % (type, ))
        if isinstance(re, basestring):
            re = globals()['re'].compile(re)
        #LOG.debug('re=%s, type=%s, choises=%s',re, type, choises)
        self.re = re
        self.type = type
        self.choises = choises


class param(object):
    names = None
    rule = None
    required = True

    def __init__(self, *names, **kwds):
        '''
        @keyword optional: set names as optional when True.
        @type optional: bool|rule

        @keyword required: set names as required when True.
        @type required: bool|rule

        @keyword re: regular expression to match over
        @type re: str|re

        @keyword choises: value choises list
        @type choies: list

        @keyword type: value type
        @keyword type: type|str
        '''
        #LOG.debug('Class param._init_:, names =%s, kwds=%s', names, kwds)
        self.names = names
        for key in ('optional', 'required'):
            if key in kwds:
                if isinstance(kwds[key], rule):
                    self.rule = kwds[key]
                del kwds[key]
                if key == 'optional':
                    self.required = False
        if not self.rule:
            self.rule = rule(**kwds)
        #LOG.debug('self.rule = `%s`', self.rule.re)
        self.vl = []

    '''
    def __call__(self, fn):
            LOG.debug('Class param.__call__ fn=%s', fn)
            if not isinstance(fn, _func_wrapper):

                    fn = _func_wrapper(fn)
            fn.params.append(self)
            return fn
    '''

    def __call__(self, fn):
        #LOG.debug('Class param.__call__ fn=%s', fn)
        if not hasattr(fn, '_validation_params'):
            fn._validation_params = []
            def wrapper(*args, **kwds):
                asp = inspect.getargspec(fn)
                values = dict(zip(asp.args, (asp.defaults,)))
                values.update(kwds)
                #LOG.debug('fn._validation_params=%s', fn._validation_params[0].names)
                #LOG.debug('asp.defaults=%s', asp.defaults)
                validate(values, fn._validation_params)
                return fn(*args, **kwds)
            wrapper._fn = fn
            wrapper._wrapped = True
        fn._validation_params.append(self)
        return wrapper

'''
class _func_wrapper(object):
        params = None
        def __init__(self, fn):
                LOG.debug('Class _func_wrapper.__init__ fn=%s', fn)
                self.fn = fn
                self.params = []

        def __call__(self, *args, **kwds):
                LOG.debug('Class _func_wrapper.__call__:self type=%s, args =%s, kwds=%s', type(self), args, kwds)
                asp = inspect.getargspec(self.fn)
                values = dict(zip(asp.args, asp.defaults))
                values.update(kwds)
                validate(values, self.params)
                LOG.debug('-> _func_wrapper.__call__:, args =%s, kwds=%s', args, kwds)
                LOG.debug('%s', self.fn)
                #TODO: insert here HAProxyAPI self object as first paramatr in args
                return self.fn(**kwds)
'''

def validate(values, params):
    #LOG.debug('validate:, values =%s, params=%s', values, params)
    for name, value in values.items():
        for param in params:
            if name in param.names:
                if not param.required and not value:
                    break

                rule = param.rule
                if param.required and not value:
                    raise ValueError(MESSAGES['empty'] % (name, ))

                if not rule.type:
                    try:
                        value = str(value)
                        LOG.debug('value = %s', value)
                    except:
                        raise ValueError(MESSAGES['type'] % (rule.type.__name__, name))
                elif rule.type.__name__ == 'int' and isinstance(value, basestring):
                    try:
                        value = int(value)
                    except:
                        raise ValueError(MESSAGES['type'] % (rule.type, '%s, value=%s'%(name,value)))

                if isinstance(value, str) and rule.re and not rule.re.search(value):
                    if rule.user_type:
                        raise ValueError(MESSAGES['type'] % (rule.user_type, name))
                    else:
                        raise ValueError(MESSAGES['re'] % (rule.re.pattern, name))
                if rule.choises and value not in rule.choises:

                    raise ValueError(MESSAGES['choises'] % (str(rule.choises), name))
                if rule.type and not isinstance(value, rule.type):
                    raise ValueError(MESSAGES['type'] % (rule.type.__name__, name))
