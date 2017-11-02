from __future__ import with_statement
'''
Created on Aug 12, 2010

@author: marat
'''

import os

def validate(*validators):
    def g(f):
        def i(*args):
            v = args[-1]
            for fn in validators:
                fn(v)
            f(*args)
        return i
    return g

def not_empty(v):
    if not v:
        raise ValueError('empty value')

def executable(v):
    file_exists(v)
    if not os.access(v, os.X_OK):
        raise ValueError('file %s is not executable' % (v,))

def file_exists(v):
    not_empty(v)
    if not os.access(v, os.F_OK):
        raise ValueError("file %s doesn't exists" % (v,))

def uuid4(v):
    not_empty(v)
    from uuid import UUID
    UUID('{%s}' % (v,))

def base64(v):
    try:
        import binascii
        binascii.a2b_base64(v)
    except binascii.Error, e:
        raise ValueError('badly base64-encoded string: %s' % (e))

class inrange:
    low = None
    high = None
    def __init__(self, low, high):
        self.low = low
        self.high = high

    def __call__(self, v):
        v = int(v)
        if not (v >= self.low and v <= self.high):
            raise ValueError('%s is not in range %s..%s' % (v, self.low, self.high))

class portnumber(inrange):
    def __init__(self):
        inrange.__init__(self, 1, 65535)
