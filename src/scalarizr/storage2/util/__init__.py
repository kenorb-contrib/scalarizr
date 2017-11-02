from __future__ import with_statement


def build_linux_args(short=None, long=None):
    short = short or []
    long = long or {}

    ret = list(short)
    for key, value in long.items():
        if value == True:
            ret.append(key)
        else:
            ret += ['--%s' % key.replace('_', '-'), value]
    return ret
