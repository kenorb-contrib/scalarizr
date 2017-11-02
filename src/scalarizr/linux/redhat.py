from __future__ import with_statement
from scalarizr import linux


def chkconfig(**long_kwds):
    return linux.system(linux.build_cmd_args(executable='/sbin/chkconfig',
            long=long_kwds))


def selinuxenabled():
    raise NotImplementedError()


def getsebool(name):
    raise NotImplementedError()


def setsebool(name, persistent=None):
    raise NotImplementedError()
