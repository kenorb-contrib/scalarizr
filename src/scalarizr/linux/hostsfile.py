from __future__ import with_statement
import collections


_HostEntry = collections.namedtuple('_HostEntry', 'ipaddr hostname aliases')


class _HostsFile(object):
    # TODO: port code from scalarizr.util.dns
    # TODO: join HostsFile and ScalrHosts into single class
    pass


def hosts():
    return _HostsFile()
