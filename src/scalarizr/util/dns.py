from __future__ import with_statement
'''
Created on Sep 28, 2011
'''
from __future__ import with_statement
from collections import namedtuple
import string
import re


HostLine=namedtuple('host', ['ipaddr', 'hostname', 'aliases'])

class Items(list):

    def __getitem__(self, index):
        if isinstance(index, (str, unicode)):
            for item in self:
                if isinstance(item, dict) and item['hostname'] == index:
                    return item
            raise KeyError(index)
        else:
            return super(Items, self).__getitem__(index)


class HostsFile(object):
    FILENAME = '/etc/hosts'

    _hosts = Items()

    def __init__(self, filename=None):
        self.filename = filename or self.FILENAME

    def _reload(self):
        self._hosts = Items()

        fp = open(self.filename, 'r')
        try:
            for line in fp:
                if line.strip() and not line.startswith('#'):
                    line = filter(None, map(string.strip, re.split(r'[\t\s]+', line)))
                    ip, hostname, aliases = line[0], line[1], line[2:]
                    try:
                        self._hosts[hostname]['aliases'].update(set(aliases))
                    except KeyError:
                        self._hosts.append({
                                'ipaddr': ip,
                                'hostname': hostname,
                                'aliases': set(aliases)
                        })
                else:
                    self._hosts.append(line)
        finally:
            fp.close()


    def _flush(self):
        fp = open(self.filename, 'w+')
        for line in self._hosts:
            if isinstance(line, dict):
                line='%s %s %s\n' % (line['ipaddr'], line['hostname'], ' '.join(line['aliases']))
            fp.write(line)
        fp.close()


    def __getitem__(self, hostname):
        self._reload()
        return HostLine(**self._hosts[hostname])


    def map(self, ipaddr, hostname, *aliases):
        '''
        Updates hostname -> ipaddr mapping and aliases
        @type hostname: str
        @type ipaddr: str
        '''
        assert ipaddr
        assert hostname

        self._reload()
        try:
            host = self._hosts[hostname]
            host['ipaddr'] = ipaddr
            host['aliases'] = set(aliases)
        except KeyError:
            self._hosts.append({
                    'ipaddr': ipaddr,
                    'hostname': hostname,
                    'aliases': set(aliases)
            })
        finally:
            return self._flush()


    def remove(self, hostname):
        '''
        Removes hostname mapping and aliases
        @type hostname:str
        '''
        self._reload()
        self._hosts.remove(self._hosts[hostname])
        self._flush()


    def alias(self, hostname, *aliases):
        '''
        Add hostname alias
        @type hostname: str
        @type *aliases: str or tuple, list
        '''
        self._reload()
        self._hosts[hostname]['aliases'].update(set(aliases))
        self._flush()


    def unalias(self, hostname, *aliases):
        '''
        Removes hostname alias
        @type hostname:str
        @type *aliases: str or tuple, list
        '''
        self._reload()
        for alias in aliases:
            try:
                self._hosts[hostname]['aliases'].remove(alias)
            except KeyError:
                pass
        return self._flush()

    def resolve(self, hostname):
        '''
        Returns ip address
        @type hostname: str
        '''
        self._reload()
        try:
            return self._hosts[hostname]['ipaddr']
        except KeyError:
            pass

    def get(self, hostname):
        '''
        Returns namedtuple(ipaddr, hostname, aliases)
        @type hostname:str
        '''
        try:
            return self[hostname]
        except KeyError:
            pass


class ScalrHosts:
    BEGIN_SCALR_HOSTS       = '# begin Scalr hosts'
    END_SCALR_HOSTS         = '# end Scalr hosts'
    HOSTS_FILE_PATH         = '/etc/hosts'

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
        with open(cls.HOSTS_FILE_PATH) as f:
            hosts = f.readlines()

            for i in range(len(hosts)):
                host_line = hosts[i].strip()
                if host_line == cls.BEGIN_SCALR_HOSTS:
                    while True:
                        i += 1
                        try:
                            host_line = hosts[i].strip()
                            if host_line == cls.END_SCALR_HOSTS:
                                return ret
                            addr, hostname = host_line.split(None, 1)
                            ret[hostname.strip()] = addr
                        except IndexError:
                            return ret

        return ret

    @classmethod
    def _write(cls, scalr_hosts):

        with open(cls.HOSTS_FILE_PATH) as f:
            host_lines = f.readlines()

        hosts = (x.strip() for x in host_lines)
        old_hosts = []

        for host in hosts:
            if host == cls.BEGIN_SCALR_HOSTS:
                while True:
                    try:
                        hostline = hosts.next()
                        if hostline == cls.END_SCALR_HOSTS:
                            break
                    except StopIteration:
                        break
            elif host != cls.END_SCALR_HOSTS:
                old_hosts.append(host)

        with open(cls.HOSTS_FILE_PATH, 'w') as f:
            for old_host in old_hosts:
                f.write('%s\n' % old_host)

            f.write('%s\n' % cls.BEGIN_SCALR_HOSTS)
            for hostname, addr in scalr_hosts.iteritems():
                f.write('%s\t%s\n' % (addr, hostname))
            f.write('%s\n' % cls.END_SCALR_HOSTS)
