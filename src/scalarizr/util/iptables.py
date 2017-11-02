'''
Created on Jul 21, 2010

@author: Dmytro Korsakov
'''

from scalarizr.util import system2, UtilError
from scalarizr import linux

import os
import logging

LOG = logging.getLogger(__name__)

P_TCP = "tcp"
P_UDP = "udp"
P_UDPLITE = "udplite"
P_ICMP = "icmp"
P_ESP = "esp"
P_AH = "ah"
P_SCTP = "sctp"
P_ALL = "all"
PROTOCOLS = (P_TCP, P_UDP, P_UDPLITE, P_ICMP, P_ESP, P_AH, P_SCTP, P_ALL)
CHKCONFIG = '/sbin/chkconfig'

class RuleSpec(object):
    specs = None

    def __init__(self, protocol=None, source=None, destination=None,
                            inint=None, outint=None, sport = None, dport = None, jump=None, custom=None):

        self.specs = {}
        self.specs['-p'] = protocol
        self.specs['-s'] = source
        self.specs['-d'] = destination
        self.specs['-i'] = inint
        self.specs['-o'] = outint
        self.specs['-j'] = jump
        self.specs['--sport'] = sport
        self.specs['--dport'] = dport
        self.specs['custom'] = custom


    def __str__(self):
        rule_spec = ''
        specs = [self.specs['-p'], self.specs['-s'], self.specs['-d'], self.specs['-i'], \
                                self.specs['-o'], self.specs['--sport'], self.specs['--dport'], self.specs['-j']]
        keys = ('-p', '-s', '-d', '-i', '-o', '--sport', '--dport', '-j')

        for item in range(0, len(specs)):
            if specs[item] not in (None, 'custom'):
                rule_spec +=' ! %s %s' % (keys[item], specs[item]) if is_inverted(specs[item]) \
                                else ' %s %s' % (keys[item],specs[item])
        if self.specs['custom']:
            rule_spec += self.specs['custom']
        return str(rule_spec)


    def __eq__(self, other):
        p = self.specs['-p'] == other.specs['-p'] or \
                (not self.specs['-p'] and other.specs['-p']=='ALL') or \
                (not other.specs['-p'] and self.specs['-p']=='ALL')

        s = self.specs['-s'] == other.specs['-s'] or \
                (not self.specs['-s'] and other.specs['-s']=='0.0.0.0/0') or \
                (not other.specs['-s'] and self.specs['-s']=='0.0.0.0/0')

        d = (self.specs['-d'] == other.specs['-d']) or \
                (not self.specs['-d'] and other.specs['-d']=='0.0.0.0/0') or \
                (not other.specs['-d'] and self.specs['-d']=='0.0.0.0/0')

        i = self.specs['-i'] == other.specs['-i']
        o = self.specs['-o'] == other.specs['-o']
        j = self.specs['-j'] == other.specs['-j']
        dport = self.specs['--dport'] == other.specs['--dport']
        sport = self.specs['--sport'] == other.specs['--sport']

        if p and s and d and i and o and j and dport and sport:
            return True
        else:
            return False

def is_inverted(param):
    return type(param) == tuple and len(param) > 1 and not param[1]

class IpTables(object):
    executable = None

    def __init__(self, executable=None):
        self.executable = executable or "/sbin/iptables"

    def append_rule(self, rule_spec, chain='INPUT'):
        rule = "%s -A %s%s" % (self.executable, chain, str(rule_spec))
        system2(rule, shell=True)

    def insert_rule(self, rule_num, rule_spec, chain='INPUT'):
        if not rule_num:
            rule_num = ''
        rule = "%s -I %s %s%s" % (self.executable, chain, str(rule_num), str(rule_spec))
        system2(rule, shell=True)

    def delete_rule(self, rule_spec, chain='INPUT'):
        rule = "%s -D %s%s" % (self.executable, chain, str(rule_spec))
        system2(rule, shell=True)

    def list_rules(self, chain='INPUT'):
        table = system2('%s --line-numbers -nvL %s' % (self.executable, chain), shell=True)[0]

        list = table.splitlines()
        rules = []
        for line in list:
            if line.find("destination")==-1 and not line.startswith('Chain') and line.strip():
                row = line.split()
                row.reverse()
                num = row.pop()
                pkts = row.pop()
                bytes = row.pop()

                for option in range(1,len(row)):
                    if row[option].startswith('!'):
                        row[option] = (row[option][1:],False)
                    elif row[option] in ('--','*'):
                        row[option] = None
                rule = RuleSpec()

                last = row.pop()
                if last not in PROTOCOLS:
                    rule.specs['-j'] = last
                    rule.specs['-p'] = row.pop()

                else:
                    rule.specs['-p'] = last
                opt = row.pop()
                rule.specs['-i'] = row.pop()
                rule.specs['-o'] = row.pop()
                rule.specs['-s'] = row.pop()
                rule.specs['-d'] = row.pop()
                if len(row):
                    for spec in row:
                        if spec.startswith('dpt'):
                            rule.specs['--dport'] = spec.split(':')[1]
                        if spec.startswith('spt'):
                            rule.specs['--sport'] = spec.split(':')[1]
                rules.append((rule, num))
        return rules

    def flush(self, chain='INPUT'):
        rule = '%s -F %s' % (self.executable, chain)
        system2(rule, shell=True)

    def usable(self):
        return os.access(self.executable, os.X_OK)

    def enabled(self):
        if linux.os.redhat_family:
            return self._chkconfig()
        else:
            return self.usable()

    def _chkconfig(self):
        '''
        returns True if iptables is enabled on any runlevel
        redhat-based only
        '''

        if not os.path.exists(CHKCONFIG):
            raise UtilError('chkconfig not found')
        out, err, retcode = system2([CHKCONFIG, '--list', 'iptables'])
        if err:
            raise UtilError(str(err))
        if out:
            raw = out.split('\n')
            for row in raw:
                if row:
                    data = row.split('\t')
                    if len(data) == 8:
                        service = data.pop(0).strip()
                        levels = []
                        for level in data:
                            levels.append(True if 'on' in level else False)
                        if len(levels) == 7 and service=='iptables' and any(levels):
                            return True
        return False


def _is_rule_not_exist(jump, port, protocol):
    '''raise exception if current rule exist'''
    jump, port, protocol = str(jump).strip(), str(port).strip(), str(protocol).strip()
    for rule in IpTables().list_rules():
        if port == rule[0].specs['--dport'] and protocol == rule[0].specs['-p'] \
                        and jump == rule[0].specs['-j']:
            raise Exception('Rule `%s` already exist' % rule[0])


def insert_rule_once(jump, port, protocol):
    '''add rule in iptables if it not exist'''
    _is_rule_not_exist(jump, port, protocol)
    ipt = IpTables()
    if ipt.usable() and protocol in PROTOCOLS:
        rspec = RuleSpec(dport=port, jump=jump, protocol=protocol)
        ipt.insert_rule(None, rule_spec=rspec)
        LOG.debug('Rule `%s` added to iptables rules', rspec)
    else:
        raise Exception('protocol `%s` is not known. It must be one of `%s`' %
                (protocol,      PROTOCOLS) if ipt.usable() else 'IpTables is not usable')


def remove_rule_once(jump, port, protocol):
    '''remove rule from iptables'''
    try:
        _is_rule_not_exist(jump, port, protocol)
        raise Exception('Rule for port=`%s`, protocol=`%s`, jump=`%s` '\
                'not exist. It can`t be removed.' % (port, protocol, jump))
    except:
        IpTables().delete_rule(rule_spec = RuleSpec(dport=port, jump=jump, protocol=protocol))
