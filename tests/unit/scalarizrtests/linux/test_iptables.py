__author__ = 'vladimir'
 
import mock
 
from scalarizr.linux import iptables
 
 
IPTABLES_LINUX = iptables.linux
 
 
class TestIptables(object):
 
    def setup(self):
        iptables.linux = mock.MagicMock()
 
    def teardown(self):
        iptables.linux = IPTABLES_LINUX
 
    def test_iptables(self):
        kwargs = {
                'append': 'INPUT',
                'protocol': 'tcp',
                'table': 'filter',
                'dport': 80,
        }
        iptables.iptables(**kwargs)
 
        iptables.linux.build_cmd_args.assert_called_once_with(
                executable='/sbin/iptables', long=kwargs)
        iptables.linux.system.assert_called_once_with(
                iptables.linux.build_cmd_args.return_value)
 
    @mock.patch('__builtin__.open')
    def test_iptables_save(self, open):
        # 1
        iptables.iptables_save()
 
        iptables.linux.build_cmd_args.assert_called_once_with(
                executable='/sbin/iptables-save', short=(), long={})
        iptables.linux.system.assert_called_once_with(
                iptables.linux.build_cmd_args.return_value)
 
        # 2
        iptables.linux.build_cmd_args.reset_mock()
        iptables.linux.system.reset_mock()
        open.return_value = type('can_write', (), {"write": None})()
 
        iptables.iptables_save('path')
 
        open.assert_called_once_with('path', 'w+')
        iptables.linux.build_cmd_args.assert_called_once_with(
                executable='/sbin/iptables-save', short=(), long={})
        iptables.linux.system.assert_called_once_with(
                iptables.linux.build_cmd_args.return_value, stdout=open.return_value)
 
        # 3
        iptables.linux.build_cmd_args.reset_mock()
        iptables.linux.system.reset_mock()
        open.reset_mock()
        file_like = type('can_write', (), {"write": None})()
 
        iptables.iptables_save(file_like)
 
        assert not open.called
        iptables.linux.build_cmd_args.assert_called_once_with(
                executable='/sbin/iptables-save', short=(), long={})
        iptables.linux.system.assert_called_once_with(
                iptables.linux.build_cmd_args.return_value, stdout=file_like)
 
    @mock.patch('__builtin__.open')
    def test_iptables_restore(self, open):
        open.return_value = type('can_write', (), {"write": None})()
 
        iptables.iptables_restore('path')
 
        open.assert_called_once_with('path')
        iptables.linux.build_cmd_args.assert_called_once_with(
                executable='/sbin/iptables-restore', short=(), long={})
        iptables.linux.system.assert_called_once_with(
                iptables.linux.build_cmd_args.return_value, stdin=open.return_value)
 
    @mock.patch('scalarizr.linux.iptables.iptables_save')
    @mock.patch('__builtin__.open')
    def test_save(self, open, iptables_save):
        # 1
        iptables.linux.os.__getitem__.return_value = "RedHat"
 
        iptables.save()
 
        assert not open.called
        iptables.linux.build_cmd_args.assert_called_once_with(
                executable='service', short=["iptables", "save"])
        iptables.linux.system.assert_called_once_with(
                iptables.linux.build_cmd_args.return_value)
 
        # 2
        iptables.linux.build_cmd_args.reset_mock()
        iptables.linux.system.reset_mock()
        iptables.linux.os.__getitem__.return_value = "Debian"
 
        iptables.save()
 
        open.assert_called_once_with('/etc/network/if-pre-up.d/iptables.sh', 'w')
        open.return_value.__enter__.return_value.write.assert_called_once_with(
                '#!/bin/bash\niptables-restore < /etc/iptables.rules')
        iptables_save.assert_called_once_with('/etc/iptables.rules')
 
    @mock.patch('scalarizr.linux.iptables.iptables')
    def test_chains(self, iptables_w):
 
        for predefined_chain in iptables._Chains._predefined:
            assert predefined_chain in iptables.chains
 
        iptables.chains.add('new')
 
        iptables.chains["new"]  # behavior was changed so add() itself would
                                                        # just make the system call, and the chain
                                                        # would appear in iptables.chains container
                                                        # only after the first __getitem__
        assert 'new' in iptables.chains
        iptables.iptables.assert_called_once_with(**{"new-chain": "new"})
 
        iptables.iptables.reset_mock()
 
        iptables.chains.remove('new')
 
        assert 'new' not in iptables.chains
        iptables.iptables.assert_called_once_with(**{"delete-chain": "new"})
 
    @mock.patch('scalarizr.linux.iptables.iptables')
    def test_list(self, iptables_w):
        out = '-P INPUT ACCEPT\n' \
                  '-A INPUT -s 192.168.0.1/32 -d 192.168.0.2/32 -p tcp -m tcp --dport 22 -j ACCEPT\n' \
                  '-A INPUT -i eth1 -m comment --comment "my local LAN" \n'
        iptables.iptables.return_value = (out, '', 0)
 
        res = iptables.list('INPUT')
 
        iptables.iptables.assert_called_once_with(**{"list-rules": "INPUT"})
        assert res == [{
                "source": "192.168.0.1/32",
                "destination": "192.168.0.2/32",
                "protocol": "tcp",
                "match": "tcp",
                "dport": "22",
                "jump": "ACCEPT",
        },
        {
                "in-interface": "eth1",
                "match": "comment",
                "comment": "my local LAN",
        }]
 
    @mock.patch('scalarizr.linux.iptables.INPUT.list')
    @mock.patch('scalarizr.linux.iptables.INPUT.insert')
    @mock.patch('scalarizr.linux.iptables.INPUT.append')
    def test_ensure(self, append_w, insert_w, list_w):
        two_rules = [{
                "source": "192.168.0.1/32",
                "destination": "192.168.0.2/32",
                "protocol": "tcp",
                "match": "tcp",
                "dport": "22",
                "jump": "ACCEPT",
        },
        {
                "in-interface": "eth1",
                "match": "comment",
                "comment": "my local LAN",
        }]
 
        # 1
        iptables.chains["INPUT"].list.return_value = two_rules
 
        iptables.ensure({"INPUT": [two_rules[0]]})
 
        iptables.chains["INPUT"].list.assert_called_once_with()
        assert not iptables.chains["INPUT"].insert.called
        assert not iptables.chains["INPUT"].append.called
 
        #
 
        iptables.chains["INPUT"].list.reset_mock()
        iptables.chains["INPUT"].insert.reset_mock()
        iptables.chains["INPUT"].append.reset_mock()
 
        # 2
 
        iptables.chains["INPUT"].list.return_value = [two_rules[1]]
 
        iptables.ensure({"INPUT": [two_rules[0]]})
 
        iptables.chains["INPUT"].list.assert_called_once_with()
        iptables.chains["INPUT"].insert.assert_called_once_with(None, two_rules[0])
        assert not iptables.chains["INPUT"].append.called
 
