import os
 
import mock
 
from scalarizr import node
from nose.tools import raises, eq_
from nose.plugins.attrib import attr
 
 
 
class TestCompound(object):
    def test_get_plain_key(self):
        store = mock.MagicMock(spec=node.Store)
        store.__getitem__.return_value = 'aaa'
 
        master = node.Compound({'plain_key': store})
 
        assert master['plain_key'] == 'aaa'
        store.__getitem__.assert_called_with('plain_key')
 
 
    def test_get_enum_key(self):
        values = {
                'server_id': '14593',
                'platform': 'ec2'
        }
        store = mock.MagicMock(spec=node.Store)
        store.__getitem__.side_effect = lambda key: values[key]
 
        master = node.Compound({'server_id,platform': store})
 
        eq_(master['server_id'], '14593')
        eq_(master['platform'], 'ec2')
 
    def test_set_enum_key(self):
        store = mock.MagicMock(spec=node.Store)
        master = node.Compound({'mike,andru,luka': store})
 
        master['mike'] = 'story'
        store.__setitem__.assert_called_with('mike', 'story')
 
 
    def test_set_undefined_key(self):
        master = node.Compound()
 
        master['key1'] = 'ooo'
        assert master['key1'] == 'ooo'
 
 
    def test_update(self):
        sub = mock.MagicMock(spec=node.Store)
        mysql = node.Compound({
                'behavior': 'percona',
                'sub_1,sub_2': sub
        })
        mysql.update({
                'replication_master': '1',
                'sub_1': 'a value',
                'sub_2': 'not bad'
        })
 
 
        assert 'replication_master' in mysql
        assert mysql['replication_master'] == '1'
        sub.__setitem__.call_args_list[0] = mock.call(mysql, 'sub_1', 'a value')
        sub.__setitem__.call_args_list[1] = mock.call(mysql, 'sub_2', 'not bad')
 
 
class TestJson(object):
    def setup(self):
        self.fixtures_dir = os.path.dirname(__file__) + '/../fixtures'
        filename = self.fixtures_dir + '/node.json'
        self.store = node.Json(filename, mock.Mock())
 
 
    def teardown(self):
        for name in ('node-test-set-dict.json', 'node-test-set-object.json'):
            filename = os.path.join(self.fixtures_dir, name)
            if os.path.isfile(filename):
                os.remove(filename)
 
 
    def test_get(self):
        val = self.store['any_key']
 
        assert val
        self.store.fn.assert_called_with(
                        type='eph',
                        id='eph-vol-592f4b8c',
                        size='80%')
 
 
    def test_set_dict(self):
        data = {'type': 'lvm', 'vg': 'mysql'}
 
        self.store.filename = self.fixtures_dir + '/node-test-set-dict.json'
        self.store['any_key'] = data
 
 
 
    def test_set_object(self):
        class _Data(object):
            def __init__(self, data):
                self.data = data
            def config(self):
                return self.data
 
        data = {'type': 'lvm', 'vg': 'mysql'}
        self.store.filename = self.fixtures_dir + '/node-test-set-object.json'
        self.store['any_key'] = _Data(data)
 
 
class TestIni(object):
    def setup(self):
        filename = os.path.dirname(__file__) + '/../fixtures/node.ini'
        self.store = node.Ini(filename, 'mysql')
 
    @attr('one')
    def test_get(self):
        assert self.store['root_password'] == 'Q9OgJxYf19ygFHpRprLF'
 
 
    @raises(KeyError)
    def test_get_nosection(self):
        self.store.section = 'undefined'
        self.store['log_file']
 
    @raises(KeyError)
    def test_get_nooption(self):
        self.store['undefined_option']
 
    @mock.patch('__builtin__.open')
    def test_set(self, open):
        with mock.patch.object(self.store, '_reload') as reload:
            self.store.ini = mock.Mock()
            self.store['new_option'] = 1
            self.store.ini.set.assert_called_with(self.store.section, 'new_option', '1')
            assert self.store.ini.write.call_count == 1
 
    def test_set_new_file(self):
        filename = os.path.dirname(__file__) + '/../fixtures/node_new.ini'
        store = node.Ini(filename, 'mysql')
        try:
            store['root_password'] = 'abs'
            assert store['root_password'] == 'abs'
        finally:
            if os.path.exists(filename):
                os.remove(filename)
 
