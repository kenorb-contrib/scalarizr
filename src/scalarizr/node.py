from __future__ import with_statement
 
import os
import re
import ConfigParser
import sys
import copy
try:
    import json
except ImportError:
    import simplejson as json 
 
from scalarizr import linux
 
 
if linux.os.windows_family:
    base_dir = r'C:\Program Files\Scalarizr\etc'
else:
    base_dir = '/etc/scalr'
private_dir = base_dir + '/private.d'
public_dir = base_dir + '/public.d'
storage_dir = private_dir + '/storage'
 
 
class Store(object):
 
    def __repr__(self):
        return '<%s at %s>' % (type(self).__name__, hex(id(self)))
 
    def __getitem__(self, key):
        raise NotImplementedError()
 
    def __setitem__(self, key, name):
        raise NotImplementedError()
 
 
class Compound(dict):
    '''
    pyline: disable=E1101
    '''
    def __init__(self, patterns=None):
        patterns = patterns or {}
        for pattern, store in patterns.items():
            keys = pattern.split(',')
            for key in keys:
                super(Compound, self).__setitem__(key, store)
 
    def __getattr__(self, name):
        try:
            return self.__getitem__(name)
        except KeyError:
            raise AttributeError(name)
 
    def __setitem__(self, key, value):
        try:
            value_now = dict.__getitem__(self, key)
        except KeyError:
            value_now = None
        if isinstance(value_now, Store):
            value_now.__setitem__(key, value)
        else:
            super(Compound, self).__setitem__(key, value)
 
 
    def __getitem__(self, key):
        value =  dict.__getitem__(self, key)
        if isinstance(value, Store):
            return value.__getitem__(key)
        else:
            return value
 
    def copy(self):
        ret = Compound()
        for key in self:
            value = dict.__getitem__(self, key)
            if isinstance(value, Store):
                value = copy.deepcopy(value)
            ret[key] = value
        return ret
 
    def update(self, values):
        for key, value in values.items():
            self[key] = value
 
    def __repr__(self):
        ret = {}
        for key in self:
            value = dict.__getitem__(self, key)
            if isinstance(value, Store):
                value = repr(value)
            ret[key] = value
        return repr(ret)
 
 
class Json(Store):
    def __init__(self, filename, fn):
        '''
        Example:
        jstore = Json('/etc/scalr/private.d/storage/mysql.json', 
                                'scalarizr.storage2.volume')
        '''
        self.filename = filename
        self.fn = fn
        self._obj = None
 
    def __getitem__(self, key):
        if not self._obj:
            try:
                with open(self.filename, 'r') as fp:
                    kwds = json.load(fp)
            except:
                raise KeyError(key)
            else:
                if isinstance(self.fn, basestring):
                    self.fn = _import(self.fn)
                self._obj = self.fn(**kwds)
        return self._obj
 
 
    def __setitem__(self, key, value):
        self._obj = value
        if hasattr(value, 'config'):
            value = value.config()
        dirname = os.path.dirname(self.filename)
        if not os.path.exists(dirname):
            os.makedirs(dirname)
        with open(self.filename, 'w+') as fp:
            json.dump(value, fp)
 
 
class Ini(Store):
    def __init__(self, filenames, section, mapping=None):
        if not hasattr(filenames, '__iter__'):
            filenames = [filenames]
        self.filenames = filenames
        self.section = section
        self.ini = None
        self.mapping = mapping or {}
 
 
    def _reload(self, only_last=False):
        self.ini = ConfigParser.ConfigParser()
        if only_last:
            self.ini.read(self.filenames[-1])
        else:
            for filename in self.filenames:
                if os.path.exists(filename):
                    self.ini.read(filename)
 
 
    def __getitem__(self, key):
        self._reload()
        if key in self.mapping:
            key = self.mapping[key]
        try:
            return self.ini.get(self.section, key)
        except ConfigParser.Error:
            raise KeyError(key)
 
 
    def __setitem__(self, key, value):
        if value is None:
            value = ''
        elif isinstance(value, bool):
            value = str(int(value))
        else:
            value = str(value)
        self._reload(only_last=True)
        if not self.ini.has_section(self.section):
            self.ini.add_section(self.section)
        if key in self.mapping:
            key = self.mapping[key]
        self.ini.set(self.section, key, value)
        with open(self.filenames[0], 'w+') as fp:
            self.ini.write(fp)
 
 
class RedisIni(Ini):
 
    def __getitem__(self, key):
        try:
            value = super(RedisIni, self).__getitem__(key)
            if key in ('use_password', 'replication_master',):
                if value in (None, ''):
                    value = True
                else:
                    value = bool(int(value))
        except KeyError:
            if 'persistence_type' == key:
                value = 'snapshotting'
                self.__setitem__(key, value)
            elif 'master_password' == key:
                value = None
            else:
                raise
        return value
 
 
class IniOption(Ini):
    def __init__(self, filenames, section, option, 
                    getfilter=None, setfilter=None):
        self.option = option
        self.getfilter = getfilter
        self.setfilter = setfilter
        super(IniOption, self).__init__(filenames, section)
 
 
    def __getitem__(self, key):
        value = super(IniOption, self).__getitem__(self.option)
        if self.getfilter:
            return self.getfilter(value)
        return value
 
 
    def __setitem__(self, key, value):
        if self.setfilter:
            value = self.setfilter(value)
        super(IniOption, self).__setitem__(self.option, value)
 
 
class File(Store):
    def __init__(self, filename):
        self.filename = filename
 
 
    def __getitem__(self, key):
        try:
            with open(self.filename) as fp:
                return fp.read().strip()
        except:
            raise KeyError(key)
 
 
    def __setitem__(self, key, value):
        with open(self.filename, 'w+') as fp:
            fp.write(str(value).strip())
 
 
class BoolFile(Store):
    def __init__(self, filename):
        self.filename = filename
 
 
    def __getitem__(self, key):
        return os.path.isfile(self.filename)
 
 
    def __setitem__(self, key, value):
        if value:
            open(self.filename, 'w+').close()
        else:
            if os.path.isfile(self.filename):
                os.remove(self.filename)
 
 
class State(Store):
    def __init__(self, key):
        self.key = key
 
    def __getitem__(self, key):
        from scalarizr.config import STATE
        return STATE[self.key]
 
    def __setitem__(self, key, value):
        from scalarizr.config import STATE
        STATE[self.key] = value
 
 
class Attr(Store):
    def __init__(self, module, attr):
        self.module = module
        self.attr = attr
        self.getter = None
 
 
    def __getitem__(self, key):
        try:
            if isinstance(self.module, basestring):
                self.module = _import(self.module)
            if not self.getter:
                def getter():
                    path = self.attr.split('.')
                    base = self.module
                    for name in path[:-1]:
                        base = getattr(base, name)
                    return getattr(base, path[-1])
                self.getter = getter
        except:
            raise KeyError(key) 
        return self.getter()
 
 
class Call(Attr):
    def __getitem__(self, key):
        attr = Attr.__getitem__(self, key)
        return attr()   
 
 
def _import(objectstr):
    try:
        __import__(objectstr)
        return sys.modules[objectstr]
    except ImportError:
        module_s, _, attr = objectstr.rpartition('.')
        __import__(module_s)
        try:
            return getattr(sys.modules[module_s], attr)
        except (KeyError, AttributeError):
            raise ImportError('No module named %s' % attr)
 
 
class ScalrVersion(Store):
    pass
 
 
node = {
        'server_id,role_id,farm_id,farm_role_id,env_id,role_name,server_index':
                                Ini(os.path.join(private_dir, 'config.ini'), 'general'),
        'message_format': Ini(os.path.join(private_dir, 'config.ini'), 'messaging_p2p'),
        'platform_name': Ini(os.path.join(public_dir, 'config.ini'), 'general'),
        'platform': Attr('scalarizr.bus', 'bus.platform'),
        'behavior': IniOption(
                                                [public_dir + '/config.ini', private_dir + '/config.ini'], 
                                                'general', 'behaviour',
                                                lambda val: val.strip().split(','),
                                                lambda val: ','.join(val)),
        'public_ip': Call('scalarizr.bus', 'bus.platform.get_public_ip'),
        'private_ip': Call('scalarizr.bus', 'bus.platform.get_private_ip'),
        'state': File(private_dir + '/.state'),
        'rebooted': BoolFile(private_dir + '/.reboot'),
        'halted': BoolFile(private_dir + '/.halt'),
        'cloud_location' : IniOption(private_dir + '/config.ini', 'general', 'region')
}
 
node['defaults'] = {
    'base': {
        'api_port': 8010,
        'messaging_port': 8013
    }
}
 
node['base'] = {
}
 
for behavior in ('mysql', 'mysql2', 'percona', 'mariadb'):
    section = 'mysql2' if behavior in ('percona', 'mariadb') else behavior
    node[behavior] = Compound({
            'volume,volume_config': 
                            Json('%s/storage/%s.json' % (private_dir, 'mysql'), 
                                    'scalarizr.storage2.volume'),
            'root_password,repl_password,stat_password,log_file,log_pos,replication_master': 
                            Ini('%s/%s.ini' % (private_dir, behavior), section),
            'mysqldump_options': 
                            Ini('%s/%s.ini' % (public_dir, behavior), section)
    })
 
node['redis'] = Compound({
    'volume,volume_config': Json(
        '%s/storage/%s.json' % (private_dir, 'redis'), 'scalarizr.storage2.volume'),
    'replication_master,persistence_type,use_password,master_password': RedisIni(
                    '%s/%s.ini' % (private_dir, 'redis'), 'redis')
})
 
 
node['rabbitmq'] = Compound({
        'volume,volume_config': Json('%s/storage/%s.json' % (private_dir, 'rabbitmq'),
                        'scalarizr.storage2.volume'),
        'password,node_type,cookie,hostname': Ini(
                                                '%s/%s.ini' % (private_dir, 'rabbitmq'), 'rabbitmq')
 
})
 
node['postgresql'] = Compound({
'volume,volume_config': Json('%s/storage/%s.json' % (private_dir, 'postgresql'),
        'scalarizr.storage2.volume'),
'replication_master,pg_version,scalr_password,root_password, root_user': Ini(
        '%s/%s.ini' % (private_dir, 'postgresql'), 'postgresql')
})
 
node['mongodb'] = Compound({
        'volume,volume_config':
                                Json('%s/storage/%s.json' % (private_dir, 'mongodb'), 'scalarizr.storage2.volume'),
        'snapshot,shanpshot_config':
                                Json('%s/storage/%s-snap.json' % (private_dir, 'mongodb'),'scalarizr.storage2.snapshot'),
        'shards_total,password,replica_set_index,shard_index,keyfile':
                                Ini('%s/%s.ini' % (private_dir, 'mongodb'), 'mongodb')
})
 
node['nginx'] = Compound({
    'binary_path,app_include_path,https_include_path,app_port,upstream_app_role':
        Ini('%s/%s.ini' % (public_dir, 'www'), 'www')
})
 
node['apache'] = Compound({
    'vhosts_path,apache_conf_path':
        Ini('%s/%s.ini' % (public_dir, 'app'), 'app')
})
 
node['cloudfoundry'] = Compound({
        'volume,volume_config': Json('%s/storage/%s.json' % (private_dir, 'cloudfoundry'), 'scalarizr.storage2.volume')
        })
 
node['tomcat'] = {}
 
 
node['ec2'] = Compound({
        't1micro_detached_ebs': State('ec2.t1micro_detached_ebs'),
        'hostname_as_pubdns': 
                                Ini('%s/%s.ini' % (public_dir, 'ec2'), 'ec2'),
        'ami_id': Call('scalarizr.bus', 'bus.platform.get_ami_id'),
        'kernel_id': Call('scalarizr.bus', 'bus.platform.get_kernel_id'),
        'ramdisk_id': Call('scalarizr.bus', 'bus.platform.get_ramdisk_id'),
        'instance_id': Call('scalarizr.bus', 'bus.platform.get_instance_id'),
        'instance_type': Call('scalarizr.bus', 'bus.platform.get_instance_type'),
        'avail_zone': Call('scalarizr.bus', 'bus.platform.get_avail_zone'),
        'region': Call('scalarizr.bus', 'bus.platform.get_region'),
        'connect_ec2': Attr('scalarizr.bus', 'bus.platform.new_ec2_conn'),
        'connect_s3': Attr('scalarizr.bus', 'bus.platform.new_s3_conn')
})
node['cloudstack'] = Compound({
        'new_conn': Call('scalarizr.bus', 'bus.platform.new_cloudstack_conn'),
        'instance_id': Call('scalarizr.bus', 'bus.platform.get_instance_id'),
        'zone_id': Call('scalarizr.bus', 'bus.platform.get_avail_zone_id'),
        'zone_name': Call('scalarizr.bus', 'bus.platform.get_avail_zone')
})
node['openstack'] = Compound({
        'new_cinder_connection': Call('scalarizr.bus', 'bus.platform.new_cinder_connection'),
        'new_nova_connection': Call('scalarizr.bus', 'bus.platform.new_nova_connection'),
        'new_swift_connection': Call('scalarizr.bus', 'bus.platform.new_swift_connection'),
        'server_id': Call('scalarizr.bus', 'bus.platform.get_server_id')
})
node['rackspace'] = Compound({
        'new_swift_connection': Call('scalarizr.bus', 'bus.platform.new_swift_connection'),
        'server_id': Call('scalarizr.bus', 'bus.platform.get_server_id')
})
 
node['gce'] = Compound({
        'compute_connection': Call('scalarizr.bus', 'bus.platform.new_compute_client'),
        'storage_connection': Call('scalarizr.bus', 'bus.platform.new_storage_client'),
        'project_id': Call('scalarizr.bus', 'bus.platform.get_project_id'),
        'instance_id': Call('scalarizr.bus', 'bus.platform.get_instance_id'),
        'zone': Call('scalarizr.bus', 'bus.platform.get_zone')
})
 
node['scalr'] = Compound({
        'version': File(private_dir + '/.scalr-version'),
        'id': Ini(private_dir + '/config.ini', 'general', {'id': 'scalr_id'})
})
 
node['messaging'] = Compound({
    'send': Attr('scalarizr.bus', 'bus.messaging_service.send')
})
__node__ = Compound(node)
 
 
 
 
 
