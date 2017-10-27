from __future__ import with_statement
from __future__ import with_statement
 
 
import os, sys
import logging
import signal, csv, cStringIO, socket
import string
import re
from threading import local
import time
import shutil
 
from scalarizr import util
from scalarizr.util import initdv2
from scalarizr.libs import metaconf
 
 
BEHAVIOUR = SERVICE_NAME = 'haproxy'
LOG = logging.getLogger(__name__)
HAPROXY_EXEC = '/usr/sbin/haproxy'
HAPROXY_CFG_PATH = '/etc/haproxy/haproxy.cfg'
 
class HAProxyError(Exception):
    pass
 
 
class ConfigurationFile(object):
 
    BACKUP_BASE_DIR = '/var/lib/scalarizr/backup'
 
    def __init__(self, path):
        self.path = path
        self.backup_dir = os.path.join(self.BACKUP_BASE_DIR, self.path[1:])
        self.local = local()
 
    def __str__(self):
        raise NotImplementedError()
 
    def __enter__(self):
        self.local.last_trans_id = str(time.time())
        if not os.path.exists(self.backup_dir):
            os.makedirs(self.backup_dir)
        shutil.copy(self.path, os.path.join(self.backup_dir, self.local.last_trans_id))
        with open(self.path, 'w+') as fp:
            fp.write(str(self))
        return self
 
    def __exit__(self, *args):
        if args[0] and hasattr(self.local, 'last_trans_id'):
            backup_path = os.path.join(self.backup_dir, self.local.last_trans_id)
            shutil.copy(backup_path, self.path)
 
    def trans(self, enter=None, exit=None):
        #raise NotImplementedError()
        return self
 
    def reload(self):
        #raise NotImplementedError()
        pass
 
 
class HAProxyCfg(ConfigurationFile):
    class slice_(dict):
        def __init__(self, conf, xpath):
            LOG.debug('slice_.__init__: xpath: `%s`', xpath)
            dict.__init__(self)
            self.conf = conf
            self.xpath = xpath
            self.name = os.path.basename(xpath)
 
        def __contains__(self, name):
            raise NotImplementedError()
 
        def __getitem__(self, name):
            raise NotImplementedError()
 
        def __setitem__(self, name, value):
            raise NotImplementedError()
 
        def __iter__(self):
            LOG.debug('slice_.__iter__')
            index = 1
            try:
                while True:
                    yield self.conf.get(self._child_xpath(index))
                    index += 1
            except metaconf.NoPathError:
                raise StopIteration()
 
        def __delitem__(self, key):
            try:
                index = self._indexof(key)
                if index != -1:
                    self.conf.remove(self._child_xpath(index))
                else:
                    try:
                        self.conf.remove(self._child_xpath(key))
                    except:
                        raise Exception, 'Not found section `%s`' % key, sys.exc_info()[2]
            except Exception, e:
                raise Exception, 'Can`t be remove, because not found in path. Details: `%s`' % e
 
        def _child_xpath(self, key):
            if isinstance(key, int):
                return '%s[%d]' % (self.xpath, key)
            return '%s/%s' % (self.xpath, key)
 
        def _indexof(self, key):
            try:
                index = 1
                for el in self:
                    if el == key or el.startswith(key):
                        return index
                    index += 1
                return -1
            except:
                raise Exception, 'HAProxyCfg.slice._indexof: details: %s' %\
                        sys.exc_info()[1], sys.exc_info()[2]
 
        def _len_xpath(self):
            return len(self.xpath.replace('./', '').split('/'))
 
        def _iter_serialize(self):
            LOG.debug('slice_._iter_serialize')
            index = 1
            try:
                if self._len_xpath() > 1:
                    LOG.debug('     self.name = `%s`', self.name)
                    section_ = self.name if self.name in HAProxyCfg.option_group.NAMES else ''
                    while True:
                        yield _serializers[section_].serialize(self.conf.get(self._child_xpath(index)))
                        index += 1
            except metaconf.NoPathError:
                raise StopIteration()
 
        def _iter_unserialize(self):
            LOG.debug('slice_._iter_unserialize')
            index = 1
            try:
                if self._len_xpath() > 1:
                    LOG.debug('     self.name = `%s`', self.name)
                    section_ = self.name if self.name in HAProxyCfg.option_group.NAMES else ''
                    while True:
                        yield _serializers[section_].unserialize(self.conf.get(self._child_xpath(index)))
                        index += 1
            except metaconf.NoPathError:
                raise StopIteration()
 
 
    class option_group(slice_):
        NAMES = ('server', 'option', 'log', 'stats', 'timeout')
 
        def __getitem__(self, name):
            LOG.debug('option_group.__getitem__: name = `%s`, xpath: `%s`', name, self.xpath)
            index = 1
            for val in self:
                if val.startswith(name + ' ') or val == name:
                    LOG.debug('     self.name = `%s`',self.name)
                    return _serializers[self.name].unserialize(self.conf.get(self._child_xpath(index))[len(name):])
                index += 1
            raise KeyError(name)
 
        def __contains__(self, name):
            name_ = name + ' '
            for val in self:
                if val == name:#val.startswith(name_):
                    return True
            return False
 
        def __setitem__(self, key, value):
            LOG.debug('option_group.__setitem__: key = `%s`, value = `%s`, xpath: `%s`', key, value, self.xpath)
 
            index = self._indexof(key)
 
            _section = self.name if self.name in self.NAMES else key
            LOG.debug('     _section = %s', _section)
            var = _serializers[_section].serialize(value)
 
            if index != -1:
                LOG.debug('     set value var = `%s`, index = `%s`, _child_xpath(index): `%s`',
                         '%s %s' % (key, var), index, self._child_xpath(index))
                if self._len_xpath() >= 2:
                    LOG.debug('     self.name = `%s`', self.name)
                    self.conf.set(self._child_xpath(index), '%s %s' % (key, var))
                else:
                    self.conf.set(self.xpath(key), var)
            else:
                if self._len_xpath() >= 2:
                    if key.strip() or var.strip():
                        self.conf.add(self.xpath, '%s %s' % (key, var))
                    index = self._indexof(key)
                    LOG.debug('     add value var = `%s`, index = `%s`, _child_xpath(index):'\
                            ' `%s`, key=`%s`', '%s %s' % (key, var), index, self._child_xpath(index), key)
                else:
                    if key.strip() or var.strip():
                        self.conf.add(self.xpath(key), var)
 
        def __iter__(self):
            LOG.debug('option_group.__iter__')
            index = 1
            try:
                while True:
                    yield self.conf.get(self._child_xpath(index)).replace('/t', ' ').split(' ')[0]
                    index += 1
            except metaconf.NoPathError:
                raise StopIteration()
 
        def __delitem__(self, key):
            LOG.debug('option_group.__delitem__: key = `%s`' % key)
            super(HAProxyCfg.option_group, self).__delitem__(key)
 
 
    class section(slice_):
        def __getitem__(self, name):
            LOG.debug('section.__getitem__: name = `%s`, xpath: `%s`', name, self.xpath)
            LOG.debug('self.name = %s', self.name)
            if name in HAProxyCfg.option_group.NAMES:
                return HAProxyCfg.option_group(self.conf, self._child_xpath(name))
            try:
                return _serializers[name].unserialize(self.conf.get(self._child_xpath(name)))
            except metaconf.NoPathError:
                raise KeyError(name)
 
        def __contains__(self, name):
            return name in self.conf.options(self.xpath)
 
        def __len__(self):
            return len(self.conf.get_list(self.xpath))
 
        def __setitem__(self, key, value):
            LOG.debug('section.__setitem__: key = `%s`, value = `%s`, xpath: `%s`',
                             key, value, self.xpath)
 
            if key in HAProxyCfg.option_group.NAMES:
                LOG.debug('     key `%s` in option_group.NAMES', key)
                try:
                    if isinstance(value, dict):
                        for key_el in value:
                            LOG.debug('el in self = `%s`', key_el)
                            og = HAProxyCfg.option_group(self.conf, self._child_xpath(key))
                            og[key_el] = value[key_el]
                    else:
                        og = HAProxyCfg.option_group(self.conf, self._child_xpath(key))
                        LOG.debug('key=`%s`, value=`%s`', key, value)
                        og[''] = value
                except Exception, e:
                    raise Exception, 'section.__setitem__: error set value=`%s`. Details: KeyError('\
                            '`%s`)'% (value, sys.exc_info()[1]), sys.exc_info()[2]
            else:
                index = self._indexof(key)
                var = _serializers[key].serialize(value)
                if index != -1:
                    self.xpath = self.conf.set(self._child_xpath(key))
                    LOG.debug('     set value var = `%s`, index = `%s`, _child_xpath(index):'\
                                    ' `%s`', var, index, self._child_xpath(index))
                    self.conf.set(self._child_xpath(index), var)
                else:
                    try:
                        self.conf.get(self._child_xpath(key))
                        self.conf.set(self._child_xpath(key), var)
                        LOG.debug('     set value var = `%s`, key = `%s`, _child_xpath(key): `%s`',
                                var, key, self._child_xpath(key))
                    except:
                        LOG.debug('     section not exist, adding value var = `%s`,'\
                                ' key = `%s`, _child_xpath(key): `%s`', var, key,
                                self._child_xpath(key))
                        if key.strip() or var.strip():
                            self.conf.add(self._child_xpath(key), var)
 
        def __delitem__(self, key):
            LOG.debug('section.__delitem__: key = `%s`' % key)
 
            super(HAProxyCfg.section, self).__delitem__(key)
 
    class section_group(slice_):
        def __len__(self):
            return sum(int(t == self.name) for t in self.conf.sections('./'))
 
        def __contains__(self, name):
            return name in self.conf.sections('./')
 
        def __getitem__(self, name):
            LOG.debug('section_group.__getitem__: name = `%s`', name)
            for index in range(1, len(self)+1):
                LOG.debug('     elem `%s` in xpath `%s`', self.conf.get(
                                        self._child_xpath(index)), self._child_xpath(index))
                if self.conf.get(self._child_xpath(index)) == name:
                    return HAProxyCfg.section(self.conf, self._child_xpath(index))
            raise KeyError('Can`t find index in path `%s`' % self.xpath)
 
        def __setitem__(self, key, value):
            LOG.debug('section_group.__setitem__: key = `%s`, value = `%s`', key, value)
 
            if isinstance(value, dict):
                ind = self._indexof(key)
                LOG.debug('     ind %s', ind)
                LOG.debug('     ind = %s', ind)
                if ind == -1:
                    LOG.debug('     path %s added', self.xpath)
                    if key.strip:
                        self.conf.add(self.xpath, key)
                    ind = self._indexof(key)
                if ind != -1:
                    section_ = HAProxyCfg.section(self.conf, self._child_xpath(ind))
                    for key_ in value.keys():
                        LOG.debug('     inside dict:    key_= `%s`, value `%s`', key_, value[key_])
                        section_[key_] = value.get(key_)
                        #because `value` is `section` class type, need use get and not []
                else:
                    raise 'section_group.__setitem__:       section `%s` was not added' % key
            else:
                raise 'section_group.__setitem__:       value `%s` type must be dict' % value
 
        def __delitem__(self, key):
            LOG.debug('section_group.__delitem__: key = `%s`' % key)
            super(HAProxyCfg.section_group, self).__delitem__(key)
 
 
    def __init__(self, path=None):
        self.conf = metaconf.Configuration('haproxy')
        self.cnf_path = path or HAPROXY_CFG_PATH
        self.conf.read(self.cnf_path)
 
    def __getitem__(self, name):
        cls = self.section_group
        if name in ('global', 'defaults'):
            cls = self.section
        return cls(self.conf, './' + name)
 
    def __setitem__(self, key, value):
        LOG.debug('HAProxyCfg.__setitem__: key = `%s`, value = `%s`', key, value)
        if isinstance(value, dict):
            sg = HAProxyCfg.section_group(self.conf, './%s' % key)
            sg[key] = value
        else:
            raise ValueError('Expected dict-like object: %s' % type(value))
 
    def __getattr__(self, name):
        _sections = {'globals':'global', 'defaults':'defaults', 'backends': 'backend',
                 'listener': 'listen', 'frontends':'frontend'}
        if name in _sections.keys():
            name_ = _sections[name]
            return self.__getitem__(name_)
 
    def __setattr__(self, name, value):
        _sections = {'globals':'global', 'defaults':'defaults', 'backends': 'backend',
                 'listener': 'listen', 'frontends':'frontend'}
        if name in _sections.keys():
            name_ = _sections[name]
            self.__setitem__(name_, value)
        else:
            object.__setattr__(self, name, value)
 
    def __delitem__(self, key):
        LOG.debug('HAProxyCfg.__delitem__: key `%s`', key)
        return NotImplementedError
 
    def sections(self, filter):
        '''
        @rtype: list
        Example: filter = `scalr:backend:role:1234:tcp:2254`
        where protocol='tcp', port=1154, server_port=2254, backend='role:1234'
        Look at services.haproxy.naming
        '''
        LOG.debug('HAProxyCfg.sections: input filter `%s`' % filter)
        params = filter.split(':')
        LOG.debug('     filter params `%s`' % params)
        path = './%s' % params[1]
        result = []
        index = 1
        for section in self.conf.get_list(path):
            LOG.debug('     section `%s`' % section)
            flag = True
            for param in params:
                if not param in section.split(':'):
                    flag = False
                    break
            if flag:
                result.append(section)
            index += 1
        return result
 
    def el_in_path(self, path, key):
        '''Find key in any of children in path. If key found return True'''
        childrens = list(set(self.conf.children(path))) # path as ./listen[0]
        for children in childrens:
            obj = self.conf.get('%s/%s' % (path, children))
            if isinstance(obj, str):
                if key in obj:
                    LOG.debug('     key `%s` found at pat=`%s`', key, '%s/%s' % (path, children))
                    return True
            else:
                index = 1
                for line in self.conf.get('%s/%s[%s]' % (path, children, index)):
                    if key in line:
                        LOG.debug('     key `%s` found at pat=`%s/%s[%s]`', key, (path, children, index))
                        return True
                    index += 1
 
    def save(self, path=None):
        '''Write cfg in path file. If path not define, it use HAProxyCfg path'''
        LOG.debug('services.haproxy.HAProxyCfg.save')
        try:
            LOG.debug('     HAProxyCfg.save: cnf_path = `%s`' % (path or self.cnf_path))
            self.conf.write(path or self.cnf_path)
        except Exception, e:
            raise HAProxyError, 'services.haproxy.HAProxyCfg.save: exception, details:'\
                    ' `%s`'%sys.exc_info()[1]
 
    def reload(self):
        '''Reload metaconf.Configuration object inside HAProxyCfg'''
        LOG.debug('services.haproxy.HAProxyCfg.reload path=`%s`', self.cnf_path)
        self.conf = metaconf.Configuration('haproxy')
        self.conf.read(self.cnf_path)
 
 
class OptionSerializer(object):
    def unserialize(self, s):
        LOG.debug('OptionSerializer.unserialize: input `%s`', s)
        value = filter(None, map(string.strip, s.replace('\t', ' ').split(' ')))\
                                        if isinstance(s, str) else s
 
        if not value or len(value) == 0:
            return True
        elif len(value) == 1:
            return value[0]
        return value
 
    def serialize(self, v):
        LOG.debug('OptionSerializer.serialize: input `%s`', v)
        if isinstance(v, list):
            return ' '.join(v)
        elif isinstance(v, dict):
            res = ''
            for key in v.keys():
                if isinstance(v[key], str):
                    res += ' %s %s' % (key, v[key])
                elif isinstance(v[key], bool):
                    res += ' ' + key
                else:
                    res += ' %s %s' % (key, self.serialize(v[key]))
            return res
        elif isinstance(v, bool):
            return ''
        else:
            return v
 
    def _parse(self, list_par):
        '''
        Pars from input list to result dict object by tags and by number of param
        arguments in dict self._number_args.
        '''
        temp = {}
        list_par = list(list_par)
        while list_par.__len__ > 0:
            if list_par:
                elem = list_par.pop(0)
            else:
                break
            for num_args in self._number_args.keys():
                if elem in self._number_args[num_args]:
                    if num_args == 0:
                        temp[elem] = True
                    elif num_args == 1:
                        if list_par:
                            temp[elem] = list_par.pop(0)
                        else:
                            temp[elem] = ''
                    else:
                        temp[elem] = []
                        while num_args > 0 and list_par.__len__ > 0:
                            temp[elem] = temp[elem].append(list_par.pop(0))
                            num_args -= 1
        return temp
 
 
class ServerSerializer(OptionSerializer):
 
    def __init__(self):
        self._number_args = {
                0:['backup', 'check', 'disabled'],
                1:['addr', 'cookie', 'error-limit', 'fall', 'id', 'inter',
                        'fastinter', 'downinter', 'maxconn', 'maxqueue', 'minconn',
                        'observe', 'on-error', 'port', 'redir', 'rise', 'slowstart',
                        'source', 'track', 'weight']}
 
    def unserialize(self, s):
        LOG.debug('ServerSerializer.unserialize: input `%s`', s)
        try:
            list_par = OptionSerializer.unserialize(self, s)
            temp = {}
            #name = list_par.pop(0)
            temp['host'] = list_par.pop(0)
            if ':' in temp['host']:
                temp['host'], temp['port'] = temp['host'].split(':')
            temp.update(self._parse(list_par))
            return temp if isinstance(temp, dict) else s
        except:
            LOG.debug("Details: %s%s", sys.exc_info()[1], sys.exc_info()[2])
            return OptionSerializer.unserialize(self, s)
 
    def serialize(self, d):
        LOG.debug('ServerSerializer.serialize: input `%s`', d)
        res = []
        if isinstance(d, dict):
            if d.get('host'):
                res.append('%s%s' % (str(d['host']), ':' + str(d.get('port')) if d.get('port') else ''))
                del d['host']
                if d.get('port'):
                    del d['port']
 
            for key in d.keys():
                if isinstance(d[key], bool): 
                    if d[key]:
                        res.append(key)
                else:
                    LOG.debug('d[key]: `%s`', d[key])
                    res.append(' '.join([key, d[key]]))
            LOG.debug('res: `%s`, res_str: `%s`'%(res, ' '.join(res)))
            return ' '.join(res)
        else:
            return ' '.join(str(d))
 
 
class  DefaultServerSerializer(OptionSerializer):
 
    def __init__(self):
        self._number_args = {
                1:['error-limit', 'fall', 'inter', 'fastinter', 'downinter', 'maxconn',
                        'maxqueue', 'minconn', 'on-error', 'port', 'rise', 'slowstart', 'weight']}
 
    def unserialize(self, s):
        LOG.debug('DefaultServerSerializer.unserialize: input `%s`', s)
        try:
            list_par = OptionSerializer.unserialize(self, s)
            temp = {}
            temp.update(self._parse(list_par))
            return temp if isinstance(temp, dict) else s
        except:
            LOG.debug("Details: %s%s", sys.exc_info()[1], sys.exc_info()[2])
            return OptionSerializer.unserialize(self, s)
 
    def serialize(self, d):
        LOG.debug('DefaultServerSerializer.serialize: input `%s`', d)
        res = []
        if isinstance(d, dict):
            for key in d.keys():
                if isinstance(d[key], bool):
                    res.append(key)
                else:
                    LOG.debug('d[key]: `%s`', str(d[key]))
                    res.append(' '.join([key, str(d[key])]))
            LOG.debug('res: `%s`, res_str: `%s`'%(res, ' '.join(res)))
            return ' '.join(res)
        else:
            return ' '.join(str(d))
 
class StatsSerializer(OptionSerializer):
    def __init__(self):
        self._number_args = {
                        0:['enable'],
                        1:['socket', 'timeout', 'maxconn', 'uid', 'user', 'gid', 'group', 'mode', 'level'],
                        2:['admin']}
 
class BindSerializer(OptionSerializer):
    pass
 
class Serializers(dict):
    def __init__(self, **kwds):
        dict.__init__(self, **kwds)
        self.update({
                'server': ServerSerializer(),
                'stats': StatsSerializer(),
                'default-server': DefaultServerSerializer(),
        })
        self.__default =  OptionSerializer()
 
    def __getitem__(self, option):
        return self.get(option, self.__default)
 
_serializers = Serializers()
 
 
 
class StatSocket(object):
    '''
    haproxy unix socket API
    - one-to-one naming
    - connect -> request -> disconnect
 
    Create object:
    >> ss = StatSocket('/var/run/haproxy-stats.sock')
 
    Show stat:
    >> ss.show_stat()
    [{'status': 'UP', 'lastchg': '68', 'weight': '1', 'slim': '', 'pid': '1', 'rate_lim': '',
    'check_duration': '0', 'rate': '0', 'req_rate': '', 'check_status': 'L4OK', 'econ': '0',
    ...'''
 
    def __init__(self, address='/var/run/haproxy-stats.sock'):
        try:
            self.sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            self.sock.connect(address)
            self.adress = address
        except:
            raise Exception, "Couldn't connect to socket on address: %s%s" % (address, sys.exc_info()[1]), sys.exc_info()[2]
 
 
    def show_stat(self):
        '''
        @rtype: list[dict]
        '''
        try:
            self.sock.send('show stat\n')
            stat = self.sock.makefile('r').read()
 
            fieldnames = filter(None, stat[2:stat.index('\n')].split(','))
            reader = csv.DictReader(cStringIO.StringIO(stat[stat.index('\n'):]), fieldnames)
            res=[]
            for row in reader:
                res.append(row)
            return res
        except:
            raise Exception, "Error working with sockets. Details: %s" % sys.exc_info()[1],\
                    sys.exc_info()[2]
 
 
def naming(type_, protocol=None, port=None, backend=None):
    ret = 'scalr:%s' % type_
    if type_ == 'backend' and backend:
        ret += ':%s' % backend
    if protocol:
        ret += ':%s' % protocol
    if port:
        ret += ':%s' % port
    return ret
 
 
class HAProxyInitScript(initdv2.InitScript):
    '''
    haproxy init script
    - start
    - stop
    - restart
    - reload
    - status
    '''
 
    def __init__(self, path=None):
        self.pid_file = '/var/run/haproxy.pid'
        self.config_path = path or HAPROXY_CFG_PATH
        self.haproxy_exec = '/usr/sbin/haproxy'
        self.socks = None
        self.timeout = 30
 
 
    def start(self):
        if self.status() == 0:
            raise initdv2.InitdError("Cannot start HAProxy. It already running.")
 
        util.system2([self.haproxy_exec, '-f', self.config_path, '-p', self.pid_file, '-D'],)
        if self.pid_file:
            try:
                util.wait_until(lambda: os.path.exists(self.pid_file), timeout=self.timeout,
                                sleep=0.2, error_text="HAProxy pid file %s does'not exist"%
                                self.pid_file)
            except:
                err = "Cannot start HAProxy: pid file %s hasn't been created. " \
                        "Details: %s" % (self.pid_file, sys.exc_info()[1])
                raise initdv2.InitdError, err, sys.exc_info()[2]
 
 
    def stop(self):
        if os.path.exists(self.pid_file):
            try:
                pid = self.pid()
                if pid:
                    os.kill(pid, signal.SIGTERM)
                    util.wait_until(lambda: not os.path.exists('/proc/%s' % pid),
                            timeout=self.timeout, sleep=0.2, error_text="Can't stop HAProxy")
                    if os.path.exists('/proc/%s' % pid):
                        os.kill(pid, signal.SIGKILL)
            except:
                err = "Error stopping service. Details: %s" % sys.exc_info()[1]
                raise initdv2.InitdError, err, sys.exc_info()[2]
            finally:
                os.remove(self.pid_file)
 
 
    def restart(self):
        try:
            self.stop()
        except:
            LOG.debug('Error stopping HAProxy. Details: %s%s'% (sys.exc_info()[1], sys.exc_info()[2]))
        self.start()
 
 
    def reload(self):
        try:
            if os.path.exists(self.pid_file):
                pid = self.pid()
                if pid:
                    args = [self.haproxy_exec, '-f', self.config_path, '-p', self.pid_file, '-D', '-sf', pid]
                    util.system2(args, close_fds=True, logger=LOG, preexec_fn=os.setsid)
                    util.wait_until(lambda: self.pid() and self.pid() != pid,
                            timeout=self.timeout, sleep=0.5, error_text="Error reloading HAProxy service process.")
                    if self.status() != 0:
                        raise initdv2.InitdError("HAProxy service not running.")
            else:
                raise LookupError('File %s not exist'%self.pid_file)
        except:
            raise initdv2.InitdError, "HAProxy service not running can't reload it."\
                    " Details: %s" % sys.exc_info()[1], sys.exc_info()[2]
 
 
    def pid(self):
        '''Read #pid of the process from pid_file'''
        if os.path.isfile(self.pid_file):
            with open(self.pid_file, 'r') as fp:
                return long(fp.read())
 
initdv2.explore(SERVICE_NAME, HAProxyInitScript)
 
