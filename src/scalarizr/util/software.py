from __future__ import with_statement
"""
Created on Sep 10, 2010
 
@author: marat
"""
 
from scalarizr.util import system2
from scalarizr import linux
from scalarizr.linux import coreutils, pkgmgr, which
import os, re, zipfile, glob, platform
 
__all__ = ('all_installed', 'software_info', 'explore', 'which')
 
def all_installed():
    ret = []
    for getinfo_func in software_list.itervalues():
        try:
            ret.append(getinfo_func())
        except:
            pass
    return ret
 
 
def software_info(name):
    if not software_list.has_key(name):
        raise Exception("Unknown software: %s" % name)
    return software_list[name]()
 
def explore(name, lookup_fn):
    if name in software_list.keys():
 
        raise Exception("'%s' software has been already explored" % name)
    software_list[name] = lookup_fn
 
 
def system_info(verbose=False):
 
    def check_module(module):
        try:
            return not coreutils.modprobe(module, dry_run=True)[2]
        except:
            return False
 
    ret = {}
    ret['software'] = []
    installed_list = all_installed()
    for software_inf in installed_list:
        v = dict(
                name=software_inf.name,
                version='.'.join([str(x) for x in software_inf.version])
        )
        if verbose:
            v['string_version'] = software_inf.string_version
 
        ret['software'].append(v)
 
 
    ret['os'] = {}
    ret['os']['version'] = '{0} {1} {2}'.format(linux.os['name'], linux.os['release'], linux.os['codename']).strip()
    ret['os']['string_version'] = ' '.join(platform.uname()).strip()
 
    ret['dist'] = {
            'distributor': linux.os['name'].lower(),
            'release': str(linux.os['release']),
            'codename': linux.os['codename']
    }
 
    ret['os']['arch'] = linux.os['arch']
 
    ret['storage'] = {}
    ret['storage']['fstypes'] = []
 
    for fstype in ['xfs', 'ext3', 'ext4']:
        try:
            retcode = coreutils.modprobe(fstype, dry_run=True)[1]
        except:
            retcode = 1
        exe = which('mkfs.%s' % fstype)
        if not retcode and exe:
            ret['storage']['fstypes'].append(fstype)
 
    # Raid levels support detection
    if which('mdadm'):
        for module in  ('raid0', 'raid1', 'raid456'):
            ret['storage'][module] = 1 if check_module(module) else 0
 
    # Lvm2 support detection
    if which('dmsetup') and all(map(check_module, ('dm_mod', 'dm_snapshot'))):
        ret['storage']['lvm'] = 1
    else:
        ret['storage']['lvm'] = 0
 
    return ret
 
 
class SoftwareError(BaseException):
    pass
 
class SoftwareInfo(object):
    name = None
    version = None
    '''
    @param version: tuple(major, minor, bugfix)
    '''
    string_version = None
 
    def __init__(self, name, version, string_version):
        self.name               = name
        self.string_version = string_version
        ver_nums                = map(int, version.split('.'))
        if len(ver_nums) < 3:
            for _ in range(len(ver_nums), 3):
                ver_nums.append(0)
        self.version = tuple(ver_nums)
 
software_list = dict()
 
def mysql_software_info():
 
    binary = which('mysqld')
    if not binary:
        raise SoftwareError("Can't find executable for MySQL server")
 
    version_string = system2((binary, '-V'))[0].strip()
    if not version_string:
        raise SoftwareError
 
    res = re.search('Ver\s+([\d\.]+)', version_string)
    if res:
        version = res.group(1)
        return SoftwareInfo('mysql', version, version_string)
    raise SoftwareError
 
 
explore('mysql', mysql_software_info)
 
def nginx_software_info():
    binary = which('nginx', path_append='/usr/local/nginx/sbin')
    if not binary:
        raise SoftwareError("Can't find executable for Nginx server")
 
    out = system2((binary, '-V'))[1]
    if not out:
        raise SoftwareError
 
    version_string = out.splitlines()[0]
    res = re.search('[\d\.]+', version_string)
    if res:
        version = res.group(0)
        return SoftwareInfo('nginx', version, out)
    raise SoftwareError
 
 
explore('nginx', nginx_software_info)
 
def memcached_software_info():
    binary = which('memcached')
    if not binary:
        raise SoftwareError("Can't find executable for Memcached")
 
    out = system2((binary, '-h'))[0]
    if not out:
        raise SoftwareError
 
    version_string = out.splitlines()[0]
 
    res = re.search('memcached\s+([\d\.]+)', version_string)
    if res:
        version = res.group(1)
        return SoftwareInfo('memcached', version, version_string)
    raise SoftwareError
 
explore('memcached', memcached_software_info)
 
def php_software_info():
    binary = which('php')
    if not binary:
        raise SoftwareError("Can't find executable for php interpreter")
 
    out = system2((binary, '-v'))[0]
    if not out:
        raise SoftwareError
 
    version_string = out.splitlines()[0]
 
    res = re.search('PHP\s+([\d\.]+)', version_string)
 
    if res:
        version = res.group(1)
        return SoftwareInfo('php', version, out)
    raise SoftwareError
 
explore('php', php_software_info)
 
def python_software_info():
    binary = which('python')
    if not binary:
        raise SoftwareError("Can't find executable for python interpreter")
 
    version_string = system2((binary, '-V'))[1].strip()
    if not version_string:
        raise SoftwareError
 
    version_string = version_string.splitlines()[0]
 
    res = re.search('Python\s+([\d\.]+)', version_string)
 
    if res:
        version = res.group(1)
        return SoftwareInfo('python', version, version_string)
 
    raise SoftwareError
 
explore('python', python_software_info)
 
def mysqlproxy_software_info():
    binary = which('mysql-proxy')
    if not binary:
        raise SoftwareError("Can't find executable for mysql-proxy")
 
    version_string = system2((binary, '-V'))[0].strip()
    if not version_string:
        raise SoftwareError
 
    version_string = version_string.splitlines()[0]
 
    res = re.search('mysql-proxy\s+([\d\.]+)', version_string)
 
    if res:
        version = res.group(1)
        return SoftwareInfo('mysql-proxy', version, version_string)
 
    raise SoftwareError
 
explore('mysql-proxy', mysqlproxy_software_info)
 
def apache_software_info():
 
    binary_name = "httpd" if linux.os.redhat_family else "apache2"
    binary = which(binary_name)
    if not binary:
        raise SoftwareError("Can't find executable for apache http server")
 
    out = system2((binary, '-V'))[0]
    if not out:
        raise SoftwareError
 
    version_string = out.splitlines()[0]
    res = re.search('[\d\.]+', version_string)
    if res:
        version = res.group(0)
 
        return SoftwareInfo('apache', version, out)
    raise SoftwareError
 
 
explore('apache', apache_software_info)
 
 
def tomcat_software_info():
    catalina_home = linux.system('echo $CATALINA_HOME', shell=True)[0].strip()
    if not catalina_home:
        try:
            catalina_home = glob.glob('/opt/apache-tomcat*')[0]
        except IndexError:
            pass
    if not catalina_home:
        try:
            catalina_home = glob.glob('/usr/share/*tomcat*')[0]
        except IndexError:
            msg = (
                "Can't find Tomcat installation\n"
                " - CATALINA_HOME env variable is unset\n"
                " - /opt/apache-tomcat*\n"
                " - /usr/share/*tomcat* search is empty\n"
            )
            raise SoftwareError(msg)
 
    catalina_jar = os.path.join(catalina_home, 'lib/catalina.jar')
    if not os.path.exists(catalina_jar):
        msg = "Can't get Tomcat version: file {0} not exists".format(catalina_jar)
        raise SoftwareError(msg)
 
    catalina = zipfile.ZipFile(catalina_jar, 'r')
    try:
        properties_file = 'org/apache/catalina/util/ServerInfo.properties'
        if not properties_file in catalina.namelist():
            raise SoftwareError("ServerInfo.properties file isn't in catalina.jar")
 
        properties = catalina.read(properties_file)
        properties = re.sub(re.compile('^#.*$', re.M), '', properties).strip()
 
        res = re.search('^server.info=Apache\s+Tomcat/([\d\.]+)', properties, re.M)
        if res:
            version = res.group(1)
            return SoftwareInfo('tomcat', version, properties)
        raise SoftwareError
    finally:
        catalina.close()
 
explore('tomcat', tomcat_software_info)
 
def varnish_software_info():
    binary = which('varnishd')
    if not binary:
        raise SoftwareError("Can't find executable for varnish HTTP accelerator")
 
    out = system2((binary, '-V'))[1].strip()
    if not out:
        raise SoftwareError
 
    version_string = out.splitlines()[0]
 
    res = re.search('varnish-([\d\.]+)', version_string)
 
    if res:
        version = res.group(1)
        return SoftwareInfo('varnish', version, out)
 
    raise SoftwareError
 
explore('varnish', varnish_software_info)
 
def rails_software_info():
    binary = which('gem')
 
    if not binary:
        raise SoftwareError("Can't find executable for ruby gem packet manager")
 
    out = system2((binary, 'list', 'rails'))[0].strip()
 
    if not out:
        raise SoftwareError
 
    res = re.search('\(([\d\.]+)\)', out)
 
    if res:
        version = res.group(1)
        return SoftwareInfo('rails', version, '')
 
    raise SoftwareError
 
explore('rails', rails_software_info)
 
def cassandra_software_info():
    cassandra_path = '/usr/share/cassandra/apache-cassandra.jar'
 
    if not os.path.exists(cassandra_path):
        raise SoftwareError("Can't find apache-cassandra.jar file with Cassandra version info")
 
    cassandra = zipfile.ZipFile(cassandra_path)
 
    try:
        properties_path = 'META-INF/MANIFEST.MF'
 
        if not properties_path in cassandra.namelist():
            raise SoftwareError("MANIFEST.MF file isn't in apache-cassandra.jar")
 
        properties = cassandra.read(properties_path)
 
        res = re.search('^Implementation-Version:\s*([\d\.]+)', properties, re.M)
        if res:
            version = res.group(1)
            return SoftwareInfo('cassandra', version, '')
        raise SoftwareError()
    finally:
        cassandra.close()
 
explore('cassandra', cassandra_software_info)
 
 
def rabbitmq_software_info():
 
    pkg_mgr = pkgmgr.package_mgr()
    version = pkg_mgr.info('rabbitmq-server')['installed']
    version = re.search('[\d\.]+', version).group(0)
    return SoftwareInfo('rabbitmq', version, version)
 
explore('rabbitmq', rabbitmq_software_info)
 
 
def redis_software_info():
 
    binary_name = "redis-server" 
    binary = which(binary_name)
    if not binary:
        raise SoftwareError("Can't find executable for redis server")
 
    out = system2((binary, '-v'))[0]
    if not out:
        raise SoftwareError()
 
    version_string = out.splitlines()[0]
    res = re.search('[\d\.]+', version_string)
    if res:
        version = res.group(0)
 
        return SoftwareInfo('redis', version, out)
    raise SoftwareError
explore('redis', redis_software_info)
 
 
def haproxy_software_info():
 
    binary_name = "haproxy"
    binary = which(binary_name)
    if not binary:
        raise SoftwareError("Can't find executable for HAProxy")
 
    out = system2((binary, '-v'))[0]
    if not out:
        raise SoftwareError()
 
    version_string = out.splitlines()[0]
    res = re.search('[\d\.]+', version_string)
    if res:
        version = res.group(0)
 
        return SoftwareInfo('haproxy', version, out)
    raise SoftwareError
explore('haproxy', haproxy_software_info)
 
 
def mongodb_software_info():
    try:
        mongod = which('mongod')
    except:
        raise SoftwareError("Can't find mongodb server executable")
    else:
        out = system2((mongod, '--version'))[0]
        version_string = out.splitlines()[0]
        m = re.search('[\d\.]+', version_string)
        if m:
            return SoftwareInfo('mongodb', m.group(0), out)
        raise SoftwareError("Can't parse `mongod --version` output")
explore('mongodb', mongodb_software_info)
 
 
def chef_software_info():
    binary = which('chef-client')
    if not binary:
        raise SoftwareError("Can't find executable for chef client")
 
    version_string = linux.system((binary, '-v'))[0].strip()
    if not version_string:
        raise SoftwareError
 
    res = re.search('Chef:\s+([\d\.]+)', version_string)
 
    if res:
        version = res.group(1)
        return SoftwareInfo('chef', version, version_string)
 
    raise SoftwareError
explore('chef', chef_software_info)
 
 
def postgresql_software_info():
    binaries = []
    amazon_linux_binpath = '/usr/bin/postgres'
    versions_dirs = glob.glob('/usr/lib/p*sql/*')
    versions_dirs.extend(glob.glob('/usr/p*sql*/'))
    versions_dirs.sort()
    versions_dirs.reverse()
    for version in versions_dirs:
        bin_path = os.path.join(version, 'bin/postgres')
        if os.path.isfile(bin_path):
            binaries.append(bin_path)
 
    if os.path.isfile(amazon_linux_binpath):
        binaries.append(amazon_linux_binpath) #Amazon Linux support
 
    for bin_path in binaries:
        version_string = system2((bin_path, '--version'))[0].strip()
        version = version_string.split()[-1]
        return SoftwareInfo('postgresql', version, version_string)
    else:
        raise SoftwareError
 
explore('postgresql', postgresql_software_info)
 
