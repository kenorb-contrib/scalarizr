from __future__ import with_statement
'''
Created on Dec 24, 2009

@author: marat
'''

import os
import re
import socket
import urllib2
import logging
import platform
import sys
import struct
import array
import threading
import ConfigParser
import time

from scalarizr import node
from scalarizr.bus import bus
from scalarizr import linux
from scalarizr.util import metadata
from scalarizr.util import LocalPool, NullPool
if linux.os.windows_family:
    import win32com.client
else:
    import fcntl


class PlatformError(BaseException):
    pass


class UserDataOptions:
    FARM_ID = "farmid"
    SERVER_ID = "serverid"
    ROLE_NAME = "realrolename"
    BEHAVIOUR = 'behaviors'
    CRYPTO_KEY = "szr_key"
    QUERYENV_URL = "queryenv_url"
    MESSAGE_SERVER_URL = "p2p_producer_endpoint"
    FARM_HASH = "hash"
    CLOUD_STORAGE_PATH = 'cloud_storage_path'
    ENV_ID = 'env_id'
    FARMROLE_ID = 'farm_roleid'
    ROLE_ID = 'roleid'
    REGION = 'region'
    MESSAGE_FORMAT = 'message_format'
    OWNER_EMAIL = 'owner_email'


class ConnectionError(Exception):
    pass


class NoCredentialsError(ConnectionError):
    pass


class InvalidCredentialsError(ConnectionError):
    pass


class ConnectionProxy(object):

    _logger = logging.getLogger(__name__)
    max_retries = 3

    def __init__(self, conn_pool, obj=None):
        self.obj = obj or conn_pool.get()
        self.conn_pool = conn_pool

    def __getattribute__(self, name):
        if re.search('^__.*__$', name):
            return getattr(object.__getattribute__(self, 'obj'), name)
        try:
            return object.__getattribute__(self, name)
        except AttributeError:
            connProxyCls = object.__getattribute__(self, '__class__')
            return connProxyCls(
                    self.conn_pool,
                    obj=getattr(object.__getattribute__(self, 'obj'), name))

    def __call__(self, *args, **kwds):
        for retry in range(self.max_retries):
            try:
                return self.invoke(*args, **kwds)
            except ConnectionError:
                break
            except:
                if retry < self.max_retries - 1:
                    time.sleep(1)
        self.conn_pool.dispose_local()
        raise sys.exc_info()[0], sys.exc_info()[1], sys.exc_info()[2]

    def invoke(self, *args, **kwds):
        '''Invoke wrapped object (override in subclass)'''
        return self.obj(*args, **kwds)


class PlatformFactory(object):
    _platforms = {}

    def new_platform(self, name):
        if not self._platforms.has_key(name):
            pl = __import__("scalarizr.platform." + name, globals(), locals(), fromlist=["get_platform"])
            self._platforms[name] = pl.get_platform()

        return self._platforms[name];


class PlatformFeatures:
    VOLUMES         = 'volumes'
    SNAPSHOTS       = 'snapshots'


class Platform():
    name = None
    _arch = None
    _userdata = None
    _logger = logging.getLogger(__name__)
    features = []
    scalrfs = None


    def __init__(self):
        self.scalrfs = self._scalrfs(self)
        node.__node__['access_data'] = {}

    def get_private_ip(self):
        return self.get_public_ip()

    def get_public_ip(self):
        return socket.gethostbyname(socket.gethostname())

    def get_user_data(self, key=None):
        if key:
            return meta.user_data().get(key)
        else:
            return meta.user_data()

    def set_access_data(self, access_data):
        node.__node__['access_data'] = access_data

    def get_access_data(self, prop=None):
        if prop:
            try:
                return node.__node__['access_data'][prop]
            except (TypeError, KeyError):
                raise PlatformError("Platform access data property '%s' doesn't exists" % (prop,))
        else:
            return node.__node__['access_data']

    def clear_access_data(self):
        node.__node__['access_data'] = {}

    def get_architecture(self):
        """
        @return Architectures
        """
        if self._arch is None:

            if linux.os.windows_family:
                if '32' in platform.architecture()[0]:
                    self._arch = Architectures.I386
                else:
                    self._arch = Architectures.X86_64
            else:
                uname = os.uname()
                if re.search("^i\\d86$", uname[4]):
                    self._arch = Architectures.I386
                elif re.search("^x86_64$", uname[4]):
                    self._arch = Architectures.X86_64
                else:
                    self._arch = Architectures.UNKNOWN
        return self._arch

    @property
    def cloud_storage_path(self):
        try:
            return bus.cnf.rawini.get('general', 'cloud_storage_path')
        except ConfigParser.NoOptionError:
            return ''

    def _parse_user_data(self, raw_userdata):
        userdata = {}
        for k, v in re.findall("([^=]+)=([^;]*);?", raw_userdata):
            userdata[k] = v
        return userdata

    def _raise_no_access_data(self):
        msg = 'There are no credentials from cloud services: %s' % self.name
        raise NoCredentialsError(msg)


    class _scalrfs(object):

        def __init__(self, platform):
            self.platform = platform
            self.ini = bus.cnf.rawini


        def root(self):
            scalr_id = ''
            if bus.queryenv_version >= (2012, 7, 1):
                queryenv = bus.queryenv_service
                scalr_id = queryenv.get_global_config()['params'].get('scalr.id', '')
            if scalr_id:
                scalr_id = '-' + scalr_id
            if bus.scalr_version >= (3, 1, 0):
                return '%s://scalr%s-%s-%s' % (
                        self.platform.cloud_storage_path.split('://')[0],
                        scalr_id,
                        self.ini.get('general', 'env_id'),
                    self.ini.get('general', 'region')
                )
            else:
                return self.platform.cloud_storage_path


        def images(self):
            if bus.scalr_version >= (3, 1, 0):
                return os.path.join(self.root(), 'images/')
            else:
                return '%s://scalr2-images-%s-%s' % (
                        self.platform.cloud_storage_path.split('://')[0],
                        self.ini.get('general', 'region'),
                        self.platform.get_account_id()
                )


        def backups(self, service):
            if bus.scalr_version >= (3, 1, 0):
                path = 'backups/%s/%s/%s-%s' % (
                        self.ini.get('general', 'farm_id'),
                        service,
                        self.ini.get('general', 'farm_role_id'),
                        self.ini.get('general', 'role_name')
                )
                return os.path.join(self.root(), path)
            else:
                return os.path.join(self.root(), '%s-backup' % service)


class Ec2LikePlatform(Platform):

    _meta_url = "http://169.254.169.254/"
    _userdata_key = 'latest/user-data'
    _metadata_key = 'latest/meta-data'
    _metadata = {}
    _userdata = None

    def __init__(self):
        Platform.__init__(self)
        self._logger = logging.getLogger(__name__)
        self._cnf = bus.cnf

    def _get_property(self, name):
        if not self._metadata.has_key(name):
            full_name = self._metadata_key + "/" + name
            self._metadata[name] = self._fetch_metadata(full_name)
        return self._metadata[name]

    def _fetch_metadata(self, key):
        url = self._meta_url + key
        try:
            r = urllib2.urlopen(url)
            return r.read().strip()
        except IOError, e:
            if isinstance(e, urllib2.HTTPError):
                if e.code == 404:
                    return ""
            raise PlatformError("Cannot fetch %s metadata url '%s'. Error: %s" % (self.name, url, e))

    def get_private_ip(self):
        return self._get_property("local-ipv4")

    def get_public_ip(self):
        return self._get_property("public-ipv4")

    def get_public_hostname(self):
        return self._get_property("public-hostname")

    def get_instance_id(self):
        return self._get_property("instance-id")

    def get_instance_type(self):
        return self._get_property("instance-type")

    def get_ami_id(self):
        return self._get_property("ami-id")

    def get_ancestor_ami_ids(self):
        return self._get_property("ancestor-ami-ids").split("\n")

    def get_kernel_id(self):
        return self._get_property("kernel-id")

    def get_ramdisk_id(self):
        return self._get_property("ramdisk-id")

    def get_avail_zone(self):
        return self._get_property("placement/availability-zone")

    def get_region(self):
        return self.get_avail_zone()[0:-1]

    def get_block_device_mapping(self):
        keys = self._get_property("block-device-mapping").split("\n")
        ret = {}
        for key in keys:
            ret[key] = self._get_property("block-device-mapping/" + key)
        return ret

    def block_devs_mapping(self):
        keys = self._get_property("block-device-mapping").split("\n")
        ret = list()
        for key in keys:
            ret.append((key, self._get_property("block-device-mapping/" + key)))
        return ret

    def get_ssh_pub_key(self):
        return self._get_property("public-keys/0/openssh-key")

class Architectures:
    I386 = "i386"
    X86_64 = "x86_64"
    UNKNOWN = "unknown"


if linux.os.windows_family:
    from scalarizr.util import coinitialized

    @coinitialized
    def net_interfaces():
        wmi = win32com.client.GetObject('winmgmts:')
        wql = "SELECT IPAddress FROM Win32_NetworkAdapterConfiguration WHERE IPEnabled = 'True'"
        result = wmi.ExecQuery(wql)
        return list({
                'iface': None,
                'ipv4': row.IPAddress[0],
                'ipv6': row.IPAddress[1] if len(row.IPAddress) > 1 else None
                } for row in result)
 
else:
    def net_interfaces():
        # http://code.activestate.com/recipes/439093-get-names-of-all-up-network-interfaces-linux-only/#c7
        is_64bits = sys.maxsize > 2**32
        struct_size = 40 if is_64bits else 32
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        max_possible = 8 # initial value
        while True:
            num_bytes = max_possible * struct_size
            names = array.array('B', '\0' * num_bytes)
            outbytes = struct.unpack('iL', fcntl.ioctl(
                s.fileno(),
                0x8912,  # SIOCGIFCONF
                struct.pack('iL', num_bytes, names.buffer_info()[0])
            ))[0]
            if outbytes == num_bytes:
                max_possible *= 2
            else:
                break
        namestr = names.tostring()
        return list({
                'iface': namestr[i:i+16].split('\0', 1)[0],
                'ipv4': socket.inet_ntoa(namestr[i+20:i+24]),
                'ipv6': None
                } for i in range(0, outbytes, struct_size))


def is_private_ip(ipaddr):
    return any(map(lambda x: ipaddr.startswith(x), ('10.', '172.', '192.168.')))

def is_local_ip(ipaddr):
    return ipaddr.startswith('127.')

def is_public_ip(ipaddr):
    return not (is_private_ip(ipaddr) or is_local_ip(ipaddr))

meta = metadata.Metadata()
