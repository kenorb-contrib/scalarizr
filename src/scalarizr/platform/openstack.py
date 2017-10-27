import logging
import urllib2
import json
import os
import re
import sys
from time import sleep
 
 
from cinderclient.v1 import client as cinder_client
from novaclient.v1_1 import client as nova_client
import swiftclient
 
 
from scalarizr import platform
from scalarizr.bus import bus
from scalarizr import linux
from scalarizr.storage.transfer import Transfer, TransferProvider
from scalarizr.storage2.cloudfs import swift as swiftcloudfs
 
 
LOG = logging.getLogger(__name__)
 
 
 
 
class OpenstackServiceWrapper(object):
    def _make_connection(self, **kwargs):
        raise NotImplementedError()
 
    def __init__(self, user, password, tenant, auth_url, region_name=None):
        self.user = user
        self.password = password
        self.tenant = tenant
        self.auth_url = auth_url
        self.region_name = region_name
        self.auth_plugin = None
        if os.environ.get('OS_AUTH_SYSTEM'):
            try:
                import novaclient.auth_plugin
                self.auth_plugin = novaclient.auth_plugin.load_plugin(os.environ['OS_AUTH_SYSTEM'])
            except ImportError:
                pass
        self.connection = None
        self.connect = self.reconnect
 
    def __getattr__(self, name):
        return getattr(self.connection, name)
 
    def reconnect(self):
        self.connection = self._make_connection()
 
    #TODO: make connection check more properly
    def has_connection(self):
        self.reconnect()
        return self.connection is not None
 
 
class CinderWrapper(OpenstackServiceWrapper):
 
    def _make_connection(self, **kwargs):
        kwargs = kwargs or {}
        kwargs.update(dict(
            auth_url=self.auth_url,
            region_name=self.region_name          
        ))
        return cinder_client.Client(self.user,
                                    self.password,
                                    self.tenant,
                                    **kwargs)
 
 
 
class NovaWrapper(OpenstackServiceWrapper):
 
    def _make_connection(self, service_type='compute', **kwargs):
        kwargs = kwargs or {}
        kwargs.update(dict(
            auth_url=self.auth_url,
            region_name=self.region_name,
            service_type=service_type           
        ))
        if self.auth_plugin:
            kwargs['auth_plugin'] = self.auth_plugin
        return nova_client.Client(self.user,
                                  self.password,
                                  self.tenant,
                                  **kwargs)
 
 
class OpenstackPlatform(platform.Platform):
 
    _meta_url = "http://169.254.169.254/openstack/latest/meta_data.json"
    _metadata = {}
    _userdata = None
 
    _ip_addr = None
 
    features = ['volumes', 'snapshots']
 
    def __init__(self):
        platform.Platform.__init__(self)
        if not linux.os.windows_family:
            # Work over [Errno -3] Temporary failure in name resolution
            # http://bugs.centos.org/view.php?id=4814
            os.chmod('/etc/resolv.conf', 0755)
 
    def _get_ip_addr(self):
        if not self._ip_addr:
            ifaces = platform.net_interfaces()
            try:
                self._ip_addr = [iface['ipv4'] for iface in ifaces 
                        if platform.is_private_ip(iface['ipv4'])][0]
            except IndexError:
                try:
                    self._ip_addr = [iface['ipv4'] for iface in ifaces 
                            if platform.is_public_ip(iface['ipv4'])][0]
                except IndexError:
                    pass
        return self._ip_addr
    get_public_ip = _get_ip_addr
    get_private_ip = _get_ip_addr
 
    def _get_property(self, name):
        if not name in self._userdata:
            self.get_user_data()
        return self._userdata[name]
 
    def get_server_id(self):
        nova = self.new_nova_connection()
        nova.connect()
        servers = nova.servers.list()
        my_ip = self.get_private_ip()
        for server in servers:
            ips = []
            ip_addr = 'private' in server.addresses and server.addresses['private'][0]['addr']
            if ip_addr:
                ips.append(ip_addr)
            else:
                ips = [address['addr'] 
                            for network in server.addresses.values()
                            for address in network]
            if my_ip in ips:
                return server.id
 
        raise BaseException("Can't get server_id because we can't get "
                            "server private ip")
 
    def get_avail_zone(self):
        return self._get_property('availability_zone')
 
    def get_ssh_pub_key(self):
        return self._get_property('public_keys')  # TODO: take one key
 
    def get_user_data(self, key=None):
        if self._userdata is None:
            self._metadata = self._fetch_metadata()
            self._userdata = self._metadata['meta']
        if key:
            return self._userdata[key] if key in self._userdata else None
        else:
            return self._userdata
 
    def _fetch_metadata(self):
        """
        Fetches whole metadata dict. Unlike Ec2LikePlatform,
        which fetches data for concrete key.
        """
 
        try:
            try:
                self._logger.debug('fetching meta-data from %s', self._meta_url)
                r = urllib2.urlopen(self._meta_url)
                response = r.read().strip()
                meta = json.loads(response) 
            except:
                self._logger.debug('failed to fetch meta-data: %s', sys.exc_info()[1])
            else:
                if meta.get('meta'):
                    return meta
                else:
                    self._logger.debug('meta-data fetched, but has empty user-data (a "meta" key), try next method')
 
            return {'meta': self._fetch_metadata_from_file()}
        except:
            raise platform.PlatformError, 'failed to fetch meta-data', sys.exc_info()[2]   
 
    def _fetch_metadata_from_file(self):
        cnf = bus.cnf
        if self._userdata is None:
            for path in ('/etc/.scalr-user-data', cnf.private_path('.user-data')):
                if os.path.exists(path):
                    rawmeta = None
                    with open(path, 'r') as fp:
                        rawmeta = fp.read()
                    if not rawmeta:
                        raise platform.PlatformError("Empty user-data")
                    return self._parse_user_data(rawmeta)
        return self._userdata
 
    def set_access_data(self, access_data):
        self._access_data = access_data
        # if it's Rackspace NG, we need to set env var CINDER_RAX_AUTH
        # and NOVA_RAX_AUTH for proper nova and cinder authentication
        if 'rackspacecloud' in self._access_data["keystone_url"]:
            # python-novaclient has only configuration with environ variables 
            # to enable Rackspace specific authentification
            os.environ["CINDER_RAX_AUTH"] = "True"
            os.environ["NOVA_RAX_AUTH"] = "True"
            os.environ["OS_AUTH_SYSTEM"] = "rackspace"
 
    def new_cinder_connection(self):
        if not self._access_data:
            return None
        api_key = self._access_data["api_key"]
        password = self._access_data["password"]
        return CinderWrapper(self._access_data["username"],
                             password or api_key,
                             self._access_data["tenant_name"],
                             self._access_data["keystone_url"],
                             self._access_data["cloud_location"])
 
    def new_nova_connection(self):
        if not self._access_data:
            return None
        api_key = self._access_data["api_key"]
        password = self._access_data["password"]
        return NovaWrapper(self._access_data["username"],
                           password or api_key,
                           self._access_data["tenant_name"],
                           self._access_data["keystone_url"],
                           self._access_data["cloud_location"])
 
    def new_swift_connection(self):
        if not self._access_data:
            return None
        api_key = self._access_data["api_key"]
        password = self._access_data["password"]
        keystone_url = self._access_data["keystone_url"]
        kwds = {}
        if 'rackspacecloud' in self._access_data["keystone_url"]:
            keystone_url = re.sub(r'v2\.\d$', 'v1.0',
                            self._access_data['keystone_url'])
            kwds['auth_version'] = '1'
        else:
            kwds['auth_version'] = '2'
            kwds['tenant_name'] = self._access_data["tenant_name"]
 
        return swiftclient.Connection(keystone_url, 
                    self._access_data["username"],
                    password or api_key,
                    **kwds)
 
 
def get_platform():
    # Filter keystoneclient* and swiftclient* log messages
    class FalseFilter:
        def filter(self, record):
            return False
    for cat in ('keystoneclient', 'swiftclient'):
        log = logging.getLogger(cat)
        log.addFilter(FalseFilter())
 
    return OpenstackPlatform()
 
 
class SwiftTransferProvider(TransferProvider):
    schema = 'swift'
 
    _logger = None
 
    def __init__(self):
        self._logger = logging.getLogger(__name__)
        self._driver = swiftcloudfs.SwiftFileSystem()  
        TransferProvider.__init__(self)   
 
    def put(self, local_path, remote_path):
        self._logger.info('Uploading %s to Swift under %s' % (local_path, remote_path))
        return self._driver.put(local_path, os.path.join(remote_path, os.path.basename(local_path)))
 
    def get(self, remote_path, local_path):
        self._logger.info('Downloading %s from Swift to %s' % (remote_path, local_path))
        return self._driver.get(remote_path, local_path)
 
 
    def list(self, remote_path):
        return self._driver.ls(remote_path)
 
 
Transfer.explore_provider(SwiftTransferProvider)
 
# Logging 
 
class OpenStackCredentialsLoggerFilter(object):
 
    request_re = re.compile('(X-Auth[^:]+:)([^\'"])+')
    response_re = re.compile('(.*)({["\']access.+})(.*)')
 
    def filter(self, record):
        message = record.getMessage()
        record.args = ()
 
        if "passwordCredentials" in message:
            record.msg = 'Requested authentication, credentials are hidden'
            return True
 
        search_res = re.search(self.response_re, message)
        if search_res:
            try:
                response_part_str = search_res.group(2)
                response = json.loads(response_part_str)
                response['access']['token'] = '<HIDDEN>'
                response['access']['user'] = '<HIDDEN>'
                altered_resp = json.dumps(response)
                record.msg = search_res.group(1) + altered_resp + search_res.group(3)
                return True
            except:
                return False
 
        if "X-Auth" in message:
            record.msg = re.sub(self.request_re, r'\1 <HIDDEN>', message)
            return True
 
 
class InfoToDebugFilter(object):
    def filter(self, record):
        if record.levelno == logging.INFO:
            record.levelno = logging.DEBUG
            record.levelname = logging.getLevelName(record.levelno)
            return True
 
 
openstack_filter = OpenStackCredentialsLoggerFilter()
for logger_name in ('keystoneclient.client', 'novaclient.client', 'cinderclient.client'):
    logger = logging.getLogger(logger_name)
    logger.addFilter(openstack_filter)
 
 
logger = logging.getLogger('requests.packages.urllib3.connectionpool')
logger.addFilter(InfoToDebugFilter())
 
 
