import logging
import urllib2
import json
import os
import re
import sys
import time


import novaclient
import swiftclient
import cinderclient.exceptions
import novaclient.exceptions

from cinderclient.v1 import client as cinder_client
from novaclient.v1_1 import client as nova_client


from scalarizr import node
from scalarizr import platform
from scalarizr.bus import bus
from scalarizr import linux
from scalarizr.util import LocalPool
from scalarizr.platform import PlatformError
from scalarizr.platform import NoCredentialsError, InvalidCredentialsError, ConnectionError
from scalarizr.storage.transfer import Transfer, TransferProvider
from scalarizr.storage2.cloudfs import swift as swiftcloudfs
from scalarizr.config import BuiltinPlatforms


LOG = logging.getLogger(__name__)


def _create_nova_connection():
    try:
        platform = node.__node__['platform']
        kwds = dict(
            auth_url=platform.get_access_data('keystone_url'),
            region_name=platform.get_access_data('cloud_location'),
            service_type='compute'
        )
        if not bool(platform.get_access_data('ssl_verify_peer')):
            kwds['insecure'] = True
        import novaclient # NameError: name 'novaclient' is not defined
        if hasattr(novaclient, '__version__') and os.environ.get('OS_AUTH_SYSTEM'):
            try:
                import novaclient.auth_plugin
                auth_plugin = novaclient.auth_plugin.load_plugin(os.environ['OS_AUTH_SYSTEM'])
                kwds['auth_plugin'] = auth_plugin
            except ImportError:
                pass
        conn = nova_client.Client(
            platform.get_access_data('username'),
            platform.get_access_data('api_key') or platform.get_access_data('password'),
            platform.get_access_data('tenant_name'),
            **kwds
        )
    except PlatformError:
        raise NoCredentialsError(sys.exc_info()[1])
    return conn


def _create_cinder_connection():
    try:
        platform = node.__node__['platform']
        kwds = dict(
            auth_url=platform.get_access_data('keystone_url'),
            region_name=platform.get_access_data('cloud_location')
        )
        if not bool(platform.get_access_data('ssl_verify_peer')):
            kwds['insecure'] = True
        conn = cinder_client.Client(
            platform.get_access_data('username'),
            platform.get_access_data('api_key') or platform.get_access_data('password'),
            platform.get_access_data('tenant_name'),
            **kwds
        )
    except PlatformError:
        raise NoCredentialsError(sys.exc_info()[1])
    return conn


def _create_swift_connection():
    try:
        platform = node.__node__['platform']
        api_key = platform.get_access_data("api_key")
        password = platform.get_access_data("password")
        auth_url = platform.get_access_data("keystone_url")
        kwds = {}
        if 'rackspacecloud' in auth_url:
            auth_url = re.sub(r'v2\.\d$', 'v1.0', auth_url)
            kwds['auth_version'] = '1'
        else:
            kwds['auth_version'] = '2'
            kwds['tenant_name'] = platform.get_access_data("tenant_name")
        if not bool(platform.get_access_data('ssl_verify_peer')):
            kwds['insecure'] = True        
        conn = swiftclient.Connection(
            authurl=auth_url,
            user=platform.get_access_data('username'),
            key=password or api_key,
            **kwds
        )
    except PlatformError:
        raise NoCredentialsError(sys.exc_info()[1])
    return conn


class NovaConnectionProxy(platform.ConnectionProxy):

    def invoke(self, *args, **kwds):
        try:
            return super(NovaConnectionProxy, self).invoke(*args, **kwds)
        except (novaclient.exceptions.Unauthorized, novaclient.exceptions.Forbidden), e:
            raise InvalidCredentialsError(e)


class CinderConnectionProxy(platform.ConnectionProxy):

    def invoke(self, *args, **kwds):
        try:
            return super(CinderConnectionProxy, self).invoke(*args, **kwds)
        except (cinderclient.exceptions.Unauthorized, cinderclient.exceptions.Forbidden), e:
            raise InvalidCredentialsError(e)


class SwiftConnectionProxy(platform.ConnectionProxy):

    def invoke(self, *args, **kwds):
        try:
            return super(SwiftConnectionProxy, self).invoke(*args, **kwds)
        except:
            e = sys.exc_info()[1]
            if isinstance(e, swiftclient.ClientException) and (
                    re.search(r'.*Unauthorised.*', e.msg) or \
                    re.search(r'.*Authorization Failure.*', e.msg)):
                raise InvalidCredentialssError(e)
            else:
                raise


class OpenstackPlatform(platform.Platform):

    _meta_url = "http://169.254.169.254/openstack/latest/meta_data.json"
    _metadata = {}
    _userdata = None

    _ip_addr = None

    features = ['volumes', 'snapshots']
    name = BuiltinPlatforms.OPENSTACK

    def __init__(self):
        platform.Platform.__init__(self)
        if not linux.os.windows_family:
            # Work over [Errno -3] Temporary failure in name resolution
            # http://bugs.centos.org/view.php?id=4814
            os.chmod('/etc/resolv.conf', 0755)
        self._nova_conn_pool = LocalPool(_create_nova_connection)
        self._swift_conn_pool = LocalPool(_create_swift_connection)
        self._cinder_conn_pool = LocalPool(_create_cinder_connection)

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
        if node.__node__['farm_role_id']:
            global_variables = bus.queryenv_service.list_global_variables()
            return global_variables['public']['SCALR_CLOUD_SERVER_ID']
        else:
            nova = self.get_nova_conn()
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
        self._logger.debug('fetching meta-data from files')
        cnf = bus.cnf
        if self._userdata is None:
            for path in ('/etc/.scalr-user-data', cnf.private_path('.user-data')):
                if os.path.exists(path):
                    self._logger.debug('using file %s', path)
                    rawmeta = None
                    with open(path, 'r') as fp:
                        rawmeta = fp.read()
                    if not rawmeta:
                        raise platform.PlatformError("Empty user-data")
                    self._logger.info('Use user-data from %s', path)
                    return self._parse_user_data(rawmeta)
        return self._userdata

    def set_access_data(self, access_data):
        platform.Platform.set_access_data(self, access_data)
        # if it's Rackspace NG, we need to set env var CINDER_RAX_AUTH
        # and NOVA_RAX_AUTH for proper nova and cinder authentication
        if 'rackspacecloud' in access_data["keystone_url"]:
            # python-novaclient has only configuration with environ variables 
            # to enable Rackspace specific authentification
            os.environ["CINDER_RAX_AUTH"] = "True"
            os.environ["NOVA_RAX_AUTH"] = "True"
            os.environ["OS_AUTH_SYSTEM"] = "rackspace"


    def get_nova_conn(self):
        return NovaConnectionProxy(self._nova_conn_pool)

    def get_cinder_conn(self):
        return CinderConnectionProxy(self._cinder_conn_pool)

    def get_swift_conn(self):
        return SwiftConnectionProxy(self._swift_conn_pool)


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


openstack_filter = OpenStackCredentialsLoggerFilter()
for logger_name in ('keystoneclient.client', 'novaclient.client', 'cinderclient.client'):
    logger = logging.getLogger(logger_name)
    logger.addFilter(openstack_filter)

