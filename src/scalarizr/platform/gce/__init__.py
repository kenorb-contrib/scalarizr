__author__ = 'Nick Demyanchuk'

import os
import sys
import base64
import logging
import urllib2
import httplib2
import threading
from httplib import BadStatusLine

try:
    import json
except ImportError:
    import simplejson as json

from oauth2client.client import SignedJwtAssertionCredentials
from apiclient.discovery import build


from scalarizr import node
from scalarizr import platform
from scalarizr.platform import NoCredentialsError, InvalidCredentialsError
from scalarizr.util import LocalPool
from scalarizr.config import BuiltinPlatforms


COMPUTE_RW_SCOPE = ('https://www.googleapis.com/auth/compute', "https://www.googleapis.com/auth/compute.readonly")
STORAGE_FULL_SCOPE = ("https://www.googleapis.com/auth/devstorage.full_control",
                      "https://www.googleapis.com/auth/devstorage.read_only",
                      "https://www.googleapis.com/auth/devstorage.read_write",
                      "https://www.googleapis.com/auth/devstorage.write_only")


LOG = logging.getLogger(__name__)


class GoogleApiClientLoggerFilter:
    def filter(self, record):
        if 'takes exactly' or 'takes at most' in record.message:
            return False
        return True


api_logger = logging.getLogger('oauth2client.util')
api_logger.addFilter(GoogleApiClientLoggerFilter())


def get_platform():
    return GcePlatform()


class BadStatusLineHandler(object):
    def __init__(self, obj):
        self._obj = obj

    def _wrap(self, fn):
        def wrapper(*args, **kwargs):
            tries = 3
            while True:
                try:
                    return fn(*args, **kwargs)
                except BadStatusLine:
                    tries -= 1
                    if not tries:
                        raise
                    LOG.warning('Caught "BadStatusLine" exception from google API, retrying')
        return wrapper

    def __getattribute__(self, item):
        try:
            return object.__getattribute__(self, item)
        except AttributeError:
            item = getattr(self._obj, item)
            if callable(item) or item.__class__.__name__ in ("HttpRequest", "Resource"):
                return BadStatusLineHandler(item)
            else:
                return item

    def __call__(self, *args, **kwargs):
        if self._obj.__name__ in ('execute', 'next_chunk', 'positional_wrapper'):
            return self._wrap(self._obj.__call__)(*args, **kwargs)
        else:
            item = self._obj(*args, **kwargs)
            if item.__class__.__name__ in ("HttpRequest", "Resource"):
                return BadStatusLineHandler(item)
            else:
                return item


class GoogleServiceManager(object):
    """
    Manages 1 service connection per thread
    Works out dead threads' connections
    """

    def __init__(self, pl, s_name, s_ver, *scope):
        self.pl = pl
        self.s_name = s_name
        self.s_ver = s_ver
        self.scope = list(scope)
        self.map = {}
        self.lock = threading.Lock()
        self.pool = []


    def get_service(self):
        current_thread = threading.current_thread()
        with self.lock:
            if not current_thread in self.map:

                # Check other threads
                for t, s in self.map.items():
                    if not t.is_alive():
                        self.pool.append(s)
                        del self.map[t]

                if self.pool:
                    s = self.pool.pop()
                    self.map[current_thread] = s
                    LOG.debug('Returning connection from pool')
                    return s

                LOG.debug('Creating new connection')
                http = self._get_auth()
                s = build(self.s_name, self.s_ver, http=http)
                wrapped = BadStatusLineHandler(s)
                self.map[current_thread] = wrapped

            LOG.debug('Returning existing connection for this thread')
            return self.map[current_thread]


    def _get_auth(self):
        http = httplib2.Http()
        email = self.pl.get_access_data('service_account_name')
        pk = base64.b64decode(self.pl.get_access_data('key'))
        cred = SignedJwtAssertionCredentials(email, pk, scope=self.scope)
        return cred.authorize(http)


class GCEConnectionPool(LocalPool):

    def __init__(self, service_name, api_version, scope):
        super(GCEConnectionPool, self).__init__(self._create_connection)
        self.service_name = service_name
        self.api_version = api_version
        self.scope = list(scope)

    def _create_connection(self):
        platform_obj = node.__node__['platform']
        http = httplib2.Http()
        try:
            email = platform_obj.get_access_data('service_account_name')
            pk = base64.b64decode(platform_obj.get_access_data('key'))
        except platform.PlatformError:
            raise NoCredentialsError(sys.exc_info()[1])
        try:
            cred = SignedJwtAssertionCredentials(email, pk, scope=self.scope)
            conn = build(self.service_name, self.api_version, http=cred.authorize(http))
        except:
            raise InvalidCredentialsError(sys.exc_info()[1])
        return BadStatusLineHandler(conn)


class GCEConnectionProxy(platform.ConnectionProxy):
    pass


class GcePlatform(platform.Platform):
    compute_api_version = 'v1'
    storage_api_version = 'v1'

    metadata_url = 'http://metadata/computeMetadata/v1/'
    _metadata = None
    name = BuiltinPlatforms.GCE

    def __init__(self):
        platform.Platform.__init__(self)
        self._compute_conn_pool = GCEConnectionPool(
                'compute', 'v1', COMPUTE_RW_SCOPE + STORAGE_FULL_SCOPE)
        self._storage_conn_pool = GCEConnectionPool(
                'storage', 'v1', STORAGE_FULL_SCOPE)

    def _get_metadata(self, key, url=None):
        if url is None:
            url = key

        if self._metadata is None:
            self._metadata = dict()

        if not url in self._metadata:
            key_url = os.path.join(self.metadata_url, url)
            req = urllib2.Request(key_url, headers={'X-Google-Metadata-Request': 'True'})
            resp = urllib2.urlopen(req)
            self._metadata[key] = resp.read()

        return self._metadata[key]


    def get_public_ip(self):
        network = self._get_metadata('network', 'instance/network-interfaces/?recursive=true')
        network = json.loads(network)
        return network[0]['accessConfigs'][0]['externalIp']


    def get_private_ip(self):
        network = self._get_metadata('network', 'instance/network-interfaces/?recursive=true')
        network = json.loads(network)
        return network[0]['ip']


    def get_project_id(self):
        return self._get_metadata('project/project-id')


    def get_zone(self):
        return self._get_metadata('instance/zone')


    def get_numeric_project_id(self):
        return self._get_metadata('project/numeric-project-id')


    def get_machine_type(self):
        return self._get_metadata('instance/machine-type')


    def get_instance_id(self):
        return self._get_metadata('instance/id')


    def get_hostname(self):
        return self._get_metadata('instance/hostname')


    def get_image(self):
        return self._get_metadata('instance/image')


    def get_compute_conn(self):
        return GCEConnectionProxy(self._compute_conn_pool)


    def get_storage_conn(self):
        return GCEConnectionProxy(self._storage_conn_pool)