from __future__ import with_statement

import os
import urllib2
import glob
import sys
import logging

from scalarizr import platform
from scalarizr import node
from scalarizr.bus import bus
from scalarizr.util import LocalPool
from scalarizr.platform import Platform, PlatformFeatures, PlatformError
from scalarizr.platform import ConnectionError, NoCredentialsError, InvalidCredentialsError
from . import storage
from scalarizr import util

import cloudstack


def get_platform():
    return CloudStackPlatform()

LOG = logging.getLogger(__name__)


def _create_connection():
    platform = node.__node__['platform']
    try:
        conn = cloudstack.Client(
            platform.get_access_data('api_url'),
            apiKey=platform.get_access_data('api_key'),
            secretKey=platform.get_access_data('secret_key'))
    except PlatformError:
        raise NoCredentialsError(sys.exc_info()[1])
    return conn


class CloudStackConnectionProxy(platform.ConnectionProxy):
    pass


class CloudStackPlatform(Platform):
    name = 'cloudstack'

    features = [PlatformFeatures.SNAPSHOTS, PlatformFeatures.VOLUMES]
    _dhcp_leases_mtime = None
    _dhcp_leases_path = None
    _router_addr = None

    def __init__(self):
        Platform.__init__(self)
        self._metadata = {}
        self._conn_pool = LocalPool(_create_connection)
        self.refresh_virtual_router_addr()

    def refresh_virtual_router_addr(self):
        leases_pattern = '/var/lib/dhc*/dhclient*.lease*'
        if not self._dhcp_leases_path:
            LOG.debug('Lookuping DHCP leases file')
            try:
                leases_files = glob.glob(leases_pattern)
                # take the most recently modified file
                leases_files = sorted(
                    leases_files, 
                    lambda x, y: cmp(os.stat(x).st_mtime, os.stat(y).st_mtime))
                self._dhcp_leases_path = leases_files[-1]
            except IndexError:
                raise PlatformError("Can't find virtual router. No file matching pattern: %s", leases_pattern)
        if os.stat(self._dhcp_leases_path).st_mtime == self._dhcp_leases_mtime:
            return

        LOG.debug('Lookuping meta-data server address')
        for line in open(self._dhcp_leases_path):
            if 'dhcp-server-identifier' in line:
                self._router_addr = filter(None, line.split(';')[0].split(' '))[2]
        LOG.debug('Meta-data server: %s', self._router_addr)
        self._dhcp_leases_mtime = os.stat(self._dhcp_leases_path).st_mtime

    def get_private_ip(self):
        return self.get_meta_data('local-ipv4')

    def get_public_ip(self):
        return self.get_meta_data('public-ipv4')


    def get_meta_data(self, key):
        self.refresh_virtual_router_addr()
        if not key in self._metadata:
            try:
                url = 'http://%s/latest/%s' % (self._router_addr, key)
                self._metadata[key] = urllib2.urlopen(url).read().strip()
            except IOError:
                exc_info = sys.exc_info()
                raise PlatformError, "Can't fetch meta-data from '%s'." \
                                " error: %s" % (url, exc_info[1]), exc_info[2]
        return self._metadata[key]

    def get_instance_id(self):
        ret = self.get_meta_data('instance-id')
        if len(ret) == 36:
            # UUID (CloudStack 3)
            return ret
        else:
            # CloudStack 2
            return self.get_meta_data('instance-id').split('-')[2]

    def get_avail_zone_id(self):
        conn = self.new_cloudstack_conn()
        return dict((zone.name, zone.id) for zone in conn.listZones())[self.get_avail_zone()]

    def get_avail_zone(self):
        return self.get_meta_data('availability-zone')

    def get_ssh_pub_key(self):
        try:
            return self.get_meta_data('public-keys')
        except:
            return ''

    def get_cloudstack_conn(self):
        return CloudStackConnectionProxy(self._conn_pool) 

    def new_cloudstack_conn(self):
        access_data = self.get_access_data()
        if access_data and 'api_url' in access_data:
            return cloudstack.Client(
                            access_data['api_url'],
                            apiKey=access_data.get('api_key'),
                            secretKey=access_data.get('secret_key'))
