from __future__ import with_statement
 
import os
import urllib2
import sys
import logging
 
from scalarizr.bus import bus
from scalarizr.platform import Platform, PlatformFeatures, PlatformError
from . import storage
from scalarizr import util
 
import cloudstack
 
 
def get_platform():
    return CloudStackPlatform()
 
LOG = logging.getLogger(__name__)
 
class CloudStackPlatform(Platform):
    name = 'cloudstack'
 
    features = [PlatformFeatures.SNAPSHOTS, PlatformFeatures.VOLUMES]
 
    def __init__(self):
        Platform.__init__(self)
 
        # Find the virtual router.
        eth0leases = util.firstmatched(lambda x: os.path.exists(x),
                                                                ['/var/lib/dhcp/dhclient.eth0.leases',
                                                                '/var/lib/dhcp3/dhclient.eth0.leases',
                                                                '/var/lib/dhclient/dhclient-eth0.leases'],
                                                                '/var/lib/dhclient/dhclient-eth0.leases')
        if not os.path.exists(eth0leases):
            raise PlatformError("Can't find virtual router. file %s not exists" % eth0leases)
 
        router = None
        for line in open(eth0leases):
            if 'dhcp-server-identifier' in line:
                router = filter(None, line.split(';')[0].split(' '))[2]
        LOG.debug('Meta-data server: %s', router)
        self._router = router
 
        self._metadata = {}
 
 
    def get_private_ip(self):
        return self.get_meta_data('local-ipv4')
 
 
    def get_public_ip(self):
        return self.get_meta_data('public-ipv4')
 
 
    def get_user_data(self, key=None):
        if self._userdata is None:
            try:
                self._userdata = self._parse_user_data(self.get_meta_data('user-data'))
            except PlatformError, e:
                if 'HTTP Error 404' in e:
                    self._userdata = {}
                else:
                    raise
        return Platform.get_user_data(self, key)
 
 
    def get_meta_data(self, key):
        if not key in self._metadata:
            try:
                url = 'http://%s/latest/%s' % (self._router, key)
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
 
 
    def new_cloudstack_conn(self):
        if self._access_data and 'api_url' in self._access_data:
            return cloudstack.Client(
                            self._access_data['api_url'],
                            apiKey=self._access_data.get('api_key'),
                            secretKey=self._access_data.get('secret_key'))
 
