from __future__ import with_statement
'''
Created on Aug 13, 2010
 
@author: marat
'''
from scalarizr.bus import bus
from scalarizr.platform import PlatformError
from scalarizr.platform.ec2 import Ec2Platform
 
import logging, os
from urlparse import urlparse
 
import boto
from boto.ec2.regioninfo import RegionInfo
from boto.s3.connection import OrdinaryCallingFormat
from M2Crypto import SSL
from scalarizr.util import firstmatched
 
def get_platform():
    return EucaPlatform()
 
 
SECTION = 'eucalyptus'
CLOUD_CERT = 'euca_cloud_cert.pem'
OPT_S3_URL = 's3_url'
OPT_EC2_URL = 'ec2_url'
OPT_CLOUD_CERT = 'cloud_cert'
OPT_CLOUD_CERT_PATH = 'cloud_cert_path'
 
class EucaPlatform(Ec2Platform):
    name = 'eucalyptus'
 
    features = []
 
    def __init__(self):
        Ec2Platform.__init__(self)
        self._logger = logging.getLogger(__name__)
 
        cnf = bus.cnf
        cnf.explore_key(CLOUD_CERT, 'Eucalyptus cloud certificate', private=False)
        # TODO: ec2_url, s3_url doesn't appears in user-data, we should remove listener?
        #cnf.on('apply_user_data', self.on_cnf_apply_user_data)
 
 
    def on_cnf_apply_user_data(self, cnf):
        user_data = self.get_user_data()
        cnf.update_ini(self.name, {self.name: {
                OPT_S3_URL      : user_data[OPT_S3_URL],
                OPT_EC2_URL : user_data[OPT_EC2_URL]
        }})
 
    def set_access_data(self, access_data):
        '''
        Eucalyptus cloud_cert, ec2_url, s3_url may be passed within access data.
        Accept them if they are not precented in configuration
        '''
        cnf = bus.cnf; ini = cnf.rawini
 
        if not os.path.exists(cnf.key_path(CLOUD_CERT, private=False)):
            cnf.write_key(CLOUD_CERT, access_data[OPT_CLOUD_CERT], private=False)
        if not ini.has_section(SECTION) or not ini.has_option(SECTION, OPT_EC2_URL):
            cnf.update_ini(self.name, {self.name: {
                    OPT_S3_URL      : access_data[OPT_S3_URL],
                    OPT_EC2_URL : access_data[OPT_EC2_URL]
            }})
 
        Ec2Platform.set_access_data(self, access_data)
 
    def get_region(self):
        return 'Eucalyptus'
 
    def get_block_device_mapping(self):
        keys = self._get_property("block-device-mapping").split("\n")
        ret = {}
        for key in keys:
            try:
                ret[key] = self._get_property('block-device-mapping/' + key)
            except PlatformError, e:
                # Workaround
                if key == 'ephemeral0' and str(e).find('HTTP Error 500') >= 0:
                    ret[key] = firstmatched(lambda x: os.path.exists(x), ('/dev/sda2', '/dev/sdb'))
                else:
                    raise
        return ret
 
    def block_devs_mapping(self):
        keys = self._get_property("block-device-mapping").split("\n")
        ret = list()
        for key in keys:
            try:
                ret.append((key, self._get_property("block-device-mapping/" + key)))
            except PlatformError, e:
                if key == 'ephemeral0' and str(e).find('HTTP Error 500') >= 0:
                    ret.append((key, firstmatched(lambda x: os.path.exists(x), ('/dev/sda2', '/dev/sdb'))))
                else:
                    raise
        return ret
 
 
    def get_ec2_cert(self):
        if not self._ec2_cert:
            cnf = bus.cnf
            cert_path = cnf.key_path(CLOUD_CERT, private=False)
            if not os.path.exists(cert_path):
                ec2_url = cnf.rawini.get(self.name, OPT_EC2_URL)
                url = urlparse(ec2_url)
                if url.schema == 'https':
                    # Open SSL connection and retrieve certificate
                    addr = (url.hostname, url.port if url.port else 443)
                    ctx = SSL.Context()
                    conn = SSL.Connection(ctx)
                    conn.set_post_connection_check_callback(None)
                    conn.connect(addr)
                    cert = conn.get_peer_cert()
                    cert.save_pem(cert_path)
 
            self._ec2_cert = cnf.read_key(CLOUD_CERT, private=False)
        return self._ec2_cert
 
    def new_ec2_conn(self):
        ''' @rtype: boto.ec2.connection.EC2Connection '''
        self._logger.debug('Creating eucalyptus ec2 connection')
        if not hasattr(self, '_ec2_conn_params'):
            url = self._cnf.rawini.get(self.name, OPT_EC2_URL)
            if not url:
                raise PlatformError('EC2(Eucalyptus) url is empty')
            u = urlparse(url)
            self._ec2_conn_params = dict(
                    is_secure = u.scheme == 'https',
                    port = u.port,
                    path = '/'+u.path,
                    region = RegionInfo(name='euca', endpoint=u.hostname)
            )
 
        return boto.connect_ec2(*self.get_access_keys(), **self._ec2_conn_params)
 
    def new_s3_conn(self):
        ''' @rtype: boto.ec2.connection.S3Connection '''
        self._logger.debug('Creating eucalyptus s3 connection')
        if not hasattr(self, '_s3_conn_params'):
            url = self._cnf.rawini.get(self.name, OPT_S3_URL)
            if not url:
                raise PlatformError('S3(Walrus) url is empty')
            u = urlparse(url)
            self._s3_conn_params = dict(
                    is_secure = u.scheme == 'https',
                    port = u.port,
                    path = '/'+u.path,
                    host = u.hostname,
                    calling_format = OrdinaryCallingFormat()
            )
 
        return boto.connect_s3(*self.get_access_keys(), **self._s3_conn_params)
 
