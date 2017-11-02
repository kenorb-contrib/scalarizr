from __future__ import with_statement

import os
import re
import sys
import urllib2
import logging

from scalarizr.bus import bus
from scalarizr.node import __node__
from scalarizr import platform
from scalarizr.util import NullPool
from scalarizr.platform import Ec2LikePlatform, PlatformError, PlatformFeatures
from scalarizr.platform import NoCredentialsError, InvalidCredentialsError, ConnectionError
from scalarizr.storage.transfer import Transfer
from .storage import S3TransferProvider

import boto
import boto.ec2


Transfer.explore_provider(S3TransferProvider)


"""
Platform configuration options
"""
OPT_ACCOUNT_ID = "account_id"
OPT_KEY = "key"
OPT_KEY_ID = "key_id"
OPT_EC2_CERT_PATH = "ec2_cert_path"
OPT_CERT_PATH = "cert_path"
OPT_PK_PATH = "pk_path"


"""
User data options
"""
UD_OPT_S3_BUCKET_NAME = "s3bucket"



def get_platform():
    return Ec2Platform()


def _create_ec2_connection():
    platform = __node__['platform']
    region = platform.get_region()
    try:
        key_id, key = platform.get_access_keys()
        conn = boto.ec2.connect_to_region(
            region,
            aws_access_key_id=key_id,
            aws_secret_access_key=key
        )
        if not conn:
            raise ConnectionError('Invalid region: %s' % region)
    except (PlatformError, boto.exception.NoAuthHandlerFound):
        raise NoCredentialsError(sys.exc_info()[1])
    return conn


def _create_s3_connection():
    platform = __node__['platform']
    region = platform.get_region()
    endpoint = platform._s3_endpoint(region)
    try:
        key_id, key = platform.get_access_keys()
        conn = boto.connect_s3(
            host=endpoint,
            aws_access_key_id=key_id,
            aws_secret_access_key=key
        )
    except (AttributeError, PlatformError, boto.exception.NoAuthHandlerFound):
        raise NoCredentialsError(sys.exc_info()[1])
    return conn


class Ec2ConnectionProxy(platform.ConnectionProxy):

    def invoke(self, *args, **kwds):
        try:
            return super(Ec2ConnectionProxy, self).invoke(*args, **kwds)
        except:
            e = sys.exc_info()[1]
            if isinstance(e, boto.exception.EC2ResponseError) and e.args[0] == 401:
                raise InvalidCredentialsError(e)
            else:
                raise


class S3ConnectionProxy(platform.ConnectionProxy):

    def invoke(self, *args, **kwds):
        try:
            return super(S3ConnectionProxy, self).invoke(*args, **kwds)
        except:
            e = sys.exc_info()[1]
            if isinstance(e, boto.exception.S3ResponseError) and e.args[0] == 401:
                raise InvalidCredentialsError(e)
            else:
                raise


class Ec2Platform(Ec2LikePlatform):
    name = "ec2"

    _userdata_key = "latest/user-data"


    instance_store_devices = (
            '/dev/sda2', '/dev/sdb', '/dev/xvdb',
            '/dev/sdc', '/dev/xvdc',
            '/dev/sdd', '/dev/xvdd',
            '/dev/sde', '/dev/xvde'
    )

    _logger = None
    _ec2_cert = None
    _cnf = None

    features = [PlatformFeatures.SNAPSHOTS, PlatformFeatures.VOLUMES]

    def __init__(self):
        platform.Ec2LikePlatform.__init__(self)
        self._ec2_conn_pool = NullPool(_create_ec2_connection)
        self._s3_conn_pool = NullPool(_create_s3_connection)

    def get_account_id(self):
        return self.get_access_data("account_id").encode("ascii")

    def get_access_keys(self):
        # Keys must be in ASCII because hmac functions doesn't works with unicode
        return (self.get_access_data("key_id").encode("ascii"), self.get_access_data("key").encode("ascii"))

    def get_cert_pk(self):
        return (self.get_access_data("cert").encode("ascii"), self.get_access_data("pk").encode("ascii"))

    def get_ec2_cert(self):
        if not self._ec2_cert:
            # XXX: not ok
            self._ec2_cert = self._cnf.read_key(os.path.join(bus.etc_path, self._cnf.rawini.get(self.name, OPT_EC2_CERT_PATH)), title="EC2 certificate")
        return self._ec2_cert

    def get_ec2_conn(self):
        return Ec2ConnectionProxy(self._ec2_conn_pool)

    def get_s3_conn(self):
        return S3ConnectionProxy(self._s3_conn_pool)

    def new_ec2_conn(self):
        """ @rtype: boto.ec2.connection.EC2Connection """
        region = self.get_region()
        self._logger.debug("Return ec2 connection (region: %s)", region)  
        key_id, key = self.get_access_keys()
        proxy = __node__['access_data'].get('proxy', {})
        if proxy.get('host'):
            self._logger.debug('Use proxy %s:%s for EC2 connection', proxy['host'], proxy.get('port'))
        return boto.ec2.connect_to_region(region, 
                aws_access_key_id=key_id, 
                aws_secret_access_key=key,
                proxy=proxy.get('host'),
                proxy_port=proxy.get('port'),
                proxy_user=proxy.get('user'),
                proxy_pass=proxy.get('pass'))

    def new_s3_conn(self):
        region = self.get_region()
        endpoint = self._s3_endpoint(region)
        key_id, key = self.get_access_keys()
        self._logger.debug("Return s3 connection (endpoint: %s)", endpoint)
        proxy = __node__['access_data'].get('proxy', {})
        if proxy.get('host'):
            self._logger.debug('Use proxy %s:%s for S3 connection', proxy['host'], proxy.get('port'))
        return boto.connect_s3(
                host=endpoint, 
                aws_access_key_id=key_id, 
                aws_secret_access_key=key, 
                proxy=proxy.get('host'),
                proxy_port=proxy.get('port'),
                proxy_user=proxy.get('user'),
                proxy_pass=proxy.get('pass'))

    @property
    def cloud_storage_path(self):
        ret = Ec2LikePlatform.cloud_storage_path.fget(self)
        if not ret:
            bucket = self.get_user_data(UD_OPT_S3_BUCKET_NAME) or ''
            ret = 's3://' + bucket
        return ret

    def _s3_endpoint(self, region):
        if region == 'us-east-1':
            return 's3-external-1.amazonaws.com'
        else:
            return 's3-%s.amazonaws.com' % region


class BotoLoggingFilter(logging.Filter):
    # removes all ERROR boto messages - we will log it on upper levels

    def filter(self, record):
        if record.name.startswith('boto') and record.levelno == logging.ERROR:
            return False
        return True

root_logger = logging.getLogger()
root_logger.addFilter(BotoLoggingFilter('boto'))


