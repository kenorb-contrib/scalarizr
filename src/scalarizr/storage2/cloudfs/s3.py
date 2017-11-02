from __future__ import with_statement
__author__ = 'vladimir'

import logging
import os
import sys

from scalarizr.node import __node__
from scalarizr.storage2.cloudfs.base import CloudFileSystem
from scalarizr.storage2.cloudfs import cloudfs_types

from boto.s3.key import Key
from boto.exception import S3ResponseError


LOG = logging.getLogger(__name__)


class S3FileSystem(CloudFileSystem):

    acl = None

    _bucket = None

    # TODO: change report frequency
    def __init__(self, acl='aws-exec-read', report_frequency=11):
        self.acl = acl
        self.report_frequency = report_frequency

    def _parse_url(self, url):
        bucket, key = super(S3FileSystem, self)._parse_url(url)
        # while S3 web interface allows having bucket names with uppercase,
        # boto throws exceptions when trying to manipulate them

        bucket_lower = bucket.lower()
        if bucket_lower != bucket:
            LOG.debug("Using bucket %s instead of %s", bucket_lower, bucket)

        return bucket_lower, key

    def ls(self, remote_path):
        bucket_name, key_name = self._parse_url(remote_path)

        connection = self._get_connection()

        if not self._bucket_check_cache(bucket_name):
            self._bucket = connection.get_bucket(bucket_name, validate=False)

        files = [self._format_url(self._bucket.name, key.name)
                         for key in self._bucket.list(prefix=key_name)]
        return tuple(files)

    def put(self, local_path, remote_path, report_to=None):
        LOG.info("Uploading '%s' to S3 under '%s'", local_path, remote_path)
        bucket_name, key_name = self._parse_url(remote_path)
        if key_name.endswith("/"):
            key_name = os.path.join(key_name, os.path.basename(local_path))
        LOG.debug("Uploading '%s'", key_name)

        try:
            connection = self._get_connection()

            if not self._bucket_check_cache(bucket_name):
                try:
                    bck = connection.get_bucket(bucket_name)
                except S3ResponseError, e:
                    if e.code == 'NoSuchBucket':
                        bck = connection.create_bucket(
                                bucket_name,
                                location=self._bucket_location(),
                                policy=self.acl
                        )
                    else:
                        raise
                # Cache bucket
                self._bucket = bck

            file_ = None
            try:
                key = Key(self._bucket)
                key.name = key_name
                file_ = open(local_path, "rb")
                LOG.debug("Actually uploading %s", os.path.basename(local_path))
                key.set_contents_from_file(file_, policy=self.acl,
                        cb=report_to, num_cb=self.report_frequency)
                LOG.debug("Finished uploading %s", os.path.basename(local_path))
                return self._format_url(bucket_name, key_name)
            finally:
                if file_:
                    file_.close()

        except:
            exc = sys.exc_info()
            LOG.debug('Caught error', exc_info=exc)
            raise

    def get(self, remote_path, local_path, report_to=None):
        LOG.info('Downloading %s from S3 to %s', remote_path, local_path)
        bucket_name, key_name = self._parse_url(remote_path)
        dest_path = os.path.join(local_path, os.path.basename(remote_path))

        connection = self._get_connection()

        if not self._bucket_check_cache(bucket_name):
            self._bucket = connection.get_bucket(bucket_name, validate=False)
        key = self._bucket.get_key(key_name)
        assert key, "No such key: %s" % key_name

        LOG.debug("Actually downloading %s", os.path.basename(dest_path))
        key.get_contents_to_filename(dest_path, cb=report_to,
                num_cb=self.report_frequency)
        LOG.debug("Finished downloading %s", os.path.basename(dest_path))
        return dest_path

    def delete(self, remote_path):
        LOG.info('Deleting %s from S3', remote_path)
        bucket_name, key_name = self._parse_url(remote_path)

        connection = self._get_connection()

        if not self._bucket_check_cache(bucket_name):
            self._bucket = connection.get_bucket(bucket_name, validate=False)
        key = self._bucket.get_key(key_name)

        return key.delete() if key else None

    def _bucket_check_cache(self, bucket):
        if self._bucket and self._bucket.name != bucket:
            self._bucket = None
        return self._bucket

    def _get_connection(self):
        return __node__['ec2'].connect_s3()

    def _bucket_location(self):
        region = __node__['ec2']['region']
        if region == 'us-east-1' or not region:
            return ''
        elif region == 'eu-west-1':
            return 'EU'
        else:
            return region


cloudfs_types["s3"] = S3FileSystem
