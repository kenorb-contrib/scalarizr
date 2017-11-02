__author__ = 'vladimir'


import os
import sys
import json
import time
import logging
import random

from apiclient.http import MediaIoBaseUpload, MediaIoBaseDownload
from apiclient.errors import HttpError

from scalarizr.storage2.cloudfs.base import CloudFileSystem
from scalarizr.storage2.cloudfs import cloudfs_types
from scalarizr.bus import bus
from scalarizr.node import __node__


# TODO: get connection from node


LOG = logging.getLogger(__name__)


class GCSFileSystem(CloudFileSystem):
    # TODO: add amazon-style 0% and 100% callbacks ?

    chunk_size = 2*1024*1024
    report_interval = 10  # percent; every <value> percent at most

    def _parse_url(self, url):
        bucket, key = super(GCSFileSystem, self)._parse_url(url)

        bucket_lower = bucket.lower()
        if bucket_lower != bucket:
            LOG.debug("Using bucket %s instead of %s", bucket_lower, bucket)

        return bucket_lower, key


    def ls(self, remote_path):
        bucket, path = self._parse_url(remote_path)

        path = path.rstrip('/') + '/' if path else ''

        req = self.cloudstorage.objects().list(
                bucket=bucket, prefix=path)
        resp = req.execute()

        items = (self._format_url(bucket, x["name"])
                         for x in resp.setdefault("items", []))
        return tuple(items)


    def get(self, remote_path, local_path, report_to=None):
        LOG.debug('Downloading %s from cloud storage (local path: %s)', remote_path, local_path)
        bucket, name = self._parse_url(remote_path)
        local_path = os.path.join(local_path, os.path.basename(remote_path))

        request = self.cloudstorage.objects().get_media(
                bucket=bucket, object=name)

        f = open(local_path, 'w')
        try:
            media = MediaIoBaseDownload(f, request, chunksize=self.chunk_size)

            last_progress = 0
            done = False
            while not done:
                status, done = media.next_chunk()
                if status:
                    percentage = int(status.progress() * 100)
                    if percentage - last_progress >= self.report_interval:
                        if report_to:
                            report_to(status.resumable_progress, status.total_size)
                        last_progress = percentage
        finally:
            f.close()

        LOG.debug("Finished downloading %s", os.path.basename(local_path))
        return local_path


    def put(self, local_path, remote_path, report_to=None):
        LOG.debug('Uploading %s to cloud storage (remote path: %s)', local_path, remote_path)
        filename = os.path.basename(local_path)
        bucket, name = self._parse_url(remote_path)
        if name.endswith("/"):
            name = os.path.join(name, filename)

        buckets = self._list_buckets()
        if bucket not in buckets:
            self._create_bucket(bucket)

        fd = open(local_path, 'rb')
        try:
            media = MediaIoBaseUpload(fd,
                    'application/octet-stream',
                    resumable=True)
            req = self.cloudstorage.objects().insert(
                    bucket=bucket, name=name, media_body=media
            )
            last_progress = 0
            response = None
            exponent_backoff = [1, 2, 4, 8, 16, 32]
            while response is None: 
                try:
                    status, response = req.next_chunk()
                    if status:
                        percentage = int(status.progress() * 100)
                        if percentage - last_progress >= self.report_interval:
                            if report_to:
                                report_to(status.resumable_progress, status.total_size)
                            last_progress = percentage
                    exponent_backoff = [1, 2, 4, 8, 16, 32]
                except HttpError, e:
                    if not exponent_backoff or not int(e.resp.status) in (500, 502, 503, 504):
                        raise

                    sec_to_wait = exponent_backoff.pop(0)
                    LOG.warning('Error while uploading chunk: %s. Retry in %s sec' % (e, sec_to_wait))
                    # add random milliseconds
                    sec_to_wait += random.random()
                    time.sleep(sec_to_wait)
        finally:
            fd.close()
        LOG.debug("Finished uploading %s", os.path.basename(local_path))
        return self._format_url(bucket, name)


    def delete(self, remote_path, delete_bucket=False):
        LOG.info('Deleting %s from GCS', remote_path)
        bucket, obj = self._parse_url(remote_path)

        req = self.cloudstorage.objects().delete(bucket=bucket, object=obj)
        try:
            req.execute()
        except HttpError, e:
            if "Not Found" in json.loads(e.content)["error"]["message"]:
                return False
            else:
                raise

        if delete_bucket:
            self._delete_bucket(bucket)

    def _list_buckets(self):
        pl = bus.platform
        proj_id = pl.get_numeric_project_id()

        req = self.cloudstorage.buckets().list(project=proj_id)
        resp = req.execute()
        if 'items' not in resp:
            return []
        buckets = [b['id'] for b in resp['items']]
        return buckets


    def _create_bucket(self, bucket_name):
        pl = bus.platform
        proj_id = pl.get_numeric_project_id()

        req_body = dict(name=bucket_name)
        req = self.cloudstorage.buckets().insert(project=proj_id, body=req_body)
        try:
            req.execute()
        except:
            e = sys.exc_info()[1]
            if not 'You already own this bucket' in str(e):
                raise


    def _delete_bucket(self, bucket_name):
        pl = bus.platform

        req = self.cloudstorage.buckets().delete(bucket=bucket_name)
        req.execute()


    @property
    def cloudstorage(self):
        pl = bus.platform
        return pl.get_storage_conn()


cloudfs_types["gcs"] = GCSFileSystem
