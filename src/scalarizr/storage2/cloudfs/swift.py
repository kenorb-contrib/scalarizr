__author__ = 'vladimir'

import os
import logging

from swiftclient.client import ClientException

from scalarizr.storage2.cloudfs.base import CloudFileSystem
from scalarizr.storage2.cloudfs import cloudfs_types
from scalarizr.node import __node__

# TODO: make progress reports possible


LOG = logging.getLogger(__name__)


class SwiftFileSystem(CloudFileSystem):

    def _get_connection(self):
        return __node__['openstack'].connect_swift()


    def ls(self, remote_path):
        container, prefix = self._parse_url(remote_path)
        conn = self._get_connection()
        objects = conn.get_container(container)[1]

        objects = (obj["name"] for obj in objects)

        if prefix:
            prefix = prefix.rstrip("/") + "/"
            objects = (obj for obj in objects if obj.startswith(prefix))

        return tuple((self._format_url(container, obj) for obj in objects))


    def put(self, local_path, remote_path, report_to=None):
        LOG.info("Uploading '%s' to Swift under '%s'", local_path, remote_path)
        container, object_ = self._parse_url(remote_path)
        if object_.endswith("/"):
            object_ = os.path.join(object_, os.path.basename(local_path))

        fd = open(local_path, 'rb')
        try:
            conn = self._get_connection()
            try:
                conn.put_object(container, object_, fd)
            except ClientException, e:
                if e.http_status == 404:
                    # stand closer, shoot again
                    conn.put_container(container)
                    conn.put_object(container, object_, fd)
                else:
                    raise
        finally:
            fd.close()

        return self._format_url(container, object_)


    def get(self, remote_path, local_path, report_to=None):
        LOG.info('Downloading %s from Swift to %s', remote_path, local_path)
        container, object_ = self._parse_url(remote_path)
        #? join only if local_path.endswith("/")
        dest_path = os.path.join(local_path, os.path.basename(remote_path))

        fd = open(dest_path, 'w')
        try:
            conn = self._get_connection()
            res = conn.get_object(container, object_)
            fd.write(res[1])
        finally:
            fd.close()
        return dest_path


    def delete(self, remote_path):
        LOG.info('Deleting %s from Swift', remote_path)
        container, object_ = self._parse_url(remote_path)

        try:
            conn = self._get_connection()
            conn.delete_object(container, object_)
        except ClientException, e:
            if e.http_status == 404:
                return False
            else:
                raise


cloudfs_types["swift"] = SwiftFileSystem
