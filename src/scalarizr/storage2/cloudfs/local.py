import logging
import os
import shutil
import errno

from scalarizr.storage2.cloudfs.base import CloudFileSystem
from scalarizr.storage2.cloudfs import cloudfs_types


LOG = logging.getLogger(__name__)


class LocalFileSystem(CloudFileSystem):
    """

    ::

            file:///one/two -> /one/two
            file://one/two -> one/two

    NOTE: ls() and delete() don't validate input, so there's a potential
    danger when using them: ls() tries to list all files recursively - that
    can take a long time if called with some top level dirs; delete() is
    dangerous since tests mostly run as root.
    """

    def _parse_url(self, url):
        try:
            schema, path = url.split("://")
        except ValueError:
            raise ValueError("Bad url")
        assert schema == self.schema, "Wrong schema %s" % schema
        return path

    def _format_url(self, path):
        return "%s://%s" % (self.schema, path)

    def ls(self, url):
        path = self._parse_url(url)

        LOG.debug("Trying to list %s", path)
        res = []
        for dirpath, dirnames, filenames in os.walk(path):
            res += map(lambda x: self._format_url(os.path.join(dirpath, x)),
                               filenames)
        return res

    def put(self, src, url, report_to=None):
        path = self._parse_url(url)

        LOG.debug("Uploading '%s' to '%s'", src, path)
        try:
            os.makedirs(os.path.dirname(path))
        except OSError, e:
            if e.errno != 17:  # 17: already exists
                raise

        shutil.copy(src, path)

        res = path
        if res.endswith("/"):
            res = os.path.join(res, os.path.basename(src))
        return self._format_url(res)

    def get(self, url, dst, report_to=None):
        path = self._parse_url(url)
        dst = os.path.join(dst, os.path.basename(path))

        LOG.debug("Downloading from '%s' to '%s'", path, dst)
        shutil.copy(path, dst)
        return dst

    def delete(self, url):
        path = self._parse_url(url)

        LOG.debug("Deleting %s", path)
        try:
            return os.remove(path)
        except OSError, e:
            if e.errno == errno.ENOENT:
                pass
            else:
                raise


cloudfs_types["file"] = LocalFileSystem
