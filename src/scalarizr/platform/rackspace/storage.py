from __future__ import with_statement
'''
Created on Nov 24, 2010
 
@author: marat
@author: Dmytro Korsakov
'''
 
from scalarizr.storage.transfer import TransferProvider, TransferError
 
import urlparse
import logging
import socket
import os
import sys
 
import cloudfiles
 
class CFTransferProvider(TransferProvider):
    schema = 'cf'
    urlparse.uses_netloc.append(schema)
 
    _username = None
    _api_key = None
 
    _logger = None
    _container = None
 
    def __init__(self):
        self._logger = logging.getLogger(__name__)
 
    def put(self, local_path, remote_path):
        self._logger.info('Uploading %s to CloudFiles under %s' % (local_path, remote_path))
        container, obj = self._parse_path(remote_path)
        obj = os.path.join(obj, os.path.basename(local_path))
 
        try:
            connection = self._get_connection()
 
            if not self._container_check_cache(container):
                try:
                    ct = connection.get_container(container)
                except cloudfiles.errors.NoSuchContainer:
                    self._logger.debug('Container %s not found. Trying to create.', container)
                    ct = connection.create_container(container)
                # Cache container object
                self._container = ct
 
            o = self._container.create_object(obj)
            o.load_from_filename(local_path)
            return self._format_path(container, obj)
 
        except (cloudfiles.errors.ResponseError, OSError, Exception, socket.timeout):
            exc = sys.exc_info()
            raise TransferError, exc[1], exc[2]
 
    def get(self, remote_path, local_path):
        self._logger.info('Downloading %s from CloudFiles to %s' % (remote_path, local_path))
        container, obj = self._parse_path(remote_path)
        dest_path = os.path.join(local_path, os.path.basename(remote_path))
 
        try:
            connection = self._get_connection()
 
            if not self._container_check_cache(container):
                try:
                    ct = connection.get_container(container)
                except cloudfiles.errors.NoSuchContainer:
                    raise TransferError("Container '%s' not found" % container)
                # Cache container object
                self._container = ct
 
            try:
                o = self._container.get_object(obj)
            except cloudfiles.errors.NoSuchObject:
                raise TransferError("Object '%s' not found in container '%s'"
                                % (obj, container))
 
            o.save_to_filename(dest_path)
            return dest_path
 
        except (cloudfiles.errors.ResponseError, OSError, Exception):
            exc = sys.exc_info()
            raise TransferError, exc[1], exc[2]
 
 
    def configure(self, remote_path, username=None, api_key=None):
        if username:
            self._username = username
            self._api_key = api_key
 
 
    def list(self, remote_path):
        container, obj = self._parse_path(remote_path)
        connection = self._get_connection()
        ct = connection.get_container(container)
        objects = container.get_objects(path=obj)
        return tuple([self._format_path(ct, obj.name) for obj in objects]) if objects else ()
 
    def _get_connection(self):
        from . import new_cloudfiles_conn
        return new_cloudfiles_conn(self._username, self._api_key)
 
    def _container_check_cache(self, container):
        if self._container and self._container.name != container:
            self._container = None
        return self._container
 
    def _format_path(self, container, obj):
        return '%s://%s/%s' % (self.schema, container, obj)
 
    def _parse_path(self, path):
        o = urlparse.urlparse(path)
        if o.scheme != self.schema:
            raise TransferError('Wrong schema')
        return o.hostname, o.path[1:]
 
