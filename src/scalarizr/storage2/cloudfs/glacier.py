from __future__ import with_statement

import os
import hashlib

from urlparse import urlparse
from scalarizr.node import __node__
from boto.glacier.writer import chunk_hashes, tree_hash, bytes_to_hex
from scalarizr.storage2.cloudfs.base import CloudFileSystem


class GlacierFilesystem(CloudFileSystem):
    def _connect_glacier(self):
        '''
        Returns Boto.Glacier.Layer1 object
        '''
        #delete this two lines after test
        from boto.glacier.layer1 import Layer1
        return Layer1()

    def _clear(self):
        self._conn = None
        self._vault_name = None
        self._part_size = None
        self._total_size = 0
        self._tree_hashes = []
        self._part_num_list = []


    def multipart_init(self, path, part_size):
        '''
        Returns upload_id
        '''
        self._clear()
        self._conn = self._connect_glacier()
        self._part_size = part_size
        self._vault_name = urlparse(path).netloc

        response = self._conn.initiate_multipart_upload(self._vault_name, part_size, None)

        return response['UploadId']

    def multipart_put(self, upload_id, part_num, part):
        fileobj = open(part, 'rb')
        bytes_to_upload = fileobj.read(self._part_size)
        part_size = os.fstat(fileobj.fileno()).st_size

        start_byte = part_num * self._part_size
        content_range = (start_byte, start_byte + part_size - 1)
        linear_hash = hashlib.sha256(bytes_to_upload).hexdigest()
        part_tree_hash = tree_hash(chunk_hashes(bytes_to_upload))
        hex_part_tree_hash = bytes_to_hex(part_tree_hash)

        self._conn.upload_part(
                self._vault_name,
                upload_id,
                linear_hash,
                hex_part_tree_hash,
                content_range,
                bytes_to_upload
        )

        if part_num not in self._part_num_list:
            self._part_num_list.append(part_num)
            self._tree_hashes.append(part_tree_hash)
            self._total_size += part_size

        fileobj.close()

    def multipart_complete(self, upload_id):
        '''
        Returns glacier://Vault_1/?avail_zone=us-east-1&archive_id=NkbByEejwEggmBz2fTHgJrg0XBoDfjP4q6iu87-TjhqG6eGoOY9Z8i1_AUyUsuhPAdTqLHy8pTl5nfCFJmDl2yEZONi5L26Omw12vcs01MNGntHEQL8MBfGlqrEXAMPLEArchiveId
        '''
        hex_tree_hash = bytes_to_hex(tree_hash(self._tree_hashes))

        response = self._conn.complete_multipart_upload(
                self._vault_name,
                upload_id,
                hex_tree_hash,
                self._total_size
        )

        path = 'glacier://' + self._vault_name + '/?avail_zone=' + self._conn.region.name + '&archive_id=' + response['ArchiveId']
        return path

    def multipart_abort(self, upload_id):
        self._conn.abort_multipart_upload(self._vault_name, upload_id)
