'''
Created on Jan 19, 2011
 
@author: marat
'''
from scalarizr.platform import rackspace
from szr_unittest.storage_test.transfer_test import TransferTestMixin
from szr_unittest import main
 
import os
import unittest
from functools import partial
 
import cloudfiles
 
rackspace.new_cloudfiles_conn = partial(rackspace.new_cloudfiles_conn, servicenet=False)
 
class CloudFilesTest(unittest.TestCase, TransferTestMixin):
    conn = None
 
    def setUp(self):
        TransferTestMixin.setUp(self)
        self.container = 'transfer-test'
        self.key = 'path/to/candies'
        self.rdst = 'cf://%s/%s' % (self.container, self.key)
        self.conn = cloudfiles.Connection(
                os.environ['CLOUD_SERVERS_USERNAME'],
                os.environ['CLOUD_SERVERS_API_KEY']
        )
 
    def tearDown(self):
        TransferTestMixin.tearDown(self)
 
    def native_upload(self, files):
        try:
            ct = self.conn.get_container(self.container)
        except cloudfiles.errors.NoSuchContainer:
            ct = self.conn.create_container(self.container)
 
        rfiles = []
        for file in files:
            name = os.path.basename(file)
            key = ct.create_object(os.path.join(self.key, name))
            key.load_from_filename(file)
            rfiles.append(os.path.join(self.rdst, name))
        return rfiles
 
if __name__ == "__main__":
    main()
    unittest.main()
 
