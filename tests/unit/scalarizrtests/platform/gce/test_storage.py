__author__ = 'Nick Demyanchuk'
 
import unittest
import uuid
import mock
import os
 
from scalarizr.bus import bus
from scalarizr.platform import gce
from szr_unittest import main
from szr_unittest.storage_test.transfer_test import TransferTestMixin
 
 
"""
Google Cloud Storage test
Set credentials to environment before running.
        - SERVICE_ACCOUNT_EMAIL
        - PRIVATE_KEY (base64 encoded)
        - PROJECT_ID
"""
bus.cnf = mock.MagicMock()
bus.platform = gce.GcePlatform()
 
proj_id = os.environ['PROJECT_ID']
bus.platform.get_numeric_project_id = mock.MagicMock()
bus.platform.get_numeric_project_id.return_value = proj_id
 
email = os.environ['SERVICE_ACCOUNT_EMAIL']
pk = os.environ['PRIVATE_KEY']
 
bus.platform.set_access_data(dict(
        key=pk, service_account_name=email
))
 
 
class CloudStorageTest(unittest.TestCase, TransferTestMixin):
 
    conn = None
 
    def setUp(self):
        TransferTestMixin.setUp(self)
        self.container = uuid.uuid4()
        self.key = 'path/to/candies'
        self.rdst = 'gcs://%s/%s' % (self.container, self.key)
 
 
    def tearDown(self):
        TransferTestMixin.tearDown(self)
 
 
    def native_upload(self, files):
        return self.trn.upload(files, self.rdst)
 
 
if __name__ == "__main__":
    main()
    unittest.main()
 
