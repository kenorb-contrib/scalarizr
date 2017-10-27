'''
Created on Jan 19, 2011
 
@author: marat
'''
 
from szr_unittest.storage_test.transfer_test import TransferTestMixin
from szr_unittest import main
 
import os
import unittest
import scalarizr.platform.ec2
 
from boto import connect_s3
from boto.s3.key import Key
from boto.exception import S3ResponseError
from scalarizr.storage import Storage
 
'''
class S3Test(unittest.TestCase, TransferTestMixin):
        conn = None
 
        def setUp(self):
                TransferTestMixin.setUp(self)
                self.container = 'transfer-test'
                self.key = 'path/to/candies'
                self.rdst = 's3://%s/%s' % (self.container, self.key)
                self.conn = connect_s3()
 
        def tearDown(self):
                TransferTestMixin.tearDown(self)
 
        def native_upload(self, files):
                try:
                        bck = self.conn.get_bucket(self.container)
                except S3ResponseError, e:
                        if e.code == 'NoSuchBucket':
                                bck = self.conn.create_bucket(self.container)
                        else:
                                raise
 
                rfiles = []
                for file in files:
                        name = os.path.basename(file)
                        key = Key(bck)
                        key.name = os.path.join(self.key, name)
                        key.set_contents_from_filename(file)
                        rfiles.append(os.path.join(self.rdst, name))
                return rfiles
'''
 
class TestEbsVolume(unittest.TestCase):
    def test_1(self):
        vol = Storage.create(
                type='ebs',
                id='vol-12345678',
                snapshot=dict(
                        id='snap-87654321',
                        type='ebs'
                )
        )
 
        pass
 
 
if __name__ == "__main__":
    main()
    unittest.main()
 
