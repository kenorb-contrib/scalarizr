'''
Created on Nov 18, 2010
 
@author: shaitanich
'''
 
from scalarizr.storage.transfer import Transfer
from scalarizr.util import system2
from szr_unittest import main
 
import os
import logging
import unittest
from random import randint
 
 
 
 
def assert_transfer(tc, src_files, dst_files, dst_path=None):
    # same quantity
    tc.assertEqual(len(src_files), len(dst_files))
 
    # same base names
    src_names = sorted(list(os.path.basename(f) for f in src_files))
    dst_names = sorted(list(os.path.basename(f) for f in dst_files))
    tc.assertEqual(src_names, dst_names)
 
    if dst_path:
        # all remote files starts with 'dst_path'
        tc.assertTrue(all(f.startswith(dst_path) for f in dst_files))
 
 
 
class TransferTestMixin(object):
    trn = None
    rdst = None
    dst = '/tmp/transfer-sandbox'
 
    def setUp(self):
        self.trn = Transfer(pool=2, max_attempts=1)
        if not os.path.exists(self.dst):
            os.makedirs(self.dst)
 
    def tearDown(self):
        self.remove_local_files()
 
    def make_local_files(self, suffix=None):
        # Create some 1Mb files
        files = []
        #for file in ('chocolate', 'caramel', 'fluids'):
        for file in ('chocolate', 'caramel'):
            file = '%s/%s%s' % (self.dst, file, suffix or '')
            system2('dd if=/dev/urandom of=%s bs=100K count=1' % file, shell=True)
            files.append(file)
        return files
 
    def remove_local_files(self):
        system2('rm -rf %s/*' % self.dst, shell=True)
 
    def test_upload(self):
        files = self.make_local_files('.upload')
        rfiles = self.trn.upload(files, self.rdst)
        assert_transfer(self, files, rfiles, self.rdst)
 
    def test_download(self):
        files = self.make_local_files('.download')
        try:
            rfiles = self.native_upload(files)
        finally:
            self.remove_local_files()
 
        files = self.trn.download(rfiles, self.dst)
        assert_transfer(self, rfiles, files, self.dst)
 
    def native_upload(self, files):
        pass
 
 
 
 
 
'''
class UploaderTest(unittest.TestCase):
 
        def setUp(self):
                self.fname = str(randint(1000000, 9999999)) + '.file'
                self.container = 'container' + str(randint(1000000, 9999999))
                self.prefix = 'remotedir' + str(randint(1000000, 9999999))
                self.content = 'random_content' + str(randint(1000000, 9999999))
                self.target_file = str(randint(1000000, 9999999))
                file = open(self.fname, 'w')
                file.write(self.content)
                file.close()
                self.files = [self.fname]
                self.obj_name = '/%s/%s'%(self.prefix, self.fname)
                self._logger = logging.getLogger(__name__)
                self.new_dir = os.path.join(os.path.curdir, 'downloads')
                if not os.path.exists(self.new_dir):
                        os.makedirs(self.new_dir)
 
        def tearDown(self):
                #cleaning downloaded junk
                for junk in os.listdir(self.new_dir):
                        os.remove(os.path.join(self.new_dir,junk))
                if os.path.exists(self.new_dir):
                        os.removedirs(self.new_dir)
                #delete file and entry in cloud
                os.remove(self.fname)
                if 'S3' in self.id():
                        try:
                                s3_con = connect_s3()
                                bucket = s3_con.get_bucket(self.container)
                                bucket.delete()
                        except:
                                pass
                else:
                        try:
                                username        = os.environ['CLOUD_SERVERS_USERNAME']
                                api_key         = os.environ['CLOUD_SERVERS_API_KEY']
                        except KeyError:
                                self._logger.error("Define environ variables 'username' and "
                                                "'api_key' to run tests operating Rackspace")
                        try:
                                conn = cloudfiles.get_connection(username=username, api_key=api_key)
                                conn.delete_bucket(self.container)
                        except:
                                pass
 
 
        def test_S3(self):
                try:
                        aws_key_id = os.environ["AWS_ACCESS_KEY_ID"]
                        aws_secret_key = os.environ["AWS_SECRET_ACCESS_KEY"]
                except:
                        raise Exception('Cannot get aws credentials from os environment.')
 
                conn = connect_s3(aws_key_id, aws_secret_key)
 
                transfer.Transfer.explore_provider(S3TransferProvider)
 
                U = transfer.Transfer(pool=2, logger=self._logger)
                url = 's3://%s/%s/' % (self.container, self.prefix)
                U.upload(self.files, url, force=True)
 
                #check container exists
 
                buckets = [b.name for b in conn.get_all_buckets()]
                self.assertTrue(self.container in buckets)
                bucket = conn.get_bucket(self.container)
 
 
                #check file uploaded and prefix is ok
 
                objects = [os.path.basename(key.name) for key in bucket.list(self.prefix)]
                self.assertTrue(self.fname in objects)
 
                #check file contains appropriate data
                U.download(url, self.new_dir)
                t_content = open(os.path.join(self.new_dir, os.path.basename(self.fname)), 'r').read()
                self.assertEquals(self.content, t_content)
 
                #delete files
                for key in bucket.get_all_keys():
                        key.delete()
                #delete bucket
                conn.delete_bucket(self.container)
 
        def test_cloud_files(self):
                transfer.Transfer.explore_provider(CFTransferProvider)
                U = transfer.Transfer(pool=2, logger=self._logger)
                #cfud = CFTransferProvider(self.cont, self.prefix)
                #U.upload(self.files, cfud)
                url = 'cf://%s/%s/' % (self.container, self.prefix)
 
                U.upload(self.files, url)
 
 
                connection = cloudfiles.get_connection(username=os.environ["username"], api_key=os.environ["api_key"], serviceNet=True)
                #check container exists
                containers = [c.name for c in connection.get_all_containers()]
 
                self.assertTrue(self.container in containers)
 
                #check file uploaded and prefix is ok
                container = connection.get_container(self.container)
                objects = [c.name for c in container.get_objects()]
                self.assertTrue(self.obj_name in objects)
 
                #check file contains appropriate data
                U.download(url, self.new_dir)
                t_content = open(os.path.join(self.new_dir, os.path.basename(self.fname)), 'r').read()
                self.assertEquals(self.content, t_content)
 
                container.delete_object(self.obj_name)
                #delete container
                connection.delete_container(self.container)
'''
 
if __name__ == "__main__":
    main()
    unittest.main()
 
