__author__ = 'spike'
"""
Google cloud storage isolated test
 
You need to export path to google compute key before running this test
 
EXPORT GCS_KEY=/path/to/my/gcs_key.p12
"""
 
import logging
import base64
import uuid
import tempfile
import shutil
import mock
import os
 
from lettuce import world, step, after
from scalarizr.storage2.cloudfs import gcs
from scalarizr.platform.gce import GoogleServiceManager, STORAGE_FULL_SCOPE
from scalarizr.linux import system
 
LOG = logging.getLogger(__name__)
 
@step(r'I upload test file to random bucket')
def upload(step):
        key_path = os.environ.get('GCS_KEY')
        with open(key_path) as f:
            key = base64.b64encode(f.read())
        access_data = dict(service_account_name='876103924605@developer.gserviceaccount.com', key=key)
 
        gcs.bus = mock.MagicMock()
        gcs.bus.platform.get_access_data = lambda k: access_data[k]
 
        gsm = GoogleServiceManager(gcs.bus.platform,
                "storage", "v1beta2", *STORAGE_FULL_SCOPE)
 
        gcs.bus.platform.get_numeric_project_id.return_value = '876103924605'
        gcs.bus.platform.new_storage_client = lambda: gsm.get_service()
 
        world.gcs = gcs.GCSFileSystem()
        world.tmpdir = tempfile.mkdtemp()
        # make random file
        tmp_file = os.path.join(world.tmpdir, 'test_file')
        system("dd if=/dev/urandom of=%s bs=1M count=1" % tmp_file, shell=True)
        world.src_md5 = system('md5sum %s' % tmp_file, shell=True)[0]
        LOG.info('MD5 : %s' % world.src_md5)
        world.bucket_name = 'scalr-tests-%s' % str(uuid.uuid4())[:8]
        LOG.info('Bucket name: %s' % world.bucket_name)
 
        world.dst_path = 'gcs://%s/test_file' % world.bucket_name
 
        try:
            world.gcs.ls(world.dst_path)
        except:
            pass
        else:
            raise Exception('Destination path already exist')
        world.gcs.put(tmp_file, world.dst_path)
        os.unlink(tmp_file)
 
 
@step(r'I can see it on remote fs')
def ls(step):
    assert world.gcs.ls('gcs://%s' % world.bucket_name), 'File does not exist on remote fs'
 
 
@step(r'I download it back')
def get(step):
    world.gcs.get(world.dst_path, world.tmpdir)
 
 
@step(r'I see same file i uploaded before')
def check(step):
    down_path = os.path.join(world.tmpdir, 'test_file')
    md5sum = system('md5sum %s' % down_path, shell=True)[0]
    LOG.info('DST MD5 %s' % md5sum)
    assert md5sum == world.src_md5, 'Files are not identical'
 
 
@step(r'I delete file on remote fs')
def delete(step):
    world.gcs.delete(world.dst_path, delete_bucket=True)
 
 
@step(r'I cannot see it on remote fs')
def check_deleted(step):
    try:
        files = world.gcs.ls('gcs://%s' % world.bucket_name)
        world.deleted = True
    except:
        pass
    else:
        raise Exception('Bucket still exist. File list: %s' % files)
 
 
@after.all
def cleanup(total):
    if hasattr(world, 'bucket_name') and not hasattr(world, 'deleted'):
        try:
            world.gcs.delete(world.dst_path, delete_bucket=True)
        except:
            LOG.warning('Failed to cleanup bucket on GCS. You should delete it manually')
    shutil.rmtree(world.tmpdir)
 
