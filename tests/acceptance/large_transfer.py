"""
Essential environment variables:
$AWS_ACCESS_KEY_ID, $AWS_SECRET_ACCESS_KEY for s3
$RS_USERNAME, $RS_API_KEY for swift (rackspace);
$ENTER_IT_USERNAME, $ENTER_IT_API_KEY for swift enter.it
 
default storage to test is local; override with $LT_TEST_STORAGE
"""
__author__ = 'vladimir'
 
 
import shutil
import tempfile
import os
import subprocess
import json
import logging
import hashlib
import base64
from copy import copy
from ConfigParser import ConfigParser
from random import choice
 
import mock
from lettuce import step, world, before, after
from boto import connect_s3
import swiftclient
 
from scalarizr.storage2.cloudfs import LargeTransfer, LOG
from scalarizr.storage2.cloudfs import s3, gcs, swift, local
from scalarizr.platform.gce import STORAGE_FULL_SCOPE, GoogleServiceManager
 
 
this_feature_only = world.ThisFeatureOnly("Large transfer")
 
 
STORAGE = "local"
if "LT_TEST_STORAGE" in os.environ:
    STORAGE = os.environ["LT_TEST_STORAGE"]
 
 
_RESTORE = []
 
@before.each_feature
@this_feature_only
def setup_feature(feat):
    # prevent ini parser from lowercasing params
    _RESTORE.append((ConfigParser, "optionxform",
                                     ConfigParser.optionxform))
    ConfigParser.optionxform = lambda self, x: x
 
    # make connections work
 
    if STORAGE == "s3":
        _RESTORE.append((s3.S3FileSystem, "_get_connection",
                                         s3.S3FileSystem._get_connection))
        s3.S3FileSystem._get_connection = lambda self: connect_s3()
        s3.S3FileSystem._bucket_location = lambda self: ''
 
    elif STORAGE == "gcs":
        def get_pk(f="gcs_pk.p12"):  # TODO:
            with open(f, "rb") as fd:
                pk = fd.read()
            return base64.b64encode(pk)
 
        ACCESS_DATA = {
                "service_account_name": '876103924605@developer.gserviceaccount.com',
                "key": get_pk(),
        }
 
        _RESTORE.append((gcs, "bus", gcs.bus))
        gcs.bus = mock.MagicMock()
        gcs.bus.platform.get_access_data = lambda k: ACCESS_DATA[k]
 
        gsm = GoogleServiceManager(gcs.bus.platform,
                "storage", "v1beta2", *STORAGE_FULL_SCOPE)
 
        gcs.bus.platform.get_numeric_project_id.return_value = '876103924605'
        gcs.bus.platform.new_storage_client = lambda: gsm.get_service()
 
    elif STORAGE == "swift":
        _RESTORE.append((swift.SwiftFileSystem, "_get_connection",
                                         swift.SwiftFileSystem._get_connection))
        swift.SwiftFileSystem._get_connection = lambda self: swiftclient.Connection(
                        "https://identity.api.rackspacecloud.com/v1.0",
                        os.environ["RS_USERNAME"], os.environ["RS_API_KEY"])
 
    elif STORAGE == "swift-enter-it":
        _RESTORE.append((swift.SwiftFileSystem, "_get_connection",
                                         swift.SwiftFileSystem._get_connection))
        swift.SwiftFileSystem._get_connection = lambda self: swiftclient.Connection(
                "http://folsom.enter.it:5000/v2.0",
                os.environ["ENTER_IT_USERNAME"], os.environ["ENTER_IT_API_KEY"], auth_version="2")
 
 
@after.each_feature
@this_feature_only
def teardown_feature(feat):
    for args in _RESTORE:
        setattr(*args)
 
 
#
# Logging
#
 
LOG.setLevel(logging.DEBUG)
LOG.addHandler(logging.FileHandler("transfer_test.log", 'w'))
 
 
#
#
#
 
 
STORAGES = {
        "s3": {
                "url": "s3://vova-new/vova_test",
                "driver": s3.S3FileSystem,
        },
        "gcs": {
                "url": "gcs://vova-test",
                "driver": gcs.GCSFileSystem,
        },
        "swift": {
                "url": "swift://vova-test",
                "driver": swift.SwiftFileSystem,
        },
        "swift-enter-it": {
                "url": "swift://vova-test",
                "driver": swift.SwiftFileSystem,
        },
        "local": {
                "url": "file:///tmp/cloudfs",
                "driver": local.LocalFileSystem,
        }
}
 
assert STORAGE in STORAGES, "%s not in %s" % (STORAGE, STORAGES.keys())
 
 
def convert_manifest(json_manifest):
    assert len(json_manifest["files"]) == 1
    assert json_manifest["files"][0]["compressor"] == "gzip"
 
    parser = ConfigParser()
    parser.add_section("snapshot")
    parser.add_section("chunks")
 
    parser.set("snapshot", "description", json_manifest["description"])
    parser.set("snapshot", "created_at", json_manifest["created_at"])
    parser.set("snapshot", "pack_method", json_manifest["files"][0]["compressor"])
 
    for chunk, md5sum, size in reversed(json_manifest["files"][0]["chunks"]):
        parser.set("chunks", chunk, md5sum)
 
    LOG.debug("CONVERT: %s", parser.items("chunks"))
    return parser
 
 
def release_local_data():
    """
    Delete everything from the basedir and return contents of the manifest.
    """
 
    # relies on "I expect manifest as a result" step and
    # LargeTransfer downloading manifest as "manifest.json"
    with open(os.path.join(world.basedir, "manifest.json")) as fd:
        manifest = json.loads(fd.read())
 
    subprocess.call(["rm -r %s/*" % world.basedir], shell=True)
 
    return manifest
 
 
@before.each_scenario
@this_feature_only
def setup_scenario(scenario):
    world.basedir = tempfile.mkdtemp()
    world.sources = []
    world.destination = None
    world.driver = None
    world.result_chunks = []
    world.dl_result = {
            'completed': [],
            'failed': [],
            'multipart_result': None,
    }
    world.deleted_chunk = None
    world._for_size_test = None
 
 
@after.each_scenario
@this_feature_only
def teardown_scenario(scenario):
    shutil.rmtree(world.basedir)
 
 
def md5(name):
    if os.path.isfile(name):
        out = subprocess.Popen(["md5sum", name], stdout=subprocess.PIPE,
                close_fds=True).communicate()
        return out[0].split()[0]
    elif os.path.isdir(name):
        dir_md5 = []
        for location, dirs, files in os.walk(name):
            files_md5 = map(lambda x: (x, md5(os.path.join(location, x))), files)
            rel_loc = location.replace(name, ".", 1)
 
            dir_md5.append((rel_loc, dirs, files_md5))
        return hashlib.md5(str(dir_md5)).hexdigest()
 
 
def make_file(name, size):
    subprocess.call([
            "dd",
            "if=/dev/urandom",
            "of=%s" % name,
            "bs=1M",
            "count=%s" % size
    ], stdout=open('/dev/null', 'w'), stderr=subprocess.STDOUT, close_fds=True)
 
    return md5(name)
 
 
@step("Initialize upload variables")
def initialize_upload_variables(step):
    world.manifest_url = None
    world.items = {}
 
 
@step(r"I have a (\d+) megabytes file (\w+)")
def i_have_a_file(step, megabytes, filename):
    world._for_size_test = int(megabytes) * 1024 * 1024
 
    filename = os.path.join(world.basedir, filename)
    world.sources.append(filename)
 
    f_md5 = make_file(filename, megabytes)
    world.items[os.path.basename(filename)] = f_md5
 
 
@step("I upload it to Storage with gzipping")
def i_upload_it_with_gzipping(step):
    world.destination = STORAGES[STORAGE]["url"]
    world.driver = STORAGES[STORAGE]["driver"]()
    world.manifest_url = LargeTransfer(world.sources[0], world.destination).run().cloudfs_path
 
 
 
@step("I upload multiple sources to Storage with gzipping")
def i_upload_multiple_sources_with_gzipping(step):
    world.destination = STORAGES[STORAGE]["url"]
    world.driver = STORAGES[STORAGE]["driver"]()
 
    def src_gen(sources=copy(world.sources)):
        for src in sources:
            yield src
 
    world.manifest_url = LargeTransfer(src_gen(), world.destination).run().cloudfs_path
 
 
@step("I expect manifest as a result")
def i_expect_manifest_as_a_result(step):
    local_path = world.driver.get(world.manifest_url, world.basedir)
 
    with open(local_path) as fd:
        data = json.loads(fd.read())
 
    for file in data["files"]:
        world.result_chunks.extend(file["chunks"])
 
 
@step("all chunks are uploaded")
def all_chunks_are_uploaded(step):
    for chunk in world.result_chunks:
        chunk_url = os.path.join(os.path.dirname(world.manifest_url), chunk[0])
        assert world.driver.exists(chunk_url), chunk_url
 
 
@step(r"I have a dir (\w+/?) with (\d+) megabytes file (\w+), with (\d+) megabytes file (\w+)")
def i_have_dir_with_files(step, dirname, f1_size, f1_name, f2_size, f2_name):
    dirname = os.path.join(world.basedir, dirname)
    world.sources.append(dirname)
 
    os.mkdir(dirname)
    f1_md5 = make_file(os.path.join(dirname, f1_name), f1_size)
    f2_md5 = make_file(os.path.join(dirname, f2_name), f2_size)
 
    if dirname.endswith('/'):
        world.items[f1_name] = f1_md5
        world.items[f2_name] = f2_md5
    else:
        world.items[os.path.basename(dirname)] = md5(dirname)
 
 
@step(r"I have a list with (\d+) megabytes stream (\w+), with (\d+) megabytes stream (\w+)")
def i_have_list_of_streams(step, s1_size, s1_name, s2_size, s2_name):
    for name, size in [(s1_name, s1_size), (s2_name, s2_size)]:
        abs_path = os.path.join(world.basedir, name)
        stream_md5 = make_file(abs_path, size)
        stream = open(abs_path, 'rb')
 
        world.sources.append(stream)
        world.items[os.path.basename(stream.name)] = stream_md5
 
 
@step("I have info from the previous upload")
def i_have_info_from_previous_upload(step):
    assert world.manifest_url
    assert world.items
 
 
@step("I download with the manifest")
def i_download_with_the_manifest(step):
    world.dl_result = LargeTransfer(world.manifest_url, world.basedir).run()
 
 
@step("I expect original items downloaded")
def i_expect_original_items_downloaded(step):
    for file, md5sum in world.items.iteritems():
        file_loc = os.path.join(world.basedir, file)
        assert os.path.exists(file_loc), file_loc
        assert md5sum == md5(file_loc), file_loc
 
 
@step("I clear the tempdir and replace the manifest with it's old representation")
def i_replace_the_manifest_with_old_repr(step):
    manifest = release_local_data()
 
    manifest_ini_path = os.path.join(world.basedir, "manifest.ini")
    with open(manifest_ini_path, 'w') as fd:
        convert_manifest(manifest).write(fd)
 
    world.driver.delete(world.manifest_url)
 
    world.manifest_url = world.driver.put(manifest_ini_path,
            os.path.join(os.path.dirname(world.manifest_url), ''))
    LOG.debug("NEW %s" % world.manifest_url)
 
 
@step("I delete one of the chunks")
def i_delete_one_of_the_chunks(step):
    manifest = release_local_data()
    remote_dir, manifest_name = os.path.split(world.manifest_url)
 
    chunk, md5sum, size = choice(choice(manifest["files"])["chunks"])
 
    chunk_url = os.path.join(remote_dir, chunk)
    world.deleted_chunk = chunk_url
    LOG.debug("Lettuce deleting %s" % chunk_url)
 
    world.driver.delete(chunk_url)
 
 
@step("I expect failed list returned")
def i_expect_failed_list_returned(step):
    # Unfortunately, this doesn't test if transfer gets killed right away
    assert world.deleted_chunk in world.dl_result["failed"][0]["src"], world.deleted_chunk
    assert len(world.dl_result["failed"]) == 1
 
 
@step(r"I have a (\d+) megabytes stream (\w+)")
def i_have_a_stream(step, megabytes, name):
    abs_path = os.path.join(world.basedir, name)
    stream_md5 = make_file(abs_path, megabytes)
    stream = open(abs_path, 'rb')
 
    world.sources.append(stream)
    world.items[os.path.basename(stream.name)] = stream_md5
 
 
@step("I upload it to Storage with intentional interrupt")
def i_upload_it_with_intentional_interrupt(step):
    world.destination = STORAGES[STORAGE]["url"]
    world.driver = STORAGES[STORAGE]["driver"]()
 
    lt = LargeTransfer(world.sources[0], world.destination, chunk_size=20, num_workers=2)
    lt.on(transfer_complete=lambda *args: lt.kill())
    lt.run()
 
    world.manifest_url = os.path.join(world.destination, lt.transfer_id)
 
 
@step("I expect cloud path cleaned")
def i_expect_path_clean(step):
    assert not world.driver.ls(world.manifest_url)
 
 
@step("chunks sizes are correct")
def chunks_sizes_are_correct(step):
    # TODO: implement stat() in drivers and make more adequate size checks
    chunk_sum = reduce(lambda sum, x: sum + x[2], world.result_chunks, 0)
    assert abs(world._for_size_test - chunk_sum) < 10 * 1024, "%s != %s" % \
                                                                                    (world._for_size_test, chunk_sum)
 
