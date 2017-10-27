"""
Run on percona-lvm instance.
 
Uses $AWS_ACCESS_KEY_ID, $AWS_SECRET_ACCESS_KEY, $AWS_ACCOUNT_ID
"""
__author__ = 'vladimir'
 
import subprocess
import time
import os
import tempfile
import string
import random
 
from lettuce import before, after, world, step
import yaml
import pymysql
 
 
this_feature_only = world.ThisFeatureOnly("Cancel messages")
 
 
cancel_msg = \
'<?xml version="1.0"?>\n' \
'<message id="43a76d0a-e264-4607-a6b3-681447f0759d" name="%(name)s"><meta>' \
'<scalr_version>4.1.0</scalr_version></meta><body><scripts/></body></message>'
 
create_databundle_msg = \
'<?xml version="1.0"?>\n' \
'<message id="d2d2251c-8f65-4175-a131-fc5b99b609e6" name="%(name)s"><meta>' \
'<scalr_version>4.1.0</scalr_version></meta><body><scripts/><percona><backup>' \
'<type>xtrabackup</type><compressor>gzip</compressor><backup_type>full' \
'</backup_type><cloudfs_target>s3://scalr-ab6d8171-3414-us-east-1/data-bundles/' \
'12416/percona/</cloudfs_target></backup></percona><storage_type>lvm' \
'</storage_type><platform_access_data><account_id>%(acc_id)s</account_id>' \
'<key_id>%(key_id)s</key_id><key>%(key)s</key></platform_access_data></body>' \
'</message>' % {
        "name": "DbMsr_CreateDataBundle",
        "acc_id": os.environ["AWS_ACCOUNT_ID"],
        "key_id": os.environ["AWS_ACCESS_KEY_ID"],
        "key": os.environ["AWS_SECRET_ACCESS_KEY"],
}
 
create_backup_msg = \
'<?xml version="1.0"?>\n' \
'<message id="a76b9e37-d333-4595-b9fe-8ddbbdaabc89" name="%(name)s"><meta>' \
'<scalr_version>4.1.0</scalr_version></meta><body><scripts/>' \
'<platform_access_data><account_id>%(acc_id)s</account_id><key_id>%(key_id)s' \
'</key_id><key>%(key)s</key></platform_access_data></body></message>' % {
        "name": "DbMsr_CreateBackup",
        "acc_id": os.environ["AWS_ACCOUNT_ID"],
        "key_id": os.environ["AWS_ACCESS_KEY_ID"],
        "key": os.environ["AWS_SECRET_ACCESS_KEY"],
}
 
MESSAGES = {
        "DbMsr_CancelDataBundle": cancel_msg % {"name": "DbMsr_CancelDataBundle"},
        "DbMsr_CancelBackup": cancel_msg % {"name": "DbMsr_CancelBackup"},
        "DbMsr_CreateDataBundle": create_databundle_msg,
        "DbMsr_CreateBackup": create_backup_msg,
}
 
 
def send_message(name):
    msg = MESSAGES[name]
 
    fd, path = tempfile.mkstemp()
    try:
        try:
            os.write(fd, msg)
        finally:
            os.close(fd)
 
        subprocess.call([
                "szradm",
                "-m",
                "-e", "http://localhost:8013",
                "-o", "control",
                "-n", name,
                "-f", path,
        ], close_fds=True)
    finally:
        os.remove(path)
 
 
def _parse_pretty_table(input):
    # TODO: use indexes of '+' for splitting, not '|'
 
    if input in ('', ' '):
        return []
 
    lines = input.splitlines()
 
    # remove horizontal borders
    lines = filter(lambda x: not x.startswith("+"), lines)
 
    # split each line
    def split_tline(line):
        return map(lambda x: x.strip("| "), line.split(" | "))
    lines = map(split_tline, lines)
 
    # get column names
    head = lines.pop(0)
 
    # [{column_name: value}]
    return [dict(zip(head, line)) for line in lines]
 
 
def list_messages(name=None):
    arglist = [
            "szradm",
            "list-messages",
    ]
    if name:
        arglist += ["-n", name]
    proc = subprocess.Popen(arglist, stdout=subprocess.PIPE, close_fds=True)
    out = proc.communicate()[0]
 
    msgs = _parse_pretty_table(out)
    return msgs
 
 
def message_info(msg_id):
    proc = subprocess.Popen([
            "szradm",
            "message-details",
            msg_id
    ], stdout = subprocess.PIPE, close_fds=True)
    out = proc.communicate()[0]
    return yaml.load(out)
 
 
def flood_db(n):
    conn = pymysql.connect(host="127.0.0.1", user=None, passwd='', db=None)
    conn.autocommit(True)
 
    def fetchall(query):
        cur = conn.cursor(None)
        cur.execute(query)
        return cur.fetchall()
 
    fetchall("DROP DATABASE IF EXISTS testdb")
    fetchall("CREATE DATABASE testdb")
    fetchall("use testdb")
    fetchall("CREATE TABLE testtable (one MEDIUMTEXT)")
 
    z = ''.join(random.choice(string.lowercase) for i in xrange(1024*1023))
 
    [fetchall("INSERT INTO testtable VALUES ('%s')" % z) for i in range(n)]
 
 
@before.each_scenario
@this_feature_only
def setup(scenario):
    world.existing = len(list_messages())
 
 
@after.each_scenario
@this_feature_only
def teardown(scenario):
    del world.existing
 
 
@step("I have used the storage for (\d+) MB")
def i_have_used_the_storage_for(step, mb):
    flood_db(int(mb))
    return
    # TODO: delay on s3 instead
    subprocess.call([
            "dd",
            "if=/dev/urandom",
            "of=%s" % "/mnt/dbstorage/for_test",
            "bs=1M",
            "count=%s" % mb,
    ], stdout=open('/dev/null', 'w'), stderr=subprocess.STDOUT, close_fds=True)
 
 
@step("I (send|have sent) (\w+) message")
def i_send_create_data_bundle_message(step, a, message_name):
    send_message(message_name)
 
 
@step("I wait for (\d+) seconds")
def i_wait_for_seconds(step, seconds):
    time.sleep(int(seconds))
 
 
@step("I expect it canceled")
def i_expect_it_canceled(step):
    # we expect to have only one outgoing result message
    # that contains "Canceled"
    new = list_messages()[world.existing:]
 
    result = filter(lambda x: x["name"] in (
            "DbMsr_CreateDataBundleResult", "DbMsr_CreateBackupResult"), new)
 
    assert len(result) == 1, "Got %s messages while running the test" % len(result)
    msg_id = result.pop()["id"]
 
    msg = message_info(msg_id)
 
    assert msg["body"]["status"] == "error", msg["body"]["status"]
    assert msg["body"]["last_error"] == "Canceled", msg["body"]["last_error"]
 
