"""
:requires: env[AWS_ACCESS_KEY_ID] env[AWS_SECRET_ACCESS_KEY]
:requires: role[percona] | role[mysql2]
"""
 
from scalarizr import linux
from scalarizr.services import backup
from scalarizr.services import mysql as mysql_svc
from scalarizr.storage2.cloudfs import s3
 
import boto
import mock
from lettuce import step, before, after, world
 
 
FEATURE = 'Percona xtrabackup'
CLOUDFS_TARGET = 's3://scalr.test_bucket/percona-xtrabackup-streaming'
 
 
@before.each_feature
def setup(feat):
    if feat.name == FEATURE:
        p = mock.patch.multiple(s3.S3FileSystem,
                        _bucket_location=mock.Mock(return_value=''),
                        _get_connection=mock.Mock(side_effect=lambda: boto.connect_s3()))
        p.start()
        world.s3_patcher = p
        world.mysql_init = mysql_svc.MysqlInitScript()
        linux.system('mysqladmin create xtrabackup', shell=True, raise_exc=False)
 
 
@after.each_feature
def teardown(feat):
    if feat.name == FEATURE:
        world.s3_patcher.stop()
        linux.system('mysqladmin --force drop xtrabackup', shell=True, raise_exc=False)
 
 
@step(u'Given i have running Percona Server')
def given_i_have_running_percona_server(step):
    if not world.mysql_init.running:
        world.mysql_init.start()
 
 
@step(u'When i create full xtrabackup')
def when_i_create_full_xtrabackup(step):
    __import__('scalarizr.services.mysql2')
    bak = backup.backup(
                    type='xtrabackup',
                    cloudfs_target=CLOUDFS_TARGET)
    restore = bak.run()
    world.restore = {'R1': dict(restore)}
 
 
@step(u'When i create incremental xtrabackup')
def when_i_create_incremental_xtrabackup(step):
    __import__('scalarizr.services.mysql2')
    bak = backup.backup(
                    type='xtrabackup',
                    backup_type='incremental',
                    prev_cloudfs_source=world.restore['R1']['cloudfs_source'],
                    cloudfs_target=CLOUDFS_TARGET)
    restore = bak.run()
    world.restore['R2'] = dict(restore)
 
 
@step(u'Then i have a restore object (.*)')
def then_i_have_a_restore_object(step, rst):
    assert hasattr(world, 'restore')
    restore = world.restore[rst]
    assert restore['type'] == 'xtrabackup'
    assert restore['cloudfs_source']
    assert restore['cloudfs_source'].endswith('.json')
    assert restore['to_lsn']
 
 
@step(u'And add some data')
def and_add_some_data(step):
    linux.system('mysql xtrabackup', shell=True, stdin='create table table1 (id integer, data text);')
 
 
@step(u'Given i have stopped Percona Server')
def given_i_have_stopped_percona_server(step):
    if world.mysql_init.running:
        world.mysql_init.stop()
 
 
@step(u'When i restore full backup (.*)')
def when_i_restore_full_backup(step, key):
    rst = backup.restore(world.restore[key])
    rst.run()
 
 
@step(u'Then i have operational Percona Server')
def then_i_have_operational_percona_server(step):
    world.mysql_init.start()
    assert linux.system('mysqladmin ping', shell=True, raise_exc=False)[2] == 0
 
 
@step(u'When i restore incremental backup (.*)')
def when_i_restore_incremental_backup(step, key):
    rst = world.restore[key]
    rst = backup.restore(rst)
    rst.run()
 
 
@step(u'And some data from incremental backup')
def and_some_data_from_incremental_backup(step):
    out = linux.system('mysql xtrabackup', shell=True, stdin='show tables;')[0]
    assert 'table1' in out
 
