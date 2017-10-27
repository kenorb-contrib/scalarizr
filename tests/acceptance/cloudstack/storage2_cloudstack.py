# -*- coding: utf-8 -*-
 
# from time import sleep
 
from os import environ
 
from lettuce import step
from lettuce import world
from lettuce import before
from lettuce import after
import mock
 
from cloudstack import Client
from scalarizr.storage2.volumes import csvol as cloudstack
 
FEATURE_NAME = 'Cloudstack storage'
 
@before.each_feature
def setup(feat):
    if feat.name == FEATURE_NAME:
        world.cloudstack_patcher = mock.patch(cloudstack.__cloudstack__, {})
        world.cloudstack_patcher.start()
        world.cloudstack_patcher['new_conn'] = Client(environ['CLOUDSTACK_API_URL'],
                                                      apiKey=environ['CLOUDSTACK_API_KEY'],
                                                      secretKey=environ['CLOUDSTACK_SECRET_KEY'])
        world.cloudstack_patcher['zone_id'] = environ['CLOUDSTACK_ZONE_ID']
        world.cloudstack_patcher['instance_id'] = ''
 
 
@after.each_feature
def teardown(feat):
    if feat.name == FEATURE_NAME:
        world.cloudstack_patcher.stop()
 
 
world.ensured = False
 
 
def init_vol(server_id, size=1):
    cloudstack.__cloudstack__['instance_id'] = server_id
    if not (hasattr(world, 'vol') and world.vol != None):
        world.vol = cloudstack.CSVolume(volume_type=None, snapshot_id=None)
    world.vol.umount = lambda: None
    world.vol.size = size
    # world.vol.avail_zone = None
    # world.vol.snapshot_id = None
 
#Scenario 1
 
@step(u'Given I create CSVolume object on server (.*)')
def create_volume(step, server_id):
    init_vol(server_id)
    assert world.vol != None, 'CSVolume failed to init'
 
    world.vol._check_connection()
    world.volume_count = len(world.vol._conn.listVolumes())
 
@step(u'When I run ensure')
def run_ensure_volume(step):
    world.vol.ensure()
    world.ensured = True
 
@step(u'Then actual volume should be created')
def chech_creation(step):
    new_volume_count = len(world.vol._conn.listVolumes())
 
    assert new_volume_count - world.volume_count == 1, 'CSVolume failed to create a volume'
    assert world.vol.id, 'CSVolume failed to update information about itself'
 
###############################################################################
#Scenario 2
 
@step(u'Given I have created CSVolume object on server (.*)')
def given_i_have_created_csvolume_object(step, server_id):
    init_vol(server_id)
    if not world.ensured:
        world.vol.ensure()
        world.ensured = True
    assert world.vol, 'CSVolume not created for this step'
 
@step(u'When I run destroy')
def when_i_run_destroy(step):
    world.volume_count = len(world.vol._conn.listVolumes())
    world.deleting_id = world.vol.id
    world.vol.destroy()
    world.ensured = False
 
@step(u'Then it should delete volume on cloudstack')
def then_it_should_delete_volume_on_cloudstack(step):
    volume_list = world.vol._conn.listVolumes(id=world.deleting_id)
    if len(volume_list) != 0:
        vol = world.vol._conn.listVolumes(id=world.deleting_id)[0]
        assert vol.status == 'deleting', 'CSVolume failed to delete a volume'
 
@step(u'And set id attribute to None')
def and_set_id_attribute_to_none(step):
    assert world.vol.id == None, 'CSVolume failed to clear a volume id'
 
###############################################################################
#Scenario 3
 
@step(u'And I save its state')
def and_i_save_its_state(step):
    world.saved_size = world.vol.size
    world.saved_snapshot_id = world.vol.snapshot_id
    world.saved_zone_id = world.vol.zone_id
    world.saved_name = world.vol.name
    world.saved_disk_offering_id = world.vol.disk_offering_id
 
@step(u'Then object should left unchanged')
def then_object_should_left_unchanged(step):
    unchanged = world.saved_size == world.vol.size and \
        world.saved_snapshot_id == world.vol.snapshot_id and \
        world.saved_zone_id == world.vol.zone_id and \
        world.saved_name == world.vol.name and \
        world.saved_disk_offering_id == world.vol.disk_offering_id
    assert unchanged, 'CSVolume has been changed with second ensure run'
 
###############################################################################
#Scenario 4
 
@step(u'But without actual attachment')
def but_without_attachment(step):
    world.vol.detach()
    vol = world.vol._conn.listVolumes(id=world.vol.id)[0]
    assert not hasattr(vol, 'virtualmachineid'), 'CSVolume failed to detach a volume'
 
@step(u'Then volume should be attached to server')
def then_volume_should_be_attached_to_server(step):
    vol = world.vol._conn.listVolumes(id=world.vol.id)[0]
    assert hasattr(vol, 'virtualmachineid'), 'Volume did not attached to the server'
 
###############################################################################
#Scenario 5
 
# @step(u'But it located in other availability zone')
# def but_it_located_in_other_availability_zone(step):
#     world.vol.avail_zone = 'another_zone'
#     world.saved_id = world.vol.id
 
# @step(u'Then volume should be moved to given zone')
# def then_volume_should_be_moved_to_given_zone(step):
#     id_has_changed = world.saved_id != world.vol.id
#     snapshot_created = world.vol.snapshot_id != None
#     assert id_has_changed, 'New volume wasn\'t created'
#     assert snapshot_created, 'Snapshot was\'t created'
 
###############################################################################
#Scenario 6
 
@step(u'When I run create snapshot')
def when_i_run_create_snapshot(step):
    world.snapshot_num = len(world.vol._conn.listSnapshots())
    world.vol.snapshot()
 
@step(u'Then actual snapshot should be created')
def then_actual_snapshot_should_be_created(step):
    new_snapshot_num = len(world.vol._conn.listSnapshots())
    assert new_snapshot_num - 1 == world.snapshot_num, 'Actual snapshot wasn\'t created'
 
###############################################################################
#Scenario 7
 
@step(u'With wrong size')
def and_i_set_it_different_size(step):
    world.saved_size = world.vol.size
    world.vol.size = 10
 
@step(u'Then CSVolume should recover its true size')
def then_csvolume_should_return_its_size_to_original(step):
    assert world.vol.size == world.saved_size, 'CSVolume failed to recover its true size'
 
