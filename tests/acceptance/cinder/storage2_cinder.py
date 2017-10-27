# -*- coding: utf-8 -*-
 
# from time import sleep
 
from lettuce import step
from lettuce import world
from scalarizr.storage2.volumes import cinder
 
# This test doesn't work. You need to
# TODO: add credentials (mock them in scalarizr.platform.openstack)
 
world.ensured = False
 
def init_cinder_vol(server_id, size=1):
    if not (hasattr(world, 'cinder_vol') and world.cinder_vol != None):
        world.cinder_vol = cinder.CinderVolume(volume_type=None, snapshot_id=None)
    world.cinder_vol._server_id = lambda: server_id
    world.cinder_vol.umount = lambda: None
    world.cinder_vol.size = size
    # world.cinder_vol.avail_zone = None
    # world.cinder_vol.snapshot_id = None
 
#Scenario 1
 
@step(u'Given I create CinderVolume object on server (.*)')
def create_volume(step, server_id):
    init_cinder_vol(server_id)
    assert world.cinder_vol != None, 'CinderVolume failed to init'
 
    world.cinder_vol._check_cinder_connection()
    world.volume_count = len(world.cinder_vol._cinder.volumes.list())
 
@step(u'When I run ensure')
def run_ensure_volume(step):
    world.cinder_vol.ensure()
    world.ensured = True
 
@step(u'Then actual volume should be created')
def chech_creation(step):
    new_volume_count = len(world.cinder_vol._cinder.volumes.list())
 
    assert new_volume_count - world.volume_count == 1, 'CinderVolume failed to create a volume'
    assert world.cinder_vol.id, 'CinderVolume failed to update information about itself'
 
###############################################################################
#Scenario 2
 
@step(u'Given I have created CinderVolume object on server (.*)')
def given_i_have_created_cindervolume_object(step, server_id):
    init_cinder_vol(server_id)
    if not world.ensured:
        world.cinder_vol.ensure()
        world.ensured = True
    assert world.cinder_vol, 'CinderVolume not created for this step'
 
@step(u'When I run destroy')
def when_i_run_destroy(step):
    world.volume_count = len(world.cinder_vol._cinder.volumes.list())
    world.deleting_id = world.cinder_vol.id
    world.cinder_vol.destroy()
    world.ensured = False
 
@step(u'Then it should delete volume on cinder')
def then_it_should_delete_volume_on_cinder(step):
    vol = world.cinder_vol._cinder.volumes.get(world.deleting_id)
    assert vol.status == 'deleting', 'CinderVolume failed to delete a volume'
 
@step(u'And set id attribute to None')
def and_set_id_attribute_to_none(step):
    assert world.cinder_vol.id == None, 'CinderVolume failed to clear a volume id'
 
###############################################################################
#Scenario 3
 
@step(u'And I save its state')
def and_i_save_its_state(step):
    world.saved_size = world.cinder_vol.size
    world.saved_snapshot_id = world.cinder_vol.snapshot_id
    world.saved_avail_zone = world.cinder_vol.avail_zone
    world.saved_tags = world.cinder_vol.tags
    world.saved_volume_type = world.cinder_vol.volume_type
 
@step(u'Then object should left unchanged')
def then_object_should_left_unchanged(step):
    unchanged = world.saved_size == world.cinder_vol.size and \
        world.saved_snapshot_id == world.cinder_vol.snapshot_id and \
        world.saved_avail_zone == world.cinder_vol.avail_zone and \
        world.saved_tags == world.cinder_vol.tags and \
        world.saved_volume_type == world.cinder_vol.volume_type
    assert unchanged, 'CinderVolume has been changed with second ensure run'
 
###############################################################################
#Scenario 4
 
@step(u'But without actual attachment')
def but_without_attachment(step):
    world.cinder_vol.detach()
    vol = world.cinder_vol._cinder.volumes.get(world.cinder_vol.id)
    assert len(vol.attachments) == 0, 'CinderVolume failed to detach a volume'
 
@step(u'Then volume should be attached to server')
def then_volume_should_be_attached_to_server(step):
    vol = world.cinder_vol._cinder.volumes.get(world.cinder_vol.id)
    assert world.cinder_vol._server_id() == vol.attachments[0]['server_id'], 'This step must be implemented'
 
###############################################################################
#Scenario 5
 
@step(u'But it located in other availability zone')
def but_it_located_in_other_availability_zone(step):
    world.cinder_vol.avail_zone = 'another_zone'
    world.saved_id = world.cinder_vol.id
 
@step(u'Then volume should be moved to given zone')
def then_volume_should_be_moved_to_given_zone(step):
    id_has_changed = world.saved_id != world.cinder_vol.id
    snapshot_created = world.cinder_vol.snapshot_id != None
    assert id_has_changed, 'New volume wasn\'t created'
    assert snapshot_created, 'Snapshot was\'t created'
 
###############################################################################
#Scenario 6
 
@step(u'When I run create snapshot')
def when_i_run_create_snapshot(step):
    world.snapshot_num = len(world.cinder_vol._cinder.volume_snapshots.list())
    world.cinder_vol.snapshot()
 
@step(u'Then actual snapshot should be created')
def then_actual_snapshot_should_be_created(step):
    new_snapshot_num = len(world.cinder_vol._cinder.volume_snapshots.list())
    assert new_snapshot_num - 1 == world.snapshot_num, 'Actual snapshot wasn\'t created'
 
###############################################################################
#Scenario 7
 
@step(u'And I set it different size')
def and_i_set_it_different_size(step):
    world.cinder_vol.size = 10
 
@step(u'Then CinderVolume should recover its true size')
def then_cindervolume_should_return_its_size_to_original(step):
    assert world.cinder_vol.size == 1, 'CinderVolume failed to recover its true size'
 
