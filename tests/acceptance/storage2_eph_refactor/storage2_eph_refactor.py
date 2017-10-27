import os
import json
import mock
import boto
import logging
 
from lettuce import step, world, after
 
from scalarizr import storage2
from scalarizr.linux import lvm2, coreutils
 
 
LOG = logging.getLogger(__name__)
 
patcher = mock.patch('scalarizr.node.__node__')
node = patcher.start()
node.__getitem__.return_value.__getitem__.return_value = boto.connect_s3
 
 
 
with open(os.path.join(os.path.dirname(__file__), 'volume_cfgs.json')) as f:
    configs = json.load(f)
 
@after.each_scenario
def cleanup(scenario):
    if hasattr(world, 'eph_vol'):
        world.eph_vol.destroy(force=True)
 
 
@step('I create eph volume from (\w+) old-style config')
def create_eph_volume(step, config_name):
    LOG.info(config_name)
    world.config = configs[config_name]
    vol = storage2.volume(**world.config)
    world.eph_vol = vol
    vol.ensure(mount=True, mkfs=True)
 
 
@step('lvm layer was created')
def lvm_layer_created(step):
    vol = world.eph_vol
    assert lvm2.vgs(world.eph_vol.vg), 'Volume group not found'
    assert os.path.exists(vol.device), 'LVM volume device not found'
    assert vol.mounted_to() == vol.mpoint
 
 
@step('I see snapshotted file')
def i_see_snapshotted_file(step):
    vol = world.eph_vol
    test_file_path = os.path.join(vol.mpoint, 'dump.6379.rdb')
    assert os.path.exists(test_file_path)
 
