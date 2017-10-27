import os
import tempfile
 
from lettuce import world, step, after
from scalarizr.storage2 import volume
 
 
@step("I have LVM layout on top of loop device")
def i_have_lvm_layout(step):
    world.tmp_mpoint = tempfile.mkdtemp()
    loop = volume(type='loop', size=0.1)
    world.loops = [loop]
    world.lvm_vol = volume(type='lvm', vg='mysql',
                                     size='80%VG', name='data',
                                     mpoint=world.tmp_mpoint,
                                     pvs=[loop])
    world.lvm_vol.ensure(mount=True, mkfs=True)
    dir_stats = os.statvfs(world.tmp_mpoint)
    world.free_space_before = dir_stats.f_bavail * dir_stats.f_bsize
 
    world.test_file = os.path.join(world.tmp_mpoint, 'test_file')
    world.test_text = "I don't wanna die"
    with open(world.test_file, 'w') as f:
        f.write(world.test_text)
 
 
@step('I extend pvs with another loop')
def i_extend_lvm_pvs_with(step):
    loop = volume(type='loop', size=0.1)
    world.loops.append(loop)
    world.lvm_vol.pvs.append(loop)
    world.lvm_vol.ensure()
 
 
@step('I see volume growth')
def i_see_volume_growth(step):
    dir_stats = os.statvfs(world.tmp_mpoint)
    new_free_space = dir_stats.f_bavail * dir_stats.f_bsize
    assert new_free_space / world.free_space_before >= 1.9
 
    if not os.path.exists(world.test_file):
        raise Exception('Test file was not found. Consistency test failed')
 
    with open(world.test_file) as f:
        text = f.read()
 
    assert text == world.test_text
 
 
@step('I create and then delete LVM snapshot')
def create_delete_lvm_snap(step):
    lvm_snap = world.lvm_vol.lvm_snapshot()
    world.lvm_snap_path = lvm_snap.device
    lvm_snap.destroy()
 
 
@step('I do not see cow device file')
def cow_is_gone(step):
    assert not os.path.exists(world.lvm_snap_path)
 
 
 
@after.all
def cleanup(total):
    if hasattr(world, 'lvm_vol'):
        world.lvm_vol.destroy(force=True)
 
    if hasattr(world, 'loops'):
        for loop in world.loops:
            loop.destroy(force=True)
 
