 
import os
import sys
import time
import tempfile
 
from scalarizr import linux
from scalarizr import storage2
from scalarizr.linux import coreutils, mdadm
 
from lettuce import world, step, after
 
 
### HELPERS ###
 
 
def parse_config(raw_cfg, dot_notation=False):
    cfg = {}
    cfg_pairs = raw_cfg.strip().split(',')
    for pair in cfg_pairs:
        k,v = pair.strip().split('=')
        if dot_notation and '.' in k:
            k, sub_k = k.split('.', 1)
            v = {sub_k: v}
        if k not in cfg:
            cfg[k] = v
        else:
            cfg[k].update(v)
    return cfg
 
 
 
def get_file_md5sum(file_path):
    o, e, code = linux.system(['md5sum', file_path])
    return o.strip().split()[0]
 
 
### STEPS ###
 
 
@step('I have (.+?) volume with (.+?) settings')
def prepare_volume(step, kind, raw_cfg):
    # Prepare config
    cfg = parse_config(raw_cfg, dot_notation=True)
    if kind == 'raid':
        disk_count = cfg.pop('disks')
        disk_config = cfg.pop('disk')
        cfg['disks'] = [disk_config] * int(disk_count)
 
    world.tmp_mount_dir = tempfile.mkdtemp()
    cfg['mpoint'] = world.tmp_mount_dir
    cfg['type'] = kind
 
    world.initial_cfg = cfg
 
    vol = world.volume = storage2.volume(**cfg)
    vol.ensure(mount=True, mkfs=True)
 
 
 
@step('When I replace disk (\d+) with disk (.+?) with (.+?) settings')
def replace_disk(step, index, kind, raw_cfg):
    # Prepare config
    cfg = parse_config(raw_cfg, dot_notation=True)
    if kind == 'raid':
        raise Exception('Wrong disk type:raid')
 
    cfg['type'] = kind
 
    world.initial_cfg = cfg
 
    vol = world.new_volume = storage2.volume(**cfg)
    vol.ensure()
 
    world.volume.replace_disk(int(index), world.new_volume)
    mdadm.mdadm('misc', world.volume.raid_pv, '--wait')
 
    #cfg['id'] = 'vol-123'
    #cfg['device'] = '/dev/loop10'
    #world.volume.replace_disk(int(index), cfg)
    #mdadm.mdadm('misc', world.volume.raid_pv, '--wait')
 
 
@step('Then I see that disk (\d+) was replaced')
def allright(step, index):
    assert world.volume.disks[int(index)].device == world.new_volume.device
    #assert world.volume.disks[int(index)].device == '/dev/loop10'
    assert not mdadm.mdadm('misc', world.volume.raid_pv, '--test')[2]
 
 
 
def teardown_scenario(*args, **kwargs):
    if hasattr(world, 'volume'):
        try:
            world.volume.destroy(force=True, remove_disks=True)
        except:
            print sys.exc_info()
 
    world.spew('volume')
    world.spew('new_volume')
 
    if hasattr(world, 'tmp_mount_dir'):
        if os.path.exists(world.tmp_mount_dir):
            try:
                os.rmdir(world.tmp_mount_dir)
            except:
                print sys.exc_info()
 
 
after.outline(teardown_scenario)
after.all(teardown_scenario)
 
