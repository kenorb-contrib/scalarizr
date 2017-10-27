__author__ = 'Nick Demyanchuk'
 
import os
import boto
import mock
import urllib2
import logging
import tempfile
 
from scalarizr import linux
from scalarizr import storage2
from scalarizr.storage2.volumes import ebs
from scalarizr.linux import coreutils, mdadm
 
from lettuce import world, step, after
 
 
 
#logging.basicConfig()
#logging.getLogger().setLevel(logging.DEBUG)
LOG = logging.getLogger(__name__)
 
 
# Unnecessary = temporary
global_artifacts = dict(unnecessary=[], other=[])
 
patcher = mock.patch('scalarizr.storage2.volumes.ebs.__node__')
mocked_node = patcher.start()
mocked_node.__getitem__.return_value.__getitem__.return_value = False
 
 
# Patch ec2
metadata = dict()
def get_from_metadata(name):
    url = "http://169.254.169.254/latest/meta-data"
    try:
        if not metadata.has_key(name):
            target_url = os.path.join(url, name)
            r = urllib2.urlopen(target_url)
            metadata[name] = r.read().strip()
        return metadata[name]
    except IOError, e:
        LOG.info("Looks like it's not ec2 instance")
 
ebs.EbsMixin._avail_zone = lambda x: get_from_metadata('placement/availability-zone')
ebs.EbsMixin._instance_id = lambda x: get_from_metadata('instance-id')
ebs.EbsMixin._instance_type = lambda x: get_from_metadata('instance-type')
ebs.EbsMixin._connect_ec2 = lambda x: boto.connect_ec2()
 
 
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
 
 
def get_device_size(device):
    o, e, code = linux.system(['df', device])
    return int(o.split('\n')[1].split()[1])
 
 
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
 
    world.size_before = get_device_size(vol.device)
 
 
@step('I create some file on it')
def create_some_file(step):
    world.fpath = os.path.join(world.tmp_mount_dir, 'myfile')
 
    dd_kwargs = {'if': '/dev/urandom', 'of': world.fpath, 'bs': '1M', 'count': 1}
    coreutils.dd(**dd_kwargs)
 
    world.file_md5 = get_file_md5sum(world.fpath)
 
 
 
class patch_grow(object):
    def __init__(self, vol, fails):
        self.vol = vol
        self.fails = fails
        self.map = dict()
 
    def patch(self, vol):
        def patch_destroy(artifact):
            orig_destroy = artifact.destroy
            def destroy(*args, **kwargs):
                artifact.destroyed = 1
                return orig_destroy(*args, **kwargs)
            artifact.destroy = destroy
 
        def patch_ensure(artifact):
            orig_ensure = artifact.ensure
            def ensure(*args, **kwargs):
                artifact.ensured = 1
                return orig_ensure(*args, **kwargs)
            artifact.ensure = ensure
 
 
        orig_snapshot = vol.snapshot
        def snap_artifact_collector(*args, **kwargs):
            snap = orig_snapshot(*args, **kwargs)
            global_artifacts['unnecessary'].append(snap)
            patch_destroy(snap)
            return snap
        vol.snapshot = snap_artifact_collector
 
        orig_clone = vol.clone
        def clone_artifact_collector(*args, **kwargs):
            clone = orig_clone(*args, **kwargs)
            global_artifacts['other'].append(clone)
            patch_destroy(clone)
            patch_ensure(clone)
            return clone
        vol.clone = clone_artifact_collector
 
    def __enter__(self):
        if not hasattr(self.vol, 'patched'):
            self.patch(self.vol)
 
        if self.fails:
            self.orig__grow = self.vol._grow
            def _grow(*args, **kwargs):
                self.orig__grow( *args, **kwargs)
                raise Exception('Force raise Exception')
            self.vol._grow = _grow
 
        if self.vol.type == 'raid':
            for disk in self.vol.disks:
                self.patch(disk)
 
        self.vol.patched = 1
 
    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.fails:
            """ restore original grow """
            self.vol._grow = self.orig__grow
 
        if exc_type:
            raise exc_type, exc_val, exc_tb
 
 
@step('I grow volume with (.+?)(and it fails)?$')
def grow_volume(step, raw_cfg, fails):
    world.grow_cfg = cfg = parse_config(raw_cfg, dot_notation=True)
 
    with patch_grow(world.volume, fails=fails):
        try:
            world.bigger_vol = world.volume.grow(**cfg)
        except:
            if not fails:
                raise
 
 
@step('I see that volume grew properly')
def check_space_increased(step):
    new_size = get_device_size(world.bigger_vol.device)
    assert world.size_before < new_size, "New size is not bigger than old"
 
    if world.bigger_vol.type == 'raid':
        each_volume_grew = world.grow_cfg.get('disks')
        if int(world.bigger_vol.level) == 5 or each_volume_grew:
            assert world.size_before < new_size, "New size is not bigger than old"
            LOG.info('Size grew on %s' % (new_size - world.size_before))
 
        mdinfo = mdadm.detail(world.bigger_vol.raid_pv)
        disk_count_should_be = int(world.grow_cfg.get('disks_count') or len(world.initial_cfg['disks']))
        assert mdinfo['raid_devices'] == disk_count_should_be, "Disk count doesn't match"
 
 
@step('I still see my \w+ file')
def check_integrity(step):
    new_md5 = get_file_md5sum(world.fpath)
    assert new_md5 == world.file_md5, 'Integrity check failed'
 
 
@step('(unnecessary|all) artifacts were destroyed')
def artifacts_were_destroyed(step, scope):
    for artifact in global_artifacts['unnecessary']:
        LOG.info('Checking artifact destroyed: %s' % artifact)
        assert artifact.destroyed == 1, 'Artifact was not destroyed: %s' % artifact
        global_artifacts['unnecessary'].remove(artifact)
 
    if scope == 'all':
        for artifact in global_artifacts['other']:
            LOG.info('Checking artifact destroyed: %s' % artifact)
            if not hasattr(artifact, 'ensured'):
                LOG.info('Artifact %s was not ensured, so it was not destroyed')
                continue
            assert artifact.destroyed == 1, 'Artifact was not destroyed: %s' % artifact
            global_artifacts['other'].remove(artifact)
 
 
@step('I destroy growed volume')
def destroy_growed_vol(step):
    world.bigger_vol.destroy(force=True, remove_disks=True)
 
 
@step('I attach my original volume back')
def attach_orig_vol_back(step):
    world.volume.ensure(mount=True)
 
 
@step('I see my original volume back')
def original_vol_is_back(step):
    world.volume.mounted_to() == world.volume.mpoint
 
 
@step("I see that EBS settings were really changed")
def ebs_is_not_the_same_anymore(step):
    conn = boto.connect_ec2()
    ebs_vol = conn.get_all_volumes([world.bigger_vol.id])[0]
 
    if world.grow_cfg.get('volume_type'):
        assert ebs_vol.type == world.grow_cfg.get('volume_type')
 
    if world.grow_cfg.get('iops'):
        assert str(ebs_vol.iops)== str(world.grow_cfg.get('iops'))
 
 
def teardown_scenario(*args, **kwargs):
    for vol in ('volume', 'bigger_vol'):
        if hasattr(world, vol):
            LOG.info('Removing %s' % vol)
            try:
                v = getattr(world, vol)
                v.destroy(force=True, remove_disks=True)
            except:
                pass
 
            world.spew(vol)
 
    if hasattr(world, 'tmp_mount_dir'):
        try:
            os.rmdir(world.tmp_mount_dir)
        except:
            pass
 
    global_artifacts = dict(unnecessary=[], other=[])
 
after.outline(teardown_scenario)
after.all(teardown_scenario)
 
