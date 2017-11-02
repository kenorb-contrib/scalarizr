import logging
import os
import re
import shutil
import sys
import time
import subprocess
import pprint
import itertools
import fileinput

from boto.ec2.blockdevicemapping import EBSBlockDeviceType
from boto.ec2.blockdevicemapping import BlockDeviceMapping

from scalarizr import linux
from scalarizr import util
from scalarizr.api.image import ImageAPIDelegate
from scalarizr.api.image import ImageAPIError
from scalarizr.config import ScalarizrCnf
from scalarizr.linux import coreutils
from scalarizr.linux import mount
from scalarizr.linux import rsync
from scalarizr.linux import pkgmgr
from scalarizr.node import __node__
from scalarizr.node import base_dir as etc_dir
from scalarizr.node import private_dir
from scalarizr.storage2 import filesystem
from scalarizr.storage2 import volume as create_volume
from scalarizr.storage2.util import loop
from scalarizr.util import system2
from scalarizr.util.deploy import HttpSource

LOG = logging.getLogger(__name__)


EPH_STORAGE_MAPPING = {
    'i386': {
        'ephemeral0': '/dev/sda2',},
    'x86_64': {
        'ephemeral0': '/dev/sdb',
        'ephemeral1': '/dev/sdc',
        'ephemeral2': '/dev/sdd',
        'ephemeral3': '/dev/sde',}}


class InstanceStoreImageMaker(object):
    
    def __init__(self,
        image_name,
        image_size,
        delegate,
        excludes=[],
        bucket_name=None,
        destination='/mnt/scalr_image'):

        self.image_name = image_name
        self.image_size = image_size
        self.environ = delegate.environ
        self.credentials = delegate.credentials
        self.ami_bin_dir = delegate.ami_bin_dir
        self.bundle_vol_cmd = delegate.bundle_vol_cmd
        self.excludes = excludes
        self.bucket_name = bucket_name
        self.destination = destination
        self.platform = __node__['platform']

        if not excludes:
            self.excludes = [
                # self.destination,
                ]

    def prepare_image(self):
        # prepares image with ec2-bundle-vol command
        if not os.path.exists(self.destination):
            os.mkdir(self.destination)
        cmd = (
            self.bundle_vol_cmd,
            '--cert', self.credentials['cert'],
            '--privatekey', self.credentials['key'],
            '--user', self.credentials['user'],
            '--arch', linux.os['arch'],
            '--size', str(self.image_size),
            '--destination', self.destination,
            # '--exclude', ','.join(self.excludes),
            # '--block-device-mapping', ,  # TODO:
            '--prefix', self.image_name,
            '--volume', '/',
            '--debug')
        LOG.debug('Image prepare command: ' + ' '.join(cmd))
        out = linux.system(cmd, 
            env=self.environ,
            stdout=subprocess.PIPE, 
            stderr=subprocess.STDOUT)[0]
        LOG.debug('Image prepare command out: %s' % out)

    def upload_image(self):
        LOG.debug('Uploading image (with ec2-upload-bundle)')
        manifest = os.path.join(self.destination, self.image_name) + '.manifest.xml'
        bucket = os.path.basename(self.platform.scalrfs.root())
        cmd = (
            os.path.join(self.ami_bin_dir, 'ec2-upload-bundle'),
            '--bucket', bucket,
            '--access-key', self.credentials['access_key'],
            '--secret-key', self.credentials['secret_key'],
            '--manifest', manifest)
        LOG.debug('Image upload command: ' + ' '.join(cmd))
        out = linux.system(cmd, env=self.environ)[0]
        LOG.debug('Image upload command out: %s' % out)
        return bucket, manifest

    def _register_image(self, bucket, manifest):
        LOG.debug('Registering image')
        s3_manifest_path = '%s/%s' % (bucket, os.path.basename(manifest))
        LOG.debug("Registering image '%s'", s3_manifest_path)

        conn = self.platform.new_ec2_conn()

        instance_id = self.platform.get_instance_id()
        instance = conn.get_all_instances([instance_id])[0].instances[0]
        
        ami_id = conn.register_image(
            name=self.image_name,
            image_location=s3_manifest_path,
            kernel_id=instance.kernel,
            virtualization_type=instance.virtualization_type,
            ramdisk_id=self.platform.get_ramdisk_id(),
            architecture=instance.architecture)

        LOG.debug("Image is registered.")
        LOG.debug('Image %s is available', ami_id)
        return ami_id

    def cleanup(self):
        # remove image from the server
        linux.system('rm -f %s/%s.*' % (self.destination, self.image_name), shell=True)

    def create_image(self):
        try:
            self.prepare_image()
            bucket, manifest = self.upload_image()
            image_id = self._register_image(bucket, manifest)
            return image_id
        finally:
            self.cleanup()


class EBSImageMaker(object):

    def __init__(self, image_name, root_disk, delegate, destination='/mnt/scalr_image'):
        self.image_name = image_name
        self.root_disk = root_disk
        self.image_size = self.root_disk.size
        self.environ = delegate.environ
        self.credentials = delegate.credentials
        self.ami_bin_dir = delegate.ami_bin_dir
        self.bundle_vol_cmd = delegate.bundle_vol_cmd
        self.platform = __node__['platform']
        self.destination = destination
        self.temp_vol = None
        self.excludes = ['/dev', '/media', '/mnt', '/proc', '/sys']

    def _assure_space(self):
        """
        Assures that there is enough free space on destination device for image
        """
        avail_space = coreutils.statvfs(self.destination)['avail'] / 1024 / 1024
        if avail_space <= self.image_size:
            os.mkdir('/mnt/temp-vol')
            LOG.debug('Making temp volume')
            self.temp_vol = self.make_volume({'size': self.image_size, 
                'tags': {'scalr-status': 'temporary'}},
                '/mnt/temp-vol',
                mount_vol=True)
            self.destination = '/mnt/temp-vol'

    def prepare_image(self):
        """Prepares imiage with ec2-bundle-vol command"""
        if not os.path.exists(self.destination):
            os.mkdir(self.destination)
        self._assure_space()
        cmd = (
            self.bundle_vol_cmd,
            '--cert', self.credentials['cert'],
            '--privatekey', self.credentials['key'],
            '--user', self.credentials['user'],
            '--arch', linux.os['arch'],
            '--size', str(self.image_size*1024),
            '--destination', self.destination,
            '--exclude', ','.join([self.destination] + self.excludes),
            '--prefix', self.image_name,
            '--volume', '/',
            '--debug')
        LOG.debug('Image prepare command: ' + ' '.join(cmd))
        out = linux.system(cmd, 
            env=self.environ,
            stdout=subprocess.PIPE, 
            stderr=subprocess.STDOUT)[0]
        LOG.debug('Image prepare command out: %s' % out)

    def make_volume(self, config, mpoint, mount_vol=False):
        config['type'] = 'ebs'

        LOG.debug('Creating ebs volume')
        fstype = None
        for v in mount.mounts('/etc/mtab'):
            if v.device.startswith('/dev'):
                fstype = v.fstype
                break
        volume = create_volume(config, fstype=filesystem(fstype))
        volume.mpoint = mpoint
        volume.ensure(mount=True, mkfs=True)
        if not mount_vol:
            volume.umount()
        LOG.debug('Volume created %s' % volume.device)
        return volume

    def fix_fstab(self, volume):
        conn = self.platform.new_ec2_conn()
        fstab_file_path = os.path.join(volume.mpoint, 'etc/fstab')
        fstab = mount.fstab(fstab_file_path)

        vol_filters = {'attachment.instance-id': self.platform.get_instance_id()}
        attached_vols = conn.get_all_volumes(filters=vol_filters)

        for vol in attached_vols:
            try:
                fstab.remove(vol.attach_data.device)
            except KeyError:
                LOG.warn("Can't remove %s from fstab" % vol.attach_data.device)

    def cleanup_ssh_keys(self, homedir):
        filename = os.path.join(homedir, '.ssh/authorized_keys')
        if os.path.exists(filename):
            LOG.debug('Removing Scalr SSH keys from %s', filename)
            fp = open(filename + '.tmp', 'w+')
            for line in open(filename):
                if 'SCALR-ROLESBUILDER' in line:
                    continue
                fp.write(line)
            fp.close()
            os.rename(filename + '.tmp', filename)

    def cleanup_user_activity(self, homedir):
        for name in (".bash_history", ".lesshst", ".viminfo",
            ".mysql_history", ".history", ".sqlite_history"):
            LOG.debug('Removing user activity file %s', name)
            filename = os.path.join(homedir, name)
            if os.path.exists(filename):
                os.remove(filename)

    def clean_snapshot(self, volume):
        LOG.debug('fixing fstab')
        self.fix_fstab(volume)

        homedirs = [os.path.join('/home', userdir) for userdir 
            in os.listdir(volume.mpoint+'/home')] + [volume.mpoint+'/root']
        for homedir in homedirs:
            self.cleanup_ssh_keys(homedir)
            self.cleanup_user_activity(homedir)
        
    def make_snapshot(self, volume):
        prepared_image_path = os.path.join(self.destination, self.image_name)
        LOG.debug('sgp_dd image into volume %s' % volume.device)
        system2(('sgp_dd',
            'if='+prepared_image_path,
            'of='+volume.device,
            'bs=8k', 
            'count=%s' % (self.image_size*1024*1024/8)))
        # coreutils.dd(**{'if': prepared_image_path, 'of': volume.device, 'bs': '8M'})

        volume.mount()
        self.clean_snapshot(volume)
        LOG.debug('detaching volume')
        volume.detach()

        LOG.debug('Making snapshot of volume %s' % volume.device)
        snapshot = volume.snapshot()
        util.wait_until(
                lambda: snapshot.status() == 'completed',
                logger=LOG,
                error_text='EBS snapshot %s wasnt completed' % snapshot.id)
        LOG.debug('Snapshot is made')

        volume.ensure(mount=True)
        return snapshot.id

    def _register_image(self, snapshot_id):
        conn = self.platform.new_ec2_conn()
    
        instance_id = self.platform.get_instance_id()
        instance = conn.get_all_instances([instance_id])[0].instances[0]

        block_device_map = BlockDeviceMapping(conn)

        root_vol = EBSBlockDeviceType(snapshot_id=snapshot_id)
        root_vol.delete_on_termination = True
        # Adding ephemeral devices
        for eph, device in EPH_STORAGE_MAPPING[linux.os['arch']].items():
            bdt = EBSBlockDeviceType(conn)
            bdt.ephemeral_name = eph
            block_device_map[device] = bdt

        root_partition = instance.root_device_name[:-1]
        if root_partition in self.platform.get_block_device_mapping().values():
            block_device_map[root_partition] = root_vol
        else:
            block_device_map[instance.root_device_name] = root_vol

        return conn.register_image(
            name=self.image_name,
            root_device_name=instance.root_device_name,
            block_device_map=block_device_map,
            kernel_id=instance.kernel,
            virtualization_type=instance.virtualization_type,
            ramdisk_id=self.platform.get_ramdisk_id(),
            architecture=instance.architecture)

    def cleanup(self):
        try:
            os.removedirs(self.destination)
        except OSError:
            pass

    def create_image(self):
        volume = None
        try:
            LOG.debug('Preparing data for snapshot')
            self.prepare_image()
            volume_config = {'size': self.root_disk.size,
                'tags': {'scalr-status': 'temporary'}}
            LOG.debug('Creating volume for snapshot')
            volume = self.make_volume(volume_config, '/mnt/img-mnt')
            LOG.debug('Making snapshot')
            snapshot_id = self.make_snapshot(volume)
            LOG.debug('Registering image')
            image_id = self._register_image(snapshot_id)
            LOG.debug('Image is registered. ID: %s' % image_id)
            return image_id
        finally:
            if volume:
                volume.destroy()
            if self.temp_vol:
                self.temp_vol.destroy()
            self.cleanup()


class EC2ImageAPIDelegate(ImageAPIDelegate):

    _tools_dir = '/var/lib/scalr/ec2-tools'
    _ruby_dir = '/var/lib/scalr/ruby-1.9.3'
    _ami_tools_name = 'ec2-ami-tools'

    def __init__(self):
        self.image_maker = None
        self.environ = os.environ.copy()
        self.excludes = None
        self.ami_bin_dir = None
        self.bundle_vol_cmd = None
        self._prepare_software()

    def _get_version(self, tools_folder_name):
        version = tools_folder_name.split('-')[-1]
        version = tuple(int(x) for x in version.split('.'))
        return version

    def _remove_old_versions(self):
        for item in os.listdir(self._tools_dir):
            if item.startswith(self._ami_tools_name):
                os.removedirs(os.path.join(self._tools_dir, item))

    def _install_ruby(self):    
        packages = None
        if linux.os['family'] == 'RedHat':
            packages = ['unzip',
                'gcc-c++',
                'patch',
                'readline',
                'readline-devel',
                'zlib',
                'zlib-devel',
                'libyaml-devel',
                'libffi-devel',
                'openssl-devel',
                'make',
                'bzip2',
                'autoconf',
                'automake',
                'libtool',
                'bison']
            if linux.os['name'] == 'CentOS' and linux.os['release'] < (5, 4):
                packages.append('iconv-devel')
        else:
            packages = ['unzip',
                'build-essential',
                'zlib1g-dev',
                'libssl-dev',
                'libreadline6-dev',
                'libyaml-dev']

        for package in packages:
            pkgmgr.installed(package)

        # update curl certificate on centos 5
        if linux.os['name'] == 'CentOS':
            system2(('curl', 
                '-L', 'http://curl.haxx.se/ca/cacert.pem',
                '-o', '/etc/pki/tls/certs/ca-bundle.crt'))

        ruby_src = HttpSource('http://cache.ruby-lang.org/pub/ruby/1.9/ruby-1.9.3-p547.tar.gz')
        ruby_src.update('/tmp')
        sources_dir = '/tmp/ruby-1.9.3-p547'
        system2(('./configure', '-prefix=%s' % self._ruby_dir), cwd=sources_dir)
        system2(('make',), cwd=sources_dir)
        system2(('make', 'install'), cwd=sources_dir)

        self.environ['PATH'] = self.environ['PATH'] + (':%s/bin' % self._ruby_dir)
        self.environ['MY_RUBY_HOME'] = self._ruby_dir

    def _install_sg3_utils(self):
        # Installs sg3_utils package for fast sgp_dd command
        if linux.os['family'] == 'RedHat' and linux.os['name'] != 'Amazon':
            pkgmgr.installed('sg3_utils')
            return

        arch = None
        lib_package = None
        utils_package = None
        pkg_mgr_cmd = None

        if linux.os['name'] == 'Amazon':
            arch = linux.os['arch']
            lib_package = 'sg3_utils-libs-1.39-1.%s.rpm' % arch
            utils_package = 'sg3_utils-1.39-1.%s.rpm' % arch
            pkg_mgr_cmd = 'rpm'
        else:
            arch = 'i386' if linux.os['arch'] == 'i386' else 'amd64'
            lib_package = 'libsgutils2-2_1.39-0.1_%s.deb' % arch
            utils_package = 'sg3-utils_1.39-0.1_%s.deb' % arch
            pkg_mgr_cmd = 'dpkg'
        
        lib_src = HttpSource('http://sg.danny.cz/sg/p/'+lib_package)
        lib_src.update('/tmp')
        system2((pkg_mgr_cmd, '-i', '/tmp/'+lib_package))

        utils_src = HttpSource('http://sg.danny.cz/sg/p/'+utils_package)
        utils_src.update('/tmp')
        system2((pkg_mgr_cmd, '-i', '/tmp/'+utils_package))

        os.remove('/tmp/'+lib_package)
        os.remove('/tmp/'+utils_package)

    def _install_ami_tools(self):
        if linux.os['name'] == 'Amazon':
            pkgmgr.installed('aws-amitools-ec2-1.5.3')
            self.bundle_vol_cmd = '/opt/aws/bin/ec2-bundle-vol'
            return

        if not os.path.exists(self._tools_dir):
            if not os.path.exists(os.path.dirname(self._tools_dir)):
                os.mkdir(os.path.dirname(self._tools_dir))
            os.mkdir(self._tools_dir)
        if not os.path.exists(self._ruby_dir):
            os.mkdir(self._ruby_dir)

        self._remove_old_versions()
        self._install_ruby()

        ami_tools_src = HttpSource('http://s3.amazonaws.com/ec2-downloads/ec2-ami-tools.zip')
        ami_tools_src.update(self._tools_dir)

        directory_contents = os.listdir(self._tools_dir)
        self.ami_bin_dir = None
        for item in directory_contents:
            if self.ami_bin_dir:
                break
            elif item.startswith('ec2-ami-tools'):
                self.ami_bin_dir = os.path.join(self._tools_dir,
                    os.path.join(item, 'bin'))

        if linux.os['name'] == 'CentOS' and linux.os['release'] < (6, 0):
            # patching ami tools so /dev/root is determinated as valid device
            ami_tools_dir = os.path.dirname(self.ami_bin_dir)
            file_to_patch = os.path.join(ami_tools_dir, 'lib/ec2/platform/linux/image.rb')
            for line in fileinput.input(file_to_patch, inplace=True):
                if 'ROOT_DEVICE_REGEX = ' in line:
                    definition_part = line.split('=')[0]
                    fixed_regex = '/^(\/dev\/(?:root|(?:xvd|sd)(?:[a-z]|[a-c][a-z]|d[a-x])))[1]?$/'
                    print '%s=%s' % (definition_part, fixed_regex)
                else:
                    print line,

            # updating mkfs cause of filesystem option setting bug
            pkgmgr.installed('texinfo')

            e2fsprogs_src = HttpSource('https://www.kernel.org/pub/linux/kernel/'
                'people/tytso/e2fsprogs/v1.42.5/e2fsprogs-1.42.5.tar.gz')
            e2fsprogs_src.update('/tmp')
            e2fs_dir = '/tmp/e2fsprogs-1.42.5'
            build_dir = os.path.join(e2fs_dir, 'build')
            os.mkdir(build_dir)
            system2(('../configure'), cwd=build_dir)
            system2(('make'), cwd=build_dir)
            system2(('make', 'install'), cwd=build_dir)

        self.bundle_vol_cmd = os.path.join(self.ami_bin_dir, 'ec2-bundle-vol')
        system2(('chmod', '-R', '0755', os.path.dirname(self._tools_dir)))

        system2(('export', 'EC2_AMITOOL_HOME=%s' % os.path.dirname(self.ami_bin_dir)),
            shell=True)

    def _prepare_software(self):
        # windows has no ami tools. Bundle is made by scalr
        if linux.os['family'] != 'Windows':
            pkgmgr.updatedb()
            self._install_sg3_utils()
            self._install_ami_tools()
            if linux.os['family'] == 'RedHat':
                pkgmgr.installed('parted')
            pkgmgr.installed('kpartx')

    def _get_root_disk(self, root_device_type, instance, ec2_conn):
        # list of all mounted devices 
        if root_device_type == 'ebs':
            vol_filters = {'attachment.instance-id': instance.id}
            attached_vols = ec2_conn.get_all_volumes(filters=vol_filters)
            for vol in attached_vols:
                # Strip ending digits to make /dev/sda1 -eq /dev/sda
                if instance.root_device_name == vol.attach_data.device or \
                    re.sub(r'\d+$', '', instance.root_device_name) == vol.attach_data.device:
                    
                    return vol
            raise ImageAPIError("Failed to find root volume")
        else:
            devices = coreutils.df()
            # root device partition like `df(device='/dev/sda2', ..., mpoint='/')
            for device in devices:
                if device.mpoint == '/':
                    return device
            raise ImageAPIError("Can't find root device")

    def _setup_environment(self):
        platform = __node__['platform']
        cnf = ScalarizrCnf(etc_dir)
        cert, pk = platform.get_cert_pk()
        access_key, secret_key = platform.get_access_keys()

        cert_path = cnf.write_key('ec2-cert.pem', cert)
        pk_path = cnf.write_key('ec2-pk.pem', pk)
        cloud_cert_path = cnf.write_key('ec2-cloud-cert.pem', platform.get_ec2_cert())

        self.environ.update({
            'EC2_CERT': cert_path,
            'EC2_PRIVATE_KEY': pk_path,
            'EC2_USER_ID': platform.get_account_id(),
            'AWS_ACCESS_KEY': access_key,
            'AWS_SECRET_KEY': secret_key})
        self.credentials = {
            'cert': cert_path,
            'key': pk_path,
            'user': self.environ['EC2_USER_ID'],
            'access_key': access_key,
            'secret_key': secret_key}

    def _get_s3_bucket_name(self):
        platform = __node__['platform']
        return 'scalr2-images-%s-%s' % \
            (platform.get_region(), platform.get_account_id())

    def prepare(self, operation, name):
        pass
        
    def snapshot(self, operation, name):
        image_name = name + "-" + time.strftime("%Y%m%d%H%M%S")

        platform = __node__['platform']
        ec2_conn = platform.new_ec2_conn()
        instance_id = platform.get_instance_id()
        try:
            instance = ec2_conn.get_all_instances([instance_id])[0].instances[0]
        except IndexError:
            msg = 'Failed to find instance %s. ' \
                'If you are importing this server, check that you are doing it from the ' \
                'right Scalr environment' % instance_id
            raise ImageAPIError(msg)
        root_device_type = instance.root_device_type  

        root_disk = self._get_root_disk(root_device_type, instance, ec2_conn)
        self._setup_environment()
        LOG.debug('device type: %s' % root_device_type)
        if root_device_type == 'ebs':
            self.image_maker = EBSImageMaker(
                    image_name,
                    root_disk,
                    self)
        else:
            self.image_maker = InstanceStoreImageMaker(
                image_name,
                int(root_disk.size/1024),
                self,
                bucket_name=self._get_s3_bucket_name())

        # system2(('/usr/local/rvm/bin/rvm use 1.9.3',), shell=True)
        image_id = self.image_maker.create_image()
        # system2(('/usr/local/rvm/bin/rvm use system',), shell=True)
        return image_id

    def finalize(self, operation, name):
        cnf = ScalarizrCnf(etc_dir)
        for key_name in ('ec2-cert.pem', 'ec2-pk.pem', 'ec2-cloud-cert.pem'):
            path = cnf.key_path(key_name)
            linux.system('chmod 755 %s' % path, shell=True)
            linux.system('rm -f %s' % path, shell=True)
