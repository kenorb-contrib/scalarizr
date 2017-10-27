from __future__ import with_statement
'''
Created on Mar 11, 2010
@author: marat
'''
 
import logging
 
import scalarizr
from scalarizr.bus import bus
from scalarizr.handlers import HandlerError, build_tags
from scalarizr.util import system2, disttool, cryptotool,\
        wait_until, firstmatched
from scalarizr import linux
from scalarizr.linux import mount
 
from scalarizr.handlers import rebundle as rebundle_hdlr
from scalarizr.handlers import Handler
from scalarizr.messaging import Messages
from scalarizr.linux import coreutils
from scalarizr.linux.tar import Tar
from scalarizr.storage2.volumes import ebs as ebsvolume
from scalarizr.storage2.cloudfs import FileTransfer
from scalarizr.storage2 import volume, filesystem
from scalarizr.libs.metaconf import Configuration
 
from M2Crypto import X509, EVP, Rand, RSA
from binascii import hexlify
from xml.dom.minidom import Document
from datetime import datetime
import time, os, re, shutil, glob
import string
 
from boto.exception import BotoServerError
from boto.ec2.blockdevicemapping import EBSBlockDeviceType, BlockDeviceMapping
 
 
# Workaround for python bug #5853
# @see http://bugs.python.org/issue5853
# @see http://groups.google.com/group/smug-dev/browse_thread/thread/47e7833edb9efbf9?pli=1
import mimetypes
mimetypes.init()
 
 
def get_handlers ():
    return [Ec2RebundleWindowsHandler()] if linux.os.windows_family else  [Ec2RebundleHandler()]
 
LOG = rebundle_hdlr.LOG
 
BUNDLER_NAME = "scalarizr"
BUNDLER_VERSION = scalarizr.__version__
BUNDLER_RELEASE = "672"
 
DIGEST_ALGO = "sha1"
CRYPTO_ALGO = "aes-128-cbc"
 
EPH_STORAGE_MAPPING = {
        'i386': {
                'ephemeral0': '/dev/sda2',
        },
        'x86_64': {
                'ephemeral0': '/dev/sdb',
                'ephemeral1': '/dev/sdc',
                'ephemeral2': '/dev/sdd',
                'ephemeral3': '/dev/sde',
        }
}
 
NETWORK_FILESYSTEMS = ('nfs', 'glusterfs')
 
 
class Ec2RebundleHandler(rebundle_hdlr.RebundleHandler):
    _ebs_strategy_cls = None
    _instance_store_strategy_cls = None
    _instance = None
 
    def __init__(self, ebs_strategy_cls=None, instance_store_strategy_cls=None):
        rebundle_hdlr.RebundleHandler.__init__(self)
 
        self._ebs_strategy_cls = ebs_strategy_cls or RebundleEbsStrategy
        self._instance_store_strategy_cls = instance_store_strategy_cls or RebundleInstanceStoreStrategy
        self._instance = None
        self._strategy = None
        bus.on(rebundle=self.on_rebundle)
 
 
    def before_rebundle(self):
        '''
        @param message.volume_size:
                New size for EBS-root device.
                By default current EBS-root size will be used (15G in most popular AMIs)
        @param message.volume_id
                EBS volume for root device copy.
        '''
 
        image_name = self._role_name + "-" + time.strftime("%Y%m%d%H%M%S")
 
        # Take rebundle strategy
        pl = bus.platform
        ec2_conn = pl.new_ec2_conn()
        instance_id = pl.get_instance_id()
        try:
            instance = ec2_conn.get_all_instances([instance_id])[0].instances[0]
        except IndexError:
            msg = 'Failed to find instance %s. ' \
                            'If you are importing this server, check that you are doing it from the ' \
                            'right Scalr environment' % instance_id
            raise HandlerError(msg)
        self._instance = instance
 
 
        """ list of all mounted devices """
        list_device = coreutils.df()
 
        """ root device partition like `df(device='/dev/sda2', ..., mpoint='/')` """
        root_disk = firstmatched(lambda x: x.mpoint == '/', list_device)
        if not root_disk:
            raise HandlerError("Can't find root device")
 
        if instance.root_device_type == 'ebs':
            # EBS-root device instance
            """ detecting root device like rdev=`sda` """
            rdev = None
            for el in os.listdir('/sys/block'):
                if os.path.basename(root_disk.device) in os.listdir('/sys/block/%s'%el):
                    rdev = el
                    break
            if not rdev and os.path.exists('/sys/block/%s'%os.path.basename(root_disk.device)):
                rdev = root_disk.device
 
            """ list partition of root device """
            list_rdevparts = [dev.device for dev in list_device
                                                    if dev.device.startswith('/dev/%s' % rdev)]
 
            if len(list(set(list_rdevparts))) > 1:
                """ size of volume in KByte"""
                volume_size = system2(('sfdisk', '-s', root_disk.device[:-1]),)
                """ size of volume in GByte"""
                volume_size = int(volume_size[0].strip()) / 1024 / 1024
                #TODO: need set flag, which be for few partitions
                #copy_partition_table = True
            else:
                """ if one partition we use old method """
                volume_size = self._rebundle_message.body.get('volume_size')
                if not volume_size:
                    volume_size = int(root_disk.size / 1000 / 1000)
 
            self._strategy = self._ebs_strategy_cls(
                    self, self._role_name, image_name, self._excludes,
                    volume_size=volume_size,  # in Gb
                    volume_id=self._rebundle_message.body.get('volume_id')
            )
        else:
            # Instance store
            self._strategy = self._instance_store_strategy_cls(
                    self, self._role_name, image_name, self._excludes,
                    image_size = root_disk.size / 1000,  # in Mb
                    s3_bucket_name = self._s3_bucket_name
            )
 
 
    def rebundle(self):
        return self._strategy.run()
 
    def on_rebundle(self, role_name, snapshot_id, rebundle_result):
        rebundle_result['aws'] = {
                'root_device_type': self._instance.root_device_type,
                'virtualization_type': self._instance.virtualization_type
        }
 
    def after_rebundle(self):
        if self._strategy:
            self._strategy.cleanup()
 
    @property
    def _s3_bucket_name(self):
        pl = bus.platform
        return 'scalr2-images-%s-%s' % (pl.get_region(), pl.get_account_id())
 
 
class RebundleStratery:
 
    _role_name = None
    _image_name = None
    _excludes = None
    _volume = None
    _image = None
 
    def __init__(self, handler, role_name, image_name, excludes, volume='/'):
        self._hdlr = handler
        self._role_name = role_name
        self._image_name = image_name
        self._excludes = excludes
        self._volume = volume
 
    def _is_super_user(self):
        return system2('id -u', shell=True)[0].strip() == '0'
 
    def _bundle_vol(self, image):
        try:
            LOG.info('Bundling volume %s', self._volume)
 
            LOG.debug("Checking that user is root")
            if not self._is_super_user():
                raise HandlerError("You need to be root to run rebundle")
            LOG.debug("User check success")
 
            # Create image from volume
            LOG.debug('Exclude list: %s', image.excludes)
            image.make()
            LOG.info("Volume bundle complete!")
 
        except (Exception, BaseException), e:
            LOG.error("Cannot bundle volume. %s", e)
            raise
 
    def _create_motd(self, image_mpoint, role_name=None):
        LOG.debug('Creating motd file')
        # Create message of the day
        for name in ("etc/motd", "etc/motd.tail"):
            motd_filename = os.path.join(image_mpoint, name)
            if os.path.exists(motd_filename):
                dist = disttool.linux_dist()
                motd = rebundle_hdlr.MOTD % dict(
                        dist_name = dist[0],
                        dist_version = dist[1],
                        bits = 64 if disttool.uname()[4] == "x86_64" else 32,
                        role_name = role_name,
                        bundle_date = datetime.today().strftime("%Y-%m-%d %H:%M")
                )
                with open(motd_filename, 'w') as fp:
                    fp.write(motd)
 
    def _fix_fstab(self, image_mpoint=None, filename=None, disable_root_fsck=True):
        assert image_mpoint or filename
        if image_mpoint:
            filename = os.path.join(image_mpoint, 'etc/fstab')
        LOG.debug('Fixing fstab at %s', filename)
        pl = bus.platform
 
        fstab = mount.fstab(filename)
 
        # Remove EBS volumes from fstab
        ec2_conn = pl.new_ec2_conn()
        instance = ec2_conn.get_all_instances([pl.get_instance_id()])[0].instances[0]
 
        ebs_devs = list(vol.attach_data.device
                                for vol in ec2_conn.get_all_volumes(filters={'attachment.instance-id': pl.get_instance_id()})
                                if vol.attach_data and vol.attach_data.instance_id == pl.get_instance_id()
                                        and instance.root_device_name != vol.attach_data.device)
 
        for device in ebs_devs:
            device = ebsvolume.name2device(device)
            LOG.debug('Remove %s from fstab', device)
            try:
                del fstab[device]
            except KeyError:
                pass
 
        # Remove Non-local filesystems
        for entry in fstab:
            if entry.fstype in NETWORK_FILESYSTEMS:
                LOG.debug('Remove %s from fstab', entry.device)
                del fstab[entry.device]
 
        # Disable fsck on root filesystem
        if disable_root_fsck:
            root_entry = fstab['/']
            del fstab['/']
            fstab.add(root_entry.device, root_entry.mpoint, root_entry.fstype,
                              root_entry.options, root_entry.dump, 0)
 
        # Ubuntu 10.04 mountall workaround
        # @see https://bugs.launchpad.net/ubuntu/+source/mountall/+bug/649591
        # @see http://alestic.com/2010/09/ec2-bug-mountall
        if disttool.is_ubuntu() and disttool.version_info() >= (10, 4):
            for entry in fstab:
                if entry.device in pl.instance_store_devices:
                    options = entry.options
                    if options.find('nobootwait') >= 0:
                        options = re.sub(r'(nobootwait),(\S+)', r'\2,\1', options)
                    else:
                        options += ',nobootwait'
                    del fstab[entry.device]
                    fstab.add(entry.device, entry.mpoint, entry.fstype, options, entry.dump, entry.fsck_order)
                    LOG.debug('Added nobootwait for %s', entry.device)
 
 
    def _cleanup_image(self, image_mpoint, role_name=None):
        # Create message of the day
        self._create_motd(image_mpoint, role_name)
        self._fix_fstab(image_mpoint)
        self._hdlr.cleanup_image(image_mpoint)
 
 
    def run(self):
        '''
        Run instance bundle
        '''
        pass
 
 
    def cleanup(self):
        '''
        Perform cleanup after bundle
        '''
        if self._image:
            try:
                self._image.cleanup()
            except (BaseException, Exception), e:
                LOG.error('Error during cleanup: %s', e)
 
 
class RebundleInstanceStoreStrategy(RebundleStratery):
    _IMAGE_CHUNK_SIZE = 10 * 1024 * 1024 # 10 MB in bytes.
    _NUM_UPLOAD_THREADS = 4
    _MAX_UPLOAD_ATTEMPTS = 5
 
    _destination = None
    _image_name = None
    _image_size = None
    _s3_bucket_name = None
    _platform = None
 
    def __init__(self, handler, role_name, image_name, excludes, volume='/',
                            destination='/mnt', image_size=None, s3_bucket_name=None):
        RebundleStratery.__init__(self, handler, role_name, image_name, excludes, volume)
        self._destination = destination
        self._image_size = image_size
        self._s3_bucket_name = s3_bucket_name
        self._platform = bus.platform
 
    def _get_arch(self):
        arch = disttool.uname()[4]
        if re.search("^i\d86$", arch):
            arch = "i386"
        return arch
 
    def _bundle_image(self, name, image_file, user, destination, user_private_key_string,
                                    user_cert_string, ec2_cert_string, key=None, iv=None):
        try:
            LOG.info("Bundling image...")
 
 
            # Create named pipes.
            digest_pipe = os.path.join('/tmp', 'ec2-bundle-image-digest-pipe')
            if os.path.exists(digest_pipe):
                os.remove(digest_pipe)
            try:
                os.mkfifo(digest_pipe)
            except:
                LOG.error("Cannot create named pipe %s", digest_pipe)
                raise
 
            # Load and generate necessary keys.
            name = os.path.basename(image_file)
            manifest_file = os.path.join(destination, name + '.manifest.xml')
            bundled_file_path = os.path.join(destination, name + '.tar.gz.enc')
            try:
                user_public_key = X509.load_cert_string(user_cert_string).get_pubkey()
            except:
                LOG.error("Cannot read user EC2 certificate")
                raise
            try:
                user_private_key = RSA.load_key_string(user_private_key_string)
            except:
                LOG.error("Cannot read user EC2 private key")
                raise
            try:
                ec2_public_key = X509.load_cert_string(ec2_cert_string).get_pubkey()
            except:
                LOG.error("Cannot read EC2 certificate")
                raise
            key = key or hexlify(Rand.rand_bytes(16))
            iv = iv or hexlify(Rand.rand_bytes(8))
            LOG.debug('Key: %s', key)
            LOG.debug('IV: %s', iv)
 
 
            # Bundle the AMI.
            # The image file is tarred - to maintain sparseness, gzipped for
            # compression and then encrypted with AES in CBC mode for
            # confidentiality.
            # To minimize disk I/O the file is read from disk once and
            # piped via several processes. The tee is used to allow a
            # digest of the file to be calculated without having to re-read
            # it from disk.
            openssl = "/usr/sfw/bin/openssl" if disttool.is_sun() else "openssl"
            tar = Tar()
            tar.create().dereference().sparse()
            tar.add(os.path.basename(image_file), os.path.dirname(image_file))
            digest_file = os.path.join('/tmp', 'ec2-bundle-image-digest.sha1')
 
            LOG.info("Encrypting image")
            system2(" | ".join([
                    "%(openssl)s %(digest_algo)s -out %(digest_file)s < %(digest_pipe)s & %(tar)s",
                    "tee %(digest_pipe)s",
                    "gzip",
                    "%(openssl)s enc -e -%(crypto_algo)s -K %(key)s -iv %(iv)s > %(bundled_file_path)s"]) % dict(
                            openssl=openssl, digest_algo=DIGEST_ALGO, digest_file=digest_file, digest_pipe=digest_pipe,
                            tar=str(tar), crypto_algo=CRYPTO_ALGO, key=key, iv=iv, bundled_file_path=bundled_file_path
            ), shell=True)
 
            try:
                # openssl produce different outputs:
                # (stdin)= 8ac0626e9a8d54e46e780149a95695ec894449c8
                # 8ac0626e9a8d54e46e780149a95695ec894449c8
                raw_digest = open(digest_file).read()
                digest = raw_digest.split(" ")[-1].strip()
            except IndexError, e:
                LOG.error("Cannot extract digest from string '%s'", raw_digest)
                raise
            except OSError, e:
                LOG.error("Cannot read file with image digest '%s'. %s", digest_file, e)
                raise
            finally:
                os.remove(digest_file)
 
            #digest = "da39a3ee5e6b4b0d3255bfef95601890afd80709"
 
            # Split the bundled AMI.
            # Splitting is not done as part of the compress, encrypt digest
            # stream, so that the filenames of the parts can be easily
            # tracked. The alternative is to create a dedicated output
            # directory, but this leaves the user less choice.
            LOG.info("Splitting image into chunks")
            part_names = coreutils.split(bundled_file_path, name, self._IMAGE_CHUNK_SIZE, destination)
            LOG.debug("Image splitted into %s chunks", len(part_names))
 
            # Sum the parts file sizes to get the encrypted file size.
            bundled_size = 0
            for part_name in part_names:
                bundled_size += os.path.getsize(os.path.join(destination, part_name))
            LOG.debug('Image size: %d bytes', bundled_size)
 
 
            # Encrypt key and iv.
            LOG.info("Encrypting keys")
            padding = RSA.pkcs1_padding
            user_encrypted_key = hexlify(user_public_key.get_rsa().public_encrypt(key, padding))
            ec2_encrypted_key = hexlify(ec2_public_key.get_rsa().public_encrypt(key, padding))
            user_encrypted_iv = hexlify(user_public_key.get_rsa().public_encrypt(iv, padding))
            ec2_encrypted_iv = hexlify(ec2_public_key.get_rsa().public_encrypt(iv, padding))
            LOG.debug("Keys encrypted")
 
            # Digest parts.
            parts = self._digest_parts(part_names, destination)
 
            # Create bundle manifest
            bdm = list((name, device) for name, device in self._platform.block_devs_mapping()
                            if not name.startswith('ephemeral'))
            bdm += EPH_STORAGE_MAPPING[disttool.arch()].items()
 
            manifest = AmiManifest(
                    name=name,
                    user=user,
                    arch=self._get_arch(),
                    parts=parts,
                    image_size=os.path.getsize(image_file),
                    bundled_size=bundled_size,
                    user_encrypted_key=user_encrypted_key,
                    ec2_encrypted_key=ec2_encrypted_key,
                    user_encrypted_iv=user_encrypted_iv,
                    ec2_encrypted_iv=ec2_encrypted_iv,
                    image_digest=digest,
                    user_private_key=user_private_key,
                    kernel_id=self._platform.get_kernel_id(),
                    ramdisk_id=self._platform.get_ramdisk_id(),
                    ancestor_ami_ids=self._platform.get_ancestor_ami_ids(),
                    block_device_mapping=bdm
            )
            manifest.save(manifest_file)
 
            LOG.info("Image bundle complete!")
            return manifest_file, manifest
        except (Exception, BaseException), e:
            LOG.error("Cannot bundle image. %s", e)
            raise
 
 
    def _digest_parts(self, part_names, destination):
        LOG.info("Generating digests for each chunk")
        part_digests = []
        for part_name in part_names:
            part_filename = os.path.join(destination, part_name)
            f = None
            try:
                f = open(part_filename)
                digest = EVP.MessageDigest(DIGEST_ALGO)
                part_digests.append((part_name, hexlify(cryptotool.digest_file(digest, f))))
            except (Exception, BaseException):
                LOG.error("Cannot generate digest for chunk '%s'", part_name)
                raise
            finally:
                if f is not None:
                    f.close()
        return part_digests
 
 
    def _upload_image(self, bucket_name, manifest_path, manifest, region=None, acl="aws-exec-read"):
        try:
            LOG.info("Uploading bundle")
 
            # Files to upload
            LOG.debug("Enqueue files to upload")
            manifest_dir = os.path.dirname(manifest_path)
            upload_files = [manifest_path]
            for part in manifest.parts:
                upload_files.append(os.path.join(manifest_dir, part[0]))
 
            return self._upload_image_files(upload_files, manifest_path)
 
        except (Exception, BaseException):
            LOG.error("Cannot upload image")
            raise
 
    def _upload_image_files(self, files, manifest_path):
        dst = self._platform.scalrfs.images()
        trn = FileTransfer(src=files, dst=dst)
        res = trn.run()
        #trn = Transfer(pool=4, max_attempts=5, logger=LOG)
        #trn.upload(upload_files, self._platform.scalrfs.images())
 
        if res["failed"]:
            sources = map(lambda job: job["src"], res["failed"])
            exceptions = map(lambda job: job["exc_info"][1], res["failed"])
            str_exceptions = map(lambda exc: ': '.join([repr(exc).split('(')[0],
                                                        exc.message]),
                                 exceptions)
            str_fails = map(lambda pair: '  ' + ' => '.join(pair), zip(sources, str_exceptions))
            msg = ("Failed uploading the image files to {dst}\n"
                   "{failed} out of {total} failed:\n"
                   "\n"
                   "{fails}"
            ).format(dst=dst,
                     failed=len(res["failed"]),
                     total=len(res["completed"]) + len(res["failed"]),
                     fails='\n'.join(str_fails)
            )
            raise HandlerError(msg)
 
        manifest_path = os.path.join(self._platform.scalrfs.images(), os.path.basename(manifest_path))
        return manifest_path.split('s3://')[1]
 
 
    def _register_image(self, s3_manifest_path):
        try:
            LOG.info("Registering image '%s'", s3_manifest_path)
            ec2_conn = self._platform.new_ec2_conn()
 
            ami_id = ec2_conn.register_image(image_location=s3_manifest_path)
            # @see http://code.google.com/p/boto/issues/detail?id=323
            #rs = ec2_conn.get_object('RegisterImage', {"ImageLocation" : s3_manifest_path}, ResultSet)
            #ami_id = getattr(rs, 'imageId', None)
 
            LOG.info("Registration complete!")
            LOG.debug('Image %s available', ami_id)
            return ami_id
        except (BaseException, Exception), e:
            LOG.error("Cannot register image on EC2. %s", e)
            raise
 
 
    def run(self):
        image_file = os.path.join(self._destination, self._image_name)
        self._image = rebundle_hdlr.LinuxLoopbackImage(self._volume, image_file, self._image_size, self._excludes)
        self._bundle_vol(self._image)
 
        # Clean up
        self._cleanup_image(self._image.mpoint, self._role_name)
 
        # Bundle image
        cert, pk = self._platform.get_cert_pk()
        manifest_path, manifest = self._bundle_image(
                                self._image_name, image_file, self._platform.get_account_id(),
                                self._destination, pk, cert, self._platform.get_ec2_cert())
 
        # Upload image to S3
        s3_manifest_path = self._upload_image(self._s3_bucket_name, manifest_path,
                                manifest, region=self._platform.get_region())
 
        # Register image on EC2
        return self._register_image(s3_manifest_path)
 
    def cleanup(self):
        RebundleStratery.cleanup(self)
        if self._image:
            for path in glob.glob(self._image.path + "*"):
                try:
                    if os.path.isdir(path):
                        shutil.rmtree(path, ignore_errors=True)
                    else:
                        os.remove(path)
                except (OSError, IOError), e:
                    LOG.error("Error during cleanup. %s", e)
 
 
class RebundleEbsStrategy(RebundleStratery):
    _volsize = None
    _volume_id = None
    _platform = None
    _snap = None
 
    _succeed = None
 
    def __init__(self, handler, role_name, image_name, excludes, volume='/',
                            volume_id=None, volume_size=None):
        RebundleStratery.__init__(self, handler, role_name, image_name, excludes, volume)
        self._volume_id = volume_id
        self._volsize = volume_size
        self._platform = bus.platform
 
 
    def _create_shapshot(self):
        self._image.umount()
        vol = self._image.ebs_volume
        LOG.info('Creating snapshot of root device image %s', vol.id)
        description = "Root device snapshot created from role: %s instance: %s" \
                                % (self._role_name, self._platform.get_instance_id())
        self._snap = vol.snapshot(description, tags=build_tags(state='temporary'), nowait=False)
 
        #LOG.debug('Checking that snapshot %s is completed', self._snap.id)
        #wait_until(lambda: self._snap.state in (storage.Snapshot.COMPLETED, storage.Snapshot.FAILED), logger=LOG)
 
        #if self._snap.state == storage.Snapshot.FAILED:
        #    raise Exception('Snapshot %s status changed to failed on EC2' % (self._snap.id, ))
        LOG.debug('Snapshot %s completed', self._snap.id)
        LOG.info('Snapshot %s of root device image %s created', self._snap.id, vol.id)
        return self._snap
 
    def _register_image(self):
        instance = self._ec2_conn.get_all_instances((self._platform.get_instance_id(), ))[0].instances[0]
 
        root_device_type = EBSBlockDeviceType()
        root_device_type.snapshot_id = self._snap.id
        root_device_type.delete_on_termination = True
 
        bdmap = BlockDeviceMapping(self._ec2_conn)
 
        # Add ephemeral devices
        for eph, device in EPH_STORAGE_MAPPING[disttool.arch()].items():
            bdt = EBSBlockDeviceType(self._ec2_conn)
            bdt.ephemeral_name = eph
            bdmap[device] = bdt
 
        # Add root device snapshot
        root_partition = instance.root_device_name[:-1]
        if root_partition in self._platform.get_block_device_mapping().values():
            bdmap[root_partition] = root_device_type
        else:
            bdmap[instance.root_device_name] = root_device_type
 
        LOG.info('Registering image')
        ami_id = self._ec2_conn.register_image(self._image_name, architecture=disttool.arch(),
                        kernel_id=self._platform.get_kernel_id(), ramdisk_id=self._platform.get_ramdisk_id(),
                        root_device_name=instance.root_device_name, block_device_map=bdmap)
 
        LOG.info('Checking that %s is available', ami_id)
        def check_image():
            try:
                return self._ec2_conn.get_all_images([ami_id])[0].state == 'available'
            except BotoServerError, e:
                if e.error_code == 'InvalidAMIID.NotFound':
                    # Sometimes it takes few seconds for EC2 to propagate new AMI
                    return False
                raise
        wait_until(check_image, logger=LOG, timeout=3600,
                        error_text="Image %s wasn't completed in a reasonable time" % ami_id)
        LOG.debug('Image %s available', ami_id)
 
        LOG.info('Image registered and available for use!')
        return ami_id
 
 
    def run(self):
        self._succeed = False
 
        # Bundle image
        self._ec2_conn = self._platform.new_ec2_conn()
        self._image = LinuxEbsImage(self._volume, self._ec2_conn,
                                self._platform.get_avail_zone(), self._platform.get_instance_id(),
                                self._volsize, self._volume_id, self._excludes)
 
        self._bundle_vol(self._image)
 
        # Clean up
        self._cleanup_image(self._image.mpoint, self._role_name)
 
        # Create snapshot from root device image
        self._create_shapshot()
 
        # Registering image
        ami_id = self._register_image()
 
        self._succeed = True
        return ami_id
 
    def cleanup(self):
        RebundleStratery.cleanup(self)
        if not self._succeed and self._snap:
            LOG.debug('Deleting snapshot %s', self._snap.id)
            self._snap.destroy()
 
 
 
 
class LinuxEbsImage(rebundle_hdlr.LinuxImage):
    '''
    This class encapsulate functionality to create a EBS from a root volume
    '''
    _ec2_conn = None
    _avail_zone = None
    _instance_id = None
    _volume_size = None
    ebs_volume = None
 
    copy_partition_table = None
 
    def __init__(self, volume, ec2_conn, avail_zone, instance_id,
                            volume_size=None, volume_id=None, excludes=None):
        rebundle_hdlr.LinuxImage.__init__(self, volume, excludes=excludes)
        self._ec2_conn = ec2_conn
        self._avail_zone = avail_zone
        self._instance_id = instance_id
        self._ebs_config = dict(type='ebs')
 
        if volume_id:
            self._ebs_config['id'] = volume_id
        else:
            self._ebs_config['size'] = volume_size
 
 
    def _create_image(self):
        self._ebs_config['tags'] = build_tags(state='temporary')
        self.ebs_volume = volume(self._ebs_config)
        self.ebs_volume.ensure()
        return self.ebs_volume.device
 
 
    def _read_pt(self, dev_name):
        """
        rtype: dict partition table one of device, example: dev_name='/dev/sda'
 
        {'/dev/sdf1': {'bootable': True, 'start': '63', 'Id': '83', 'size': '192717'},
         '/dev/sdf3': {'start': '21800205', 'Id': '82', 'size': '4209030'},
         ...
         '/dev/sdf4': {'start': '0', 'Id': '0', 'size': '0'}}
        """
 
        pt = [el for el in [dev for dev in system2(('sfdisk', '-d', dev_name),
                                )[0].split('\n') if dev.startswith('/dev')]]
        res = {}
        for line in pt:
            dev_name, params = map(string.strip, line.split(':'))
            params = map(string.strip, params.split(','))
            res[dev_name] = {}
            for val in params:
                tmp = map(string.strip, val.split('=')) if '=' in val else [val.strip(), True]
                res[dev_name].update({tmp[0]:tmp[1]})
        return res
 
    def make_partitions(self):
 
        self.devname = self._create_image()
        LOG.debug('Created volume and attached as `%s`', self.devname)
 
        system2("sync", shell=True)
        # Flush so newly formatted filesystem is ready to mount.
 
        list_devices = coreutils.df()
 
        """ rdev_partition is like `/dev/sda1` """
        rdev_partition = firstmatched(lambda x: x.mpoint == '/', list_devices).device
 
        """ detect root device rdev_name is like '/dev/sda' """
        for el in os.listdir('/sys/block'):
            if os.path.basename(rdev_partition) in os.listdir('/sys/block/%s'%el):
                rdev_name = '/dev/%s' % el
                break
        if not rdev_name:
            rdev_name = rdev_partition[-1]
        LOG.debug('root dev: `%s`; root part: `%s`', rdev_name, rdev_partition)
 
        """ copy bootloader and MBR from root device to new EBS volume """
        system2(('dd', 'if=%s'%rdev_name, 'of=%s'%self.devname, 'bs=512', 'count=1'))
        system2(('sfdisk', '-R', self.devname))
        wait_until(lambda: os.path.exists('%s1'%self.devname), sleep=0.2,
                 timeout=5, start_text='check refresh partition table on %s'%self.devname,
                 error_text='device `%s` not exist'%self.devname)
        LOG.debug('Copied MBR from %s to %s device', rdev_name, self.devname)
 
        """ list with device, source which will be copying """
        from_devs = [dev for dev in list_devices if dev.device.startswith(rdev_name)]
        LOG.debug('list from_devs `%s`', from_devs)
 
        """ Dict with device(distination) partition table params. """
        to_devs = self._read_pt(self.devname)
        LOG.debug('list to_devs `%s`', to_devs)
 
        """  used for detecting fs type of device, list of device """
        lparts = [line.split() for line in system2(('df', '-hT'),)[0].split('\n') if line.startswith(rdev_name)]
 
        """ make fs on volume's partitions """
        for to_dev in to_devs.keys():
            part_id = int(to_devs[to_dev]['Id'])
 
            """ try detect type_fs with `df -hT` of root device """
            type_fs = None
            for part in lparts:
                """ compaire partition's names by last symbol of root part and distination"""
                if len(part[0]) == len(to_dev) and part[0][-1] == to_dev[-1]:
                    type_fs = part[1]
                    break
 
            """ check partition Id(Hex) and create fs """
            if part_id != 0 and part_id != 82:
                filesystem(type_fs or 'ext3').mkfs(to_dev)
                #storage.Storage.lookup_filesystem(type_fs or 'ext3').mkfs(to_dev)
            elif part_id == 82:
                """ swap partition """
                out, err, ret_code = system2(('mkswap', '-L', 'swap', to_dev),)
                if ret_code:
                    raise HandlerError("Can't create fs on device %s:\n%s" %
                                                    (to_dev, err))
 
 
        """ mounting and copy partitions """
        for from_dev in from_devs:
            num = os.path.basename(from_dev.device)[-1]
 
            if os.path.join(self.mpoint, '%s%s' % (os.path.basename(self.devname), num)) in self._mtab or \
                            os.path.join(self.mpoint, os.path.basename(from_dev.device)) in self._mtab:
                raise HandlerError("Partition already mounted")
 
            """ dev like `sdg1` """
            dev = '%s%s' % (os.path.basename(self.devname), num)
 
            to_mpoint = os.path.join(self.mpoint, dev)
 
            LOG.debug('try mount dev(distination) `/dev/%s` like `%s`', dev, to_mpoint)
            """ mount partition seems like /mnt/img-mnt/sdh1 """
            mount.mount('/dev/%s' % dev, to_mpoint)
 
            if os.path.basename(from_dev.device) != os.path.basename(rdev_partition):
                """ copying not root partition """
                from_mpoint = os.path.join(self.mpoint, os.path.basename(from_dev.device))
                LOG.debug('try mount dev(source) `%s` like `%s`', from_dev.device, from_mpoint)
 
                """ mount source volume partition """
                mount.mount(from_dev.device, from_mpoint)
                """ copy all consitstant"""
                excludes = self.excludes
                self.excludes = tuple()
                self._copy_rec('%s/'%from_mpoint if from_mpoint[-1] != '/' else from_mpoint, to_mpoint)
                LOG.debug('Copied sucesfull from %s to %s', from_mpoint, to_mpoint)
                self.excludes = excludes
            else:
                """ copying root partition """
                old_mpoint = self.mpoint
                self.mpoint = to_mpoint
                self._make_special_dirs()
                self._copy_rec(self._volume, '%s/'%self.mpoint if self.mpoint[-1]!='/' else self.mpoint)
                LOG.debug('Copied sucesfull from %s to %s', self._volume, self.mpoint)
                self.mpoint = old_mpoint
 
        system2("sync", shell=True) #Flush buffers
 
        self.mpoint = os.path.join(self.mpoint, '%s%s'%(os.path.basename(self.devname),
                                os.path.basename(rdev_partition)[-1]))
 
        return self.mpoint
 
 
    def make(self):
        LOG.info("Make EBS volume (size: %sGb) from volume %s (excludes: %s)",
                        self._ebs_config['size'], self._volume, ":".join(self.excludes))
 
        #TODO: need transmit flag `copy_partition_table` from Ec2RebundleHandler.before_rebundle
        """ list of all mounted devices """
        list_device = coreutils.df()
        """ root device partition like `df(device='/dev/sda2', ..., mpoint='/')` """
        root_disk = firstmatched(lambda x: x.mpoint == '/', list_device)
        if not root_disk:
            raise HandlerError("Can't find root device")
        """ detecting root device like rdev=`sda` """
        rdev = None
        for el in os.listdir('/sys/block'):
            if os.path.basename(root_disk.device) in os.listdir('/sys/block/%s'%el):
                rdev = el
                break
        if not rdev and os.path.exists('/sys/block/%s'%os.path.basename(root_disk.device)):
            rdev = root_disk.device
        """ list partition of root device """
        list_rdevparts = [dev.device for dev in list_device
                                                        if dev.device.startswith('/dev/%s' % rdev)]
        """ if one partition we use old method """
        if len(list(set(list_rdevparts))) > 1:
            self.copy_partition_table = True
 
 
        """ for one partition in root device EBS volume using LinuxImage.make(self)
                else copy partitions of root device """
        if self.copy_partition_table:
            self.make_partitions()
        else:
            rebundle_hdlr.LinuxImage.make(self)
 
 
    def umount(self):
        if self.copy_partition_table:
            """ self.mpoint like `/mnt/img-mnt/sda2' root partition copy
                    finding all new mounted partitions in `/mnt/img-mnt`... """
            mpt = '/'+ '/'.join(filter(None, self.mpoint.split('/'))[:-1])
            mparts = [dev.mpoint for dev in coreutils.df() if dev.mpoint.startswith(mpt)]
            LOG.debug('Partitions which will be unmounting: %s' % mparts)
            for mpt in mparts:
                if mpt in self._mtab:
                    LOG.debug("Unmounting '%s'", mpt)
                    system2("umount -d " + mpt, shell=True, raise_exc=False)
        else:
            rebundle_hdlr.LinuxImage.umount(self)
 
 
    def cleanup(self):
        self.umount()
        mp = None
        if self.copy_partition_table:
            """ self.mountpoint like /mnt/img-mnt/sdg2 """
            mp = '/'+ '/'.join(filter(None, self.mpoint.split('/'))[:-1])
            """ removing all directories inside mountpoint"""
            if os.path.exists(mp):
                for el in os.listdir(mp):
                    os.rmdir(os.path.join(mp, el))
 
        if os.path.exists(mp or self.mpoint):
            LOG.debug('Remove dirrectory: `%s`' % (mp or self.mpoint))
            os.rmdir(mp or self.mpoint)
 
        if self.ebs_volume:
            self.ebs_volume.destroy()
            self.ebs_volume = None
 
 
class AmiManifest:
 
    VERSION = "2007-10-10"
 
    name = None
    user = None
    arch = None
    parts = None
    image_size = None
    bundled_size = None
    bundler_name = None,
    bundler_version = None,
    bundler_release = None,
    user_encrypted_key = None
    ec2_encrypted_key = None
    user_encrypted_iv = None
    ec2_encrypted_iv = None
    image_digest = None
    digest_algo = None
    crypto_algo = None
    user_private_key = None
    kernel_id = None
    ramdisk_id = None
    product_codes = None
    ancestor_ami_ids = None
    block_device_mapping = None
 
 
    def __init__(self, name=None, user=None, arch=None,
                            parts=None, image_size=None, bundled_size=None, user_encrypted_key=None,
                            ec2_encrypted_key=None, user_encrypted_iv=None, ec2_encrypted_iv=None,
                            image_digest=None, digest_algo=DIGEST_ALGO, crypto_algo=CRYPTO_ALGO,
                            bundler_name=BUNDLER_NAME, bundler_version=BUNDLER_VERSION, bundler_release=BUNDLER_RELEASE,
                            user_private_key=None, kernel_id=None, ramdisk_id=None, product_codes=None,
                            ancestor_ami_ids=None, block_device_mapping=None):
        for key, value in locals().items():
            if key != "self" and hasattr(self, key):
                setattr(self, key, value)
 
 
    def save(self, filename):
        LOG.info("Generating manifest file '%s'", filename)
 
        out_file = open(filename, "wb")
        doc = Document()
 
        def el(name):
            return doc.createElement(name)
        def txt(text):
            return doc.createTextNode('%s' % (text))
        def ap(parent, child):
            parent.appendChild(child)
 
        manifest_elem = el("manifest")
        ap(doc, manifest_elem)
 
        #version
        # /manifest/version
        version_elem = el("version")
        version_value = txt(self.VERSION)
        ap(version_elem, version_value)
        ap(manifest_elem, version_elem)
 
        #bundler info
        # /manifest/bundler
        bundler_elem = el("bundler")
 
        bundler_name_elem = el("name")
        bundler_name_value = txt(self.bundler_name)
        ap(bundler_name_elem, bundler_name_value)
        ap(bundler_elem, bundler_name_elem)
 
        bundler_version_elem = el("version")
        bundler_version_value = txt(self.bundler_version)
        ap(bundler_version_elem, bundler_version_value)
        ap(bundler_elem, bundler_version_elem)
 
        release_elem = el("release")
        release_value = txt(self.bundler_release)
        ap(release_elem, release_value)
        ap(bundler_elem, release_elem)
 
        ap(manifest_elem, bundler_elem)
 
 
        #machine config
        # /manifest/machine_configuration
        machine_config_elem = el("machine_configuration")
        ap(manifest_elem, machine_config_elem)
 
        arch_elem = el("architecture")
        arch_value = txt(self.arch)
        ap(arch_elem, arch_value)
        ap(machine_config_elem, arch_elem)
 
 
        #block device mapping
        # /manifest/machine_configuration/block_device_mapping
        if self.block_device_mapping:
            block_dev_mapping_elem = el("block_device_mapping")
            for virtual, device in self.block_device_mapping:
                mapping_elem = el("mapping")
 
                virtual_elem = el("virtual")
                virtual_value = txt(virtual)
                ap(virtual_elem, virtual_value)
                ap(mapping_elem, virtual_elem)
 
                device_elem = el("device")
                device_value = txt(device)
                ap(device_elem, device_value)
                ap(mapping_elem, device_elem)
 
                ap(block_dev_mapping_elem, mapping_elem)
 
            ap(machine_config_elem, block_dev_mapping_elem)
 
        # /manifest/machine_configuration/product_codes
        if self.product_codes:
            product_codes_elem = el("product_codes")
            for product_code in self.product_codes:
                product_code_elem = el("product_code");
                product_code_value = txt(product_code)
                ap(product_code_elem, product_code_value)
                ap(product_codes_elem, product_code_elem)
            ap(machine_config_elem, product_codes_elem)
 
 
        #kernel and ramdisk
        # /manifest/machine_configuration/kernel_id
        if self.kernel_id:
            kernel_id_elem = el("kernel_id")
            kernel_id_value = txt(self.kernel_id)
            ap(kernel_id_elem, kernel_id_value)
            ap(machine_config_elem, kernel_id_elem)
 
        # /manifest/machine_configuration/ramdisk_id
        if self.ramdisk_id:
            ramdisk_id_elem = el("ramdisk_id")
            ramdisk_id_value = txt(self.ramdisk_id)
            ap(ramdisk_id_elem, ramdisk_id_value)
            ap(machine_config_elem, ramdisk_id_elem)
 
 
        # /manifest/image
        image_elem = el("image")
        ap(manifest_elem, image_elem)
 
        #name
        # /manifest/image/name
        image_name_elem = el("name")
        image_name_value = txt(self.name)
        ap(image_name_elem, image_name_value)
        ap(image_elem, image_name_elem)
 
        #user
        # /manifest/image/user
        user_elem = el("user")
        user_value = txt(self.user)
        ap(user_elem, user_value)
        ap(image_elem, user_elem)
 
        #type
        # /manifest/image/type
        image_type_elem = el("type")
        image_type_value = txt("machine")
        ap(image_type_elem, image_type_value)
        ap(image_elem, image_type_elem)
 
 
        #ancestor ami ids
        # /manifest/image/ancestry
        if self.ancestor_ami_ids:
            ancestry_elem = el("ancestry")
            for ancestor_ami_id in self.ancestor_ami_ids:
                ancestor_id_elem = el("ancestor_ami_id");
                ancestor_id_value = txt(ancestor_ami_id)
                ap(ancestor_id_elem, ancestor_id_value)
                ap(ancestry_elem, ancestor_id_elem)
            ap(image_elem, ancestry_elem)
 
        #digest
        # /manifest/image/digest
        image_digest_elem = el("digest")
        image_digest_elem.setAttribute('algorithm', self.digest_algo.upper())
        image_digest_value = txt(self.image_digest)
        ap(image_digest_elem, image_digest_value)
        ap(image_elem, image_digest_elem)
 
        #size
        # /manifest/image/size
        image_size_elem = el("size")
        image_size_value = txt(self.image_size)
        ap(image_size_elem, image_size_value)
        ap(image_elem, image_size_elem)
 
        #bundled size
        # /manifest/image/bundled_size
        bundled_size_elem = el("bundled_size")
        bundled_size_value = txt(self.bundled_size)
        ap(bundled_size_elem, bundled_size_value)
        ap(image_elem, bundled_size_elem)
 
        #key, iv
        # /manifest/image/ec2_encrypted_key
        ec2_encrypted_key_elem = el("ec2_encrypted_key")
        ec2_encrypted_key_value = txt(self.ec2_encrypted_key)
        ec2_encrypted_key_elem.setAttribute("algorithm", self.crypto_algo.upper())
        ap(ec2_encrypted_key_elem, ec2_encrypted_key_value)
        ap(image_elem, ec2_encrypted_key_elem)
 
        # /manifest/image/user_encrypted_key
        user_encrypted_key_elem = el("user_encrypted_key")
        user_encrypted_key_value = txt(self.user_encrypted_key)
        user_encrypted_key_elem.setAttribute("algorithm", self.crypto_algo.upper())
        ap(user_encrypted_key_elem, user_encrypted_key_value)
        ap(image_elem, user_encrypted_key_elem)
 
        # /manifest/image/ec2_encrypted_iv
        ec2_encrypted_iv_elem = el("ec2_encrypted_iv")
        ec2_encrypted_iv_value = txt(self.ec2_encrypted_iv)
        ap(ec2_encrypted_iv_elem, ec2_encrypted_iv_value)
        ap(image_elem, ec2_encrypted_iv_elem)
 
        # /manifest/image/user_encrypted_iv
        user_encrypted_iv_elem = el("user_encrypted_iv")
        user_encrypted_iv_value = txt(self.user_encrypted_iv)
        ap(user_encrypted_iv_elem, user_encrypted_iv_value)
        ap(image_elem, user_encrypted_iv_elem)
 
 
        #parts
        # /manifest/image/parts
        parts_elem = el("parts")
        parts_elem.setAttribute("count", str(len(self.parts)))
        part_number = 0
        for part in self.parts:
            part_elem = el("part")
            filename_elem = el("filename")
            filename_value = txt(part[0])
            ap(filename_elem, filename_value)
            ap(part_elem, filename_elem)
 
            #digest
            part_digest_elem = el("digest")
            part_digest_elem.setAttribute('algorithm', self.digest_algo.upper())
            part_digest_value = txt(part[1])
            ap(part_digest_elem, part_digest_value)
            ap(part_elem, part_digest_elem)
            part_elem.setAttribute("index", str(part_number))
 
            ap(parts_elem, part_elem)
            part_number += 1
        ap(image_elem, parts_elem)
 
 
        # Get the XML for <machine_configuration> and <image> elements and sign them.
        string_to_sign = machine_config_elem.toxml() + image_elem.toxml()
 
        digest = EVP.MessageDigest(self.digest_algo.lower())
        digest.update(string_to_sign)
        sig = hexlify(self.user_private_key.sign(digest.final()))
        del digest
 
        # /manifest/signature
        signature_elem = el("signature")
        signature_value = txt(sig)
        ap(signature_elem, signature_value)
        ap(manifest_elem, signature_elem)
 
        out_file.write(doc.toxml())
        out_file.close()
 
    def load(self, filename):
        # TODO: implement
        pass
 
    def startElement(self, name, attrs):
        pass
 
    def characters(self, value):
        pass
 
    def endElement(self, name):
        pass
 
 
class Ec2RebundleWindowsHandler(Handler):
    logger = None
 
    def __init__(self):
        self._logger = logging.getLogger(__name__)
 
    def accept(self, message, queue, behaviour=None, platform=None, os=None, dist=None):
        return message.name == Messages.WIN_PREPARE_BUNDLE
 
    def on_Win_PrepareBundle(self, message):
        try:
            """
            cmd = ('ec2config.exe -sysprep')
            sysprep = Popen(cmd)
            ret = sysprep.wait()
            if ret:
                raise HandlerError('Ec2config finished with return code %s' % ret)
            """
            ec2config_cnf_path = 'C:\\Program Files\\Amazon\\Ec2ConfigService\\Settings\\config.xml'
            if os.path.exists(ec2config_cnf_path):
                ec2_conf = Configuration('xml')
                ec2_conf.read(ec2config_cnf_path)
                i = 1
                while True:
                    try:
                        opt_name = ec2_conf.get('./Ec2ConfigurationSettings/Plugins/Plugin[%s]/Name' % i)
                        if opt_name in ('Ec2SetPassword', 'Ec2SetComputerName'):
                            ec2_conf.set('./Ec2ConfigurationSettings/Plugins/Plugin[%s]/State' % i, 'Enabled')
                        i += 1
                    except:
                        break
 
                ec2_conf.write(ec2config_cnf_path)
 
            os_info = {}
            uname = disttool.uname()
            os_info['version'] = uname[2]
            os_info['string_version'] = ' '.join(uname).strip()
 
            self.send_message(Messages.WIN_PREPARE_BUNDLE_RESULT, dict(
                status = "ok",
                bundle_task_id = message.bundle_task_id,
                os = os_info
            ))
 
        except (Exception, BaseException), e:
            self._logger.exception(e)
            last_error = hasattr(e, "error_message") and e.error_message or str(e)
            self.send_message(Messages.WIN_PREPARE_BUNDLE_RESULT, dict(
                status = "error",
                last_error = last_error,
                bundle_task_id = message.bundle_task_id
            ))
 
