from __future__ import with_statement
'''
Created on Feb 15, 2011
 
@author: spike
'''
from scalarizr.bus import bus
from scalarizr.handlers import Handler, HandlerError
from scalarizr.handlers.rebundle import RebundleLogHandler
from scalarizr.messaging import Messages
from scalarizr.config import ScalarizrState
from scalarizr.util import system2, software, cryptotool, disttool, wait_until
from scalarizr.util.software import which
from scalarizr.linux.coreutils import statvfs
from scalarizr.linux.coreutils import truncate
from scalarizr.linux.rsync import rsync
from scalarizr.linux.tar import Tar
from scalarizr.linux import mount
from datetime import datetime
from tempfile import mkdtemp
import logging
import os
import re
import time
import shutil
import pexpect
import sys
 
# TODO: unrefactored
 
def get_handlers():
    return [NimbulaRebundleHandler()]
    #return [NimbulaSnapshotRebundleHandler()]
 
 
class NimbulaSnapshotRebundleHandler(Handler):
    def __init__(self):
        self._logger    = logging.getLogger(__name__)
        self._log_hdlr  = RebundleLogHandler()
        bus.define_events(
                # Fires before rebundle starts
                # @param role_name
                "before_rebundle",
 
                # Fires after rebundle complete
                # @param role_name
                # @param snapshot_id
                "rebundle",
 
                # Fires on rebundle error
                # @param role_name
                # @param last_error
                "rebundle_error",
 
                # Fires on bundled volume cleanup. Usefull to remove password files, user activity, logs
                # @param image_mpoint
                "rebundle_cleanup_image"
        )
 
    def accept(self, message, queue, behaviour=None, platform=None, os=None, dist=None):
        return message.name == Messages.REBUNDLE
 
    def on_Rebundle(self, message):
        pl = bus.platform
        cnf = bus.cnf
        self._log_hdlr.bundle_task_id = message.bundle_task_id
        self._logger.addHandler(self._log_hdlr)
 
        try:
            role_name = message.role_name.encode("ascii")
            image_name = role_name + "-" + time.strftime("%Y%m%d%H%M%S")
            conn = pl.new_nimbula_connection()
 
            bus.fire("before_rebundle", role_name=role_name)
 
            old_state = cnf.state
            cnf.state = ScalarizrState.REBUNDLING
            try:
                self._logger.info('Creating snapshot (instance: %s)', pl.get_instance_id())
                snap = conn.add_snapshot(pl.get_instance_id(), image_name)
                self._logger.info('Checking that snapshot %s is completed', snap.name)
                wait_until(lambda: snap.update().state in ('complete', 'error'),
                                timeout=600, logger=self._logger,
                                error_text="Snapshot %s wasn't completed in a reasonable time" % snap.name)
                if snap.state == 'error':
                    raise HandlerError("Snapshot creation failed: snapshot status becomes 'error'")
                self._logger.info('Image %s completed and available for use!', snap.machineimage)
            finally:
                cnf.state = old_state
 
            msg_data = dict(
                    status='ok',
                    snapshot_id = snap.machineimage,
                    bundle_task_id = message.bundle_task_id
            )
            self._logger.debug("Updating message with OS and software info")
            msg_data.update(software.system_info())
 
            self.send_message(Messages.REBUNDLE_RESULT, msg_data)
 
            bus.fire("rebundle", role_name=role_name, snapshot_id=snap.machineimage)
 
        except:
            exc = sys.exc_info()
            self._logger.error(exc[1], exc_info=exc)
            self.send_message(Messages.REBUNDLE_RESULT, dict(
                    status = "error",
                    last_error = exc[1],
                    bundle_task_id = message.bundle_task_id
            ))
 
        finally:
            self._log_hdlr.bundle_task_id = None
            self._logger.removeHandler(self._log_hdlr)
 
 
root_uuid = "97e128e3-a209-4a34-81f7-c35fb9053e25"
 
MOTD = '''Scalr image
%(dist_name)s %(dist_version)s %(bits)d-bit
Role: %(role_name)s
Bundled: %(bundle_date)s
'''
 
class NimbulaRebundleHandler(Handler):
 
    loop                    = None
    root_mpoint             = None
    rebundle_dir    = None
    mounted                 = None
    _logger                 = None
 
    def __init__(self):
        self._logger    = logging.getLogger(__name__)
        self._log_hdlr  = RebundleLogHandler()
        self.platform   = bus.platform
        bus.define_events(
                # Fires before rebundle starts
                # @param role_name
                "before_rebundle",
 
                # Fires after rebundle complete
                # @param role_name
                # @param snapshot_id
                "rebundle",
 
                # Fires on rebundle error
                # @param role_name
                # @param last_error
                "rebundle_error",
 
                # Fires on bundled volume cleanup. Usefull to remove password files, user activity, logs
                # @param image_mpoint
                "rebundle_cleanup_image"
        )
 
    def accept(self, message, queue, behaviour=None, platform=None, os=None, dist=None):
        return message.name == Messages.REBUNDLE
 
    def on_Rebundle(self, message):
        self._log_hdlr.bundle_task_id = message.bundle_task_id
        self._logger.addHandler(self._log_hdlr)
        try:
            excludes                                = ('/dev', '/media', '/mnt', '/proc', '/sys', '/cdrom', '/tmp')
            root_size = swap_size   = 0
            self.rebundle_dir               = mkdtemp()
            self.tmp_root_dir               = mkdtemp()
            targz_name                              = message.role_name + '.tar.gz'
            targz_path                              = os.path.join(self.rebundle_dir, targz_name)
            image_name                              = message.role_name + '.img'
            image_path                              = os.path.join(self.rebundle_dir, image_name)
 
            """ Getting root device size """
            self._logger.debug('Getting root device size')
            fs_stats = statvfs('/')
            root_size = fs_stats['size']
            root_free = fs_stats['size'] - fs_stats['used']
            self._logger.debug('Root device size: %s', root_size)
 
            used_space = root_size - root_free
            required_space = used_space * 2.5
            if required_space < root_free:
                raise HandlerError("Not enough free disk space for rebundle." +
                                                        "Free space on device: %sMb, Required at least: %sMb" %
                                                        (root_free/1024, required_space/1024))
 
            """ Getting swap size """
            self._logger.debug('Getting swap size')
            raw_swap_list = system2('swapon -s', shell = True)[0].splitlines()
            if len(raw_swap_list) > 1:
                swap_size = int(raw_swap_list[1].split()[2])
            self._logger.debug('Swap device size: %s', swap_size)
 
            bus.fire("before_rebundle", role_name=message.role_name)
 
            """ Copying root to temp directory """
            self._logger.info("Copying / to temporary dir %s" % self.tmp_root_dir)
            self._rsync('/', self.tmp_root_dir, excludes=excludes)
            self._logger.info('Making special dirs')
            self._make_spec_dirs(self.tmp_root_dir, excludes)
 
            """ Distro-based adaptation"""
            self.adapter.adapt(self)
 
            """ Image creation """
            self._logger.info('Creating image file: %s' % image_path)
            f = open(image_path, 'w')
            f.seek((root_size + swap_size) * 1024 + 63*512 - 1)
            f.write('\0')
            f.close()
 
            """ Partitioning """
            self._logger.info('Partitioning image.')
            start = 62
            part_string = '%s,%s,L,*\n' % (start+1, root_size*2)
            if swap_size:
                start += root_size*2
                part_string += '%s,%s,S\n' % (start+1, swap_size*2)
            out, err, ret_code = system2('sfdisk -uS --force %s' % image_path, stdin=part_string, shell=True, raise_exc=False)
            if ret_code:
                raise HandlerError('Error occured while partitioning image.\n%s' % err)
 
            """ Mapping partitions """
            self._logger.info('Creating device map on image.')
            out, err, ret_code = system2('kpartx -av %s' % image_path, shell=True, raise_exc=False)
            if ret_code:
                raise HandlerError('Error occured while creating device map.\n%s' % err)
            self.loop = re.search('(/dev/loop\d+)', out).group(1)
            root_dev_name = '/dev/mapper/%sp1' % self.loop.split('/')[-1]
 
            """ Create file systems on the partitions """
            self._logger.info('Creating filesystem on root partition.')
            out, err, ret_code = system2('mkfs -t ext3 -L root -m 0 -I 128 %s' % root_dev_name, shell=True)
            if ret_code:
                raise HandlerError("Can't create filesystem on device %s:\n%s" % (root_dev_name, err))
            if swap_size:
                swap_dev_name = '/dev/mapper/%sp2' % self.loop.split('/')[-1]
                self._logger.debug('Creating filesystem on swap partition.')
                out, err, ret_code = system2('mkswap -L swap %s' % swap_dev_name, shell=True)
                if ret_code:
                    raise HandlerError("Can't create filesystem on device %s:\n%s" % (root_dev_name, err))
 
            """ Setting UUID for root device """
            self._logger.debug('Setting predefined UUID to root device.')
            system2('tune2fs -i 0 -U %s %s' % (root_uuid, root_dev_name), shell=True, raise_exc=False)
 
            """ Root device mount """
            self.root_mpoint = os.path.join(self.rebundle_dir, 'mnt')
            self._logger.debug('Mounting root device to %s.' % self.root_mpoint)
            if not os.path.isdir(self.root_mpoint):
                os.mkdir(self.root_mpoint)
            mount.mount(root_dev_name, self.root_mpoint)
            try:
                """ Snapshot copy from temp dir to image """
                cnf = bus.cnf
                old_state = cnf.state
                cnf.state = ScalarizrState.REBUNDLING
                try:
                    self._logger.info('Copying snapshot %s to image %s', self.tmp_root_dir, self.root_mpoint)
                    self._rsync(self.tmp_root_dir + os.sep, self.root_mpoint, excludes=excludes)
                    self._make_spec_dirs(self.root_mpoint, excludes)
                    self._cleanup_image(self.root_mpoint, message.role_name)
                finally:
                    cnf.state = old_state
 
                """ Scalr user creation """
                self._logger.debug('Creating "scalr" user')
                shell = pexpect.spawn('/bin/sh')
                try:
                    shell.expect('#')
                    shell.sendline('chroot %s' % self.root_mpoint)
                    shell.expect('#')
                    c_count = shell.sendline('useradd scalr -g0')
                    shell.expect('#')
                    out = shell.before[c_count].strip()
                    if out and not "already exists" in out:
                        raise HandlerError("Cannot add 'scalr' user: %s" % out)
 
                    self._logger.info('Updating password of "scalr" user.')
                    scalr_password = cryptotool.pwgen(10)
                    shell.sendline('passwd scalr')
                    shell.expect('Enter new UNIX password:')
                    shell.sendline(scalr_password)
                    shell.expect('Retype new UNIX password:')
                    shell.sendline(scalr_password)
                    shell.expect('passwd: password updated successfully')
                except pexpect.TIMEOUT:
                    raise HandlerError('Error occured while creating scalr user. Out: %s' % shell.before)
                finally:
                    shell.close()
            finally:
                self._logger.debug('Unmounting root device.')
                mount.umount(root_dev_name)
 
            """ Unmap image partitions """
            system2('kpartx -d %s' % image_path, shell=True)
            self.loop = self.root_mpoint = None
 
            """ Grub installation """
            self._logger.info('Installing grub to the image %s' % image_name)
            grub_path = which('grub')
            if not grub_path:
                raise HandlerError("Grub executable was not found.")
            grub_path = self.tmp_root_dir + grub_path
            stdin = 'device (hd0) %s\nroot (hd0,0)\nsetup (hd0)\n' % image_path
            system2('%s --batch --no-floppy --device-map=/dev/null' % grub_path, stdin = stdin, shell=True)
 
            """ Image compression """
            self._logger.info('Tarring image %s into %s', image_name, targz_path)
            tar = Tar()
            tar.create().gzip().sparse()
            tar.archive(targz_path)
            tar.add(image_name, self.rebundle_dir)
            system2(str(tar), shell=True)
 
            """ Uploading image """
            self._logger.info('Uploading image %s', targz_path)
            nimbula_conn = self.platform.new_nimbula_connection()
            image = nimbula_conn.add_machine_image(message.role_name, file=targz_path)
 
            """ Creating and sending message """
            msg_data = dict(
                status = "ok",
                bundle_task_id = message.bundle_task_id,
                ssh_user = 'scalr',
                ssh_password = scalr_password,
                snapshot_id = image.name
            )
            self._logger.debug("Updating message with OS and software info")
            msg_data.update(software.system_info())
            self.send_message(Messages.REBUNDLE_RESULT, msg_data)
            bus.fire("rebundle", role_name=message.role_name, snapshot_id=image.name)
 
        except (Exception, BaseException), e:
            self._logger.exception(e)
            last_error = str(e)
 
            """ Send sad message """
            self.send_message(Messages.REBUNDLE_RESULT, dict(
                    status = "error",
                    last_error = last_error,
                    bundle_task_id = message.bundle_task_id
            ))
            bus.fire("rebundle_error", role_name=message.role_name, last_error=last_error)
 
        finally:
            """ Perform cleanup """
            self._log_hdlr.bundle_task_id = None
            self._logger.removeHandler(self._log_hdlr)
            if self.loop:
                system2('kpartx -d %s' % image_path, shell=True)
                self.loop = None
            shutil.rmtree(self.rebundle_dir)
            shutil.rmtree(self.tmp_root_dir)
 
    @property
    def adapter(self):
        if not hasattr(self, '_adapter'):
            self._adapter = RedHatAdapter() if disttool._is_redhat_based else DebianAdapter()
        return self._adapter
 
    def _make_spec_dirs(self, root_path, excludes):
        for dir in excludes:
            spec_dir = root_path + dir
            if os.path.exists(dir) and not os.path.exists(spec_dir):
                self._logger.debug("Create spec dir %s", dir)
                os.makedirs(spec_dir)
                if dir == '/tmp':
                    os.chmod(spec_dir, 01777)
 
    def _cleanup_image(self, image_mpoint, role_name=None):
        # Create message of the day
        self._create_motd(image_mpoint, role_name)
 
        # Truncate logs
        logs_path = os.path.join(image_mpoint, "var/log")
        for basename in os.listdir(logs_path):
            filename = os.path.join(logs_path, basename)
            if os.path.isfile(filename):
                try:
                    truncate(filename)
                except OSError, e:
                    self._logger.error("Cannot truncate file '%s'. %s", filename, e)
 
        # Cleanup user activity
        for filename in ("root/.bash_history", "root/.lesshst", "root/.viminfo",
                                        "root/.mysql_history", "root/.history", "root/.sqlite_history"):
            filename = os.path.join(image_mpoint, filename)
            if os.path.exists(filename):
                os.remove(filename)
 
        # Cleanup scalarizr private data
        etc_path = os.path.join(image_mpoint, bus.etc_path[1:])
        privated = os.path.join(etc_path, "private.d")
        if os.path.exists(privated):
            shutil.rmtree(privated)
            os.mkdir(privated)
            os.chmod(privated, 0775)
 
        bus.fire("rebundle_cleanup_image", image_mpoint=image_mpoint)
 
    def _create_motd(self, image_mpoint, role_name=None):
        # Create message of the day
        for name in ("etc/motd", "etc/motd.tail"):
            motd_filename = os.path.join(image_mpoint, name)
            if os.path.exists(motd_filename):
                dist = disttool.linux_dist()
                motd = MOTD % dict(
                        dist_name = dist[0],
                        dist_version = dist[1],
                        bits = 64 if disttool.uname()[4] == "x86_64" else 32,
                        role_name = role_name,
                        bundle_date = datetime.today().strftime("%Y-%m-%d %H:%M")
                )
                with open(motd_filename, 'w') as fp:
                    fp.write(motd)
 
    def _rsync(self, src, dst, excludes=None, xattr=True):
        rsync_longs = dict(archive=True, sparse=True)
        if xattr:
            rsync_longs['xattributes'] = True
 
        if excludes:
            rsync_longs['exclude'] = excludes
 
        out, err, ret_code = rsync(src, dst, **rsync_longs)
 
        if ret_code == 24:
            self._logger.warn(
                    "rsync exited with error code 24. This means a partial transfer due to vanished " +
                    "source files. In most cases files are copied normally"
            )
        if ret_code == 23:
            self._logger.warn(
                    "rsync seemed successful but exited with error code 23. This probably means " +
            "that your version of rsync was built against a kernel with HAVE_LUTIMES defined, " +
    "although the current kernel was not built with this option enabled. The bundling " +
                    "process will thus ignore the error and continue bundling.  If bundling completes " +
            "successfully, your image should be perfectly usable. We, however, recommend that " +
                    "you install a version of rsync that handles this situation more elegantly.")
        elif ret_code == 1 and xattr:
            self._logger.warn(
                    "rsync with preservation of extended file attributes failed. Retrying rsync " +
            "without attempting to preserve extended file attributes...")
            self._rsync(src, dst, excludes, xattr=False)
        elif ret_code > 0:
            raise HandlerError('rsync failed with exit code %s' % (ret_code,))
 
class RedHatAdapter:
    def adapt(self, handler):
        """ Grub configuration """
        handler._logger.info('Configuring Grub')
        kernels = [file for file in os.listdir(os.path.join(handler.tmp_root_dir, 'boot')) if file.startswith('vmlinuz-')]
        grubdir = os.path.join(handler.tmp_root_dir, 'usr', 'share', 'grub', '%s-redhat' % disttool.arch())
        boot_grub_path = os.path.join(handler.tmp_root_dir, 'boot', 'grub')
        for file in os.listdir(grubdir):
            shutil.copy(os.path.join(grubdir, file), boot_grub_path)
 
        grub_conf_path = os.path.join(handler.tmp_root_dir, 'boot', 'grub', 'grub.conf')
        grub_conf = 'default 0\ntimeout 5\nroot (hd0,0)\n'
        for kernel in kernels:
            grub_conf += 'title %s\n' % kernel
            grub_conf += 'kernel /boot/%s root=UUID=%s ro console=tty1 console=ttyS0\n' % (kernel, root_uuid)
            grub_conf += 'initrd /boot/%s\n' % (kernel.replace('vmlinuz-', 'initrd-') + '.img')
            grub_conf += 'boot\n'
        with open(grub_conf_path, 'w') as fp:
            fp.write(grub_conf)
 
        """ Enable login on serial console """
        handler._logger.info('Enabling login to serial console')
        inittab_path    = os.path.join(handler.tmp_root_dir, 'etc', 'inittab')
        inittab                 = ''
        with open(inittab_path, 'r') as fp:
            inittab = fp.read()
        inittab                 += 'T0:12345:respawn:/sbin/agetty -L ttyS0 38400\n'
        with open(inittab_path, 'w') as fp:
            fp.write(inittab)
 
        securetty_path  = os.path.join(handler.tmp_root_dir, 'etc', 'securetty')
        secure_tty              = ''
        with open(securetty_path, 'r') as fp:
            secure_tty = fp.read()
        secure_tty              += 'ttyS0\n'
        with open(securetty_path, 'w') as fp:
            fp.write(secure_tty)
 
 
class DebianAdapter:
    def adapt(self, handler):
        """ Grub configuration """
        handler._logger.info('Configuring Grub')
 
        boot_grub_path = os.path.join(handler.tmp_root_dir, 'boot', 'grub')
        if not os.path.exists(boot_grub_path):
            os.mkdir(boot_grub_path)
 
        grub_dir = os.path.join(handler.tmp_root_dir, 'usr', 'lib', 'grub', '%s-pc' % disttool.arch())
        for filename in os.listdir(grub_dir):
            shutil.copy(os.path.join(grub_dir, filename), boot_grub_path)
 
        device_map_path = os.path.join(handler.tmp_root_dir, 'boot', 'grub', 'device.map')
        device_map = '(hd0)  /dev/sda\n'
        with open(device_map_path, 'w') as fp:
            fp.write(device_map)
        mount.mount('/dev', handler.tmp_root_dir + '/dev', bind=True)
        try:
            shell = pexpect.spawn('/bin/sh')
            try:
                shell.expect('#')
                shell.sendline('chroot %s' % handler.tmp_root_dir)
                shell.expect('#')
                shell.sendline('update-grub -y')
                shell.expect('#')
                shell.sendline('sed -i s/^# kopt=.*$/# kopt=root=UUID=97e128e3-a209-4a34-81f7-c35fb9053e25 ro console=tty1 console=ttyS0/ /boot/grub/menu.lst')
                shell.expect('#')
                shell.sendline('sed -i s/^# defoptions=.*$/# defoptions=/ /boot/grub/menu.lst')
                shell.expect('#')
                shell.sendline('sed -i s/^# groot=.*$/# groot=(hd0,0)/ /boot/grub/menu.lst')
                shell.expect('#')
                shell.sendline('update-grub')
                shell.expect('#')
            except pexpect.TIMEOUT:
                raise HandlerError('Error occured while updating grub. Out: %s' % shell.before)
            finally:
                shell.close()
        finally:
            mount.umount(handler.tmp_root_dir + '/dev')
 
        """ Enable login on serial console """
        handler._logger.info('Enabling login to serial console')
        inittab_path    = os.path.join(handler.tmp_root_dir, 'etc', 'inittab')
        inittab                 = ''
        with open(inittab_path, 'r') as fp:
            inittab = fp.read()
        inittab                 += 'T0:23:respawn:/sbin/getty -L ttyS0 38400 vt100\n'
        with open(inittab_path, 'w') as fp:
            fp.write(inittab)
 
