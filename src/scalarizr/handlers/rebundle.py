"""
Created on Sep 7, 2011

@author: marat
"""

import os
import logging
import shutil

from scalarizr.bus import bus
from scalarizr.config import ScalarizrState
from scalarizr.handlers import Handler, HandlerError
from scalarizr.messaging import Messages, Queues
from scalarizr.storage2 import filesystem
from scalarizr.storage2.util import loop
from scalarizr.util import system2, software
from scalarizr import linux
from scalarizr.linux import mount, coreutils, rsync



LOG = logging.getLogger(__name__)

WALL_MESSAGE = 'Server is going to rebundle'

MOTD = '''Scalr image
%(dist_name)s %(dist_version)s %(bits)d-bit
Role: %(role_name)s
Bundled: %(bundle_date)s
'''


class StopRebundle(BaseException):
    '''
    Special exception for raising from 'before_rebundle' event listener to stop rebundle process
    '''
    pass



class RebundleLogHandler(logging.Handler):
    def __init__(self, bundle_task_id=None):
        logging.Handler.__init__(self, logging.INFO)
        self.bundle_task_id = bundle_task_id
        self._msg_service = bus.messaging_service

    def emit(self, record):
        msg = self._msg_service.new_message(Messages.REBUNDLE_LOG, body=dict(
                bundle_task_id = self.bundle_task_id,
                message = str(record.msg) % record.args if record.args else str(record.msg)
        ))
        self._msg_service.get_producer().send(Queues.LOG, msg)


def plug_rebundle_log(on_rebundle):
    '''
    RebundleLogHandler pluggin for on_Rebundle
    '''
    def wrapper(self, message):
        try:
            if not hasattr(on_rebundle, '_log_hdlr'):
                on_rebundle._log_hdlr = RebundleLogHandler()
            on_rebundle._log_hdlr.bundle_task_id = message.bundle_task_id
            LOG.addHandler(on_rebundle._log_hdlr)

            on_rebundle(self, message)
        finally:
            LOG.removeHandler(on_rebundle._log_hdlr)
    return wrapper


class PrepHandler(Handler):

    def accept(self, message, queue, **kwds):
        return message.name == Messages.REBUNDLE


    @plug_rebundle_log
    def on_Rebundle(self, message):
        pass



class RebundleHandler(Handler):

    def __init__(self):
        self._rebundle_message = self._role_name = self._excludes = None
        bus.define_events(
                # Fires before rebundle starts
                "before_rebundle",

                # Fires after rebundle complete
                # @param param:
                "rebundle",

                # Fires on rebundle error
                # @param role_name
                "rebundle_error",

                # Fires on bundled volume cleanup. Usefull to remove password files, user activity, logs
                # @param rootdir
                "rebundle_cleanup_image"
        )


    def accept(self, message, queue, **kwds):
        return message.name == Messages.REBUNDLE


    @plug_rebundle_log
    def on_Rebundle(self, message):
        try:
            self._role_name = message.role_name.encode("ascii")
            self._excludes = message.excludes.encode("ascii").split(":") \
                            if message.body.has_key("excludes") and message.excludes else []
            self._rebundle_message = message

            # Preparing...
            self.before_rebundle()
            bus.fire("before_rebundle", role_name=self._role_name)

            # Send wall message before rebundling. So console users can run away
            if not system2(('which', 'wall'), raise_exc=False)[2]:
                system2(('wall'), stdin=WALL_MESSAGE, raise_exc=False)

            # Do actual rebundle work
            cnf = bus.cnf
            saved_state = cnf.state
            try:
                cnf.state = ScalarizrState.REBUNDLING
                image_id = self.rebundle()
            finally:
                cnf.state = saved_state

            # Creating message
            result = dict(
                    status = "ok",
                    snapshot_id = image_id,
                    bundle_task_id = message.bundle_task_id
            )

            # Updating message with OS, software and modules info
            result.update(software.system_info())

            # Fire 'rebundle'diss
            bus.fire("rebundle", role_name=self._role_name, snapshot_id=image_id, rebundle_result=result)

            # Notify Scalr
            self.send_message(Messages.REBUNDLE_RESULT, result)

            LOG.info('Rebundle complete! If you imported this server to Scalr, '
                            'you can terminate Scalarizr now.')

        except (Exception, BaseException), e:
            LOG.exception(e)
            last_error = hasattr(e, "error_message") and e.error_message or str(e)

            # Send message to Scalr
            self.send_message(Messages.REBUNDLE_RESULT, dict(
                    status = "error",
                    last_error = last_error,
                    bundle_task_id = message.bundle_task_id
            ))

            # Fire 'rebundle_error'
            bus.fire("rebundle_error", role_name=self._role_name, last_error=last_error)

        finally:
            self.after_rebundle()
            self._rebundle_message = self._role_name = self._excludes = None


    def cleanup_image(self, rootdir):
        LOG.info('Performing image cleanup')
        # Truncate logs
        LOG.debug('Cleanuping image')

        LOG.debug('Truncating log files')
        logs_path = os.path.join(rootdir, 'var/log')
        if os.path.exists(logs_path):
            for basename in os.listdir(logs_path):
                filename = os.path.join(logs_path, basename)
                if os.path.isfile(filename):
                    try:
                        coreutils.truncate(filename)
                    except OSError, e:
                        self._logger.error("Cannot truncate file '%s'. %s", filename, e)
            shutil.rmtree(os.path.join(logs_path, 'scalarizr/scripting'))

        # Cleanup users homes
        LOG.debug('Removing users activity')
        for homedir in ('root', 'home/ubuntu', 'home/scalr'):
            homedir = os.path.join(rootdir, homedir)
            self._cleanup_user_activity(homedir)
            self._cleanup_ssh_keys(homedir)

        # Cleanup scalarizr private data
        LOG.debug('Removing scalarizr private data')
        etc_path = os.path.join(rootdir, bus.etc_path[1:])
        privated = os.path.join(etc_path, "private.d")
        if os.path.exists(privated):
            shutil.rmtree(privated)
            os.mkdir(privated)

        bus.fire("rebundle_cleanup_image", rootdir=rootdir)

        # Sync filesystem buffers
        system2('sync')

        LOG.debug('Cleanup completed')


    def _cleanup_user_activity(self, homedir):
        for name in (".bash_history", ".lesshst", ".viminfo",
                                ".mysql_history", ".history", ".sqlite_history"):
            filename = os.path.join(homedir, name)
            if os.path.exists(filename):
                os.remove(filename)


    def _cleanup_ssh_keys(self, homedir):
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


    def before_rebundle(self):
        LOG.debug('Called before_rebundle')


    def rebundle(self):
        LOG.debug('Called rebundle')


    def after_rebundle(self):
        LOG.debug('Called after_rebundle')



class LinuxImage:
    SPECIAL_DIRS = ('/dev', '/media', '/mnt', '/proc', '/sys', '/cdrom', '/tmp')

    _volume = None

    path = None
    """
    Image file
    """

    devname = None
    """
    Image device name
    Returned by _create_image def
    """

    mpoint = None
    """
    Image mount point
    """

    excludes = None
    """
    Directories excludes list
    """

    _excluded_mpoints = None

    _mtab = None

    def __init__(self, volume, path=None, excludes=None):
        self._mtab = mount.mounts()
        self._volume = volume
        self.mpoint = '/mnt/img-mnt'
        self.path = path

        # Create rsync excludes list
        self.excludes = set(self.SPECIAL_DIRS)  # Add special dirs
        self.excludes.update(excludes or ())  # Add user input
        self.excludes.add(self.mpoint)  # Add image mount point
        if self.path:
            self.excludes.add(self.path)  # Add image path
        # Add all mounted filesystems, except bundle volume
        self._excluded_mpoints = list(entry.mpoint
                        for entry in self._mtab.list_entries()
                        if entry.mpoint.startswith(self._volume) and entry.mpoint != self._volume)
        self.excludes.update(self._excluded_mpoints)


    def make(self):
        self.devname = self._create_image()
        self._format_image()
        system2("sync", shell=True)  # Flush so newly formatted filesystem is ready to mount.
        self._mount_image()
        self._make_special_dirs()
        self._copy_rec(self._volume, self.mpoint)
        system2("sync", shell=True)  # Flush buffers
        return self.mpoint


    def cleanup(self):
        self.umount()
        if os.path.exists(self.mpoint):
            os.rmdir(self.mpoint)

    def umount(self):
        if self.mpoint in self._mtab:
            LOG.debug("Unmounting '%s'", self.mpoint)
            system2("umount -d " + self.mpoint, shell=True, raise_exc=False)

    def _format_image(self):
        LOG.info("Formatting image")

        vol_entry = [v for v in self._mtab
                                        if v.device.startswith('/dev')][0]
        if vol_entry.device == '/dev/root' and not os.path.exists(vol_entry.device):
            vol_entry = [v for v in mount.mounts('/etc/mtab')
                            if v.device.startswith('/dev')][0]
        fs = filesystem(vol_entry.fstype)

        # create filesystem
        fs.mkfs(self.devname)

        # set EXT3/4 options
        if fs.type.startswith('ext'):
            # max mounts before check (-1 = disable)
            system2(('/sbin/tune2fs', '-c', '1', self.devname))
            # time based (3m = 3 month)
            system2(('/sbin/tune2fs', '-i', '3m', self.devname))

        # set label
        label = fs.get_label(vol_entry.device)
        if label:
            fs.set_label(self.devname, label)

        LOG.debug('Image %s formatted', self.devname)

    def _create_image(self):
        pass


    def _mount_image(self, options=None):
        LOG.info("Mounting image")
        if self.mpoint in  self._mtab:
            raise HandlerError("Image already mounted")
        options = options or []
        mount.mount(self.devname, self.mpoint, *options)


    def _make_special_dirs(self):
        LOG.info('Making special directories')

        # Create empty special dirs
        for dirname in self.SPECIAL_DIRS:
            spec_dir = self.mpoint + dirname
            if os.path.exists(dirname) and not os.path.exists(spec_dir):
                LOG.debug("Create spec dir %s", dirname)
                os.makedirs(spec_dir)
                if dirname == '/tmp':
                    os.chmod(spec_dir, 01777)

        # Create excluded mpoints dirs (not under special dirs)
        for dirname in self._excluded_mpoints:
            if not list(dirname for spec_dir in self.SPECIAL_DIRS if dirname.startswith(spec_dir)):
                if not os.path.exists(self.mpoint + dirname):
                    LOG.debug('Create mpoint dir %s', dirname)
                    os.makedirs(self.mpoint + dirname)

        # MAKEDEV is incredibly variable across distros, so use mknod directly.
        devdir = os.path.join(self.mpoint, 'dev')
        mknod = '/bin/mknod'
        nods = (
                'console c 5 1',
                'full c 1 7',
                'null c 1 3',
                'zero c 1 5',
                'tty c 5 0',
                'tty0 c 4 0',
                'tty1 c 4 1',
                'tty2 c 4 2',
                'tty3 c 4 3',
                'tty4 c 4 4',
                'tty5 c 4 5',
                'xvc0 c 204 191'
        )
        for nod in nods:
            nod = nod.split(' ')
            nod[0] = devdir + '/' + nod[0]
            system2([mknod] + nod)

        LOG.debug("Special directories were created")


    def _copy_rec(self, source, dest, xattr=True):
        LOG.info("Copying %s into the image %s", source, dest)
        rsync_longs = dict(archive=True,
                                           sparse=True,
                                           times=True)
        if self.excludes:
            rsync_longs['exclude'] = list(self.excludes)
        #rsync = filetool.Rsync()
        #rsync.archive().times().sparse().links().quietly()
        #rsync.archive().sparse().xattributes()
        #rsync.archive().sparse().times()

        if xattr:
            rsync_longs['xattrs'] = True
        try:
            rsync.rsync(source, dest, **rsync_longs)
        except linux.LinuxError, e:
            if e.returncode == 24:
                LOG.warn(
                        "rsync exited with error code 24. This means a partial transfer due to vanished " +
                        "source files. In most cases files are copied normally"
                )
            elif e.returncode == 23:
                LOG.warn(
                        "rsync seemed successful but exited with error code 23. This probably means " +
                "that your version of rsync was built against a kernel with HAVE_LUTIMES defined, " +
        "although the current kernel was not built with this option enabled. The bundling " +
                        "process will thus ignore the error and continue bundling.  If bundling completes " +
                "successfully, your image should be perfectly usable. We, however, recommend that " +
                        "you install a version of rsync that handles this situation more elegantly.")
            elif e.returncode == 1 and xattr:
                LOG.warn(
                        "rsync with preservation of extended file attributes failed. Retrying rsync " +
                "without attempting to preserve extended file attributes...")
                self._copy_rec(source, dest, xattr=False)
            else:
                raise


class LinuxLoopbackImage(LinuxImage):
    """
    This class encapsulate functionality to create an file loopback image
    from a volume. The image is created using dd. Sub-directories of the
    volume, including mounts of local filesystems, are copied to the image.
    Symbolic links are preserved.
    """

    MAX_IMAGE_SIZE = 10*1024
    _size = None

    def __init__(self, volume, image_file, image_size, excludes=None):
        '''
        @param volume: Path to mounted volume to create the bundle from. Ex: '/'
        @param image_file:  Destination file to store the bundled image. Ex: /mnt/img
        @param image_size: Image file size in Mb. Ex: 1408 (1Gb)
        @param excludes: list of directories and files to exclude. Ex: /mnt, /root/.*
        '''
        LinuxImage.__init__(self, volume, image_file, excludes)
        self._size = image_size or self.MAX_IMAGE_SIZE

    def make(self):
        LOG.info("Make image %s from volume %s (excludes: %s)",
                        self.path, self._volume, ":".join(self.excludes))
        LinuxImage.make(self)

    def _create_image(self):
        LOG.debug('Creating loop device (file: %s, size: %s)', self.path, self._size)
        devname = loop.mkloop(self.path, size=self._size, quick=True)
        LOG.debug('Created loop device %s associated with file %s', devname, self.path)
        return devname

    def cleanup(self):
        LinuxImage.cleanup(self)
        if self.devname and os.path.exists(self.devname):
            loop.rmloop(self.devname)
