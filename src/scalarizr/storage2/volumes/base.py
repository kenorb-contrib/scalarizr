from __future__ import with_statement
import os
import sys
import uuid
import string
import glob

from scalarizr.bus import bus
from scalarizr import storage2, linux
from scalarizr.libs import bases
from scalarizr.linux import coreutils, mount as mod_mount


LOG = storage2.LOG


class Base(bases.ConfigDriven):

    def __init__(self,
                            version='2.0',
                            type='base',
                            id=None,
                            tags=None,
                            **kwds):
        super(Base, self).__init__(
                        version=version, type=type,
                        id=id, tags=tags or {}, **kwds)
        self.error_messages.update({
                'restore_unsupported': 'Restores from snapshot not supported '
                                                                'by this volume type: %s',
        })


    def _genid(self, prefix=''):
        return '%s%s-%s' % (prefix, self.type, uuid.uuid4().hex[0:8])


class Volume(Base):
    """
    Base class for all volume types
    """
    MAX_SIZE = None

    def __init__(self,
                            device=None,
                            fstype='ext3',
                            mpoint=None,
                            snap=None,
                            recreate_if_missing=False,
                            template=None,
                            **kwds):

        # Get rid of fscreated flag
        kwds.pop('fscreated', None)

        #Backwards compatibility with block_device handler
        from_template_if_missing = kwds.pop('from_template_if_missing', False)

        recreate_if_missing = recreate_if_missing or from_template_if_missing

        super(Volume, self).__init__(
                        device=device,
                        fstype=fstype,
                        mpoint=mpoint,
                        snap=snap,
                        recreate_if_missing=recreate_if_missing,
                        template=template,
                        **kwds)
        self.features.update({'restore': True, 'grow': False, 'detach': True})


    def ensure(self, mount=False, mkfs=False, fstab=True, **updates):
        """
        Make sure that volume is attached and ready for use.

        :param mount: if set, volume eventually will be mounted to it's mpoint
        :param mkfs: if set, volume will have corresponding fs eventually
        :return:
        """
        if not self.features['restore']:
            self._check_restore_unsupported()
        if self.snap and isinstance(self.snap, Snapshot):
            self.snap = self.snap.config()
        try:
            self._ensure()
        except storage2.VolumeNotExistsError, e:
            LOG.debug('recreate_if_missing: %s', self.recreate_if_missing)
            if self.recreate_if_missing:
                LOG.warning(e)
                LOG.info('Volume %s not exists, re-creating %s from template', self.id, self.type)
                template = dict(self.template or self.clone())
                LOG.debug('Template: %s', template)
                vol = storage2.volume(**template)
                vol.ensure(mount=bool(vol.mpoint), mkfs=True)
                self._config = vol.config()
            else:
                raise
        self._check_attr('device')
        if not self.id:
            self.id = self._genid('vol-')
        if mount:
            if not self.is_fs_created() and mkfs:
                LOG.debug('Creating %s filesystem: %s', self.fstype, self.id)
                self.mkfs()
            in_fstab = os.path.realpath(self.device) in mod_mount.fstab()
            if not in_fstab:
                self.mount()
            if fstab and not in_fstab:
                LOG.debug('Adding to fstab: %s', self.id)
                fsoptions = ['defaults']
                if linux.os.ubuntu:
                    fsoptions.append('nobootwait')
                else:
                    fsoptions.append('nofail')
                fsoptions.append('comment=scalr')
                mod_mount.fstab().add(self.device, self.mpoint, self.fstype, ','.join(fsoptions))
        return self.config()


    def snapshot(self, description=None, tags=None, **kwds):

        return self._snapshot(description, tags, **kwds)


    def destroy(self, force=False, **kwds):
        LOG.debug('Destroying volume %s', self.id)
        if self.device:
            self.detach(force, **kwds)
        self._destroy(force, **kwds)
        LOG.debug('Volume %s destroyed', self.id)


    def detach(self, force=False, **kwds):
        LOG.debug('Detaching volume %s', self.id)
        if not self.device:
            LOG.debug('Volume %s has no device, nothing to detach', self.id)
            return
        self.umount()
        fstab = mod_mount.fstab()
        if self.device in fstab:
            fstab.remove(self.device)
        self._detach(force, **kwds)
        if self.features['detach']:
            self.device = None
        LOG.debug('Volume %s detached', self.id)


    def mount(self):
        self._check(mpoint=True)
        mounted_to = self.mounted_to()
        if mounted_to == self.mpoint:
            return
        elif mounted_to:
            LOG.debug('Umounting %s from %s', self.id, mounted_to)
            self.umount()
        if not os.path.exists(self.mpoint):
            os.makedirs(self.mpoint)
        LOG.debug('Mounting %s to %s', self.id, self.mpoint)
        mod_mount.mount(self.device, self.mpoint)
        bus.fire("block_device_mounted", volume=self)


    def umount(self):
        try:
            self._check(fstype=False, device=True)
        except:
            return

        mod_mount.umount(self.device)


    def mounted_to(self):
        try:
            self._check(fstype=False, device=True)
        except:
            return False

        try:
            # mounts() resolve symlinks in MountEntry (e.g. /dev/group/lvol becomes /dev/md-N)
            # we need to do the same to prevent KeyError for mounted device
            device = os.path.realpath(self.device)
            return mod_mount.mounts()[device].mpoint
        except KeyError:
            return False


    def is_fs_created(self):
        self._check()
        fstype = coreutils.blkid(self.device).get('type')

        if fstype is None:
            return False
        else:
            self.fstype = fstype
            return True


    def mkfs(self, force=False):
        self._check()
        if not force and self.is_fs_created():
            raise storage2.OperationError(
                                            'Filesystem on device %s is already created' % self.device)

        fs = storage2.filesystem(self.fstype)
        LOG.info('Creating filesystem on %s', self.device)
        fs.mkfs(self.device)


    def clone(self):
        config = self._config.copy()
        config.pop('id', None)
        config.pop('fscreated', None)
        config.pop('device', None)
        self._clone(config)
        return storage2.volume(config)


    def grow(self, **growth):
        """
        Grow (and/or alternate, e.g.: change ebs type to io1) volume and fs.
        Method creates clone of current volume, increases it's size and
        attaches it to the same place. In case of error, old volume attaches back.

        Old volume detached, but not destroyed.

        :param growth: Volume type-dependent rules for volume growth
        :type growth: dict
        :param resize_fs: Resize fs on device after it's growth or not
        :type resize_fs: bool
        :return: New, bigger (or altered) volume instance
        :rtype: Volume
        """

        if not self.features.get('grow'):
            raise storage2.StorageError("%s volume type does not'"
                                                                    " support grow." % self.type)

        # No id, no growth
        if not self.id:
            raise storage2.StorageError('Failed to grow volume: '
                                                            'volume has no id.')

        # Resize_fs is true by default
        resize_fs = growth.pop('resize_fs', True)

        self.check_growth(**growth)
        was_mounted = self.mounted_to() if self.device else False

        new_vol = None
        try:
            LOG.info("Marking current volume archived")
            self.apply_tags({"scalr-status": "archived"})
            LOG.info('Detaching volume %s', self.id)
            self.detach()
            new_vol = self.clone()
            self._grow(new_vol, **growth)
            if resize_fs:
                fs_created = new_vol.detect_fstype()

                if self.fstype:
                    LOG.info('Resizing filesystem')
                    fs = storage2.filesystem(fstype=self.fstype)
                    umount_on_resize = fs.features.get('umount_on_resize')

                    if fs_created:
                        if umount_on_resize:
                            if new_vol.mounted_to():
                                new_vol.umount()
                            fs.resize(new_vol.device)
                            if was_mounted:
                                new_vol.mount()
                        else:
                            new_vol.mount()
                            fs.resize(new_vol.device)
                            if not was_mounted:
                                new_vol.umount()

        except:
            err_type, err_val, trace = sys.exc_info()
            LOG.warn('Failed to grow volume: %s. Trying to attach old volume', err_val)
            try:
                if new_vol:
                    try:
                        new_vol.destroy(force=True, remove_disks=True)
                    except:
                        destr_err = sys.exc_info()[1]
                        LOG.error('Enlarged volume destruction failed: %s' % destr_err)

                self.ensure(mount=bool(was_mounted))
                LOG.info("Marking old volume active again.")
                self.apply_tags({"scalr-status": "active"})
            except:
                e = sys.exc_info()[1]
                err_val = str(err_val) + '\nFailed to restore old volume: %s' % e

            err_val = 'Volume growth failed: %s' % err_val
            raise storage2.StorageError, err_val, trace
        else:
            LOG.info("The process of growing the storage volume completed.")

        return new_vol


    def _grow(self, bigger_vol, **growth):
        """
        Create, attach and do everything except mount.
        All cleanup procedures and artifact removal should be
        performed in this method

        :param growth: Type-dependant config for volume growth
        :type growth: dict
        :rtype: Volume
        """
        pass


    def _check(self, fstype=True, device=True, **kwds):
        if fstype:
            self._check_attr('fstype')
        if device:
            self._check_attr('device')
        for name in kwds:
            self._check_attr(name)


    def _check_attr(self, name):
        assert hasattr(self, name) and getattr(self, name) is not None,  \
                        self.error_messages['empty_attr'] % name


    def _check_restore_unsupported(self):
        if self.snap:
            msg = self.error_messages['restore_unsupported'] % self.type
            #FIXME: eph volume in NewMasterUp raises error here
            LOG.debug(msg)
            LOG.debug('Some details: features=%s, config=%s', self.features, self.config())
            #raise NotImplementedError(msg)


    def check_growth(self, **growth):
        pass


    def detect_fstype(self):
        self._check_attr('device')
        blk_info = coreutils.blkid(self.device)
        return blk_info.get('type')


    def _ensure(self):
        # Base volume doesn't guarantee that device 'self.device' exists
        # TODO: Add explanatory comment
        pass


    def _snapshot(self, description, tags, **kwds):
        raise NotImplementedError()


    def _detach(self, force, **kwds):
        pass


    def _destroy(self, force, **kwds):
        pass

    def _clone(self, config):
        pass

    def apply_tags(self, tags, async=True):
        pass


storage2.volume_types['base'] = Volume


class Snapshot(Base):
    QUEUED = 'queued'
    IN_PROGRESS = 'in-progress'
    COMPLETED = 'completed'
    FAILED = 'failed'
    UNKNOWN = 'unknown'

    def __init__(self, **kwds):
        super(Snapshot, self).__init__(**kwds)
        if not self._config.get('id'):
            self._config['id'] = self._genid('snap-')


    def restore(self):
        vol = storage2.volume(type=self.type, snap=self)
        vol.ensure()
        return vol


    def destroy(self):
        return self._destroy()


    def status(self):
        return self._status()


    def _destroy(self):
        pass

    def _status(self):
        pass


storage2.snapshot_types['base'] = Snapshot


def taken_devices():
    devs = glob.glob('/dev/xvd*') + glob.glob('/dev/vd*') + glob.glob('/dev/sd*')
    devs = [os.path.realpath(x) for x in devs if x[-1] in string.ascii_lowercase]
    return set(devs)

def taken_letters():
    lets = [x[-1] for x in taken_devices()]
    return set(lets)
