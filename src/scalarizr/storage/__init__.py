from __future__ import with_statement
'''
Created on Nov 11, 2010

@author: marat
'''
from __future__ import with_statement


from scalarizr.util import system2, PopenError
from scalarizr.linux import mount
from scalarizr import linux

import logging
import threading
import os
import sys
import re
import uuid
from scalarizr.libs.bases import Observable
from functools import partial
import re


try:
    import json
except ImportError:
    import simplejson as json

MKFS_EXEC               = '/sbin/mkfs'
MOUNT_EXEC              = '/bin/mount'
UMOUNT_EXEC             = '/bin/umount'
SYNC_EXEC               = '/bin/sync'

VOL_STATE_ATTACHED = 'attached'
VOL_STATE_DETACHED = 'detached'

logger = logging.getLogger(__name__)

class StorageError(PopenError):
    pass

def system(*popenargs, **kwargs):
    kwargs['logger'] = logger
    kwargs['exc_class'] = StorageError
    return system2(*popenargs, **kwargs)

class Storage:
    maintain_volume_table = False
    providers = {}
    default_vol_provider = None
    default_snap_provider = None

    _fs_drivers = {}

    _obs = Observable()
    _obs.define_events(
            'attach',
            'detach',
            'destroy'
    )
    on, un, fire = _obs.on, _obs.un, _obs.fire

    volumes = dict()


    @staticmethod
    def volume_table():
        if Storage.maintain_volume_table:
            from scalarizr.bus import bus
            conn = bus.db
            cur = conn.cursor()
            try:
                cur.execute('SELECT * FROM storage')
                return cur.fetchall()
            finally:
                cur.close()
        else:
            return ()


    @staticmethod
    def lookup_filesystem(fstype):
        self = Storage
        if fstype in self._fs_drivers:
            return self._fs_drivers[fstype]
        try:
            mod = __import__('scalarizr.storage.fs.%s' % fstype, globals(), locals(), ('__filesystem__',), -1)
            self._fs_drivers[fstype] = mod.__filesystem__()
            return self.lookup_filesystem(fstype)
        except ImportError:
            raise LookupError('Unknown filesystem %s' % fstype)


    @staticmethod
    def explore_filesystem(fstype, FileSystemClass):
        self = Storage
        self._fs_drivers[fstype] = FileSystemClass()


    @staticmethod
    def explore_provider(PvdClass, default_for_vol=False, default_for_snap=False):
        self = Storage
        type = PvdClass.type
        self.providers[type] = PvdClass
        if default_for_vol:
            self.default_vol_provider = type
        if default_for_snap:
            self.default_snap_provider = type


    @staticmethod
    def lookup_provider(pvd_type=None, for_snap=False):
        '''
        XXX: for_snap confuse
        '''
        self = Storage

        if not pvd_type:
            pvd_type = self.default_snap_provider if for_snap else self.default_vol_provider
        try:
            pvd = self.providers[pvd_type]
        except KeyError:
            try:
                __import__('scalarizr.storage.' + pvd_type, globals=globals(), locals=locals())
                pvd = self.providers[pvd_type]
            except (ImportError, KeyError):
                raise LookupError('Unknown volume provider "%s"' % (pvd_type,))
        if hasattr(pvd, '__bases__'):
            pvd = pvd()
            self.providers[pvd_type] = pvd
        return pvd


    @staticmethod
    def get(vol_id):
        try:
            return Storage.volumes[vol_id]
        except KeyError:
            raise StorageError('Unknown volume with id="%s"' % vol_id)


    @staticmethod
    def create(*args, **kwargs):
        '''
        @raise LookupError: When volume provider cannot be resolved
        @raise StorageError: General error for all cases
        '''
        self = Storage
        args = list(args) if args else list()
        kwargs = kwargs.copy() if kwargs else dict()
        from_snap = False

        if args:
            if isinstance(args[0], dict):
                kwargs = args[0]
            else:
                kwargs['device'] = args[0]

        id = kwargs.get('id')
        if id in self.volumes:
            return self.volumes[id]

        if 'snapshot' in kwargs:
            # Save original kwargs
            from_snap = True
            orig_kwargs = kwargs.copy()

            # Set kwargs to snapshot kwargs
            kwargs = kwargs['snapshot'].config() if isinstance(kwargs['snapshot'], Snapshot) else kwargs['snapshot']
            if not isinstance(kwargs, dict):
                args = None
                kwargs = dict(device=kwargs)
            if kwargs['id']:
                kwargs['snapshot_id'] = kwargs['id']
            # Update kwargs with original one
            del orig_kwargs['snapshot']
            kwargs.update(orig_kwargs)

        if 'disks' in kwargs:
            disks = []
            errors = []
            threads = []

            def _create(self, i, item):
                try:
                    disk = self.create(**item) if isinstance(item, dict) else self.create(item)
                    disks.append((i, disk))
                except:
                    e = sys.exc_info()[0]
                    errors.append(e)

            for i, item in enumerate(kwargs['disks']):
                if isinstance (item, Volume):
                    disks.append((i,item))
                    continue

                t = threading.Thread(target=_create, args=(self, i, item))
                t.start()
                threads.append(t)

            for t in threads:
                t.join()

            if errors:
                raise StorageError('Failed to attach disks:\n%s' % '\n'.join([str(e) for e in errors]))

            disks.sort(lambda x,y: cmp(x[0], y[0]))
            disks = [disk for i, disk in disks]

            kwargs['disks'] = disks

        if 'disk' in kwargs:
            disk = kwargs['disk']
            if not isinstance(disk, Volume):
                kwargs['disk'] = self.create(**disk) if isinstance(disk, dict) else self.create(disk)


        # Find provider
        pvd = self.lookup_provider(kwargs.get('type'), from_snap)
        attaching = kwargs.get('device') and not os.path.exists(kwargs['device'])
        vol = getattr(pvd, 'create_from_snapshot' if from_snap else 'create').__call__(**kwargs)
        if attaching:
            Storage.fire('attach', vol)

        self.volumes[vol.id] = vol
        return vol


    @staticmethod
    def detach(vol, force=False):
        Storage.fire('detach', vol)
        id = vol.id
        pvd = Storage.lookup_provider(vol.type)
        ret = pvd.detach(vol, force)
        vol.detached = True
        if id in Storage.volumes:
            del Storage.volumes[id]
        return ret


    @staticmethod
    def destroy(vol, force=False, **kwargs):
        id = vol.id
        Storage.fire('destroy', vol)
        pvd = Storage.lookup_provider(vol.type)
        pvd.destroy(vol, force, **kwargs)
        if id in Storage.volumes:
            del Storage.volumes[id]


    @staticmethod
    def backup_config(cnf, filename):
        logger.debug('Backuping volume config {id: %s, type: %s ...} into %s',
                                cnf.get('id'), cnf.get('type'), filename)
        fp = open(filename, 'w+')
        try:
            fp.write(json.dumps(cnf, indent=4))
        finally:
            fp.close()


    @staticmethod
    def restore_config(filename):
        fp = open(filename, 'r')
        try:
            ret = json.load(fp)
            return dict(zip(list(key.encode("ascii") for key in ret.keys()), ret.values()))
        finally:
            fp.close()


    @staticmethod
    def blank_config(cnf):
        cnf = cnf.copy()
        cnf.pop('id', None)
        pvd = Storage.lookup_provider(cnf['type'])
        pvd.blank_config(cnf)
        return cnf


def _update_volume_tablerow(vol, state=None):
    if Storage.maintain_volume_table:
        from scalarizr.bus import bus
        conn = bus.db
        cur = conn.cursor()
        cur.execute('UPDATE storage SET state = ? WHERE device = ?', (state, vol.device))
        if not cur.rowcount:

            _logger = logging.getLogger(__name__)
            _logger.debug('INSERT INTO storage VALUES (%s, %s, %s, %s)' % (vol.id, vol.type, vol.device, state))

            cur.execute('INSERT INTO storage VALUES (?, ?, ?, ?)',
                            (vol.id, vol.type, vol.device, state))
        conn.commit()

Storage.on(
        attach=partial(_update_volume_tablerow, state=VOL_STATE_ATTACHED),
        detach=partial(_update_volume_tablerow, state=VOL_STATE_DETACHED)
)


class VolumeConfig(object):
    type = 'base'
    device = None
    mpoint = None
    fstype = None
    fs_created = None
    _id_format = '%s-%s'
    _id = None
    _ignores = ()

    def _id_setter(self, id):
        self._id = id

    def _id_getter(self):
        if not self._id:
            self._id = self._id_format % (self.type, uuid.uuid4().hex[0:8])
        return self._id

    id = property(_id_getter, _id_setter)

    def config(self, as_snapshot=False):
        base = tuple(base for base in self.__class__.__bases__ if base.__name__.endswith('Config'))[0]
        attrs = tuple(attr for attr in dir(base) if not (attr.startswith('_') or attr in self._ignores))
        ret = dict()
        for attr in attrs:
            if attr == 'config':
                continue
            elif attr == 'disks':
                ret['disks'] = tuple(disk.config() for disk in self.disks)
                if as_snapshot:
                    ret['disks'] = tuple(dict(snapshot=disk) for disk in ret['disks'])
            elif attr == 'disk':
                ret['disk'] = self.disk.config() if hasattr(self.disk, 'config') else self.disk
            else:
                ret[attr] = getattr(self, attr)
        return ret


def _fs_should_be_set(f):
    def d(*args):
        if args[0]._fs is None:
            raise StorageError('Filesystem is not set')
        return f(*args)
    return d


def devname_not_empty(f):
    def d(*args, **kwargs):
        if not args[1].devname:
            raise StorageError('Device name is empty.')
        return f(*args, **kwargs)
    return d


RHEL_DEVICE_ORDERING_BUG = False
if linux.os.redhat_family:
    # Check that system is affected by devices ordering bug
    # https://bugzilla.redhat.com/show_bug.cgi?id=729340
    rootfs = [entry for entry in mount.mounts() if entry.mpoint == '/' and entry.device.startswith('/dev')][0]
    RHEL_DEVICE_ORDERING_BUG = rootfs.device.startswith('/dev/xvde')


def get_system_devname(devname):
    if '/xvd' in devname:
        return devname
    ret = devname.replace('/sd', '/xvd') if os.path.exists('/dev/xvda1') or RHEL_DEVICE_ORDERING_BUG else devname
    if RHEL_DEVICE_ORDERING_BUG:
        ret = ret[0:8] + chr(ord(ret[8])+4) + ret[9:]
    return ret

def get_cloud_devname(devname):
    if '/sd' in devname:
        return devname
    ret = devname
    if RHEL_DEVICE_ORDERING_BUG:
        ret = ret[0:8] + chr(ord(ret[8])-4) + ret[9:]
    return ret.replace('/xvd', '/sd')


class Volume(VolumeConfig, Observable):
    detached = False
    lock = None
    _non_blocking_methods = ['status']

    _logger = None
    _fs = None
    _id_format = '%s-vol-%s'

    def __init__(self, device=None, mpoint=None, fstype=None, type=None, *args, **kwargs):
        self.lock = threading.RLock()
        self._logger = logging.getLogger(__name__)
        super(Volume, self).__init__()

        if not device:
            raise ValueError('device name should be non-empty')

        if device == '/dev/sda2' and not os.path.exists(get_system_devname('/dev/sda2')):
            # sometimes on m1.small instead of sda2 we saw sdb
            sdb = get_system_devname('/dev/sdb')
            if os.path.exists(sdb):
                device = sdb
            else:
                # ugly
                from scalarizr.bus import bus
                bdm = bus.platform.get_block_device_mapping()
                if 'ephemeral0' in bdm:
                    device = get_system_devname(bdm['ephemeral0'])
        elif device.startswith('/dev/sd') and not os.path.exists(device):
            # ephemeral block device -> xen device
            device = get_system_devname(device)


        self.device = device

        self.mpoint = mpoint
        if fstype:
            self.fstype = fstype
        if type:
            self.type = type
        for k, v in kwargs.items():
            if hasattr(self, k):
                setattr(self, k, v)

        self.define_events(
                'before_detach', 'after_detach', 'detach_failed',
                'before_snapshot', 'after_snapshot', 'snapshot_failed',
                'before_mkfs', 'after_mkfs', 'mkfs_failed',
                'before_mount', 'after_mount', 'mount_failed',
                'before_umount', 'after_umount', 'umount_failed'
        )


    def __getattribute__(self, item):
        attr = super(Volume, self).__getattribute__(item)
        non_blocking = super(Volume, self).__getattribute__('_non_blocking_methods')
        if item in non_blocking:
            return attr

        if getattr(attr, 'im_self', None) is self:
            def with_lock(*args, **kwargs):
                with self.lock:
                    return attr(*args, **kwargs)

            return with_lock
        else:
            return attr

    @property
    def devname(self):
        return self.device


    def _fstype_setter(self, fstype):
        self._fs = Storage.lookup_filesystem(fstype)


    def _fstype_getter(self):
        return self._fs.name if self._fs else None


    fstype = property(_fstype_getter, _fstype_setter)


    def mkfs(self, fstype=None):
        if self.fs_created:
            return

        fstype = fstype or self.fstype or 'ext3'
        if not fstype:
            raise ValueError('Filesystem cannot be None')
        fs = Storage.lookup_filesystem(fstype)
        logger.info('Creating filesystem on %s', self.device)
        fs.mkfs(self.devname)
        self.fstype = fstype
        self.fs_created = True
        self._fs = fs


    @_fs_should_be_set
    def resize(self, size=None, **fsargs):
        fsargs = fsargs or dict()
        return self._fs.resize(self.devname, **fsargs)


    @_fs_should_be_set
    def _get_label(self):
        return self._fs.get_label(self.devname)


    @_fs_should_be_set
    def _set_label(self, lbl):
        self._fs.set_label(self.devname, lbl)


    label = property(_get_label, _set_label)


    @_fs_should_be_set
    def freeze(self):
        return self._fs.freeze(self.devname)


    @_fs_should_be_set
    def unfreeze(self):
        return self._fs.unfreeze(self.devname)


    def mounted(self):
        res = re.search('%s\s+on\s+(?P<mpoint>.+)\s+type' % self.devname, system(MOUNT_EXEC)[0])
        return bool(res)


    def mount(self, mpoint=None):
        self.fire('before_mount')
        mpoint = mpoint or self.mpoint
        cmd = (MOUNT_EXEC, self.devname, mpoint)
        try:
            system(cmd, error_text='Cannot mount device %s' % self.devname)
        except StorageError, e:
            try:
                if 'you must specify the filesystem type' in str(e):
                    if self.fs_created is True:
                        raise

                    self.mkfs()
                    system(cmd, error_text='Cannot mount device %s' % self.devname)
                else:
                    raise
            except:
                self.fire('mount_failed')
                raise

        self.fs_created = True
        self.mpoint = mpoint
        self.fire('after_mount')


    def umount(self, lazy=False):
        self.fire('before_umount')
        cmd = (UMOUNT_EXEC, '-l' if lazy else '-f' , self.devname)
        try:
            system(cmd, error_text='Cannot umount device %s' % self.devname)
        except (Exception, BaseException), e:
            if not 'not mounted' in str(e):
                self.fire('umount_failed')
                raise
        else:
            self.fire('after_umount')


    def snapshot(self, description=None, **kwargs):
        self.fire('before_snapshot')
        # Freeze filesystem
        if self._fs:
            system(SYNC_EXEC)
            # When using XFS over eph storage (lvm+lvm-snap) LVM freeze failed with
            # device-mapper: suspend ioctl failed: Device or resource busy
            #self.freeze()

        try:
            # Create snapshot
            pvd = Storage.lookup_provider(self.type)
            conf = self.config()
            del conf['id']
            snap = pvd.snapshot_factory(description, **conf)
            return pvd.create_snapshot(self, snap, **kwargs)
        finally:
            # Unfreeze filesystem
            #if self._fs:
            #       self.unfreeze()
            pass


    def detach(self, force=False):
        self.fire('before_detach')
        was_mounted = self.mounted()
        if was_mounted:
            self.umount()
        try:
            ret = Storage.detach(self, force)
        except BaseException, e:
            self.fire('detach_failed')
            self._logger.error('Storage detach failed with error %s' % e)
            if was_mounted:
                self.mount()
            raise
        else:
            self.fire('after_detach')
            return ret


    def destroy(self, force=False, **kwargs):
        Storage.destroy(self, force, **kwargs)


    def __str__(self):
        fmt = '[volume:%s] %s\n' + '%-10s : %s\n'*3
        return fmt % (
                self.type, self.devname,
                'id', self.id or '',
                'mpoint', self.mpoint or '',
                'fstype', self.fstype or ''
        )


    def status(self):
        pvd = Storage.lookup_provider(self.type)
        return pvd.status(self)


class Snapshot(VolumeConfig):
    CREATING = 'creating'
    CREATED = 'created'
    COMPLETED = 'completed'
    FAILED = 'failed'

    version = '0.7'
    type = None
    description = None
    _id_format = '%s-snap-%s'

    def __init__(self, type=None, description=None, **kwargs):
        self.type = type
        self.description = description
        for k, v in kwargs.items():
            if hasattr(self, k):
                setattr(self, k, v)

    def __str__(self):
        fmt = '[snapshot(v%s):%s] %s\n%s'
        return str(fmt % (self.version, self.type, self.id, self.description or '')).strip()

    def config(self):
        cnf = VolumeConfig.config(self, as_snapshot=True)
        cnf['description'] = self.description
        return cnf

    @property
    def state(self):
        pvd = Storage.lookup_provider(self.type, True)
        return pvd.get_snapshot_state(self)

    def destroy(self):
        pvd = Storage.lookup_provider(self.type, True)
        return pvd.destroy_snapshot(self)


class VolumeProvider(object):
    type = 'base'
    vol_class = Volume
    snap_class = Snapshot

    def create(self, **kwargs):
        device = kwargs['device']
        del kwargs['device']
        if not kwargs.get('type'):
            kwargs['type'] = self.type
        return self.vol_class(device, **kwargs)

    def create_from_snapshot(self, **kwargs):
        if 'id' in kwargs:
            del kwargs['id']
        return self.create(**kwargs)

    def snapshot_factory(self, description=None, **kwargs):
        kwargs['description'] = description
        kwargs['type'] = self.type
        return self.snap_class(**kwargs)

    def create_snapshot(self, vol, snap, **kwargs):
        return snap

    def get_snapshot_state(self, snap):
        return Snapshot.COMPLETED

    def destroy_snapshot(self, snap):
        pass

    def destroy(self, vol, force=False, **kwargs):
        if not vol.devname and not vol.detached:
            raise StorageError("Can't destroy volume: device name is empty.")
        if not vol.detached:
            self._umount(vol, force)

    def detach(self, vol, force=False):
        if not vol.detached and vol.mounted():
            self._umount(vol, force)

    def blank_config(self, cnf):
        raise NotImplementedError()

    def _umount(self, vol, force=False):
        try:
            vol.umount()
        except:
            if force:
                vol.umount(lazy=True)
            else:
                raise

Storage.explore_provider(VolumeProvider, default_for_vol=True, default_for_snap=True)
