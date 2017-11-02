from __future__ import with_statement

import logging
import re
import operator
import Queue
import sys
import threading

from scalarizr import linux


LOG = logging.getLogger(__name__)


volume_types = dict()
snapshot_types = dict()
filesystem_types = dict()


def volume(*args, **kwds):
    """
    Takes volume configuration or volume instance, returns volume instance
    """

    if args:
        if isinstance(args[0], dict):
            return volume(**args[0])
        elif isinstance(args[0], basestring):
            return volume(device=args[0])
        else:
            return args[0]
    type_ = kwds.get('type', 'base')
    if type_ not in volume_types:
        try:
            __import__('scalarizr.storage2.volumes.%s' % type_)
        except ImportError:
            pass
    try:
        cls = volume_types[type_]
    except KeyError:
        msg = "Unknown volume type '%s'. " \
                        "Have you registered it in storage2.volume_types?" % type_
        raise KeyError(msg)
    return cls(**kwds)


def snapshot(*args, **kwds):
    """
    Takes snapshot configuration or snapshot instance, returns snapshot instance
    """
    if args:
        if isinstance(args[0], dict):
            return snapshot(**args[0])
        else:
            return args[0]
    type_ = kwds.get('type', 'base')
    if type_ not in snapshot_types:
        try:
            __import__('scalarizr.storage2.volumes.%s' % type_)
        except ImportError:
            pass
    try:
        cls = snapshot_types[type_]
    except KeyError:
        msg = "Unknown snapshot type '%s'. " \
                        "Have you registered it in storage2.snapshot_types?" % type_
        raise KeyError(msg)
    return cls(**kwds)


def filesystem(fstype=None):
    """
    :return: Filesystem object
    :rtype: scalarizr.storage2.filesystems.FileSystem
    """
    fstype = fstype or 'ext3'
    if not fstype in filesystem_types:
        try:
            __import__('scalarizr.storage2.filesystems.%s' % fstype)
        except ImportError:
            pass
    try:
        cls = filesystem_types[fstype]
    except KeyError:
        msg = "Unknown filesystem type '%s'. " \
                        "Have you registered it in storage2.filesystem_types?" % fstype
        raise KeyError(msg)
    return cls()


def concurrent_snapshot(volumes, description, tags=None, **kwds):
    '''
    Concurrently calls vol.snapshot() and
    returns list of snapshot in correct order
    '''
    results = []
    def snapshot(index, volume, description, tags=None, **kwds):
        try:
            snap = volume.snapshot(description, tags, **kwds)
            results.append((1, index, snap))
        except:
            exc_info = sys.exc_info()
            LOG.warn('Failed to create snapshot of %s(%s): %s',
                            vol.id, vol.type, exc_info[1], exc_info=exc_info)
            results.append((0, index, exc_info))

    threads = []
    index = 0
    for vol in volumes:
        sindex = str(index)
        description0 = description.replace('${index}', sindex)
        tags0 = tags and tags.copy() or {}
        for k, v in tags0.items():
            tags0[k] = unicode(v).replace('${index}', sindex)
        thread = threading.Thread(
                                target=snapshot,
                                args=(index, vol, description0, tags0),
                                kwargs=kwds)
        thread.start()
        threads.append(thread)
        index += 1

    for thread in threads:
        thread.join()

    # sort results by index
    results = sorted(results, key=operator.itemgetter(1))
    if not all((r[0] for r in results)):
        # delete created snapshots to rollback
        for r in results:
            if not r[0]: continue
            snap = r[2]
            try:
                snap.destroy(force=True)
            except:
                exc_info = sys.exc_info()
                LOG.warn('Failed to delete snapshot %s(%s): %s',
                                snap.id, snap.type, exc_info[1], exc_info=exc_info)
        raise StorageError(
                        'Failed to create one or more snapshots. '
                        'Successfuly created snapshots were deleted to rollback. '
                        'See log for detailed report about each failed snapshot')
    return tuple(r[2] for r in results)


class StorageError(linux.LinuxError):
    pass


class NoOpError(StorageError):
    pass

class VolumeNotExistsError(StorageError):
    def __str__(self):
        return ('Volume not found: {0}. Most likely it was deleted. '
            'You can check "Regenerate storage if missing volumes" in UI '
            'to create clean storage volume with the same settings').format(self.args[0])

class OperationError(StorageError):
    pass

RHEL_DEVICE_ORDERING_BUG = False
if linux.os['release'] and linux.os['family'] == 'RedHat':
    # Check that system is affected by devices ordering bug
    # https://bugzilla.redhat.com/show_bug.cgi?id=729340
    from scalarizr.linux import mount
    try:
        entry = mount.mounts()['/dev/xvde1']
        RHEL_DEVICE_ORDERING_BUG = entry.mpoint == '/'
    except KeyError:
        pass
