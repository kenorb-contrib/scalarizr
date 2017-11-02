from __future__ import with_statement
'''
Created on Jan 6, 2011

@author: marat
'''

from . import VolumeConfig, Volume, Snapshot, VolumeProvider, Storage, StorageError, system
from .util.loop import mkloop, rmloop, listloop

import os
import sys
import time
import shutil
import random
from scalarizr.util import wait_until

class LoopConfig(VolumeConfig):
    type = 'loop'
    file = None
    size = None
    zerofill = None

class LoopVolume(Volume, LoopConfig):
    pass

class LoopSnapshot(Snapshot, LoopConfig):
    pass


class LoopVolumeProvider(VolumeProvider):
    type = 'loop'
    vol_class = LoopVolume
    snap_class = LoopSnapshot

    def create(self, **kwargs):

        '''
        @param file: Filename for loop device
        @type file: basestring

        @param size: Size in MB or % of root device
        @type size: int | str

        @param zerofill: Fill device with zero bytes. Takes more time, but greater GZip compression
        @type zerofill: bool
        '''
        size = kwargs.get('size')
        file = kwargs.get('file')
        device = kwargs.get('device')

        if not (device and file and listloop().get(device) == file):
            # Construct volume
            if (not size and (not file or not os.path.exists(file))):
                raise StorageError('You must specify size of new loop device or existing file.')

            if not file:
                file = '/mnt/loopdev%s' % repr(time.time())
            if not os.path.exists(file):
                try:
                    size = int(float(size) * 1024)
                except ValueError:
                    if isinstance(size, basestring) and '%root' in size.lower():
                        # Getting size in percents
                        try:
                            size_pct = int(size.lower().replace('%root', ''))
                        except:
                            raise StorageError('Incorrect size format: %s' % size)
                        # Retrieveing root device size and usage
                        root_size, used_pct = (system(('df', '-P', '-B', '1024', '/'))[0].splitlines()[1].split()[x] for x in (1,4))
                        root_size = int(root_size) / 1024
                        used_pct = int(used_pct[:-1])

                        if size_pct > (100 - used_pct):
                            raise StorageError('No enough free space left on device.')
                        # Calculating loop device size in Mb
                        size = (root_size / 100) * size_pct
                    else:
                        raise StorageError('Incorrect size format: %s' % size)

            kwargs['file']  = file
            existed = filter(lambda x: x[1] == file, listloop().iteritems())
            if existed:
                kwargs['device'] = existed[0][0]
            else:
                kwargs['device'] = mkloop(file, device=device, size=size, quick=not kwargs.get('zerofill'))

        return super(LoopVolumeProvider, self).create(**kwargs)


    def create_from_snapshot(self, **kwargs):
        file = kwargs.get('file')
        try:
            base = file.split('.')[0]
            new_file = base + time.strftime('.%d-%m_%H:%M:%S_') + str(random.randint(1,1000))
            shutil.copy(file, new_file)
        except:
            e,t = sys.exc_info()[1:]
            raise Exception, "Can't copy snapshot file %s: %s" % (file, e), t

        kwargs['file'] = new_file
        return self.create(**kwargs)

    def create_snapshot(self, vol, snap, tags=None):
        backup_filename = vol.file + '.%s.bak' % time.strftime('%d-%m_%H:%M:%S')
        shutil.copy(vol.file, backup_filename)
        snap.file = backup_filename
        return snap

    def detach(self, vol, force=False):
        super(LoopVolumeProvider, self).detach(vol, force)
        rmloop(vol.devname)
        vol.device = None
        vol.detached = True
        return vol.config()

    def destroy(self, vol, force=False, **kwargs):
        super(LoopVolumeProvider, self).destroy(vol, force, **kwargs)
        wait_until(self._rmloop, (vol.devname, ),
                        sleep=1, timeout=60, error_text='Cannot detach loop device %s' % vol.devname)
        if force:
            os.remove(vol.file)
        vol.device = None

    def blank_config(self, cnf):
        cnf.pop('file', None)

    def _rmloop(self, device):
        try:
            rmloop(device)
            return True
        except StorageError, e:
            if 'Device or resource busy' in str(e):
                return False
            raise

Storage.explore_provider(LoopVolumeProvider)
