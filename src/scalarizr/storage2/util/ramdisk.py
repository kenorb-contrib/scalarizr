from __future__ import with_statement
'''
Created on Mar 2, 2011

@author: spike
'''
from scalarizr.storage2 import MOUNT_EXEC, UMOUNT_EXEC, system, StorageError
import os

FREE_EXEC='/usr/bin/free'

def create(size, mpoint):
    if not os.path.exists(mpoint):
        os.makedirs(mpoint)
    cmd = (MOUNT_EXEC, '-t', 'tmpfs', '-o','size=%sM'%size, 'tmpfs', mpoint)
    system(cmd, error_text="Can't create ramdisk.")

def destroy(mpoint, force=False):
    if not os.path.ismount(mpoint):
        raise StorageError('Directory %s is not valid mount point' % mpoint)
    cmd = [UMOUNT_EXEC, mpoint]
    if force:
        cmd.insert(1, '-f')
    system(tuple(cmd), error_text="Can't destroy ramdisk.")

def free():
    cmd = (FREE_EXEC, '-m')
    out_lines = system(cmd)[0].splitlines()
    free_ram = int(out_lines[1].split()[3])
    free_swap = int(out_lines[3].split()[3])
    return (free_ram, free_swap)
