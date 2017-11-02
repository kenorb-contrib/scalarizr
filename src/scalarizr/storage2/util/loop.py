from __future__ import with_statement
'''
Created on Jan 6, 2011

@author: marat
'''

from scalarizr.linux import system, LinuxError

import os


LOSETUP_EXEC = '/sbin/losetup'

def mkloop(filename, device=None, size=None, quick=False):
    ''' Create loop device '''
    if size and not os.path.exists(filename):
        cmd = ['dd', 'if=/dev/zero', 'of=%s' % filename, 'bs=1M']
        if quick:
            cmd.extend(['seek=%d' % (size - 1,), 'count=1'])
        else:
            cmd.extend(['count=%d' % size])
        system(cmd)
    if not device:
        system((LOSETUP_EXEC, '-f', filename))
        loops = listloop()
        for loopdev, fname in loops.iteritems():
            if fname == filename:
                return loopdev
        else:
            raise Exception('Cannot find loop device with %s file' % file)
    else:
        system((LOSETUP_EXEC, device, filename))
    return device

def listloop():
    ret = {}
    loop_lines = system((LOSETUP_EXEC, '-a'))[0].strip().splitlines()
    for loop_line in loop_lines:
        words = loop_line.split()
        ret[words[0][:-1]] = words[-1][1:-1]
    return ret


def rmloop(device):
    try:
        system((LOSETUP_EXEC, '-d', device))
    except LinuxError, e:
        if 'No such device or address' in e.err:
            ''' Silently pass non-existed loop removal '''
            pass
        else:
            raise
