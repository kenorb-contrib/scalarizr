from __future__ import with_statement
 
import os
 
from scalarizr.linux import pkgmgr
from scalarizr.linux import build_cmd_args
from scalarizr.linux import system
 
 
def rsync(src, dst, **long_kwds):
    if not os.path.exists('/usr/bin/rsync'):
        pkgmgr.installed('rsync')
    system(['sync'])
    output = system(build_cmd_args(executable='/usr/bin/rsync',
                                   long=long_kwds,
                                   params=[src, dst],
                                   duplicate_keys=True))
    system(['sync'])
    return output
 
