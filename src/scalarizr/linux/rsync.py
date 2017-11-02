from __future__ import with_statement

import os

from scalarizr import linux
from scalarizr.node import __node__



def rsync(src, dst, **long_kwds):
    linux.system(['sync'])
    output = linux.system(linux.build_cmd_args(
            executable=os.path.join(__node__['embedded_bin_dir'], 'rsync'),
            long=long_kwds,
            params=[src, dst],
            duplicate_keys=True))
    linux.system(['sync'])
    return output
