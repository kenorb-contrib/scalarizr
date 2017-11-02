"""
Created on Aug 29, 2012

@author: marat
"""

from scalarizr import storage2
from scalarizr.storage2.filesystems import ext3


class Ext4FileSystem(ext3.ExtFileSystem):
    type = 'ext4'


storage2.filesystem_types[Ext4FileSystem.type] = Ext4FileSystem
