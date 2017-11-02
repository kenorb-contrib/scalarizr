from __future__ import with_statement
'''
Created on Nov 24, 2010

@author: spike
@author: marat
'''

from .ext3 import ExtFileSystem

class Ext4FileSystem(ExtFileSystem):
    name = 'ext4'

__filesystem__ = Ext4FileSystem
