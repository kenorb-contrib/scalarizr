from __future__ import with_statement
'''
Created on Mar 1, 2010

@author: marat
'''

from scalarizr.handlers.block_device import BlockDeviceHandler
from scalarizr.storage2.volumes import ebs

from scalarizr import linux


def get_handlers ():
	return [EbsHandler()]

class EbsHandler(BlockDeviceHandler):

    def __init__(self):
        BlockDeviceHandler.__init__(self, 'ebs')

    def get_devname(self, devname):
        return ebs.device2name(devname)
