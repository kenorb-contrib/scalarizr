'''
Created on Aug 20, 2012

@author: marat
'''
from scalarizr.platform.cloudstack import CloudStackPlatform
from scalarizr.config import BuiltinPlatforms


def get_platform():
    return IDCFPlatform()

class IDCFPlatform(CloudStackPlatform):
    name = BuiltinPlatforms.IDCF
