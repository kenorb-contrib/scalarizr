from __future__ import with_statement
'''
Created on Aug 20, 2012
 
@author: marat
'''
from scalarizr.platform.cloudstack import CloudStackPlatform
 
 
def get_platform():
    return IDCFPlatform()
 
class IDCFPlatform(CloudStackPlatform):
    name = 'idcf'
 
