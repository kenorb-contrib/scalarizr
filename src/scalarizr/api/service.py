'''
Created on Aug 16, 2012
 
@author: Dmytro Korsakov
'''
 
from __future__ import with_statement
from scalarizr import rpc
 
services = {}
 
 
class ServiceAPI(object):
 
 
    @rpc.query_method
    def get_preset(self, behavior):
        if behavior not in services:
            raise AssertionError('Behaviour %s is not registred in ServiceAPI')
 
        provider = services[behavior]
        manifest = provider.get_manifest(behavior)
        if manifest:
            return provider.get_preset(manifest)
        else:
            raise BaseException('Cannot retrieve preset: Manifest not found.')
 
 
    @rpc.command_method
    def set_preset(self, behavior, values):
        if behavior not in services:
            raise AssertionError('Behaviour %s is not registred in ServiceAPI')
 
        provider = services[behavior]
        manifest = provider.get_manifest(behavior)
        if manifest:
            provider.set_preset(values, manifest)
        else:
            raise AssertionError('Cannot apply preset: Manifest not found.')
 
