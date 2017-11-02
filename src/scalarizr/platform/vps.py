from __future__ import with_statement
'''
Created on Dec 24, 2009

@author: marat
'''

from scalarizr.platform import Platform
import socket

def get_platform():
    return VpsPlatform()

class VpsPlatform(Platform):
    name = "vps"

    features = []

    def get_private_ip(self):
        return self.get_public_ip()

    def get_public_ip(self):
        return socket.gethostbyname(socket.gethostname())
