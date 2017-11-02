
from scalarizr.platform import Platform
from scalarizr import linux
import socket


def get_platform():
    return LxcPlatform()

class LxcPlatform(Platform):
    name = "lxc"

    features = []

    def get_private_ip(self):
        return self.get_public_ip()

    def get_public_ip(self):
    	out = linux.system('cat /var/lib/dhcp*/*.eth0.leases | grep fixed | tail -1', shell=True)[0]
    	return out.strip().split()[-1][:-1]
