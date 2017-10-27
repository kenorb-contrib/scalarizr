from __future__ import with_statement
'''
@author: Dmytro Korsakov
'''
from __future__ import absolute_import
import platform
import re
import os
import string
 
from scalarizr import linux
 
_uname = None
_linux_dist = None
_dist_base = None
 
_is_linux = _is_win = _is_sun = False
_is_debian_based = _is_redhat_based = False
_is_debian = _is_ubuntu = False
_is_rhel = _is_centos = _is_fedora = False
 
_debian_based_dists = ['debian', 'ubuntu', 'gcel']
_redhat_based_dists = map(string.lower, (
        'centos',
        'centos linux', #centos60 only
        'rhel',
        'redhat',
        'fedora',
        'Red Hat Enterprise Linux Server release',
        'Red Hat Enterprise Linux Server',
        'Enterprise Linux Enterprise Linux Server' # OEL 5.0 - 5.2
))
 
_uname = platform.uname()
os_name = _uname[0].lower()
_is_linux = os_name == "linux"
_is_win = os_name == "windows"
_is_sun = os_name == "sunos"
 
if _is_linux:
    _linux_dist = None
    if os.path.exists("/etc/lsb-release"):
        fp = open("/etc/lsb-release")
        lsb = fp.readlines()
        fp.close()
        if len(lsb) >= 3:
            _linux_dist = tuple(map(lambda i: lsb[i].split('=')[1].strip(), range(3)))
    if not _linux_dist and hasattr(platform, "linux_distribution"):
        _linux_dist = platform.linux_distribution()
    else:
        _linux_dist = platform.dist()
 
    dist_name = _linux_dist[0].lower()
    _is_redhat_based = linux.os['family'] == 'RedHat' #fix for Amazon Linux
    _is_debian_based = dist_name in _debian_based_dists
    _is_debian = dist_name == "debian"
    _is_ubuntu = dist_name in ("ubuntu", "gcel")
    _is_fedora = dist_name == "fedora" or (dist_name == 'redhat' and _linux_dist[2].lower() == 'werewolf')
    _is_centos = dist_name == "centos" or dist_name == "centos linux"
    _is_rhel = dist_name in ('red hat enterprise linux server', 'rhel',
                                                     'red hat enterprise linux server release', 'redhat')
 
 
def is_linux(): return _is_linux
def is_win(): return _is_win
def is_sun(): return _is_sun
 
def is_debian_based(): return _is_debian_based
def is_redhat_based(): return _is_redhat_based
 
def is_fedora(): return _is_fedora
def is_centos(): return _is_centos
def is_ubuntu(): return _is_ubuntu
def is_debian(): return _is_debian
def is_rhel(): return _is_rhel
 
def uname(): return _uname
def linux_dist(): return _linux_dist
def version_info(): return tuple(map(int, str(linux.os['release']).split('.'))) #LooseVersion->tuple
 
redhat = is_redhat_based() and version_info() or (0, 0)
ubuntu = is_ubuntu() and version_info() or (0, 0)
 
 
def arch():
    if re.search("^i\\d86$", _uname[4]):
        return Architectures.I386
    elif re.search("^x86_64$", _uname[4]):
        return Architectures.X86_64
    return Architectures.UNKNOWN
 
def arch_bits():
    if arch() == Architectures.X86_64:
        return 64
    else:
        return 32
 
class Architectures:
    I386 = "i386"
    X86_64 = "x86_64"
    UNKNOWN = "unknown"
 
