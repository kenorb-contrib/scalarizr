from __future__ import with_statement
'''
Created on 29.02.2012
 
@author: marat
@author: sam
'''
 
from scalarizr.util import disttool
from scalarizr.util import system2, PopenError
 
import logging
import re
import string
import sys, os
import imp
import time
import ConfigParser as configparser
 
 
LOG = logging.getLogger(__name__)
 
'''----------------------------------
# Package managers
----------------------------------'''
class PackageMgr(object):
    def __init__(self):
        self.proc = None
 
    def install(self, name, version, *args):
        ''' Installs a `version` of package `name` '''
        raise NotImplementedError()
 
    def installed(self, name):
        ''' Return installed package version '''
        raise NotImplementedError()
 
    def _join_packages_str(self, sep, name, version, *args):
        packages = [(name, version)]
        if args:
            for i in xrange(0, len(args), 2):
                packages.append(args[i:i+2])
        format = '%s' + sep +'%s'
        return ' '.join(format % p for p in packages)
 
    def updatedb(self):
        ''' Updates package manager internal database '''
        raise NotImplementedError()
 
    def check_update(self, name):
        ''' Returns info for package `name` '''
        raise NotImplementedError()
 
    def candidates(self, name):
        ''' Returns all available installation candidates for `name` '''
        raise NotImplementedError()
 
 
class AptPackageMgr(PackageMgr):
    def apt_get_command(self, command, **kwds):
        kwds.update(env={
                'DEBIAN_FRONTEND': 'noninteractive',
                'DEBIAN_PRIORITY': 'critical',
                'PATH': '/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/usr/games'
        })
        return system2(('/usr/bin/apt-get',
                                        '-q', '-y', '--force-yes',
                                        '-o Dpkg::Options::=--force-confold') + \
                                        tuple(filter(None, command.split())), **kwds)
 
 
    def apt_cache_command(self, command, **kwds):
        return system2(('/usr/bin/apt-cache',) + tuple(filter(None, command.split())), **kwds)
 
    def updatedb(self):
        self.apt_get_command('update')
 
    def candidates(self, name):
        version_available_re = re.compile(r'^\s{5}([^\s]+)\s{1}')
        version_installed_re = re.compile(r'^\s{1}\*\*\*|s{1}([^\s]+)\s{1}')
 
        versions = []
 
        for line in self.apt_cache_command('policy %s' % name)[0].splitlines():
            m = version_available_re.match(line)
            if m:
                versions.append(m.group(1))
            m = version_installed_re.match(line)
            if m:
                break
 
        versions.reverse()
        return versions
 
    def check_update(self, name):
        installed_re = re.compile(r'^\s{2}Installed: (.+)$')
        candidate_re = re.compile(r'^\s{2}Candidate: (.+)$')
        installed = candidate = None
 
        for line in self.apt_cache_command('policy %s' % name)[0].splitlines():
            m = installed_re.match(line)
            if m:
                installed = m.group(1)
                if installed == '(none)':
                    installed = None
                continue
 
            m = candidate_re.match(line)
            if m:
                candidate = m.group(1)
                continue
 
        if candidate and installed:
            if not system2(('usr/bin/dpkg', '--compare-versions', candidate, 'gt',
                                                                            installed), raise_exc = False)[2]:
                return candidate
 
    def install(self, name, version, *args):
        for _ in range(0, 30):
            try:
                self.apt_get_command('install %s' % self._join_packages_str('=', name,
                                                                        version, *args), raise_exc=True)
                break
            except PopenError, e:
                if not 'E: Could not get lock' in e.err:
                    raise
                time.sleep(2)
 
    def installed(self, name):
        version_re = re.compile(r'^Version: (.+)$')
        status_re = re.compile(r'^Status: (.+)$')
        out, code = system2(('/usr/bin/dpkg', '--status', name), raise_exc=False)[::2]
        if not code:
            for line in out.splitlines():
                m = status_re.match(line)
                if m and ('deinstall' in m.group(1) or 'not-installed' in m.group(1)):
                    # package was removed/purged
                    return
                m = version_re.match(line)
                if m:
                    return m.group(1)
 
 
class RpmVersion(object):
 
    def __init__(self, version):
        self.version = version
        self._re_not_alphanum = re.compile(r'^[^a-zA-Z0-9]+')
        self._re_digits = re.compile(r'^(\d+)')
        self._re_alpha = re.compile(r'^([a-zA-Z]+)')
 
    def __iter__(self):
        ver = self.version
        while ver:
            ver = self._re_not_alphanum.sub('', ver)
            if not ver:
                break
 
            if ver and ver[0].isdigit():
                token = self._re_digits.match(ver).group(1)
            else:
                token = self._re_alpha.match(ver).group(1)
 
            yield token
            ver = ver[len(token):]
 
        raise StopIteration()
 
    def __cmp__(self, other):
        iter2 = iter(other)
 
        for tok1 in self:
            try:
                tok2 = iter2.next()
            except StopIteration:
                return 1
 
            if tok1.isdigit() and tok2.isdigit():
                c = cmp(int(tok1), int(tok2))
                if c != 0:
                    return c
            elif tok1.isdigit() or tok2.isdigit():
                return 1 if tok1.isdigit() else -1
            else:
                c = cmp(tok1, tok2)
                if c != 0:
                    return c
 
        try:
            iter2.next()
            return -1
        except StopIteration:
            return 0
 
 
class YumPackageMgr(PackageMgr):
 
    def yum_command(self, command, **kwds):
        return system2((('/usr/bin/yum', '-d0', '-y') + tuple(filter(None,
                                                                                         command.split()))), **kwds)
    def rpm_ver_cmp(self, v1, v2):
        return cmp(RpmVersion(v1), RpmVersion(v2))
 
    def yum_list(self, name):
        out = self.yum_command('list --showduplicates %s' % name)[0].strip()
 
        version_re = re.compile(r'[^\s]+\s+([^\s]+)')
        lines = map(string.strip, out.splitlines())
 
        try:
            line = lines[lines.index('Installed Packages')+1]
            installed = version_re.match(line).group(1)
        except ValueError:
            installed = None
 
        if 'Available Packages' in lines:
            versions = [version_re.match(line).group(1) for line in lines[lines.index('Available Packages')+1:]]
        else:
            versions = []
 
        return installed, versions
 
 
    def candidates(self, name):
        installed, versions = self.yum_list(name)
 
        if installed:
            versions = [v for v in versions if self.rpm_ver_cmp(v, installed) > 0]
 
        return versions
 
    def updatedb(self):
        self.yum_command('clean expire-cache')
 
    def check_update(self, name):
        out, _, code = self.yum_command('check-update %s' % name)
        if code == 100:
            return filter(None, out.strip().split(' '))[1]
 
    def install(self, name, version, *args):
        self.yum_command('install %s' %  self._join_packages_str('-', name,
                                                                                version, *args), raise_exc=True)
 
    def installed(self, name):
        return self.yum_list(name)[0]
 
def package_mgr():
    return AptPackageMgr() if disttool.is_debian_based() else YumPackageMgr()
 
class ImpLoader(object):
    '''
    Extension for standard import
    '''
 
    DEFAULT_MANIFEST = os.path.abspath(os.path.dirname(__file__) + '/../import.manifest')
 
    manifest = None
 
    sections = None
    '''
    For Ubuntu: ('apt', 'apt:ubuntu', 'apt:ubuntu10.04')
    For Debian: ('apt', 'apt:debian', 'apt:debian6')
    For CentOS/RHEL/OEL: ('yum', 'yum:el', 'yum:el5')
    For Fedora: ('yum', 'yum:fedora', 'yum:fedora16')
    '''
 
    def __init__(self, manifest=None):
        mgr_name = 'apt' if disttool.is_debian_based() else 'yum'
        self.mgr = AptPackageMgr() if disttool.is_debian_based() else YumPackageMgr()
 
        self.manifest = os.path.abspath(manifest or self.DEFAULT_MANIFEST)
        self.conf = configparser.ConfigParser()
        self.conf.read(self.manifest)
 
        dist_id, release = disttool.linux_dist()[0:2]
        if disttool.is_redhat_based() and not disttool.is_fedora():
            dist_id = 'el'
        dist_id = dist_id.lower()
        major_release = release.split('.')[0]
        self.sections = [s % locals() for s in ('%(mgr_name)s',
                                                                                '%(mgr_name)s:%(dist_id)s',
                                                                                '%(mgr_name)s:%(dist_id)s%(major_release)s',
                                                                                '%(mgr_name)s:%(dist_id)s%(release)s')]
        self.sections.reverse()
 
        LOG.debug('Initialized ImpLoader with such settings\n'
                        '  manifest: %s\n'
                        '  sections: %s\n',
                        self.manifest, self.sections)
 
 
    def install_python_package(self, package):
        LOG.info('Resolving OS package for %s', package)
        package = package.lower()
        for section in self.sections:
            if self.conf.has_option(section, package):
                os_packages = map(string.strip, self.conf.get(section, package).split(','))
                LOG.debug('  %s -> %s', package, ', '.join(os_packages))
                LOG.debug('    %d OS package(s) will be installed', len(os_packages))
                install_args = []
                for os_package in os_packages:
                    candidates = self.mgr.candidates(os_package)
                    if not candidates:
                        raise ImportError("There are no installation candidates "
                                                        "for OS package %s" % (os_package, ))
                    install_args += [os_package, candidates[-1]]
 
                LOG.debug('    Installing %s', ', '.join(['%s == %s' % tuple(install_args[i:i+2])
                                                                                        for i in xrange(0, len(install_args), 2)]) )
                try:
                    self.mgr.install(*install_args)
                except:
                    raise ImportError("Failed to install OS packages. "
                                                            "Error: %s" % (sys.exc_info()[1], ))
 
                LOG.debug('  Successfully installed %s', package)
                break
        else:
            raise ImportError("Unknown package. There are no mappings for package '%s' "
                                            "to OS package in manifest '%s'" % (package, self.manifest))
 
 
    def find_module(self, fullname, path=None):
        if fullname in sys.modules:
            return self
 
        try:
            name = fullname.split('.')[-1]
            package = fullname.split('.')[0]
            try:
                self.file, self.filename, self.etc = imp.find_module(name, path)
                return self
            except:
                if package not in sys.modules:
                    self.install_python_package(package)
                self.file, self.filename, self.etc = imp.find_module(name, path)
                return self
        except:
            LOG.error('%s: %s', sys.exc_info()[0].__name__, sys.exc_info()[1])
            raise
 
 
    def load_module(self, fullname):
        if fullname in sys.modules:
            return sys.modules[fullname]
 
        return imp.load_module(fullname, self.file, self.filename, self.etc)
 
 
def setup():
    sys.meta_path += [ImpLoader()]
 
