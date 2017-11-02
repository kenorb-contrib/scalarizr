'''
Created on Aug 28, 2012

@author: marat
'''

import logging
import glob
import re
import os
import sys
import string
import time
import urllib2
import subprocess
import shutil
import tempfile
import distutils.version

from scalarizr import linux, util
from scalarizr.linux import coreutils
from scalarizr.bus import bus
from urlparse import urlparse

from pkg_resources import parse_requirements

LOG = logging.getLogger(__name__)


class Repository(object):
    filename_tpl = None
    config_tpl = '{url}'

    def __init__(self, name, url):
        kwds = {
            'name': name,
            'url': url,
            'arch': linux.os['arch']
        }
        self.name = name
        self.url = url.format(**kwds)
        self.filename = self.filename_tpl.format(**kwds)
        self.config = self.config_tpl.format(**kwds)

    def ensure(self):
        assert self.filename and self.config
        LOG.info('Ensuring repository "%s" (file: %s)', self.name, self.filename)
        with open(self.filename, 'w+') as fp:
            fp.write(self.config)

    def update_config_file(self, linefn):
        tmpname = '%s.tmp' % self.filename
        dst = open(tmpname, 'w')
        try:
            for line in open(self.filename, 'r'):
                dst.write(linefn(line))
            dst.close()
        except:
            os.remove(tmpname)
        else:
            shutil.move(tmpname, self.filename)
        
    def _comment(self, line):
        return '#' + line
    
    def _uncomment(self, line):
        return line[1:] if line.startswith('#') else line
    
    def disable(self):
        if self.enabled():
            self.update_config_file(self._comment)
    
    def enable(self):
        if not self.enabled():
            self.update_config_file(self._uncomment)

    def enabled(self):
        return not all(line.startswith('#') for line in open(self.filename))


class PackageMgr(object):
    backup_dir = '/var/cache/scalr/pkgmgr'
    backup_count = 5


    def info(self, name):
        '''
        Returns info about package
        Example:
                {'installed': '2.6.7-ubuntu1',
                'candidate': '2.6.7-ubuntu5',
                'backup_id': '20140129171501-2.6.7-ubuntu1'}
        installed is None, if package not installed
        candidate is None, if latest version of package is installed
        backup_id is None, if no backup was created with a package installation
        '''
        installed, candidate = self._installed_and_candidate(name)
        backup_dir = self._find_backup_dir(name, installed) if installed else None
        return {
            'installed': installed,
            'candidate': candidate if candidate and installed != candidate else None,
            'backup_id': os.path.basename(backup_dir) if backup_dir else None
        }


    def install(self, name, version=None, updatedb=False, backup=False, **kwds):
        '''
        Installs a `version` of package `name`
        '''
        name_version = self._format_name_version(name, version)
        if updatedb:
            self.updatedb()

        if backup:
            backup_id = self._create_backup(name, version, [])
            backup_dir = os.path.join(self.backup_dir, name, backup_id)
            try:
                # In some cases package installation order is important:
                # when A depends from B it's better to install B first.
                # high level managers like Apt/Yum does this, 
                # but here we use lower level Dpkg/Rpm - they just install packages one by one.
                # so we should get here installation order from Apt/Yum and use it with Dpkg/Rpm.
                # 
                # Apt downloads files in correct installation order, so we can use ctime
                # to maintain order. 
                # 
                # We havn't checked Yum for download order, 
                # but as problems with dependencies were only on Debian-based distros,
                # for RedHat-based it should matter

                self._install_download_only(name_version, backup_dir, **kwds)
                files = list(os.path.join(backup_dir, name) 
                        for name in os.listdir(backup_dir))
                files.sort(lambda x, y: cmp(os.stat(x).st_ctime, os.stat(y).st_ctime))
                self._install_file(*files)
            except:
                shutil.rmtree(backup_dir)
                raise
        else:
            self._install_package(name_version, **kwds)


    def remove(self, name, purge=False):
        ''' Removes package with given name. '''
        raise NotImplementedError()


    def list(self):
        '''
        Returns dict of installed packages
        :returns: dict
        Example:
            {
                'python': '2.6.7-ubuntu1',
            }
        '''
        raise NotImplementedError()


    def updatedb(self, **kwds):
        ''' Updates package manager internal database '''
        raise NotImplementedError()


    def repos(self):
        ''' List enabled repositories '''
        raise NotImplementedError()


    def version_cmp(self, name_1, name_2):
        ''' Compares 2 package versions '''
        raise NotImplementedError()


    def installed(self, name, version=None, updatedb=False, **kwds):
        '''
        Ensure that package installed
        '''
        if updatedb:
            self.updatedb()

        installed = self.info(name)['installed']
        if not installed:
            LOG.info('Installing %s=%s', name, version or 'latest')
            self.install(name, version, **kwds)


    def latest(self, name, updatedb=True, **kwds):
        '''
        Ensure that latest version of package installed
        '''
        if updatedb:
            self.updatedb()

        info_dict = self.info(name)
        candidate = info_dict['candidate']
        installed = info_dict['installed']

        if candidate or not installed:
            LOG.info('Installing %s=%s', name, candidate)
            self.install(name, candidate, **kwds)


    def removed(self, name, purge=False):
        '''
        Ensure that package removed (purged)
        '''
        installed = self.info(name)['installed']
        if purge or installed:
            if installed:
                LOG.info('Uninstalling %s=%s', name, installed)
            else:
                LOG.info('Uninstalling %s', name)
            self.remove(name, purge)


    def _format_name_version(self, name, version):
        raise NotImplementedError()


    def _installed_and_candidate(self, name):
        raise NotImplementedError()


    def _install_package(self, name_version, **kwds):
        raise NotImplementedError()


    def _install_file(self, *files):
        raise NotImplementedError()

    def _install_download_only(self, name_version, download_dir, **kwds):
        raise NotImplementedError()


    def _create_backup(self, name, version, package_files):
        # Keep only latest self.backup_count backups
        backup_dir = os.path.join(self.backup_dir, name)
        if os.path.exists(backup_dir):
            names_to_delete = list(reversed(sorted(os.listdir(backup_dir))))[self.backup_count:]
            for name in names_to_delete:
                path = os.path.join(backup_dir, name)
                shutil.rmtree(path)

        # Create new backup dir and copy files
        backup_id = '{0}-{1}'.format(version, time.strftime('%Y%m%d%H%S')) 
        backup_dir = os.path.join(backup_dir, backup_id)
        if not os.path.exists(backup_dir):
            os.makedirs(backup_dir, 0755)
        for filename in package_files:
            shutil.copy(filename, backup_dir)

        return backup_id


    def _find_backup_dir(self, name, version):
        backup_dir = os.path.join(self.backup_dir, name, version + '-*')
        try:
            return glob.glob(backup_dir)[0]
        except IndexError:
            return None


    def restore_backup(self, name, backup_id):
        backup_dir = os.path.join(self.backup_dir, name, backup_id)
        msg = 'Failed to restore package {0} from backup {1}'.format(name, backup_id)
        try:
            files = [os.path.join(backup_dir, f) for f in os.listdir(backup_dir)]
            self._install_file(*files)
        except:
            raise Exception('%s. %s' % (msg, sys.exc_info()[1]))


class AptPackageMgr(PackageMgr):
    def updatedb(self, **kwds):
        try:
            coreutils.clean_dir('/var/lib/apt/lists/partial', recursive=False)
        except OSError:
            pass
        path = '/var/lib/apt/lists'
        for name in os.listdir(path):
            filename = os.path.join(path, name)
            if name != 'lock' and os.path.isfile(filename):
                os.remove(filename)
        cmd = ''
        if kwds.get('apt_repository'):
            cmd += ('--no-list-cleanup '
                    '-c {0}/updclient/apt-preserve-update-success-stamp.conf '
                    '-o Dir::Etc::sourcelist=sources.list.d/{1}.list '
                    '-o Dir::Etc::sourceparts=- '
                    ).format(bus.share_path, kwds['apt_repository'])
        cmd += 'update'
        try:
            self.apt_get_command(cmd)
        except linux.LinuxError, e:
            if e.returncode != 100:
                raise


    def repos(self):
        files = glob.glob('/etc/apt/sources.list.d/*.list')
        names = [os.path.basename(os.path.splitext(f)[0]) for f in files]
        return names


    def list(self):
        '''
        Returns dict of installed packages
        :returns: dict
        Example:
            {
                'python':'2.6.7-ubuntu1',
            }
        '''
        out, err, code = linux.system(
                ("dpkg-query", "-W", "-f=${Status}|${Package}|${Version}\n",),
                raise_exc=True
                )
        if err:
            raise Exception("'dpkg-query -W' command failed. Out: %s \nErrors: %s" % (out, err))
        pkgs = dict([(line.split('|')[1], line.split('|')[2].split(':')[-1]) \
                for line in out.split('\n') if line.split('|')[0] in ('install ok installed', 'hold ok installed')])
        return pkgs


    def remove(self, name, purge=False):
        command = 'purge' if purge else 'remove'
        try:
            self.apt_get_command('%s %s' % (command, name), raise_exc=True)
        except linux.LinuxError, e:
            if 'Unable to locate package {0}'.format(name) not in e.err:
                raise


    def restore_backup(self, name, backup_id):
        def dpkg_configure(raise_exc=False):
            cmd = ('dpkg', '--configure', '-a')
            return linux.system(cmd, raise_exc=raise_exc)

        packages = linux.system(("dpkg-query", "-W", "-f=${Status}|${Package}\n"))[0].strip().split('\n')
        problem_packages = (package.split('|')[1] for package in packages
                            if package.split('|')[0] in ('install ok unpacked', 'install ok half-configured'))

        # delete postinst scripts of problem packages
        for name in problem_packages:
            postinst_path = '/var/lib/dpkg/info/{0}.postinst'.format(name)
            coreutils.remove(postinst_path)

        dpkg_configure(raise_exc=True)

        return super(AptPackageMgr, self).restore_backup(name, backup_id)


    def version_cmp(self, name_1, name_2):
        if name_1 == name_2:
            return 0
        return_code = linux.system(('/usr/bin/dpkg', '--compare-versions', str(name_1), '>', str(name_2)), 
                    raise_exc=False)[2]
        return 1 if not return_code else -1


    def _installed_and_candidate(self, name):
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

        return installed, candidate


    def _format_name_version(self, name, version):
        if version:
            name += '={0}'.format(version)
        return name


    def _install_download_only(self, name_version, download_dir, **kwds):
        partial_dir = os.path.join(download_dir, 'partial')
        if not os.path.exists(partial_dir):
            os.makedirs(partial_dir)
        cmd = 'install --download-only -o=Dir::Cache::Archives={0} {1}'.format(
                download_dir, name_version)
        self.apt_get_command(cmd, raise_exc=True)
        # remove all not *.deb 
        for name in os.listdir(download_dir):
            if not name.endswith('.deb'):
                coreutils.remove(os.path.join(download_dir, name))


    def _install_package(self, name_version, **kwds):
        cmd = 'install {0}'.format(name_version)
        self.apt_get_command(cmd, raise_exc=True) 


    def _install_file(self, *files):
        cmd = ['dpkg', '--install', 
                '--force-downgrade', 
                '--force-confold'] + list(files)
        linux.system(cmd, raise_exc=True)


    def apt_get_command(self, command, **kwds):
        kwds.update(
            env={'DEBIAN_FRONTEND': 'noninteractive',
                'DEBIAN_PRIORITY': 'critical',
                'PATH': '/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/usr/games'},
            error_text='Package manager command failed'
        )
        max_attempt = 17  
        for attempt in range(max_attempt+1):  # timeout approx 3 minutes 
            try:
                return linux.system(('/usr/bin/apt-get',
                                    '-q', '-y', '--force-yes',
                                    '-o Dpkg::Options::=--force-confold') + \
                                    tuple(filter(None, command.split())), **kwds)
            except linux.LinuxError, e:
                if attempt < max_attempt and 'is another process using it?' in e.err \
                        or 'Could not get lock' in e.err:
                    LOG.debug('Could not get dpkg lock (perhaps, another process is using it.)')
                    time.sleep(10)
                    continue
                raise


    def apt_cache_command(self, command, **kwds):
        return linux.system(('/usr/bin/apt-cache',) + tuple(filter(None, command.split())), **kwds)


class AptRepository(Repository):
    filename_tpl = '/etc/apt/sources.list.d/{name}.list'
    config_tpl = 'deb {url}'


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
    def updatedb(self, **kwds):
        self.yum_command('clean all')


    def remove(self, name, purge=False):
        self.yum_command('remove '+name, raise_exc=True)


    def repos(self):
        ret = []
        repo_re = re.compile(r'Repo-id\s+:\s(.*)')
        out = linux.system(('/usr/bin/yum', 'repolist', '--verbose'))[0]
        for line in out.splitlines():
            m = repo_re.search(line)
            if m:
                ret.append(m.group(1))
        return map(string.lower, ret)


    def list(self):
        '''
        Returns dict of installed packages
        :returns: dict
        Example:
            {
                'python':'2.6.7-ubuntu1',
            }
        '''
        out, err, code = linux.system(
                ('rpm', '-qa', '--queryformat', '%{NAME}|%{VERSION}\n',),
                raise_exc=True
                )
        if err:
            raise Exception("'rpm -qa' command failed. Out: %s \nErrors: %s" % (out, err))
        pkgs = dict([_.split('|') for _ in out.split('\n') if _])
        return pkgs





    def version_cmp(self, name_1, name_2):
        return cmp(RpmVersion(name_1), RpmVersion(name_2))


    def _format_name_version(self, name, version):
        if version:
            name += '-{0}'.format(version)
        return name


    def _installed_and_candidate(self, name):
        try:
            out = self.yum_command('list --showduplicates %s' % name)[0].strip()
        except linux.LinuxError, e:
            if 'No matching Packages to list' in e.err:
                return None, None
            else:
                raise

        version_re = re.compile(r'[^\s]+\s+([^\s]+)')
        lines = map(string.strip, out.splitlines())

        try:
            line = lines[lines.index('Installed Packages')+1]
            installed = version_re.match(line).group(1)
        except ValueError:
            installed = None

        if 'Available Packages' in lines:
            versions = [version_re.match(line).group(1) for line in lines[lines.index('Available Packages')+1:]
                        if version_re.match(line)]
        else:
            versions = [None]

        return installed, versions[-1]


    def _install_package(self, name_version, **kwds):
        cmd = 'install {0}'.format(name_version)
        out, err, _ = self.yum_command(cmd, raise_exc=True)
        if kwds.get('rpm_raise_scriptlet_errors') \
            and re.search(r'(Non-fatal|Error in) (PREIN|PRERM|POSTIN|POSTRM) scriptlet', err):
            raise Exception(out)


    def _install_file(self, *files):
        out, err, _ = linux.system(['/usr/bin/rpm', '-U', '--force', '--nodeps'] + list(files))
        if 'scriptlet failed' in err:
            raise Exception(err)


    def _install_download_only(self, name_version, download_dir, version=None, **kwds):
        cmd = 'install --downloadonly --downloaddir={0} {1}'.format(download_dir, name_version)
        code = self.yum_command(cmd, debug_level=1, raise_exc=False)[2]
        if not os.listdir(download_dir):
            msg = 'yum {0} downloaded nothing and exited with code: {1}'.format(cmd, code)
            raise Exception(msg)  


    def localinstall(self, name):
        def do_localinstall(filename):
            self.yum_command('localinstall --nogpgcheck %s' % filename, raise_exc=True)

        if name.startswith('http://'):
            filename = os.path.join('/tmp', os.path.basename(name))
            urlretrieve(name, filename)
            try:
                do_localinstall(filename)
            finally:
                os.remove(filename)
        else:
            do_localinstall(name)


    def yum_command(self, command, debug_level=0, **kwds):
        # explicit exclude was added after yum tried to install iptables.i686
        # on x86_64 amzn
        exclude = ()
        if linux.os["arch"] == "x86_64":
            exclude = (
                "--exclude", "*.i386",
                "--exclude", "*.i486",
                "--exclude", "*.i686",
            )
        elif linux.os["arch"] == "i386":
            exclude = ("--exclude", "x86_64")

        return linux.system((('/usr/bin/yum', '-d{0}'.format(debug_level), '-y') + tuple(filter(None, command.split())) + exclude), **kwds)


class YumRepository(Repository):
    filename_tpl = '/etc/yum.repos.d/{name}.repo'
    config_tpl = (
        '[{name}]\n'
        'name={name}\n'
        'baseurl={url}\n'
        'enabled=1\n'
        'gpgcheck=0\n'
    )


class RpmPackageMgr(PackageMgr):

    def rpm_command(self, command, **kwds):
        return linux.system(['/usr/bin/rpm', ] + filter(None, command.split()), **kwds)


    def install(self, name, version=None, updatedb=False, **kwds):
        ''' Installs a package from file or url with `name' '''
        self.rpm_command('-Uvh '+name, raise_exc=True, **kwds)


    def remove(self, name, purge=False):
        self.rpm_command('-e '+name, raise_exc=True)


    def _version_from_name(self, name):
        ''' Returns version of package that contains in its name
                Example:
                        name = vim-common-7.3.682-1.fc17.x86_64
                        version = 7.3.682-1.fc17.x86_64
        '''
        # TODO: remove architecture info from version string
        name = urlparse(name).path.split('/')[-1]
        name = name.replace('.rpm', '')

        version = re.findall(r'-[0-9][^-]*\..*', name)[0][1:]
        return version


    def info(self, name):
        name = urlparse(name).path.split('/')[-1]
        name = name.replace('.rpm', '')

        out, _, code = self.rpm_command('-q '+name, raise_exc=False)
        installed = not code
        installed_version = self._version_from_name(out)

        return {'installed': installed_version if installed else None,
                'candidate': None,
                'backup_id': None}


    def updatedb(self, **kwds):
        pass


if linux.os.windows_family:
    import posixpath


    class WinPackageMgr(object):
        SOURCES_DIR = os.path.join(util.reg_value('InstallDir'), 'etc')


        def __init__(self):
            self.index = None
   

        def install(self, name, version=None, updatedb=False, **kwds):
            ''' Installs a `version` of package `name` '''
            if not self.index or updatedb:
                self.updatedb()
            packageurl = self.index[name]  # simplification. candidate is always the one.

            tmp_dir = tempfile.mkdtemp()
            try:
                packagefile = os.path.join(tmp_dir, os.path.basename(packageurl))
                LOG.info('Downloading %s', packageurl)
                urlretrieve(packageurl, packagefile)

                LOG.info('Executing installer')
                p = subprocess.Popen('start "Installer" /wait "%s" /S' % packagefile, shell=True)
                out, err = p.communicate()
                LOG.debug('out: %s', out)
                LOG.debug('err: %s', err)
                if p.returncode:
                    msg = 'Installation failed. <out>: %s, <err>: %s' % (out, err)
                    raise Exception(msg)
                LOG.info('Done')
            finally:
                shutil.rmtree(tmp_dir)
    

        def installed_version(self, name):
            ''' Return installed package version '''
            return '{0}-{1}'.format(util.reg_value('DisplayVersion'), util.reg_value('DisplayRelease'))

        
        def _join_packages_str(self, sep, name, version, *args):
            packages = [(name, version)]
            if args:
                for i in xrange(0, len(args), 2):
                    packages.append(args[i:i+2])
            format = '%s' + sep +'%s'
            return ' '.join(format % p for p in packages)       
        

        def updatedb(self, **kwds):
            tmp_dir = tempfile.mkdtemp()

            self.index = {}
            try:        
                LOG.debug('Scan package sources dir')
                for sourcesfile in glob.glob(os.path.join(self.SOURCES_DIR, '*.winrepo')):
                    LOG.debug('Process sources file %s', sourcesfile)
                    urls = open(sourcesfile).read().splitlines()
                    urls = map(string.strip, urls)
                    urls = filter(lambda u: u and not u.startswith('#'), urls)
                    for url in urls:
                        if not re.search(r'{0}/?'.format(linux.os['arch']), url):
                            url = posixpath.join(url, linux.os['arch'])
                        dst = os.path.join(tmp_dir, url.replace('/', '_'))
                        src = posixpath.join(url, 'index')
                        LOG.debug('Fetching index file %s', src)
                        urlretrieve(src, dst)

                        with open(dst) as fp:
                            for line in fp:
                                colums = map(string.strip, line.split(' '))
                                colums = filter(None, colums)
                                package = colums[0]
                                packagefile = colums[1]
                                self.index[package] = posixpath.join(url, packagefile)
            finally:
                shutil.rmtree(tmp_dir)


        def info(self, name):
            if not self.index:
                self.updatedb()

            try:
                installed = self.installed_version(name)
            except:
                installed = None
            try:
                candidate = os.path.basename(self.index[name])
                candidate = candidate.split('_', 1)[1].rsplit('.', 2)[0]
            except KeyError:
                candidate = None
            if installed and installed == candidate:
                candidate = None

            LOG.debug('Package %s info. installed: %s, candidate: %s', name, installed, candidate)
            return {
                'installed': installed,
                'candidate': candidate
            }


        def version_cmp(self, name_1, name_2):
            return cmp(distutils.version.LooseVersion(name_1), 
                distutils.version.LooseVersion(name_2))


        def list(self):
            return []


    class WinRepository(Repository):
        filename_tpl = os.path.join(WinPackageMgr.SOURCES_DIR, '{name}.winrepo')
        config_tpl = '{url}'


def package_mgr():
    if linux.os['family'] == 'Windows':
        return WinPackageMgr()
    elif linux.os['family'] in ('RedHat', 'Oracle'):
        return YumPackageMgr()
    return AptPackageMgr()


def repository(*args, **kwds):
    cls = None
    if linux.os['family'] == 'Windows':
        cls = WinRepository
    elif linux.os['family'] in ('RedHat', 'Oracle'):
        cls = YumRepository
    else:
        cls = AptRepository
    return cls(*args, **kwds)


def epel_repository():
    '''
    Ensure EPEL repository for RHEL based servers.
    Figure out linux.os['arch'], linux.os['release']
    '''
    if linux.os['family'] not in ('RedHat', 'Oracle'):
        return

    mgr = RpmPackageMgr()
    if linux.os['release'] >= (7, 0):
        url = 'http://dl.fedoraproject.org/pub/epel/7/x86_64/e/epel-release-7-5.noarch.rpm'
    elif linux.os['release'] >= (6, 0):
        url = 'http://dl.fedoraproject.org/pub/epel/6/x86_64/epel-release-6-8.noarch.rpm'
    else:
        url = 'http://dl.fedoraproject.org/pub/epel/5/x86_64/epel-release-5-4.noarch.rpm'

    installed = mgr.info(url)['installed']
    if not installed:
        mgr.install(url)


def apt_source(name, sources, gpg_keyserver=None, gpg_keyid=None):
    '''
    @param sources: list of apt sources.list entries.
    Example:
            ['deb http://repo.percona.com/apt ${codename} main',
            'deb-src http://repo.percona.com/apt ${codename} main']
            All ${var} templates should be replaced with
            scalarizr.linux.os['var'] substitution
    if gpg_keyserver:
            apt-key adv --keyserver ${gpg_keyserver} --recv ${gpg_keyid}
    Creates file /etc/apt/sources.list.d/${name}
    '''
    if linux.os['family'] in ('RedHat', 'Oracle'):
        return

    def _vars(s):
        vars_ = re.findall('\$\{.+?\}', s)
        return map((lambda name: name[2:-1]), vars_) #2 is len of '${'

    def _substitude(s):
        for var in _vars(s):
            s = s.replace('${'+var+'}', linux.os[var])
        return s

    prepared_sources = map(_substitude, sources)
    with open('/etc/apt/sources.list.d/' + name, 'w+') as fp:
        fp.write('\n'.join(prepared_sources))

    if gpg_keyserver and gpg_keyid:
        if gpg_keyid not in linux.system(('apt-key', 'list'))[0]:
            linux.system(('apt-key', 'adv',
                                      '--keyserver', gpg_keyserver,
                                      '--recv', gpg_keyid),
                                     raise_exc=False)

def urlretrieve(url, dst):
    resp = urllib2.urlopen(url)
    with open(dst, 'w+') as fp:
        while True:
            buf = resp.read(4096)
            if not buf:
                break
            fp.write(buf)

# Deprecated functions

def updatedb():
    return package_mgr().updatedb()

def installed(name, version=None, updatedb=False):
    return package_mgr().installed(name, version, updatedb)

def latest(name, updatedb=True):
    return package_mgr().latest(name, updatedb)

def removed(name, purge=False):
    return package_mgr().removed(name, purge)


class SoftwareError(Exception):
    pass


class NotInstalledError(SoftwareError):
    pass


class VersionMismatchError(SoftwareError):
    pass


def check_software(required, system_packages=None):
    '''
    :param required: list
        The syntax of a requirement specifier can be defined in EBNF as follows:
            requirement  ::= project_name versionspec? extras?
            versionspec  ::= comparison version (',' comparison version)*
            comparison   ::= '<' | '<=' | '!=' | '==' | '>=' | '>'
            extras       ::= '[' extralist? ']'
            extralist    ::= identifier (',' identifier)*
            project_name ::= identifier
            identifier   ::= [-A-Za-z0-9_]+
            version      ::= [-A-Za-z0-9_.]+
        Some examples of valid requirement specifiers:
            ['FooProject >= 1.2']
            ['FooProject >= 1.2', 'BarProject <= 1.2']
            ['PickyThing<1.6,>1.9,!=1.9.6,<2.0a0,==2.4c1']
            ['SomethingWhoseVersionIDontCareAbout']
    :param system_packages: dict
        Example:
            {
                'python':'2.6.7-ubuntu1',
            }
    '''
    if not required:
        return
    system_packages = system_packages or package_mgr().list()
    installed, not_installed, vers_mismatched = list(), list(), list()
    for requirement in parse_requirements(required):
        name = requirement.project_name
        required_vers = ','.join([''.join(_) for _ in requirement.specs])
        if name not in system_packages:
            not_installed.append((name, required_vers))
            continue
        vers = system_packages[name]
        if vers not in requirement:
            vers_mismatched.append((name, vers))
            continue
        installed.append((name, vers))
    if not_installed:
        raise NotInstalledError(not_installed)
    if vers_mismatched:
        raise VersionMismatchError(vers_mismatched)
    return installed


def check_any_software(required_list, system_packages=None):
    not_installed = list()
    vers_mismatched = list()
    for required in required_list:
        try:
            return check_software(required, system_packages)
        except NotInstalledError:
            not_installed += sys.exc_info()[1][0]
            continue
        except VersionMismatchError:
            vers_mismatched += sys.exc_info()[1][0]
            continue
    else:
        if vers_mismatched:
            raise VersionMismatchError(vers_mismatched)
        if not_installed:
            raise NotInstalledError(not_installed)
        raise

