'''
Created on Nov 2, 2012
 
@author: uty
'''
import os
 
from scalarizr import linux
from scalarizr.linux import pkgmgr
 
import mock
 
@mock.patch.dict('scalarizr.linux.os', {'family': 'RedHat'})
def test_package_mgr():
    mgr = pkgmgr.package_mgr()
    assert isinstance(mgr, pkgmgr.YumPackageMgr)
 
 
@mock.patch.dict('scalarizr.linux.os', {'family': 'RedHat', 'name': 'CentOS'})
@mock.patch.object(pkgmgr.RpmPackageMgr, 'info')
@mock.patch.object(pkgmgr.RpmPackageMgr, 'install')
@mock.patch('scalarizr.linux.system')
def test_epel_repository(system, install, info):
    info.return_value = {'installed': None}
    pkgmgr.epel_repository()
    install.assert_called_once_with(pkgmgr.EPEL_RPM_URL)
 
 
@mock.patch.dict('scalarizr.linux.os', {'family': 'Debian',
                                                                                'codename1': 'c1',
                                                                                'codename12': 'c12',
                                                                                'codename2': 'c2',
                                                                                'codename22': 'c22',
                                                                                'arch': 'x86_64'})
@mock.patch('__builtin__.open')
@mock.patch('scalarizr.linux.system')
def test_apt_source(system, open):
    name = 'test_list'
    sources = ['deb http://test.repo/apt/${arch} ${codename1} ${codename12} main',
                       'deb-src http://test.repo/apt ${codename2} ${codename22} main']
    gpg_keyserver = 'key_server'
    gpg_keyid = 'key_id'
    file_contents = 'deb http://test.repo/apt/x86_64 c1 c12 main\ndeb-src http://test.repo/apt c2 c22 main'
    pkgmgr.apt_source(name, sources, gpg_keyserver, gpg_keyid)
 
    open.assert_called_once_with('/etc/apt/sources.list.d/'+name, 'w+')
    write_mock = open.return_value.__enter__().write
    write_mock.assert_called_once_with(file_contents)
 
    linux.system.assert_called_with(('apt-key', 'adv',
                                                                              '--keyserver', 'key_server',
                                                                              '--recv', 'key_id'),
                                                                             raise_exc=False)
 
 
@mock.patch('scalarizr.linux.pkgmgr.package_mgr')
def test_installed(mgr):
    mgr().info.return_value = {'installed': None}
 
    pkgmgr.installed('thing', '1.0', True)
 
    mgr().install.assert_called_once_with('thing', '1.0')
 
 
@mock.patch('scalarizr.linux.pkgmgr.package_mgr')
def test_latest(mgr):
    mgr().info.return_value = {'candidate': None,
                                                       'installed': None}
    pkgmgr.latest('thing', True)
    mgr().install.assert_called_once_with('thing', None)
 
    mgr().info.return_value = {'candidate': '2.0',
                                                       'installed': None}
    pkgmgr.latest('thing', False)
    mgr().install.assert_called_with('thing', '2.0')
 
 
@mock.patch('scalarizr.linux.pkgmgr.package_mgr')
def test_removed(mgr):
    mgr().installed.return_value = True
 
    pkgmgr.removed('thing', True)
 
    mgr().remove.assert_called_once_with('thing', True)
 
 
class TestYumPackageMgr(object):
    @mock.patch('scalarizr.linux.system')
    def test_repos(self, s):
        fixture = os.path.dirname(__file__) + '/../../fixtures/linux/yum.repolist.out'
        s.return_value = [open(fixture).read(), '', 0]
 
        mgr = pkgmgr.YumPackageMgr()
        repos = sorted(mgr.repos())
 
        assert repos, ['aegisco', 'fedora', 'updates']
 
 
class TestAptPackageMgr(object):
    @mock.patch('glob.glob')
    def test_repos(self, g):
        g.return_value = [
                        '/etc/apt/sources.list.d/percona.list',
                        '/etc/apt/sources.list.d/scalr-stable.list']
 
        mgr = pkgmgr.AptPackageMgr()
        repos = mgr.repos()
 
        assert repos, ['percona', 'scalr-stable']
 
 
#RpmPackageMgr class tests
class TestRPMPackageMgr(object):
 
    @mock.patch('scalarizr.linux.system')
    def test_rpm_command(self, system):
        mgr = pkgmgr.RpmPackageMgr()
        mgr.rpm_command('-Uvh test.rpm', raise_exc=True)
 
        system.assert_called_once_with(['/usr/bin/rpm', '-Uvh', 'test.rpm'], raise_exc=True)
 
 
    @mock.patch.object(pkgmgr.RpmPackageMgr, 'rpm_command')
    def test_install(self, rpm_command):
        mgr = pkgmgr.RpmPackageMgr()
        mgr.install('test.rpm', 1.1, True, test_kwd=True)
 
        rpm_command.assert_called_once_with('-Uvh test.rpm', raise_exc=True, test_kwd=True)
 
 
    @mock.patch.object(pkgmgr.RpmPackageMgr, 'rpm_command')
    def test_remove(self, rpm_command):
        mgr = pkgmgr.RpmPackageMgr()
        mgr.remove('test.rpm', True)
 
        rpm_command.assert_called_once_with('-e test.rpm', raise_exc=True)
 
 
    @mock.patch('scalarizr.linux.system')
    def test_updatedb(self, system):
        mgr = pkgmgr.RpmPackageMgr()
        mgr.updatedb()
 
        assert not system.called
 
    @mock.patch.object(pkgmgr.RpmPackageMgr, 'rpm_command')
    def test_info(self, rpm_command):
        test_pkg = 'http://www.site.com/rpms/vim-common-7.3.682-1.fc17.x86_64.rpm'
        rpm_command.return_value = ('vim-common-7.3.682-1.fc17.x86_64', '', 0)
 
        mgr = pkgmgr.RpmPackageMgr()
        result = mgr.info(test_pkg)
 
        rpm_command.assert_called_once_with('-q vim-common-7.3.682-1.fc17.x86_64', raise_exc=False)
        assert result == {'candidate': None,
                                          'installed': '7.3.682-1.fc17.x86_64'}
 
