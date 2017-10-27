'''
Created on 22.03.2012
 
@author: sam
'''
 
import os
import sys
import tempfile
 
from scalarizr.util import disttool, dynimp
 
import mock
from nose.tools import raises, assert_equals
import shutil
 
 
 
class TestImpLoader(object):
    def setup(self):
        self.tmp = tempfile.mkdtemp()
        self.site_packages = os.path.join(self.tmp, 'site-packages')
        os.makedirs(self.site_packages)
 
        self.manifest = os.path.dirname(__file__) + '/../../fixtures/util/dynimp-manifest.ini'
 
        disttool.linux_dist = mock.Mock(return_value = ('', '', ''))
        disttool.is_debian_based = mock.Mock(return_value=False)
        disttool.is_redhat_based = mock.Mock(return_value=False)
        disttool.is_fedora = mock.Mock(return_value=False)
 
    def teardown(self):
        shutil.rmtree(self.tmp)
 
    def env_ubuntu(self):
        disttool.linux_dist.return_value = ('Ubuntu', '12.04', 'oneiric')
        disttool.is_redhat_based.return_value = False
        disttool.is_debian_based.return_value = True
 
 
    def env_rhel5(self):
        disttool.linux_dist.return_value = ('redhat', '5.6', 'final')
        disttool.is_redhat_based.return_value = True
        disttool.is_debian_based.return_value = False
 
    def test_redhat_sections(self):
        self.env_rhel5()
        self.imp = dynimp.ImpLoader(self.manifest)
        assert_equals(self.imp.sections, ['yum:el5.6', 'yum:el5', 'yum:el', 'yum'])
 
    def test_ubuntu_sections(self):
        self.env_ubuntu()
        self.imp = dynimp.ImpLoader(self.manifest)
        assert_equals(self.imp.sections, ['apt:ubuntu12.04', 'apt:ubuntu12', 'apt:ubuntu', 'apt'])
 
    def test_redhat_install_python_package(self):
        self.env_rhel5()
 
        self.imp = dynimp.ImpLoader(self.manifest)
        self.imp.mgr.install = mock.Mock()
        self.imp.mgr.candidates = mock.Mock(return_value = ['2.0', '3.10'])
 
        self.imp.install_python_package('PyYAML')
        self.imp.mgr.install.assert_called_with('python26-pyyaml', '3.10')
 
 
    def test_ubuntu_install_python_package(self):
        self.env_ubuntu()
 
        self.imp = dynimp.ImpLoader(self.manifest)
        self.imp.mgr.install = mock.Mock()
        self.imp.mgr.candidates = mock.Mock(return_value = ['2.0', '3.10'])
 
        self.imp.install_python_package('PyYAML')
        self.imp.mgr.install.assert_called_with('python-pyyaml', '3.10')
 
 
    @raises(ImportError)
    def test_install_python_package_os_package_has_no_candidates(self):
        self.env_rhel5()
 
        self.imp = dynimp.ImpLoader(self.manifest)
        self.imp.mgr.install = mock.Mock()
        self.imp.mgr.candidates = mock.Mock(return_value = [])
 
        self.imp.install_python_package('Freak')
        assert False, 'ImportError expected but never raised'
 
    @raises(ImportError)
    def test_install_python_package_no_os_package_mapping(self):
        self.env_rhel5()
 
        self.imp = dynimp.ImpLoader(self.manifest)
        self.imp.mgr.install = mock.Mock()
 
        self.imp.install_python_package('unknown-package')
        assert False, 'ImportError expected but never raised'
 
 
    def test_import(self):
        self.env_ubuntu()
 
        self.imp = dynimp.ImpLoader(self.manifest)
        def install(*args):
            open(self.site_packages + '/mypackage.py', 'w').close()
        self.imp.mgr.install = mock.Mock(side_effect=install)
        self.imp.mgr.candidates = mock.Mock(return_value = ['1.01a-1ubuntu0'])
 
        sys.meta_path += [self.imp]
        sys.path += [self.site_packages]
        try:
            __import__('mypackage')
            assert 'mypackage' in sys.modules
        finally:
            sys.path.remove(self.site_packages)
            sys.meta_path.remove(self.imp)
 
    def test_import_subpackage(self):
        self.env_ubuntu()
 
        self.imp = dynimp.ImpLoader(self.manifest)
        def install(*args):
            os.makedirs(self.site_packages + '/mypackage2/mysubpackage')
            open(self.site_packages + '/mypackage2/__init__.py', 'w').close()
            open(self.site_packages + '/mypackage2/mysubpackage/__init__.py', 'w').close()
            open(self.site_packages + '/mypackage2/mysubpackage/mymodule.py', 'w').close()
        self.imp.mgr.install = mock.Mock(side_effect=install)
        self.imp.mgr.candidates = mock.Mock(return_value = ['1.0'])
 
        sys.meta_path += [self.imp]
        sys.path += [self.site_packages]
        try:
            __import__('mypackage2.mysubpackage.mymodule')
            assert 'mypackage2.mysubpackage' in sys.modules
            assert 'mymodule' in sys.modules['mypackage2.mysubpackage'].__dict__
        finally:
            sys.path.remove(self.site_packages)
            sys.meta_path.remove(self.imp)
 
    def test_std(self):
        self.imp = dynimp.ImpLoader(self.manifest)
        sys.meta_path += [self.imp]
 
        from xml.dom.minidom import parseString
 
