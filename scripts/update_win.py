__author__ = 'spike'
 
 
import os
import re
import sys
import shutil
import urllib
import tempfile
import urllib2
import logging
import platform
import subprocess
 
import servicemanager
import _winreg as winreg
import win32serviceutil
 
 
logger = logging.getLogger('Update')
 
base_repo_url = "http://buildbot.scalr-labs.com/win"
 
 
class ScalarizrDevTools(win32serviceutil.ServiceFramework):
    _svc_name_            = "szr-devtools"
    _svc_display_name_    = "Scalarizr Devtools"
 
    def SvcDoRun(self):
        servicemanager.LogMsg(servicemanager.EVENTLOG_INFORMATION_TYPE,
                              servicemanager.PYS_SERVICE_STARTED,
                              (self._svc_name_,''))
 
        try:
            logfile_path = os.path.join(os.path.dirname(__file__), 'install.log')
 
            _format = '%(asctime)s - %(message)s'
            logging.basicConfig(level=logging.INFO, format=_format)
            hdlr = logging.FileHandler(logfile_path)
            formatter = logging.Formatter(_format)
            hdlr.setFormatter(formatter)
            logger.addHandler(hdlr)
            logger.setLevel(logging.DEBUG)
 
            logger.info('Starting scalarizr update')
 
            logger.info('Fetching user data')
            userdata_url  = "http://169.254.169.254/latest/user-data"
            try:
                r = urllib2.urlopen(userdata_url)
                raw_userdata = r.read().strip()
                userdata = {}
                for k, v in re.findall("([^=]+)=([^;]*);?", raw_userdata):
                    userdata[k] = v
                branch = userdata['custom.scm_branch']
                branch = branch.replace('/','-').replace('.','').strip()
            except:
                e = sys.exc_info()[1]
                logger.debug('Could not obtain userdata: %s. Using master branch' % e)
                branch = 'master'
 
            logger.info('Detecting architecture')
            arch = platform.uname()[4]
            if '64' in arch:
                arch = 'x86_64'
            elif '86' in arch:
                arch = 'i386'
            else:
                raise Exception('Unknown architecture "%s"' % arch)
            logger.info('Architecture: %s', arch)
 
            packages = dict()
            index = urllib2.urlopen('/'.join((base_repo_url, branch, arch, 'index'))).read().strip()
            for pkg in index.splitlines():
                pkg_name, pkg_latest = pkg.split(None, 1)
                packages[pkg_name] = pkg_latest
 
            try:
                latest_package_url = '/'.join((base_repo_url, branch, arch, packages['scalarizr']))
            except KeyError:
                logger.error('Repository does not contain scalarizr package')
                sys.exit(1)
 
            tmp_dir = tempfile.mkdtemp()
            file_path = os.path.join(tmp_dir, 'scalarizr.exe')
            logger.info('Downloading newest scalarizr package. URL: %s', latest_package_url)
 
            try:
                ''' Download install package '''
                urllib.urlretrieve(latest_package_url, file_path)
 
                logger.info('Stopping scalarizr service.')
                try:
                    win32serviceutil.StopService('Scalarizr')
                except:
                    pass
 
                logger.info('Running package.')
                p = subprocess.Popen('start "Installer" /wait "%s" /S' % file_path, shell=True)
                err = p.communicate()[1]
                if p.returncode:
                    raise Exception("Error occured while installing scalarizr: %s" % err)
                logger.info('Successfully installed scalarizr' )
            finally:
                shutil.rmtree(tmp_dir)
        except (Exception, BaseException), e:
            logger.info("Update failed. %s", e)
            sys.exit(1)
 
 
if __name__ == '__main__':
    if '--install' in sys.argv:
        sys.argv = [sys.argv[0], '--startup', 'auto', 'install']
        win32serviceutil.HandleCommandLine(ScalarizrDevTools)
        win32serviceutil.StartService(ScalarizrDevTools._svc_name_)
 
 
