__author__ = 'spike'


import os
import sys
import shutil
import logging
import urllib2
import tempfile
import mimetypes
from urlparse import urlparse

from scalarizr import linux
from scalarizr.bus import bus
from scalarizr.linux import pkgmgr
from scalarizr.util import system2

class SourceError(BaseException):
    pass


class UndefinedSourceError(SourceError):
    pass

class Source(object):
    def update(self, workdir):
        raise NotImplementedError()

    @staticmethod
    def from_type(srctype, **init_kwargs):
        clsname = srctype.capitalize() + 'Source'
        assert clsname in globals(), 'implementation class %s of source type %s is undefined' % (clsname, srctype)
        return globals()[clsname](**init_kwargs)

class SvnSource(Source):
    EXECUTABLE = '/usr/bin/svn'

    def __init__(self, url=None, login=None, password=None, executable=None):
        self._logger = logging.getLogger(__name__)
        self._client_version = None
        self.url = url if not url.endswith('/') else url[:-1]
        self.login = login
        self.password = password
        self.executable = self.EXECUTABLE

    def update(self, workdir):
        log = bus.init_op.logger if bus.init_op else self._logger
        if not os.access(self.executable, os.X_OK):
            log.info('Installing Subversion SCM...')
            pkgmgr.installed('subversion', updatedb=True)

        do_update = False
        if os.path.exists(os.path.join(workdir, '.svn')):
            out = system2(('svn', 'info', workdir))[0]
            try:
                svn_url = filter(lambda line: line.startswith('URL:'), out.split('\n'))[0].split(':', 1)[1].strip()
            except IndexError:
                raise SourceError('Cannot extract Subversion URL. Text:\n %s', out)
            if svn_url != self.url:
                #raise SourceError('Working copy %s is checkouted from different repository %s' % (workdir, svn_url))
                self._logger.info('%s is not origin of %s (%s is)', self.url, workdir, svn_url)
                self._logger.info('Remove all files in %s and checkout from %s', workdir, self.url)
                shutil.rmtree(workdir)
                os.mkdir(workdir)
            else:
                do_update = True

        args = [
            'svn' ,
            'update' if do_update else 'co'
        ]
        if self.login and self.password:
            args += [
                '--username', self.login,
                '--password', self.password,
                '--non-interactive'
            ]
            if self.client_version >= (1, 5, 0):
                args += ['--trust-server-cert']

        if args[1] == 'co':
            args += [self.url]
        args += [workdir]

        log.info('Updating source from %s into working dir %s', self.url, workdir)
        out = system2(args)[0]
        self._logger.info(out)
        log.info('Deploying %s to %s has been completed successfully.',
                        self.url, workdir)

    @property
    def client_version(self):
        if not self._client_version:
            version_str = system2(('svn', '--version', '--quiet'))[0]
            self._client_version = tuple(map(int, version_str.strip().split('.')))
        return self._client_version


class GitSource(Source):
    EXECUTABLE = '/usr/bin/git'
    ssh_tpl = '#!/bin/bash\nexec ssh -o StrictHostKeyChecking=no -o BatchMode=yes -i %s "$@"'

    def __init__(self, url, ssh_private_key=None, executable=None):
        self._logger = logging.getLogger(__name__)
        self.url = url
        self.executable = executable or self.EXECUTABLE
        self.private_key = ssh_private_key


    def update(self, workdir):
        log = bus.init_op.logger if bus.init_op else self._logger
        if not os.access(self.executable, os.X_OK):
            log.info('Installing Git SCM...')
            if linux.os['family'] == 'Debian':
                package = 'git-core'
            else:
                package = 'git'
            pkgmgr.installed(package, updatedb=True)

        #if not os.path.exists(workdir):
        #    self._logger.info('Creating destination directory')
        #    os.makedirs(workdir)

        tmpdir = tempfile.mkdtemp()
        env = {}

        try:
            if self.private_key:
                pk_path = os.path.join(tmpdir, 'pk.pem')
                with open(pk_path, 'w') as fp:
                    fp.write(self.private_key)
                os.chmod(pk_path, 0400)

                git_ssh_path = os.path.join(tmpdir, 'git_ssh.sh')
                with open(git_ssh_path, 'w') as fp:
                    fp.write(self.ssh_tpl % pk_path)
                os.chmod(git_ssh_path, 0755)

                env.update(dict(GIT_SSH=git_ssh_path))

            if os.path.exists(os.path.join(workdir, '.git')):
                origin_url = system2(('git', 'config', '--get', 'remote.origin.url'), cwd=workdir, raise_exc=False)[0]
                if origin_url.strip() != self.url.strip():
                    self._logger.info('%s is not origin of %s (%s is)', self.url, workdir, origin_url)
                    self._logger.info('Remove all files in %s and checkout from %s', workdir, self.url )
                    shutil.rmtree(workdir)
                    os.mkdir(workdir)

                    out, err, ret_code = system2(('git', 'clone', self.url, workdir), env=env)
                else:
                    log.info('Updating directory %s (git-pull)', workdir)
                    out, err, ret_code = system2(('git', 'pull'), env=env, cwd=workdir)
            else:
                log.info('Checkout from %s', self.url)
                out, err, ret_code = system2(('git', 'clone', '--recursive', self.url, workdir), env=env)

            if ret_code:
                raise Exception('Git failed to clone repository. %s' % out)

            log.info('Successfully deployed %s from %s', workdir, self.url)
        finally:
            shutil.rmtree(tmpdir)



class HttpSource(Source):
    def __init__(self, url=None):
        self._logger = logging.getLogger(__name__)
        self.url = url

    def update(self, workdir):
        log = bus.init_op.logger if bus.init_op else self._logger

        if not os.path.exists(workdir):
            os.makedirs(workdir)

        purl = urlparse(self.url)

        log.info('Downloading %s', self.url)
        try:
            hdlrs = [urllib2.HTTPRedirectHandler()]
            if purl.scheme == 'https':
                hdlrs.append(urllib2.HTTPSHandler())
            opener = urllib2.build_opener(*hdlrs)
            resp = opener.open(self.url)
        except urllib2.URLError, e:
            raise SourceError('Downloading %s failed. %s' % (self.url, e))

        tmpdir = tempfile.mkdtemp(dir='/tmp/')

        tmpdst = os.path.join(tmpdir, os.path.basename(purl.path))
        fp = open(tmpdst, 'w+')
        num_read = 0
        while True:
            buf = resp.read(8192)
            if not buf:
                break
            num_read += len(buf)
            self._logger.debug('%d bytes downloaded', num_read)
            fp.write(buf)
        fp.close()
        log.info('File saved as %s', tmpdst)

        try:
            mime = mimetypes.guess_type(tmpdst)

            if mime[0] in ('application/x-tar', 'application/zip'):
                unar = None
                if mime[0] == 'application/x-tar':
                    unar = ['tar']
                    if mime[1] == 'gzip':
                        unar += ['-xzf']
                    elif mime[1] in ('bzip', 'bzip2'):
                        unar += ['-xjf']
                    else:
                        raise UndefinedSourceError()
                    unar += [tmpdst, '-C', workdir]

                elif mime[0] == 'application/zip':
                    if not linux.which('unzip'):
                        log.info('Installing unzip de-archiver')
                        pkgmgr.installed('unzip', updatedb=True)
                    unar = ['unzip', tmpdst, '-d', workdir]
                else:
                    raise UndefinedSourceError('Unexpected archive format %s' % str(mime))

                log.info('Extracting source from %s into %s', tmpdst, workdir)
                out = system2(unar)[0]
                self._logger.info(out)
            else:
                log.info('Moving source from %s to %s', tmpdst, workdir)
                dst = os.path.join(workdir, os.path.basename(tmpdst))
                if os.path.isfile(dst):
                    self._logger.debug('Removing already existed file %s', dst)
                    os.remove(dst)
                shutil.move(tmpdst, workdir)
                self._logger.info('Deploying %s to %s has been completed successfully.',
                        self.url, dst)

        except:
            exc = sys.exc_info()
            if isinstance(exc[0], SourceError):
                raise
            raise SourceError, exc[1], exc[2]
        finally:
            if os.path.exists(tmpdst):
                os.remove(tmpdst)
            if os.path.exists(tmpdir):
                shutil.rmtree(tmpdir)
