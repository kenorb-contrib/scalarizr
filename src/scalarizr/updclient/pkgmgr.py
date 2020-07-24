import os
import re
import gzip
import json
import time
import shutil
import string
import hashlib
import logging
import itertools
import posixpath
from datetime import datetime
from cStringIO import StringIO
from operator import attrgetter
import xml.etree.ElementTree as ET
from abc import ABCMeta, abstractmethod
from distutils.version import LooseVersion
from collections import namedtuple, defaultdict, Sized

import requests

import scalarizr
from scalarizr import util
from scalarizr.linux import system, coreutils

LOG = logging.getLogger(__name__)


STORAGE_DIR = "/var/cache/scalr/updclient/pkgmgr"
ARCHIVE_SIZE = 2


class Error(Exception):
    pass

class RepositoryError(Error):
    pass

class IntegrityError(Error):
    pass

class PackageNotFoundError(Error):
    pass

class RpmError(Error):
    pass

class DpkgError(Error):
    pass

class MetadataError(Error):
    pass


def requests_get(url, **kwargs):
    exc_class = kwargs.pop('exc_class')
    message = kwargs.pop('exc_message')
    try:
        r = requests.get(url, **kwargs)
    except requests.RequestException as e:
        raise exc_class("{0} Requests error: {1}".format(message, e))
    if not r.ok:
        raise exc_class("{0} HTTP error code: {1}".format(message, r.status_code))
    return r


class Metadata(object):
    """
    A simple json db to store metadata for fetched packages.
    """
    def __init__(self, path):
        self._metadata = {}
        self._metadata_path = os.path.join(path, 'metadata')
        self.exists = os.path.exists(self._metadata_path)
        if self.exists:
            try:
                with open(self._metadata_path) as f:
                    LOG.debug('Loading metadata')
                    self._metadata = json.load(f)
            except ValueError:
                raise MetadataError("Invalid meadata file: {0}".format(self._metadata_path))


    def get(self, value, default=None):
        return self._metadata.get(value, default)


    def set(self, **kwargs):
        self._metadata.update(kwargs)
        with open(self._metadata_path, 'w') as f:
            LOG.debug('Saving metadata')
            json.dump(self._metadata, f)


Package = namedtuple('Package', "name, version, location, hash, hash_type")


class PackageList(Sized):
    """
    Packages stored as a dict of lists. Need this because we can
    have many versions of the same package in the repo.
    """
    def __init__(self, packages):
        self._packages = defaultdict(list)
        for package in packages:
            self._packages[package.name].append(package)


    def __len__(self):
        return sum(len(entry) for entry in self._packages.itervalues())


    def get_all(self, name):
        packages = self._packages.get(name)
        if not packages:
            raise PackageNotFoundError("Package {0} can't be found in the repository".format(name))
        return packages


    def get(self, name, version=None):
        packages = self.get_all(name)
        if version:
            for pkg in packages:
                if pkg.version == version:
                    return pkg
            raise PackageNotFoundError("Package {0} version {1} can't be found in the repository".format(name, version))
        else:
            # Return the newest version if it's not specified
            return max(packages, key=lambda pkg: LooseVersion(pkg.version))


class AbstractPackageManager(object):
    """
    Common functionality for fetching, installing, querying status and updating history.
    History code is platform-independent and other methods use a lot of template methods
    that do the job on the actual platforms (rpm/dpkg).
    """

    __metaclass__ = ABCMeta

    def __init__(self, repo_string):
        self._set_repo_string(repo_string)


    @abstractmethod
    def _set_repo_string(self, repo_string):
        pass


    @abstractmethod
    def _get_repo_metadata(self):
        pass


    @abstractmethod
    def _parse_repo_metadata(self, metadata):
        pass


    @abstractmethod
    def _get_packages_metadata(self, path, checksum):
        pass


    @abstractmethod
    def _parse_packages_metadata(self, metadata):
        pass


    @abstractmethod
    def _get_installed_version(self, name):
        pass


    @abstractmethod
    def _install(self, packages_path, metadata):
        pass


    def _get_history(self, name):
        LOG.debug('Getting history')
        history = []
        package_dir = os.path.join(STORAGE_DIR, name)
        if os.path.exists(package_dir):
            for hash in os.listdir(package_dir):
                metadata = Metadata(os.path.join(package_dir, hash))
                if not metadata.exists:
                    continue
                history_entry = {'hash': hash,
                                 'version': metadata.get('version'),
                                 'download_date': datetime.fromtimestamp(metadata.get('download_date'))}
                if metadata.get('install_date'):
                    history_entry['install_date'] = datetime.fromtimestamp(metadata.get('install_date'))
                history.append(history_entry)
        return history


    def _update_fetch_history(self, storage_dir, package, deps):
        LOG.debug('Updating history')
        # Add index to allow access to our fetched files only knowing the hash
        index = os.path.join(STORAGE_DIR, '.index')
        if not os.path.exists(index):
            os.makedirs(index)
        symlink = os.path.join(index, package.hash)
        if not os.path.exists(symlink):
            os.symlink(storage_dir, symlink)

        # Write down version, date and package order
        Metadata(storage_dir).set(version=package.version,
                                  download_date=time.time(),
                                  packages=map(lambda pkg: posixpath.basename(pkg.location), deps + [package]))


    def _cleanup_history(self, name):
        """
        We only keep last `ARCHIVE_SIZE` versions in our cache. Remove the rest.
        """
        history = sorted(self._get_history(name),
                         key=lambda history: history['download_date'],
                         reverse=True)
        old_entries = list(history)[ARCHIVE_SIZE:]

        for entry in old_entries:
            if entry['version'] == self._get_installed_version(name):
                continue
            path = os.path.join(STORAGE_DIR, name, entry['hash'])
            LOG.debug("Removing old entry from the pkgmgr cache: %s", path)
            shutil.rmtree(path)
            symlink = os.path.join(STORAGE_DIR, '.index', entry['hash'])
            if os.path.lexists(symlink):
                LOG.debug("Removing symlink to the old entry in the pkgmgr cache: %s", symlink)
                os.unlink(symlink)

        # We can have a directory without metadata file, which means
        # UpdClient was interrupted during fetch. Let's clean this up.
        # [SCALARIZR-1854]
        pkg_dir = os.path.join(STORAGE_DIR, name)
        if os.path.exists(pkg_dir):
            broken_entries = [os.path.join(pkg_dir, dir) for dir in os.listdir(pkg_dir)
                              if 'metadata' not in os.listdir(os.path.join(pkg_dir, dir))]

            for entry in broken_entries:
                LOG.debug("Removing broken entry from the pkgmgr cache: %s", entry)
                shutil.rmtree(entry)


    def _update_install_history(self, metadata):
        metadata.set(install_date=time.time())


    def updatedb(self):
        LOG.info('Syncing repository info')
        repo_metadata = self._get_repo_metadata()
        packages_path, checksum = self._parse_repo_metadata(repo_metadata)
        packages_metadata = self._get_packages_metadata(packages_path, checksum)
        packages = self._parse_packages_metadata(packages_metadata)
        self.packages = PackageList(packages)


    def fetch(self, name, version, deps=None):
        """
        Directory structure for fetched files:

        STORAGE_DIR/
            .index/
                $hash (symlinks to the actual folders below)
                $hash
                $hash
            $name/
                $hash/
                    package1.rpm
                    package2.rpm
                    metadata
                $hash/
                    package1.rpm
                    package2.rpm
                    metadata
            $name/
                $hash/
                    package1.rpm
                    package2.rpm
                    metadata

        Where `metadata` is a json containing file where we store version,
        download date, install date and package installation order.
        """

        def _fetch(pkg, dir):
            hasher = hashlib.new(pkg.hash_type)
            path = os.path.join(dir, posixpath.basename(pkg.location))
            LOG.debug("Fetching package from %s", path)
            r = requests_get(posixpath.join(self.baseurl, pkg.location), stream=True,
                             exc_class=RepositoryError,
                             exc_message="Cannot fetch packages from the repository")
            with open(path, 'wb') as f:
                for chunk in r.iter_content(1024):
                    f.write(chunk)
                    hasher.update(chunk)
            if pkg.hash != hasher.hexdigest():
                os.unlink(path)
                raise IntegrityError("Package {0} checksum doesn't match".format(name))

        self._cleanup_history(name)
        LOG.info('Fetching packages')
        package = self.packages.get(name, version)
        storage_dir = os.path.join(STORAGE_DIR, name, package.hash)
        if not os.path.exists(storage_dir):
            os.makedirs(storage_dir)

        deps = [self.packages.get(**dep) for dep in deps] if deps else []
        for pkg in deps + [package]:
            _fetch(pkg, storage_dir)

        self._update_fetch_history(storage_dir, package, deps)
        return package.hash


    def status(self, name):
        package_versions = self.packages.get_all(name)

        versions_available = list(sorted(map(attrgetter('version'), package_versions), key=LooseVersion))
        version_installed = self._get_installed_version(name)
        history = self._get_history(name)
        result = {'name': name,
                  'installed': version_installed,
                  'available': versions_available,
                  'history': history}

        max_available = self.packages.get(name).version
        update_available = LooseVersion(max_available) > LooseVersion(version_installed or '0')
        result['candidate'] = max_available if update_available else None

        for entry in history:
            if entry['version'] == version_installed:
                result['installed_hash'] = entry['hash']
                break
        return result


    def install(self, hash):
        LOG.info('Installing packages')
        packages_path = os.path.join(STORAGE_DIR, '.index', hash)
        LOG.debug("Installing packages from %s", packages_path)
        if not os.path.exists(packages_path):
            raise PackageNotFoundError("Package hashed as {0} can't be found in the cache directory {1}".format(hash, STORAGE_DIR))
        metadata = Metadata(packages_path)
        self._install(packages_path, metadata)
        self._update_install_history(metadata)


class YumManager(AbstractPackageManager):
    REPO_NAMESPACE = "{http://linux.duke.edu/metadata/repo}"
    COMMON_NAMESPACE = "{http://linux.duke.edu/metadata/common}"

    def _set_repo_string(self, url):
        # We might get an url in this format: http://rpm-delayed.scalr.net/rpm/rhel/$releasever/$basearch
        # Substitute $releasever and $basearch with actual values if they are present.
        release = str(scalarizr.linux.os['release'])
        if scalarizr.linux.os['name'] != 'Amazon':
            release = release.split('.')[0]
        url = re.sub(r'\$releasever', release, url)
        url = re.sub(r'\$basearch', scalarizr.linux.os['arch'], url)

        self.baseurl = url


    def _get_repo_metadata(self):
        r = requests_get(posixpath.join(self.baseurl, "repodata/repomd.xml"),
                         exc_class=RepositoryError,
                         exc_message="Cannot retrie repo metadata from {0}.".format(self.baseurl))
        return r.text


    def _parse_repo_metadata(self, metadata):
        ns = YumManager.REPO_NAMESPACE
        tree = ET.fromstring(metadata)
        data = tree.findall(ns+"data")
        for el in data:
            if el.attrib.get('type') == 'primary':
                location = el.find(ns+"location")
                primary_path = location.attrib['href']
                checksum = el.find(ns+"checksum")
                checksum_type = checksum.attrib['type'].lower()
                # From the createrepo man: `The older default was "sha", which is actually "sha1"`
                checksum_type = 'sha1' if checksum_type == 'sha' else checksum_type
        return primary_path, {'type': checksum_type, 'value': checksum.text}


    def _get_packages_metadata(self, path, checksum):
        r = requests_get(posixpath.join(self.baseurl, path),
                         exc_class=RepositoryError,
                         exc_message="Cannot get packages metadata")
        hasher = hashlib.new(checksum['type'])
        hasher.update(r.content)
        if checksum['value'] != hasher.hexdigest():
            raise IntegrityError("Packages file has wrong sha1 sum")
        metadata = gzip.GzipFile(fileobj=StringIO(r.content)).read()
        return metadata


    def _parse_packages_metadata(self, metadata):
        ns = YumManager.COMMON_NAMESPACE
        tree = ET.fromstring(metadata)
        entries = tree.findall(ns+"package")

        def make_package(element):
            name = element.find(ns+'name').text
            version = element.find(ns+'version').attrib['ver']
            location = element.find(ns+'location').attrib['href']
            hash = element.find(ns+'checksum')
            hash_type = hash.attrib['type'].lower()
            hash_type = 'sha1' if hash_type == 'sha' else hash_type
            return Package(name, version, location, hash.text, hash_type)

        return map(make_package, entries)


    def _get_installed_version(self, name):
        out, err, returncode = system(('rpm', '-q', '--queryformat', '%{VERSION}', name), raise_exc=False)
        version = None if returncode else out
        return version


    def _install(self, packages_path, metadata):
        packages = tuple(metadata.get('packages'))
        our, err, returncode = system(('rpm', '-U', '--nosignature', '--replacepkgs', '--oldpackage') + packages, cwd=packages_path, exc_class=RpmError)
        # The only way to detect the failure of a postinstall script, because rpm doesn't forward the return code
        if 'scriptlet failed' in err:
            raise RpmError(err)


    def uninstall(self, name):
        LOG.info('Uninstalling packages')
        system(('rpm', '-e', '--noscripts', '--nodeps', name), exc_class=RpmError)


    def purge(self, name):
        self.uninstall(name)


class AptManager(AbstractPackageManager):
    # Default checksum to use for verifing files
    HASH_TYPE = 'SHA1'

    ARCH = 'amd64' if scalarizr.linux.os['arch'] == 'x86_64' else 'i386'

    def _set_repo_string(self, repo_string):
        parts = repo_string.split()
        if len(parts) == 2:
            # APT-plain repo
            # "http://stridercd.scalr-labs.com/apt-plain stable"
            self.plain = True
        elif len(parts) == 3:
            # APT-pool repo
            # "http://stridercd.scalr-labs.com/apt/develop main main"
            self.plain = False
            self.component = parts[2]
        else:
            raise RepositoryError("Invalid repo string: {0}".format(repo_string))
        self.baseurl, self.distr = parts[0], parts[1]

        # Added this to handle the the case when distribution is specified as
        # single slash, like this: http://repo.scalr.net/apt-plain/latest /
        # It breaks `posixpath.join`.
        self.distr = "" if self.distr == "/" else self.distr


    def _get_repo_metadata(self):
        if self.plain:
            url = posixpath.join(self.baseurl, self.distr, "Release")
        else:
            url = posixpath.join(self.baseurl, "dists", self.distr, "Release")
        r = requests_get(url,
                         exc_class=RepositoryError,
                         exc_message="Cannot retrive repo metadata from {0}.".format(url))
        return r.text.split('\n')


    def _parse_repo_metadata(self, metadata):
        if self.plain:
            packages_path = "Packages"
            full_packges_path = posixpath.join(self.baseurl, self.distr, packages_path)
        else:
            packages_path = posixpath.join(self.component, "binary-{}/Packages".format(self.ARCH))
            full_packges_path = posixpath.join(self.baseurl, "dists", self.distr, packages_path)

        hashes = itertools.dropwhile(lambda s: not s.strip().startswith("{0}:".format(AptManager.HASH_TYPE)), metadata)
        packages = itertools.dropwhile(lambda s: not s.strip().endswith(packages_path), hashes)
        try:
            packages_hash = list(packages)[0].strip().split()[0]
        except IndexError:
            LOG.debug("Repository metadata doesn't contain checksums for Packages file.")
            packages_hash = None
        return full_packges_path, packages_hash


    def _get_packages_metadata(self, path, hash):
        r = requests_get(path,
                         exc_class=RepositoryError,
                         exc_message="Cannot retrive packages metadata")
        if hash:
            hasher = hashlib.new(AptManager.HASH_TYPE)
            hasher.update(r.content)
            if hash != hasher.hexdigest():
                raise IntegrityError("Packages file has wrong checksum")
        return r.text


    def _parse_packages_metadata(self, metadata):
        def make_package(entry):
            arch = re.search(r"^Architecture:(.*)", entry, re.M).group(1).strip()
            if arch not in (self.ARCH, 'all'):
                return None

            name = re.search(r"^Package:(.*)", entry, re.M).group(1).strip()
            version = re.search(r"^Version:(.*)", entry, re.M).group(1).strip()
            location = re.search(r"^Filename:(.*)", entry, re.M).group(1).strip()
            hash = re.search(r"^{0}:(.*)".format(AptManager.HASH_TYPE), entry, re.M).group(1).strip()
            return Package(name, version, location, hash, AptManager.HASH_TYPE)

        entries = metadata.strip().split('\n\n')
        return filter(None, map(make_package, entries))


    def _get_installed_version(self, name):
        out, err, returncode = system(('dpkg-query', '-W', '-f=${Version} ${Status}', name), raise_exc=False)
        if returncode == 0:
            version, status = out.split(' ', 1)
            if status == 'install ok installed':
                return version
        return None


    def _install(self, packages_path, metadata):
        # Restore system state if we have half-configured packages before install
        def dpkg_configure(raise_exc=False):
            cmd = ('dpkg', '--configure', '-a')
            return system(cmd, raise_exc=raise_exc)

        packages = system(("dpkg-query", "-W", "-f=${Status}|${Package}\n"))[0].strip().split('\n')
        problem_packages = (package.split('|')[1] for package in packages
                            if package.split('|')[0] in ('install ok unpacked', 'install ok half-configured'))

        # delete postinst scripts of problem packages
        for name in problem_packages:
            postinst_path = '/var/lib/dpkg/info/{0}.postinst'.format(name)
            coreutils.remove(postinst_path)

        dpkg_configure(raise_exc=True)

        packages = tuple(metadata.get('packages'))
        system(('dpkg', '-i', '--force-downgrade') + packages, cwd=packages_path, exc_class=DpkgError)


    def uninstall(self, name):
        LOG.info('Uninstalling package')
        system(('dpkg', '-r', '--force-depends', name), exc_class=DpkgError)


    def purge(self, name):
        LOG.info('Purging package')
        system(('dpkg', '-P', '--force-depends', name), exc_class=DpkgError)


class DepricatedWinManager(object):
    """
    Extracted from scalarizr/linux/pkgmgr.py
    """
    def __init__(self, repo_string):
        self.index = {}
        self.repo_string = repo_string
        if not re.search(r'{0}/?'.format(scalarizr.linux.os['arch']), repo_string):
            self.repo_string = posixpath.join(repo_string, scalarizr.linux.os['arch'])


    def updatedb(self):
        LOG.info('Syncing repository info')
        index_file = posixpath.join(self.repo_string, 'index')
        LOG.debug('Fetching index file %s', index_file)
        r = requests_get(index_file, exc_class=RepositoryError, exc_message="Cant get repo metadata")
        for line in r.text.strip().split('\n'):
            colums = map(string.strip, line.split(' '))
            colums = filter(None, colums)
            package = colums[0]
            packagefile = colums[1]
            self.index[package] = posixpath.join(self.repo_string, packagefile)


    def status(self, name):
        try:
            installed = '{0}-{1}'.format(util.reg_value('DisplayVersion'), util.reg_value('DisplayRelease'))
        except:
            installed = None
        try:
            candidate = os.path.basename(self.index[name])
            candidate = candidate.split('_', 1)[1].rsplit('.', 2)[0]
        except KeyError:
            candidate = None

        available = [candidate]

        if (LooseVersion(candidate or '0') <= LooseVersion(installed or '0')):
            candidate = None

        LOG.debug('Package %s info. installed: %s, candidate: %s, available: %s', name, installed, candidate, available)
        return {
            'installed': installed,
            'candidate': candidate,
            'available': available,
        }


def create_pkgmgr(repo_url):
    if scalarizr.linux.os['family'] == 'Windows':
        return DepricatedWinManager(repo_url)
    elif scalarizr.linux.os['family'] in ('RedHat', 'Oracle'):
        return YumManager(repo_url)
    return AptManager(repo_url)


if __name__ == '__main__':
    # APT-pool
    #apt = AptManager("deb http://stridercd.scalr-labs.com/apt/develop feature-omnibus-integration main")
    #apt.updatedb()
    #print apt.status('scalarizr')
    #hash = apt.fetch('scalarizr', version='3.1.b7073.271f12f-1', deps=[{'name': 'scalarizr-ec2', 'version': '3.1.b7073.271f12f-1'}])
    #apt.install(hash)
    #print apt.status('scalarizr')

    # APT-plain
    #apt = AptManager("deb http://stridercd.scalr-labs.com/apt-plain stable")
    #apt.updatedb()
    #print apt.status('scalarizr')
    #hash = apt.fetch('scalarizr', version)
    #apt.install(package.hash)
    #print apt.status('scalarizr')

    #RPM
    yum = YumManager("http://rpm.scalr.net/rpm/rhel/latest/x86_64/")
    yum.updatedb()
    print yum.status('python-argparse')
    hash = yum.fetch('python-argparse', version='1.0.1')
    yum.install(hash)
    print yum.status('python-argparse')
    yum.uninstall('python-argparse')
