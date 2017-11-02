from __future__ import with_statement
import re
import os as osmod
import platform
import types
import distutils.version

from scalarizr import util


class LinuxError(util.PopenError):
    pass


def which(exe, path_append=None):
    if exe and exe.startswith('/') and \
                    osmod.access(exe, osmod.X_OK):
        return exe
    exe = osmod.path.basename(exe)
    path = '/bin:/sbin:/usr/bin:/usr/sbin:/usr/libexec:/usr/local/bin'
    if path_append:
        path = '{0}:{1}'.format(path, path_append)
    if osmod.environ.get('PATH'):
        path += ':' + osmod.environ['PATH']
    for p in set(path.split(osmod.pathsep)):
        full_path = osmod.path.join(p, exe)
        if osmod.access(full_path, osmod.X_OK):
            return full_path
    return None


def system(*args, **kwds):
    args = list(args)
    kwds['exc_class'] = LinuxError
    kwds['close_fds'] = system.close_fds
    try:
        is_windows = os.windows
    except NameError:
        # When system is called during 'os' dict initialization
        is_windows = platform.uname()[0].lower() == 'windows'
    if not is_windows and not kwds.get('shell') and \
            not osmod.access(args[0][0], osmod.X_OK):
        executable = which(args[0][0])
        if not executable:
            msg = "Executable '%s' not found" % args[0][0]
            raise LinuxError(msg)
        args[0] = list(args[0])
        args[0][0] = executable
    return util.system2(*args, **kwds)
system.close_fds = platform.uname()[0].lower() != 'windows'


class Version(distutils.version.LooseVersion):
    def __cmp__(self, other):
        if type(other) in (types.TupleType, types.ListType):
            other0 = Version()
            other0.version = list(other)
            other = other0
        return distutils.version.LooseVersion.__cmp__(self, other)


class __os(dict):
    def __init__(self, *args, **kwds):
        dict.__init__(self, *args, **kwds)
        self._detect_dist()
        if not self['family'] == 'Windows':
            self._detect_kernel()


    def __getattr__(self, name):
        name = name.lower()
        if name.endswith('_family'):
            return self['family'].lower() == name[0:-7]
        else:
            return self['name'].lower() == name

    def _detect_dist(self):
        # Detect windows
        _uname = platform.uname()
        if _uname[0].lower() == 'windows':
            win32_ver = platform.win32_ver()
            self['family'] = 'Windows'
            self['name'] = 'Windows'
            self['release'] = win32_ver[1]
            self['codename'] = win32_ver[0]
            self['arch'] = 'x86_64' if platform.uname()[4].upper() == 'AMD64' else 'i386'

        elif osmod.path.isfile('/etc/lsb-release'):
            for line in open('/etc/lsb-release').readlines():
                # Matches any possible format:
                #     DISTRIB_ID="Ubuntu"
                #     DISTRIB_ID='Mageia'
                #     DISTRIB_ID=Fedora
                #     DISTRIB_RELEASE='10.10'
                #     DISTRIB_CODENAME='squeeze'
                #     DISTRIB_DESCRIPTION='Ubuntu 10.10'
                regex = re.compile('^DISTRIB_((?:ID|RELEASE|CODENAME|DESCRIPTION))=(?:\'|")?([\w\s\.-_]+)(?:\'|")?')
                match = regex.match(line)
                if match:
                    # Adds: lsb_{id,release,codename,description}
                    self['lsb_%s' % match.groups()[0].lower()] = match.groups()[1].rstrip()
        try:
            import lsb_release
            release = lsb_release.get_distro_information()
            for key, value in release.iteritems():
                self['lsb_%s' % key.lower()] = value  # override /etc/lsb-release
        except ImportError:
            pass
        if 'lsb_codename' in self:
            self['codename'] = self['lsb_codename']
        if 'lsb_release' in self:
            self['release'] = self['lsb_release']

        if osmod.path.isfile('/etc/arch-release'):
            self['name'] = 'Arch'
            self['family'] = 'Arch'
        elif osmod.path.isfile('/etc/debian_version'):
            self['name'] = 'Debian'
            self['family'] = 'Debian'
            if 'lsb_id' in self:
                if 'GCEL' in self['lsb_id']:
                    self['name'] = 'GCEL'
                if 'Ubuntu' in self['lsb_id']:
                    self['name'] = 'Ubuntu'
                elif osmod.path.isfile('/etc/issue.net') and \
                        'Ubuntu' in open('/etc/issue.net').readline():
                    self['name'] = 'Ubuntu'
        elif osmod.path.isfile('/etc/gentoo-release'):
            self['name'] = 'Gentoo'
            self['family'] = 'Gentoo'
        elif osmod.path.isfile('/etc/fedora-release'):
            self['name'] = 'Fedora'
            self['family'] = 'RedHat'
        elif osmod.path.isfile('/etc/mandriva-version'):
            self['name'] = 'Mandriva'
            self['family'] = 'Mandriva'
        elif osmod.path.isfile('/etc/mandrake-version'):
            self['name'] = 'Mandrake'
            self['family'] = 'Mandriva'
        elif osmod.path.isfile('/etc/mageia-version'):
            self['name'] = 'Mageia'
            self['family'] = 'Mageia'
        elif osmod.path.isfile('/etc/meego-version'):
            self['name'] = 'MeeGo'
            self['family'] = 'MeeGo'
        elif osmod.path.isfile('/etc/vmware-version'):
            self['name'] = 'VMWareESX'
            self['family'] = 'VMWare'
        elif osmod.path.isfile('/etc/bluewhite64-version'):
            self['name'] = 'Bluewhite64'
            self['family'] = 'Bluewhite'
        elif osmod.path.isfile('/etc/slamd64-version'):
            self['name'] = 'Slamd64'
            self['family'] = 'Slackware'
        elif osmod.path.isfile('/etc/slackware-version'):
            self['name'] = 'Slackware'
            self['family'] = 'Slackware'
        elif osmod.path.isfile('/etc/enterprise-release'):
            self['family'] = 'Oracle'
            if osmod.path.isfile('/etc/ovs-release'):
                self['name'] = 'OVS'
            else:
                self['name'] = 'OEL'
        elif osmod.path.isfile('/etc/redhat-release'):
            self['family'] = 'RedHat'
            data = open('/etc/redhat-release', 'r').read()
            if 'centos' in data.lower():
                self['name'] = 'CentOS'
            elif 'scientific' in data.lower():
                self['name'] = 'Scientific'
            elif 'goose' in data.lower():
                self['name'] = 'GoOSe'
            else:
                self['name'] = 'RedHat'
        elif osmod.path.isfile('/etc/system-release'):
            self['family'] = 'RedHat'
            data = open('/etc/system-release', 'r').read().strip()
            if data.lower().startswith('amazon'):
                self['name'] = 'Amazon'
                self['release'] = data[-7:]
        elif osmod.path.isfile('/etc/SuSE-release'):
            self['family'] = 'Suse'
            data = open('/etc/SuSE-release', 'r').read()
            if 'SUSE LINUX Enterprise Server' in data:
                self['name'] = 'SLES'
            elif 'SUSE LINUX Enterprise Desktop' in data:
                self['name'] = 'SLED'
            elif 'openSUSE' in data:
                self['name'] = 'openSUSE'
            else:
                self['name'] = 'SUSE'
        name, release, codename = platform.dist()
        if not 'name' in self:
            self['name'] = name
        if not 'release' in self:
            self['release'] = release
        self['release'] = Version(self['release'])
        self['version'] = self['release']
        if not 'codename' in self:
            self['codename'] = codename


        if not 'name' in self:
            self['name'] = 'Unknown %s' % self['kernel']
            self['family'] = 'Unknown'
        if not 'family' in self:
            self['family'] = 'Unknown'

    def _detect_kernel(self):
        o, e, ret_code = system(['modprobe', '-l'], raise_exc=False, silent=True)
        self['mods_enabled'] = 0 if ret_code else 1

        uname = osmod.uname()
        self['kernel_release'] = Version(uname[2])
        arch = uname[-1]
        if re.search(r'i\d86', arch):
            arch = 'i386'
        self['arch'] = arch

    def _detect_cloud(self):
        pass

os = __os()


ubuntu_release_to_codename = {
        '8.04': 'hardy',
        '8.10': 'intrepid',
        '9.04': 'jaunty',
        '9.10': 'karmic',
        '10.04': 'lucid',
        '10.10': 'maverick',
        '11.04': 'natty',
        '11.10': 'oneiric',
        '12.04': 'precise',
        '12.10': 'quantal',
        '13.04': 'raring',
        '13.10': 'saucy',
        '14.04': 'trusty',
        '14.10': 'utopic'
}

def build_cmd_args(executable=None,
                                   short=None,
                                   long=None,
                                   params=None,
                                   duplicate_keys=False):
    ret = []
    if executable:
        ret += [executable]
    if short:
        ret += list(short)
    if long:
        for key, value in long.items():
            cmd_key = '--%s' % key.replace('_', '-')
            ret.append(cmd_key)
            if type(value) == bool and value:
                continue
            elif type(value) in (list, tuple):
                if duplicate_keys:
                    ret.append(value[0])
                    for v in value[1:]:
                        ret.extend([cmd_key, v])
                else:
                    ret.extend(value)
                continue
            ret.append(value)
    if params:
        ret += list(params)
    return map(str, ret)
