from __future__ import with_statement
'''
Created on Nov 25, 2011
 
@author: marat
 
Pluggable API to get system information similar to SNMP, Facter(puppet), Ohai(chef)
'''
 
 
import os
import re
import glob
import logging
import platform
import threading
import time
import signal
import binascii
import weakref
import subprocess
 
from multiprocessing import pool
 
from scalarizr import rpc, linux
from scalarizr.bus import bus
from scalarizr import util
from scalarizr.util import system2, dns, disttool
from scalarizr.linux import mount
from scalarizr.util import kill_childs
from scalarizr.queryenv import ScalingMetric
from scalarizr.handlers.script_executor import logs_dir
 
LOG = logging.getLogger(__name__)
 
 
max_log_size = 5*1024*1024
 
 
class _ScalingMetricStrategy(object):
    """Strategy class for custom scaling metric"""
 
    @staticmethod
    def _get_execute(metric):
        if not os.access(metric.path, os.X_OK):
            raise BaseException("File is not executable: '%s'" % metric.path)
 
        exec_timeout = 3
        close_fds = not linux.os.windows_family
        proc = subprocess.Popen(metric.path, stdout=subprocess.PIPE, stderr=subprocess.PIPE, close_fds=close_fds)
 
        timeout_time = time.time() + exec_timeout
        while time.time() < timeout_time:
            if proc.poll() is None:
                time.sleep(0.2)
            else:
                break
        else:
            kill_childs(proc.pid)
            if hasattr(proc, 'terminate'):
                # python >= 2.6
                proc.terminate()
            else:
                os.kill(proc.pid, signal.SIGTERM)
            raise BaseException('Timeouted')
 
        stdout, stderr = proc.communicate()
 
        if proc.returncode > 0:
            raise BaseException(stderr if stderr else 'exitcode: %d' % proc.returncode)
 
        return stdout.strip()
 
 
    @staticmethod
    def _get_read(metric):
        try:
            with open(metric.path, 'r') as fp:
                value = fp.readline()
        except IOError:
            raise BaseException("File is not readable: '%s'" % metric.path)
 
        return value.strip()
 
 
    @staticmethod
    def get(metric):
        error = ''
        try:
            if metric.retrieve_method == ScalingMetric.RetriveMethod.EXECUTE:
                value = float(_ScalingMetricStrategy._get_execute(metric))
            elif metric.retrieve_method == ScalingMetric.RetriveMethod.READ:
                value = float(_ScalingMetricStrategy._get_read(metric))
            else:
                raise BaseException('Unknown retrieve method %s' % metric.retrieve_method)
        except (BaseException, Exception), e:
            value = 0.0
            error = str(e)[0:255]
 
        return {'id':metric.id, 'name':metric.name, 'value':value, 'error':error}
 
 
class SystemAPI(object):
 
    _HOSTNAME = '/etc/hostname'
    _DISKSTATS = '/proc/diskstats'
    _PATH = ['/usr/bin/', '/usr/local/bin/']
    _CPUINFO = '/proc/cpuinfo'
    _NETSTATS = '/proc/net/dev'
    _LOG_FILE = '/var/log/scalarizr.log'
    _DEBUG_LOG_FILE = '/var/log/scalarizr_debug.log'
    _UPDATE_LOG_FILE = '/var/log/scalarizr_update.log'
 
    def _readlines(self, path):
        with open(path, "r") as fp:
            return fp.readlines()
 
 
    def add_extension(self, extension):
        '''
        @param extension: Object with some callables to extend SysInfo public interface
        @type extension: object
        @note: Duplicates resolves by overriding old function with a new one
        '''
 
        for name in dir(extension):
            attr = getattr(extension, name)
            if not name.startswith('_') and callable(attr):
                if hasattr(self, name):
                    LOG.warn('Duplicate attribute %s. Overriding %s with %s', 
                            name, getattr(self, name), attr)
                setattr(self, name, attr)
 
 
    @rpc.query_method
    def call_auth_shutdown_hook(self):
        script_path = '/usr/local/scalarizr/hooks/auth-shutdown'
        LOG.debug("Executing %s" % script_path)
        if os.access(script_path, os.X_OK):
            return subprocess.Popen(script_path, stdout=subprocess.PIPE,
                stderr=subprocess.PIPE, close_fds=True).communicate()[0].strip()
        else:
            raise Exception('File not exists: %s' % script_path)
 
 
    @rpc.command_method
    def reboot(self):
        if os.fork():
            return
        util.daemonize()
        system2(('reboot',))
        exit(0)
 
    @rpc.command_method
    def set_hostname(self, hostname=None):
        '''
        Get/Update host FQDN
        @param hostname: Fully Qualified Domain Name to set for this host
        @rtype: str: Current FQDN
        '''
        assert hostname
        system2(('hostname', hostname))
        with open(self._HOSTNAME, 'w+') as fp:
            fp.write(hostname) 
        hosts = dns.HostsFile()
        if not hosts.resolve('localhost'):
            hosts.map('127.0.0.1', 'localhost', hostname)
        else:
            hosts.alias('localhost', hostname)
 
 
        '''
        TODO: test and correct this code 
        # changing permanent hostname
        try:
            if os.path.exists(self._HOSTNAME):
                with open(self._HOSTNAME, 'r') as fp:
                    old_hn = fp.readline().strip()
            with open(self._HOSTNAME, 'w+') as fp:
                fp.write('%s\n' % hostname)
        except:
            raise Exception, 'Can`t write file `%s`.' % \
                self._HOSTNAME, sys.exc_info()[2]
        # changing runtime hostname
        try:
            system2(('hostname', hostname))
        except:
            with open(self._HOSTNAME, 'w+') as fp:
                fp.write('%s\n' % old_hn)
            raise Exception('Failed to change the hostname to `%s`' % hostname)
        # changing hostname in hosts file
        if old_hn:
            hosts = dns.HostsFile()
            hosts._reload()
            if hosts._hosts:
                for index in range(0, len(hosts._hosts)):
                    if isinstance(hosts._hosts[index], dict) and \
                                    hosts._hosts[index]['hostname'] == old_hn:
                        hosts._hosts[index]['hostname'] = hostname
                hosts._flush()
        '''
        return hostname
 
 
    @rpc.query_method
    def get_hostname(self):
        return system2(('hostname', ))[0].strip()
 
 
    @rpc.query_method
    def block_devices(self):
        '''
        Block devices list
        @return: List of block devices including ramX and loopX
        @rtype: list
        '''
 
        lines = self._readlines(self._DISKSTATS)
        devicelist = []
        for value in lines:
            devicelist.append(value.split()[2])
        return devicelist
 
 
    @rpc.query_method
    def uname(self):
        '''
        Return system information
        @rtype: dict
 
        Sample:
        {'kernel_name': 'Linux',
        'kernel_release': '2.6.41.10-3.fc15.x86_64',
        'kernel_version': '#1 SMP Mon Jan 23 15:46:37 UTC 2012',
        'nodename': 'marat.office.webta',           
        'machine': 'x86_64',
        'processor': 'x86_64',
        'hardware_platform': 'x86_64'}
        '''
 
        uname = disttool.uname()
        return {
            'kernel_name': uname[0],
            'nodename': uname[1],
            'kernel_release': uname[2],
            'kernel_version': uname[3],
            'machine': uname[4],
            'processor': uname[5],
            'hardware_platform': disttool.arch()
        }
 
 
    @rpc.query_method
    def dist(self):
        '''
        Return Linux distribution information 
        @rtype: dict
 
        Sample:
        {'distributor': 'ubuntu',
        'release': '12.04',
        'codename': 'precise'}
        '''
        return {
            'distributor': linux.os['name'].lower(),
            'release': str(linux.os['release']),
            'codename': linux.os['codename']
        }
 
 
    @rpc.query_method
    def pythons(self):
        '''
        Return installed Python versions
        @rtype: list
 
        Sample:
        ['2.7.2+', '3.2.2']
        '''
 
        res = []
        for path in self._PATH:
            pythons = glob.glob(os.path.join(path, 'python[0-9].[0-9]'))
            for el in pythons:
                res.append(el)
        #check full correct version
        LOG.debug('variants of python bin paths: `%s`. They`ll be checking now.', list(set(res)))
        result = []
        for pypath in list(set(res)):
            (out, err, rc) = system2((pypath, '-V'), raise_exc=False)
            if rc == 0:
                result.append((out or err).strip())
            else:
                LOG.debug("Can`t execute `%s -V`, details: %s",
                        pypath, err or out)
        return map(lambda x: x.lower().replace('python', '').strip(), sorted(list(set(result))))
 
 
    @rpc.query_method
    def cpu_info(self):
        '''
        Return CPU info from /proc/cpuinfo
        @rtype: list
        '''
 
        lines = self._readlines(self._CPUINFO)
        res = []
        index = 0
        while index < len(lines):
            core = {}
            while index < len(lines):
                if ':' in lines[index]:
                    tmp = map(lambda x: x.strip(), lines[index].split(':'))
                    (key, value) = list(tmp) if len(list(tmp)) == 2 else (tmp, None)
                    if key not in core.keys():
                        core.update({key:value})
                    else:
                        break
                index += 1
            res.append(core)
        return res
 
 
    @rpc.query_method
    def cpu_stat(self):
 
        '''
        Return CPU stat from /proc/stat
        @rtype: dict
 
        Sample: {
            'user': 8416,
            'nice': 0,
            'system': 6754,
            'idle': 147309
        }
        '''
        cpu = open('/proc/stat').readline().strip().split()
        return {
            'user': int(cpu[1]),
            'nice': int(cpu[2]),
            'system': int(cpu[3]),
            'idle': int(cpu[4])
        }
 
 
    @rpc.query_method
    def mem_info(self):
        '''
        Return Memory information from /proc/meminfo
        @rtype: dict
 
        Sample: {
            'total_swap': 0,
            'avail_swap': 0,
            'total_real': 604364,
            'total_free': 165108,
            'shared': 168,
            'buffer': 17832,
            'cached': 316756
        }
        '''
        info = {}
        for line in open('/proc/meminfo'):
            pairs = line.split(':', 1)
            info[pairs[0]] = int(pairs[1].strip().split(' ')[0])
        return {
            'total_swap': info['SwapTotal'],
            'avail_swap': info['SwapFree'],
            'total_real': info['MemTotal'],
            'total_free': info['MemFree'],
            'shared': info.get('Shmem', 0),
            'buffer': info['Buffers'],
            'cached': info['Cached']
        }
 
 
    @rpc.query_method
    def load_average(self):
        '''
        Return Load average (1, 5, 15) in 3 items list  
        '''
 
        return os.getloadavg()
 
 
    @rpc.query_method
    def disk_stats(self):
        '''
        Disks I/O statistics
        @rtype: {
            <device>: {
                <read>: {
                    <num>: total number of reads completed successfully
                    <sectors>: total number of sectors read successfully
                    <bytes>: total number of bytes read successfully
                }
                <write>: {
                    <num>: total number of writes completed successfully
                    <sectors>: total number of sectors written successfully
                    <bytes>: total number of bytes written successfully
                },
            ...
        }
        '''
        #http://www.kernel.org/doc/Documentation/iostats.txt
 
        lines = self._readlines(self._DISKSTATS)
        devicelist = {}
        for value in lines:
            params = value.split()[2:]
            device = params[0]
            for i in range(1, len(params)-1):
                params[i] = int(params[i])
            if len(params) == 12:
                read = {'num': params[1], 'sectors': params[3], 'bytes': params[3]*512}
                write = {'num': params[5], 'sectors': params[7], 'bytes': params[7]*512}
            elif len(params) == 5:
                read = {'num': params[1], 'sectors': params[2], 'bytes': params[2]*512}
                write = {'num': params[3], 'sectors': params[4], 'bytes': params[4]*512}
            else:
                raise Exception, 'number of column in %s is unexpected. Count of column =\
                     %s' % (self._DISKSTATS, len(params)+2)
            devicelist[device] = {'write': write, 'read': read}
        return devicelist
 
 
    @rpc.query_method
    def net_stats(self):
        '''
        Network I/O statistics
        @rtype: {
            <iface>: {
                <receive>: {
                    <bytes>: total received bytes
                    <packets>: total received packets
                    <errors>: total receive errors
                }
                <transmit>: {
                    <bytes>: total transmitted bytes
                    <packets>: total transmitted packets
                    <errors>: total transmit errors
                }
            },
            ...
        }
        '''
 
        lines = self._readlines(self._NETSTATS)
        res = {}
        for row in lines:
            if ':' not in row:
                continue
            row = row.split(':')
            iface = row.pop(0).strip()
            columns = map(lambda x: x.strip(), row[0].split())
            res[iface] = {
                'receive': {'bytes': columns[0], 'packets': columns[1], 'errors': columns[2]},
                'transmit': {'bytes': columns[8], 'packets': columns[9], 'errors': columns[10]},
            }
 
        return res
 
 
    @rpc.query_method
    def statvfs(self, mpoints=None):
        if not isinstance(mpoints, list):
            raise Exception('Argument "mpoints" should be a list of strings, '
                        'not %s' % type(mpoints))
 
        res = dict()
        mounts = mount.mounts()
        for mpoint in mpoints:
            try:
                assert mpoint in mounts
                mpoint_stat = os.statvfs(mpoint)
                res[mpoint] = dict()
                res[mpoint]['total'] = (mpoint_stat.f_bsize * mpoint_stat.f_blocks) / 1024
                res[mpoint]['free'] = (mpoint_stat.f_bsize * mpoint_stat.f_bavail) / 1024
            except:
                res[mpoint] = None
 
        return res
 
 
    @rpc.query_method
    def mounts(self):
        skip_mpoint_re = re.compile(r'/(sys|proc|dev|selinux)')
        skip_fstype = ('tmpfs', 'devfs')
        ret = {}
        for m in mount.mounts():
            if not (skip_mpoint_re.search(m.mpoint) or m.fstype in skip_fstype):
                entry = m._asdict()
                entry.update(self.statvfs([m.mpoint])[m.mpoint])
                ret[m.mpoint] = entry
        return ret
 
 
    @rpc.query_method
    def scaling_metrics(self):
        '''
        @return list of scaling metrics
        @rtype: list
 
        Sample: [{
            'id': 101011, 
            'name': 'jmx.scaling', 
            'value': 1, 
            'error': None
        }, {
            'id': 202020,
            'name': 'app.poller',
            'value': None,
            'error': 'Couldnt connect to host'
        }]
        '''
 
        # Obtain scaling metrics from Scalr.
        scaling_metrics = bus.queryenv_service.get_scaling_metrics()
        if not scaling_metrics:
            return []
 
        if not hasattr(threading.current_thread(), '_children'):
            threading.current_thread()._children = weakref.WeakKeyDictionary()
 
        wrk_pool = pool.ThreadPool(processes=10)
 
        try:
            return wrk_pool.map_async(_ScalingMetricStrategy.get, scaling_metrics).get()
        finally:
            wrk_pool.close()
            wrk_pool.join()
 
 
    @rpc.query_method
    def get_script_logs(self, exec_script_id, maxsize=max_log_size):
        '''
        :return: out and err logs
        :rtype: dict(stdout: base64encoded, stderr: base64encoded)
        '''
        stdout_match = glob.glob(os.path.join(logs_dir, '*%s-out.log' % exec_script_id))
        stderr_match = glob.glob(os.path.join(logs_dir, '*%s-err.log' % exec_script_id))
 
        if not stdout_match:
            stdout = binascii.b2a_base64(u'log file not found')
        else:
            stdout_path = stdout_match[0]
            stdout = binascii.b2a_base64(_get_log(stdout_path))
        if not stderr_match:
            stderr = binascii.b2a_base64(u'errlog file not found')
        else:
            stderr_path = stderr_match[0]
            stderr = binascii.b2a_base64(_get_log(stderr_path))
 
        return dict(stdout=stdout, stderr=stderr)
 
    @rpc.query_method
    def get_debug_log(self):
        return binascii.b2a_base64(_get_log(self._DEBUG_LOG_FILE, -1))
 
    @rpc.query_method
    def get_update_log(self):
        return binascii.b2a_base64(_get_log(self._UPDATE_LOG_FILE, -1))     
 
    @rpc.query_method
    def get_log(self):
        return binascii.b2a_base64(_get_log(self._LOG_FILE, -1))   
 
 
def _get_log(logfile, maxsize=max_log_size):
    if maxsize != -1 and (os.path.getsize(logfile) > maxsize):
        return u'Unable to fetch Log file %s: file is larger than %s bytes' % (logfile, maxsize)
    try:
        with open(logfile, "r") as fp:
            ret = unicode(fp.read(int(maxsize)), 'utf-8')
            return ret.encode('utf-8')
    except IOError:
        return u'Log file %s is not readable' % logfile
 
 
if linux.os.windows_family:
    from win32com import client
    from scalarizr.util import coinitialized
 
    class WindowsSystemAPI(SystemAPI):
 
        _LOG_FILE = r'C:\Program Files\Scalarizr\var\log\scalarizr.log'
        _DEBUG_LOG_FILE = r'C:\Program Files\Scalarizr\var\log\scalarizr_debug.log'
        _UPDATE_LOG_FILE = r'C:\Program Files\Scalarizr\var\log\scalarizr_update.log'  
 
        @coinitialized
        @rpc.command_method
        def reboot(self):
            wmi = client.GetObject('winmgmts:')
            for opsys in wmi.InstancesOf('Win32_OperatingSystem'):
                opsys.Reboot()
 
        @coinitialized
        @rpc.command_method
        def set_hostname(self, hostname=None):
            # Can't change hostname without reboot or knowing administrator password
            # Probably a way: http://timnew.github.io/blog/2012/04/13/powershell-script-to-rename-computer-without-reboot/
            raise NotImplementedError('Not available on windows platform')
 
        @coinitialized
        @rpc.query_method
        def get_hostname(self):
            try:
                wmi = client.GetObject('winmgmts:')
                for computer in wmi.InstancesOf('Win32_ComputerSystem'):
                    return computer.Name
            except:
                return ''
 
        @coinitialized
        @rpc.query_method
        def disk_stats(self):
            wmi = client.GetObject('winmgmts:')
 
            res = dict()
            for disk in wmi.InstancesOf('Win32_PerfRawData_PerfDisk_LogicalDisk'):
                # Skip Total
                if disk.Name == '_Total':
                    continue
 
                res[disk.Name] = dict(
                    read=dict(
                        bytes=int(disk.AvgDiskBytesPerRead)
                    ),
                    write=dict(
                        bytes=int(disk.AvgDiskBytesPerWrite)
                    )
                )
            return res
 
        @coinitialized
        @rpc.query_method
        def block_devices(self):
            wmi = client.GetObject('winmgmts:')
 
            res = list()
            for disk in wmi.InstancesOf('Win32_PerfRawData_PerfDisk_LogicalDisk'):
                if disk.Name == '_Total':
                    continue
                res.append(disk.Name)
            return res
 
        @coinitialized
        @rpc.query_method
        def dist(self):
            uname = platform.uname()
            return dict(system=uname[0], release=uname[2], version=uname[3])
 
        @coinitialized
        @rpc.query_method
        def net_stats(self):
            wmi = client.GetObject('winmgmts:')
 
            res = dict()
            for iface in wmi.InstancesOf('Win32_PerfRawData_Tcpip_NetworkInterface'):
                if iface.Name == '_Total':
                    continue
 
                res[iface.Name] = dict(
                    receive=dict(
                        bytes=int(iface.Properties_['BytesReceivedPersec']),
                        packets=int(iface.Properties_['PacketsReceivedPersec']),
                        errors=int(iface.Properties_['PacketsReceivedErrors'])
                    ),
                    transmit=dict(
                        bytes=int(iface.Properties_['BytesSentPersec']),
                        packets=int(iface.Properties_['PacketsSentPersec']),
                        errors=int(iface.Properties_['PacketsOutboundErrors'])
                    )
                )
            return res
 
        @rpc.query_method
        def load_average(self):
            raise Exception('Not available on windows platform')
 
        @rpc.query_method
        def uname(self):
            uname = platform.uname()
            return dict(zip(
                ('system', 'node', 'release', 'version', 'machine', 'processor'), uname
            ))
 
        @coinitialized
        @rpc.query_method
        def cpu_stat(self):
            wmi = client.GetObject('winmgmts:')
 
            processors = wmi.InstancesOf('Win32_Processor')
            avg_percentage = float(sum([cpu.LoadPercentage for cpu in processors])) / len(processors)
 
            return {
                'user': avg_percentage,
                'system': 0,
                'idle': 100 - avg_percentage,
                'nice': 0
            }
 
        @coinitialized
        @rpc.query_method
        def mem_info(self):
            wmi = client.GetObject('winmgmts:')
 
            meminfo = wmi.InstancesOf('Win32_PerfFormattedData_PerfOS_Memory')[0]
            sysinfo = wmi.InstancesOf('Win32_ComputerSystem')[0]
            return {
                'total_swap': int(meminfo.CommitLimit) / 1024,
                'avail_swap': (int(meminfo.CommitLimit) - int(meminfo.CommittedBytes)) / 1024,
                'total_real': int(sysinfo.Properties_('totalphysicalmemory')) / 1024,
                'total_free': int(meminfo.Properties_('AvailableKBytes'))
            }
 
        @coinitialized
        @rpc.query_method
        def statvfs(self, mpoints=None):
            wmi = client.GetObject('winmgmts:')
 
            # mpoints == disks letters on Windows
            mpoints = map(lambda s: s[0].lower(), mpoints)
            if not isinstance(mpoints, list):
                raise Exception('Argument "mpoints" should be a list of strings, '
                            'not %s' % type(mpoints))
            ret = {}
            for disk in wmi.InstancesOf('Win32_LogicalDisk'):
                letter = disk.DeviceId[0].lower()
                if letter in mpoints:
                    ret[letter] = self._format_statvfs(disk)
            return ret
 
 
        @coinitialized
        @rpc.query_method
        def mounts(self):
            wmi = client.GetObject('winmgmts:') 
 
            ret = {}
            for disk in wmi.InstancesOf('Win32_LogicalDisk'):
                letter = disk.DeviceId[0].lower()
                entry = {
                    'device': letter,
                    'mpoint': letter       
                }
                entry.update(self._format_statvfs(disk))
                ret[letter] = entry
            return ret
 
        def _format_statvfs(self, disk):
            return {
                'total': int(disk.Size) / 1024,  # Kb
                'free': int(disk.FreeSpace) / 1024  # Kb        
            }
 
 
    SystemAPI = WindowsSystemAPI
 
 
