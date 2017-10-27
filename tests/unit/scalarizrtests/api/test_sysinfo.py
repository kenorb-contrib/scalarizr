'''
Created on 07.02.2012
 
@author: sam
'''
 
import os, stat
import unittest
import mock
import glob
import subprocess as subps
 
from scalarizr.api import system
from scalarizr.util import system2
 
DISKSTATS_ = "\
   1       0 ram0 0 0 0 0 0 0 0 0 0 0 0\n\
   1       1 ram1 0 0 0 0 0 0 0 0 0 0 0\n\
   1       2 ram2 0 0 0 0 0 0 0 0 0 0 0\n\
   1       3 ram3 0 0 0 0 0 0 0 0 0 0 0\n\
   7       0 loop0 0 0 0 0 0 0 0 0 0 0 0\n\
   7       1 loop1 0 0 0 0 0 0 0 0 0 0 0\n\
   7       2 loop2 0 0 0 0 0 0 0 0 0 0 0\n\
   7       3 loop3 0 0 0 0 0 0 0 0 0 0 0\n\
   8       0 sda 511024 336986 26373020 6011416 348728 1602629 92458986 109450764 0 3666720 115466896\n\
   8       1 sda1 454 208 4158 2756 24 9 138 1472 0 3424 4228\n\
   8       2 sda2 2 0 12 328 0 0 0 0 0 328 328\n"
 
CPUINFO_ = "\
processor   : 0\n\
vendor_id   : GenuineIntel\n\
cpu family  : 6\n\
model       : 42\n\
model name  : Intel(R) Core(TM) i5-2310 CPU @ 2.90GHz\n\
stepping    : 7\n\
microcode   : 0x14\n\
cpu MHz     : 1600.000\n\
cache size  : 6144 KB\n\
physical id : 0\n\
siblings    : 4\n\
core id     : 0\n\
cpu cores   : 4\n\
apicid      : 0\n\
initial apicid  : 0\n\
fpu     : yes\n\
fpu_exception   : yes\n\
cpuid level : 13\n\
wp      : yes\n\
flags       : fpu vme de pse tsc msr pae mce cx8 apic sep mtrr pge mca cmov pat pse36 clflush dts acpi mmx fxsr sse sse2 ss ht tm pbe syscall nx rdtscp lm constant_tsc arch_perfmon pebs bts rep_good nopl xtopology nonstop_tsc aperfmperf pni pclmulqdq dtes64 monitor ds_cpl vmx est tm2 ssse3 cx16 xtpr pdcm pcid sse4_1 sse4_2 popcnt tsc_deadline_timer aes xsave avx lahf_lm ida arat epb xsaveopt pln pts dtherm tpr_shadow vnmi flexpriority ept vpid\n\
bogomips    : 5785.58\n\
clflush size    : 64\n\
cache_alignment : 64\n\
address sizes   : 36 bits physical, 48 bits virtual\n\
power management:\n\
\n\
processor   : 1\n\
vendor_id   : GenuineIntel\n\
cpu family  : 6\n\
model       : 42\n\
model name  : Intel(R) Core(TM) i5-2310 CPU @ 2.90GHz\n\
stepping    : 7\n\
microcode   : 0x14\n\
cpu MHz     : 1600.000\n\
cache size  : 6144 KB\n\
physical id : 0\n\
siblings    : 4\n\
core id     : 1\n\
cpu cores   : 4\n\
apicid      : 2\n\
initial apicid  : 2\n\
fpu     : yes\n\
fpu_exception   : yes\n\
cpuid level : 13\n\
wp      : yes\n\
flags       : fpu vme de pse tsc msr pae mce cx8 apic sep mtrr pge mca cmov pat pse36 clflush dts acpi mmx fxsr sse sse2 ss ht tm pbe syscall nx rdtscp lm constant_tsc arch_perfmon pebs bts rep_good nopl xtopology nonstop_tsc aperfmperf pni pclmulqdq dtes64 monitor ds_cpl vmx est tm2 ssse3 cx16 xtpr pdcm pcid sse4_1 sse4_2 popcnt tsc_deadline_timer aes xsave avx lahf_lm ida arat epb xsaveopt pln pts dtherm tpr_shadow vnmi flexpriority ept vpid\n\
bogomips    : 5785.20\n\
clflush size    : 64\n\
cache_alignment : 64\n\
address sizes   : 36 bits physical, 48 bits virtual\n\
power management:\n\
\n\
processor   : 2\n\
vendor_id   : GenuineIntel\n\
cpu family  : 6\n\
model       : 42\n\
model name  : Intel(R) Core(TM) i5-2310 CPU @ 2.90GHz\n\
stepping    : 7\n\
microcode   : 0x14\n\
cpu MHz     : 1600.000\n\
cache size  : 6144 KB\n\
physical id : 0\n\
siblings    : 4\n\
core id     : 2\n\
cpu cores   : 4\n\
apicid      : 4\n\
initial apicid  : 4\n\
fpu     : yes\n\
fpu_exception   : yes\n\
cpuid level : 13\n\
wp      : yes\n\
flags       : fpu vme de pse tsc msr pae mce cx8 apic sep mtrr pge mca cmov pat pse36 clflush dts acpi mmx fxsr sse sse2 ss ht tm pbe syscall nx rdtscp lm constant_tsc arch_perfmon pebs bts rep_good nopl xtopology nonstop_tsc aperfmperf pni pclmulqdq dtes64 monitor ds_cpl vmx est tm2 ssse3 cx16 xtpr pdcm pcid sse4_1 sse4_2 popcnt tsc_deadline_timer aes xsave avx lahf_lm ida arat epb xsaveopt pln pts dtherm tpr_shadow vnmi flexpriority ept vpid\n\
bogomips    : 5785.20\n\
clflush size    : 64\n\
cache_alignment : 64\n\
address sizes   : 36 bits physical, 48 bits virtual\n\
power management:\n\
\n\
processor   : 3\n\
vendor_id   : GenuineIntel\n\
cpu family  : 6\n\
model       : 42\n\
model name  : Intel(R) Core(TM) i5-2310 CPU @ 2.90GHz\n\
stepping    : 7\n\
microcode   : 0x14\n\
cpu MHz     : 1600.000\n\
cache size  : 6144 KB\n\
physical id : 0\n\
siblings    : 4\n\
core id     : 3\n\
cpu cores   : 4\n\
apicid      : 6\n\
initial apicid  : 6\n\
fpu     : yes\n\
fpu_exception   : yes\n\
cpuid level : 13\n\
wp      : yes\n\
flags       : fpu vme de pse tsc msr pae mce cx8 apic sep mtrr pge mca cmov pat pse36 clflush dts acpi mmx fxsr sse sse2 ss ht tm pbe syscall nx rdtscp lm constant_tsc arch_perfmon pebs bts rep_good nopl xtopology nonstop_tsc aperfmperf pni pclmulqdq dtes64 monitor ds_cpl vmx est tm2 ssse3 cx16 xtpr pdcm pcid sse4_1 sse4_2 popcnt tsc_deadline_timer aes xsave avx lahf_lm ida arat epb xsaveopt pln pts dtherm tpr_shadow vnmi flexpriority ept vpid\n\
bogomips    : 5785.19\n\
clflush size    : 64\n\
cache_alignment : 64\n\
address sizes   : 36 bits physical, 48 bits virtual\n\
power management:\n"
 
NETSTAT_ = "\
Inter-|   Receive                                                |  Transmit\n\
 face |bytes    packets errs drop fifo frame compressed multicast|bytes    packets errs drop fifo colls carrier compressed\n\
    lo: 19325974   81717    0    0    0     0          0         0 19325974   81717    0    0    0     0       0          0\n\
  eth0: 1554522775 1160244    0    0    0     0          0         0 60669160  671950    0    0    0     0       0          0\n\
lxcbr0:       0       0    0    0    0     0          0         0   193001    2548    0    0    0     0       0          0\n"
 
DISKSTATS = '/tmp/diskstats'
CPUINFO = '/tmp/cpuinfo'
NETSTATS = '/tmp/netstat'
 
 
class TestSysInfoAPI(unittest.TestCase):
 
    def __init__(self, methodName='runTest'):
        unittest.TestCase.__init__(self, methodName=methodName)
        self.info = system.SystemAPI()
        with open(DISKSTATS, 'w+') as fp:
            fp.writelines(DISKSTATS_)
        with open(CPUINFO, 'w+') as fp:
            fp.writelines(CPUINFO_)
        with open(NETSTATS, 'w+') as fp:
            fp.writelines(NETSTAT_)
 
        self.info._DISKSTATS = DISKSTATS
        self.info._CPUINFO = CPUINFO
        self.info._NETSTATS = NETSTATS
 
 
    def test_fqdn(self):
        (out, err, rc) = system2(('hostname'))
        self.assertEqual(out.strip(), self.info.fqdn())
        old_name = out.strip()
 
        self.info.fqdn('Scalr-Role-12345')
        (out, err, rc) = system2(('hostname'))
        self.assertEqual(out.strip(), 'Scalr-Role-12345')
        self.info.fqdn(old_name)
 
 
    def test_block_devices(self):
        self.assertIsNotNone(self.info.block_devices())
 
 
    def test_uname(self):
        self.assertTrue(isinstance(self.info.uname(), dict) and self.info.uname())
 
 
    def test_dist(self):
        with mock.patch.dict('scalarizr.linux.os', {'name':'Ubuntu', 'release':'12.10', 'codename':'precise'}):
            self.assertEqual(self.info.dist(), {'distributor': 'ubuntu', 'release': '12.10', 'codename': 'precise'})
 
 
    def test_pythons(self):
        maybe_python = [el for path_ in ['/usr/bin', '/usr/local/bin'] for el in glob.glob(os.path.join(path_, 'python[0-9].[0-9]'))]
        pythons = [py for py in maybe_python if os.path.exists(py)]
        right_result = []
        for py in pythons:
            proc = subps.Popen([py, '-V'], stdout=subps.PIPE, stderr=subps.PIPE)
            proc.wait()
            right_result.append(proc.stderr.readline())
 
        self.assertIsNotNone(self.info.pythons())
        self.assertEqual(self.info.pythons(), sorted(map(lambda x: x.lower().replace('python', '').strip(), list(set(right_result)))))
 
 
    def test_cpu_info(self):
        cpu = self.info.cpu_info()
        self.assertEqual(cpu, [{'vendor_id': 'GenuineIntel', 'cpu family': '6', 'cache_alignment': '64', 'cpu cores': '4', 'bogomips': '5785.58', 'core id': '0', 'apicid': '0', 'fpu_exception': 'yes', 'stepping': '7', 'wp': 'yes', 'clflush size': '64', 'microcode': '0x14', 'cache size': '6144 KB', 'power management': '', 'cpuid level': '13', 'physical id': '0', 'fpu': 'yes', 'flags': 'fpu vme de pse tsc msr pae mce cx8 apic sep mtrr pge mca cmov pat pse36 clflush dts acpi mmx fxsr sse sse2 ss ht tm pbe syscall nx rdtscp lm constant_tsc arch_perfmon pebs bts rep_good nopl xtopology nonstop_tsc aperfmperf pni pclmulqdq dtes64 monitor ds_cpl vmx est tm2 ssse3 cx16 xtpr pdcm pcid sse4_1 sse4_2 popcnt tsc_deadline_timer aes xsave avx lahf_lm ida arat epb xsaveopt pln pts dtherm tpr_shadow vnmi flexpriority ept vpid', 'cpu MHz': '1600.000', 'model name': 'Intel(R) Core(TM) i5-2310 CPU @ 2.90GHz', 'siblings': '4', 'model': '42', 'processor': '0', 'initial apicid': '0', 'address sizes': '36 bits physical, 48 bits virtual'}, {'vendor_id': 'GenuineIntel', 'cpu family': '6', 'cache_alignment': '64', 'cpu cores': '4', 'bogomips': '5785.20', 'core id': '1', 'apicid': '2', 'fpu_exception': 'yes', 'stepping': '7', 'wp': 'yes', 'clflush size': '64', 'microcode': '0x14', 'cache size': '6144 KB', 'power management': '', 'cpuid level': '13', 'physical id': '0', 'fpu': 'yes', 'flags': 'fpu vme de pse tsc msr pae mce cx8 apic sep mtrr pge mca cmov pat pse36 clflush dts acpi mmx fxsr sse sse2 ss ht tm pbe syscall nx rdtscp lm constant_tsc arch_perfmon pebs bts rep_good nopl xtopology nonstop_tsc aperfmperf pni pclmulqdq dtes64 monitor ds_cpl vmx est tm2 ssse3 cx16 xtpr pdcm pcid sse4_1 sse4_2 popcnt tsc_deadline_timer aes xsave avx lahf_lm ida arat epb xsaveopt pln pts dtherm tpr_shadow vnmi flexpriority ept vpid', 'cpu MHz': '1600.000', 'model name': 'Intel(R) Core(TM) i5-2310 CPU @ 2.90GHz', 'siblings': '4', 'model': '42', 'processor': '1', 'initial apicid': '2', 'address sizes': '36 bits physical, 48 bits virtual'}, {'vendor_id': 'GenuineIntel', 'cpu family': '6', 'cache_alignment': '64', 'cpu cores': '4', 'bogomips': '5785.20', 'core id': '2', 'apicid': '4', 'fpu_exception': 'yes', 'stepping': '7', 'wp': 'yes', 'clflush size': '64', 'microcode': '0x14', 'cache size': '6144 KB', 'power management': '', 'cpuid level': '13', 'physical id': '0', 'fpu': 'yes', 'flags': 'fpu vme de pse tsc msr pae mce cx8 apic sep mtrr pge mca cmov pat pse36 clflush dts acpi mmx fxsr sse sse2 ss ht tm pbe syscall nx rdtscp lm constant_tsc arch_perfmon pebs bts rep_good nopl xtopology nonstop_tsc aperfmperf pni pclmulqdq dtes64 monitor ds_cpl vmx est tm2 ssse3 cx16 xtpr pdcm pcid sse4_1 sse4_2 popcnt tsc_deadline_timer aes xsave avx lahf_lm ida arat epb xsaveopt pln pts dtherm tpr_shadow vnmi flexpriority ept vpid', 'cpu MHz': '1600.000', 'model name': 'Intel(R) Core(TM) i5-2310 CPU @ 2.90GHz', 'siblings': '4', 'model': '42', 'processor': '2', 'initial apicid': '4', 'address sizes': '36 bits physical, 48 bits virtual'}, {'vendor_id': 'GenuineIntel', 'cpu family': '6', 'cache_alignment': '64', 'cpu cores': '4', 'bogomips': '5785.19', 'core id': '3', 'apicid': '6', 'fpu_exception': 'yes', 'stepping': '7', 'wp': 'yes', 'clflush size': '64', 'microcode': '0x14', 'cache size': '6144 KB', 'power management': '', 'cpuid level': '13', 'physical id': '0', 'fpu': 'yes', 'flags': 'fpu vme de pse tsc msr pae mce cx8 apic sep mtrr pge mca cmov pat pse36 clflush dts acpi mmx fxsr sse sse2 ss ht tm pbe syscall nx rdtscp lm constant_tsc arch_perfmon pebs bts rep_good nopl xtopology nonstop_tsc aperfmperf pni pclmulqdq dtes64 monitor ds_cpl vmx est tm2 ssse3 cx16 xtpr pdcm pcid sse4_1 sse4_2 popcnt tsc_deadline_timer aes xsave avx lahf_lm ida arat epb xsaveopt pln pts dtherm tpr_shadow vnmi flexpriority ept vpid', 'cpu MHz': '1600.000', 'model name': 'Intel(R) Core(TM) i5-2310 CPU @ 2.90GHz', 'siblings': '4', 'model': '42', 'processor': '3', 'initial apicid': '6', 'address sizes': '36 bits physical, 48 bits virtual'}])
 
 
    def test_load_average(self):
        # TODO
        pass
 
 
    def test_disk_stats(self):
        self.assertEqual(self.info.disk_stats(), {
            'ram0':{
                'write': {'num': 0, 'bytes': 0, 'sectors': 0},
                'read': {'num': 0, 'bytes': 0, 'sectors': 0}},
            'ram1':{
                'write': {'num': 0, 'bytes': 0, 'sectors': 0},
                'read': {'num': 0, 'bytes': 0, 'sectors': 0}},
            'ram2':{
                'write': {'num': 0, 'bytes': 0, 'sectors': 0},
                'read': {'num': 0, 'bytes': 0, 'sectors': 0}},
            'ram3':{
                'write': {'num': 0, 'bytes': 0, 'sectors': 0},
                'read': {'num': 0, 'bytes': 0, 'sectors': 0}},
            'loop0':{
                'write': {'num': 0, 'bytes': 0, 'sectors': 0},
                'read': {'num': 0, 'bytes': 0, 'sectors': 0}},
            'loop1':{
                'write': {'num': 0, 'bytes': 0, 'sectors': 0},
                'read': {'num': 0, 'bytes': 0, 'sectors': 0}},
            'loop2':{
                'write': {'num': 0, 'bytes': 0, 'sectors': 0},
                'read': {'num': 0, 'bytes': 0, 'sectors': 0}},
            'loop3':{
                'write': {'num': 0, 'bytes': 0, 'sectors': 0},
                'read': {'num': 0, 'bytes': 0, 'sectors': 0}},
            'sda':{
                'write': {'num': 348728, 'bytes': 47339000832, 'sectors': 92458986},
                'read': {'num': 511024, 'bytes': 13502986240, 'sectors': 26373020}},
            'sda1':{
                'write': {'num': 24, 'bytes': 70656, 'sectors': 138},
                'read': {'num': 454, 'bytes': 2128896, 'sectors': 4158}},
            'sda2':{
                'write': {'num': 0, 'bytes': 0, 'sectors': 0},
                'read': {'num': 2, 'bytes': 6144, 'sectors': 12}}})
 
 
    def test_net_stats(self):
        self.assertEqual(self.info.net_stats(), {
                'lo': {'receive': {'packets': '81717', 'errors': '0', 'bytes': '19325974'}, 'transmit': {'packets': '81717', 'errors': '0', 'bytes': '19325974'}},
                'lxcbr0': {'receive': {'packets': '0', 'errors': '0', 'bytes': '0'}, 'transmit': {'packets': '2548', 'errors': '0', 'bytes': '193001'}},
                'eth0': {'receive': {'packets': '1160244', 'errors': '0', 'bytes': '1554522775'}, 'transmit': {'packets': '671950', 'errors': '0', 'bytes': '60669160'}}})
 
 
    @mock.patch('scalarizr.api.system.bus')
    def test_scaling_metrics_read(self, bus_mock):
        bus_mock.queryenv_service = mock.Mock()
 
        m = mock.Mock()
        m.id = '777'
        m.name = 'test_name'
        with open('/tmp/test_custom_scaling_metric_read', 'w+') as fp:
            fp.writelines('555')
        m.path = '/tmp/test_custom_scaling_metric_read'
        m.retrieve_method = 'read'
        system.bus.queryenv_service.get_scaling_metrics.return_value = [m]
        assert self.info.scaling_metrics() == [{'error': '', 'id': '777', 'value': 555.0, 'name': 'test_name'}]
        os.remove('/tmp/test_custom_scaling_metric_read')
 
 
    @mock.patch('scalarizr.api.system.bus')
    def test_scaling_metrics_read_error(self, bus_mock):
        bus_mock.queryenv_service = mock.Mock()
 
        m = mock.Mock()
        m.id = '777'
        m.name = 'test_name'
        m.path = '/tmp/this_file_dosnt_exist'
        m.retrieve_method = 'read'
        system.bus.queryenv_service.get_scaling_metrics.return_value = [m]
        assert self.info.scaling_metrics() == [{'error': "File is not readable: '/tmp/this_file_dosnt_exist'", 'id': '777', 'value': 0.0, 'name': 'test_name'}]
 
 
    @mock.patch('scalarizr.api.system.bus')
    def test_scaling_metrics_execute(self, bus_mock):
        bus_mock.queryenv_service = mock.Mock()
 
        m = mock.Mock()
        m.id = '777'
        m.name = 'test_name'
        with open('/tmp/test_custom_scaling_metric_execute.sh', 'w+') as fp:
            fp.writelines('#!/bin/sh\necho "555"\n')
            os.chmod('/tmp/test_custom_scaling_metric_execute.sh', stat.S_IEXEC)
        m.path = '/tmp/test_custom_scaling_metric_execute.sh'
        m.retrieve_method = 'execute'
        system.bus.queryenv_service.get_scaling_metrics.return_value = [m]
        assert self.info.scaling_metrics() == [{'error': '', 'id': '777', 'value': 555.0, 'name': 'test_name'}]
        os.remove('/tmp/test_custom_scaling_metric_execute.sh')
 
 
    @mock.patch('scalarizr.api.system.bus')
    def test_scaling_metrics_execute_error(self, bus_mock):
        bus_mock.queryenv_service = mock.Mock()
 
        m = mock.Mock()
        m.id = '777'
        m.name = 'test_name'
        with open('/tmp/test_custom_scaling_metric_execute.sh', 'w+') as fp:
            fp.writelines('#!/bin/sh\nreturn 1\n')
            os.chmod('/tmp/test_custom_scaling_metric_execute.sh', stat.S_IEXEC)
        m.path = '/tmp/test_custom_scaling_metric_execute.sh'
        m.retrieve_method = 'execute'
        system.bus.queryenv_service.get_scaling_metrics.return_value = [m]
        assert self.info.scaling_metrics() == [{'error': 'exitcode: 1', 'id': '777', 'value': 0.0, 'name': 'test_name'}]
        os.remove('/tmp/test_custom_scaling_metric_execute.sh')
 
 
    @mock.patch('scalarizr.api.system.bus')
    def test_scaling_metrics_execute_timeout(self, bus_mock):
        bus_mock.queryenv_service = mock.Mock()
 
        m = mock.Mock()
        m.id = '777'
        m.name = 'test_name'
        with open('/tmp/test_custom_scaling_metric_execute.sh', 'w+') as fp:
            fp.writelines('#!/bin/sh\nsleep 10s\nreturn 1\n')
            os.chmod('/tmp/test_custom_scaling_metric_execute.sh', stat.S_IEXEC)
        m.path = '/tmp/test_custom_scaling_metric_execute.sh'
        m.retrieve_method = 'execute'
        system.bus.queryenv_service.get_scaling_metrics.return_value = [m]
        assert self.info.scaling_metrics() == [{'error': 'Timeouted', 'id': '777', 'value': 0.0, 'name': 'test_name'}]
 
        ps = subps.Popen(['ps -ef'], shell=True, stdout=subps.PIPE)
        output = ps.stdout.read()
        ps.stdout.close()
        ps.wait()
        assert 'test_custom_scaling_metric_execute.sh' not in output
 
        os.remove('/tmp/test_custom_scaling_metric_execute.sh')
 
 
    @mock.patch('scalarizr.api.system.bus')
    def test_scaling_metrics_multi(self, bus_mock):
        bus_mock.queryenv_service = mock.Mock()
 
        m = mock.Mock()
        m.id = '777'
        m.name = 'test_name'
        with open('/tmp/test_custom_scaling_metric_read', 'w+') as fp:
            fp.writelines('555')
        m.path = '/tmp/test_custom_scaling_metric_read'
        m.retrieve_method = 'read'
        system.bus.queryenv_service.get_scaling_metrics.return_value = [m for _ in range(27)]
        assert self.info.scaling_metrics() == [{'error': '', 'id': '777', 'value': 555.0, 'name': 'test_name'} for _ in range(27)]
        os.remove('/tmp/test_custom_scaling_metric_read')
 
 
def tearDownModule():
    os.remove(DISKSTATS)
    os.remove(CPUINFO)
    os.remove(NETSTATS)
 
 
if __name__ == "__main__":
    unittest.main()
 
