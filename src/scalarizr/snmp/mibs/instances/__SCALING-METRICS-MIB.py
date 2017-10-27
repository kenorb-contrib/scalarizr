from __future__ import with_statement
'''
Created on Sep 27, 2010
 
@author: marat
'''
from scalarizr.bus import bus
from scalarizr.queryenv import ScalingMetric
from scalarizr.util import read_shebang
import signal
import os, time, logging
from subprocess import Popen, PIPE
from scalarizr.util import system, kill_childs
from threading import Thread
from Queue import Queue, Empty
 
# SNMP imports
from pysnmp.smi.builder import MibBuilder
from scalarizr.config import ScalarizrState
 
 
(Bits, Counter32, Integer32, ModuleIdentity, MibIdentifier, NotificationType,
 MibScalar, MibTable, MibTableRow, MibTableColumn,
 Opaque, TimeTicks, enterprises, MibScalarInstance ) = mibBuilder.importSymbols(
"SNMPv2-SMI", "Bits", "Counter32", "Integer32", "ModuleIdentity", "MibIdentifier",
"NotificationType", "MibScalar", "MibTable", "MibTableRow",
"MibTableColumn", "Opaque", "TimeTicks", "enterprises", "MibScalarInstance")
 
(DisplayString, ) = mibBuilder.importSymbols('SNMPv2-TC', 'DisplayString')
 
 
CACHE_TIME = 600 # 10 minutes
 
_metrics = None
_metrics_timestamp = 0
logger = logging.getLogger('scalarizr.snmp.mibs.SCALING-METRICS-MIB')
 
 
class MtxTableImpl(MibTable):
 
    EXEC_TIMEOUT = 3
    '''
    Executing timeout for script when obtain metric with 'execute' method
    '''
 
    CACHE_TIME = 5
 
    _last_request_time = None
 
    def __init__(self, name):
        MibTable.__init__(self, name)
 
    def getNextNode(self, name, idx):
        mibBuilder.lastBuildId += 1
 
        now = time.time()
        if self._last_request_time is None or now - self._last_request_time > self.CACHE_TIME:
            self._last_request_time = now
            # Update with new values
            mibBuilder.mibSymbols['__SCALING-METRICS-MIB'] = values()
        return MibTable.getNextNode(self, name, idx)
 
 
class GetAuthShutdown():
    def __init__(self, i=None):
        self.i = i
    def clone(self):
        script_path = '/usr/local/scalarizr/hooks/auth-shutdown'
        ret = 1
        if os.access(script_path, os.X_OK):
            try:
                logger.debug('Executing %s', metric.path)
                proc = Popen(metric.path, stdout=PIPE, stderr=PIPE, close_fds=True)
                ret = int(proc.communicate()[0])
            except:
                pass
        return authShutdown.getSyntax().clone(ret)
 
 
 
scalr = ModuleIdentity((1, 3, 6, 1, 4, 1, 36632))
mtxTable = MtxTableImpl((1, 3, 6, 1, 4, 1, 36632, 5))
mtxEntry = MibTableRow((1, 3, 6, 1, 4, 1, 36632, 5, 1)).setIndexNames((0, "SCALING-METRICS-MIB", "mtxIndex"))
mtxIndex = MibTableColumn((1, 3, 6, 1, 4, 1, 36632, 5, 1, 1), Integer32()).setMaxAccess("readonly")
mtxId = MibTableColumn((1, 3, 6, 1, 4, 1, 36632, 5, 1, 2), Integer32()).setMaxAccess("readonly")
mtxName = MibTableColumn((1, 3, 6, 1, 4, 1, 36632, 5, 1, 3), DisplayString()).setMaxAccess("readonly")
mtxValue = MibTableColumn((1, 3, 6, 1, 4, 1, 36632, 5, 1, 4), DisplayString()).setMaxAccess("readonly")
mtxError = MibTableColumn((1, 3, 6, 1, 4, 1, 36632, 5, 1, 5), DisplayString()).setMaxAccess("readonly")
 
uglyFeatures = MibIdentifier((1, 3, 6, 1, 4, 1, 36632, 6))
authShutdown = MibScalar((1, 3, 6, 1, 4, 1, 36632, 6, 1), Integer32()).setMaxAccess("readonly")
 
__authShutdown = MibScalarInstance(authShutdown.name, (0,), GetAuthShutdown(0))
 
 
def values():
    global _metrics
    global _metrics_timestamp
 
    queryenv = bus.queryenv_service
    cnf = bus.cnf
    ret = {
            'scalr'    : scalr,
            'mtxTable' : mtxTable,
            'mtxIndex' : mtxIndex,
            'mtxId'    : mtxId,
            'mtxName'  : mtxName,
            'mtxValue' : mtxValue,
            'mtxError' : mtxError,
            'mtxEntry' : mtxEntry,
            'uglyfeatures': uglyFeatures,
            'autoshutdown': authShutdown,
            '__autoshutdown': __authShutdown
    }
 
 
    if cnf.state != ScalarizrState.RUNNING:
        return ret
 
    # Obtain scaling metrics from Scalr. Cache result
    now = time.time()
    if _metrics is None or now - _metrics_timestamp > CACHE_TIME:
        logger.debug('Obtain scaling metrics from QueryEnv')
        _metrics = queryenv.get_scaling_metrics()
        _metrics_timestamp = now
    else:
        logger.debug('Use cached scaling metrics. Expires: %s',
                        time.strftime('"%Y-%m-%d %H:%M:%S', time.localtime(_metrics_timestamp + CACHE_TIME)))
 
    index = 0
    queue = Queue()
    for metric in _metrics:
        queue.put(metric)
 
    workers = []
    for i in range(len(_metrics)):
        index += 1
        worker = Thread(target = update_metric, name = 'Worker-%s' % i, args = (queue, index, ret))
        worker.start()
        workers.append(worker)
 
    for worker in workers:
        worker.join()
 
    return ret
 
def _get_execute(metric):
    if not os.access(metric.path, os.X_OK):
        raise BaseException("File is not executable: '%s'" % metric.path)
 
    logger.debug('Executing %s', metric.path)
    proc = Popen(metric.path, stdout=PIPE, stderr=PIPE, close_fds=True)
    start_time = time.time()
    while time.time() - start_time < MtxTableImpl.EXEC_TIMEOUT:
        if proc.poll() is None:
            time.sleep(0.1)
        else:
            break
    else:
        if hasattr(proc, 'kill'):
            # python >= 2.6
            kill_childs(proc.pid)
            proc.terminate()
        else:
            kill_childs(proc.pid)
            os.kill(proc.pid, signal.SIGTERM)
        raise BaseException('Timeouted')
 
    stdout, stderr = proc.communicate()
 
    if proc.returncode > 0:
        raise BaseException(stderr if stderr else 'exitcode: %d' % proc.returncode)
 
    return stdout
 
def _get_read( metric):
    if not os.access(metric.path, os.R_OK):
        raise BaseException("File is not readable: '%s'" % metric.path)
 
    file = None
    try:
        file = open(metric.path, 'r')
        value = file.readline()
    finally:
        if file:
            file.close()
        del file
 
    return value
 
def update_metric(queue, index, ret):
    error = ''
    value = 0.0
    try:
        metric = queue.get(False)
    except Empty:
        return
 
    try:
        # Retrieve metric value
        logger.debug('Updating metric %s', metric)
        if ScalingMetric.RetriveMethod.EXECUTE == metric.retrieve_method:
            value = _get_execute(metric)
        elif ScalingMetric.RetriveMethod.READ  == metric.retrieve_method:
            value = _get_read(metric)
        else:
            raise BaseException('Unknown retrieve method %s' % metric.retrieve_method)
 
        # Convert value to float
        try:
            value = float(value)
        except ValueError, e:
            raise ValueError("Cannot convert value '%s' to float" % value)
 
    except (BaseException, Exception), e:
        value = 0.0
        error = str(e)[0:255]
 
 
    # Export MibTableRow
 
    ret.update({
            'mtxIndex%s' % index : MibScalarInstance(mtxIndex.getName(), (index,), mtxIndex.getSyntax().clone(
                    index
            )),
            'mtxId%s' % index : MibScalarInstance(mtxId.getName(), (index,), mtxId.getSyntax().clone(
                    int(metric.id)
            )),
            'mtxName%s' % index : MibScalarInstance(mtxName.getName(), (index,), mtxName.getSyntax().clone(
                    metric.name
            )),
            'mtxValue%s' % index : MibScalarInstance(mtxValue.getName(), (index,), mtxValue.getSyntax().clone(
                    '%.7f' % value
            )),
            'mtxError%s' % index : MibScalarInstance(mtxError.getName(), (index,), mtxError.getSyntax().clone(
                    error
            ))
    })
 
mibBuilder.mibSymbols["__SCALING-METRICS-MIB"] = values()
 
