from __future__ import with_statement
'''
Created on Jun 4, 2010
 
@author: marat
@author: spike
'''
 
import os, logging
try:
    import select
except ImportError:
    import selectmodule as select
 
from pysnmp.entity import engine, config
from pysnmp.entity.rfc3413 import cmdrsp, context
from pysnmp.carrier.asynsock.dgram import udp
from pysnmp.carrier.error import CarrierError
from pysnmp.smi.error import SmiError
 
known_modules = (
        '__UCD-SNMP-MIB',
        '__UCD-DISKIO-MIB',
        '__IF-MIB',
        '__HOST-RESOURCES-MIB',
        '__SCALING-METRICS-MIB'
)
 
class SnmpServer():
    port = None
    _security_name = None
    _community_name = None
    _engine = None
    _modules = None
 
    def __init__(self, port=None, security_name=None, community_name=None, modules=None):
        self._logger = logging.getLogger(__name__)
        self.port = port
        self._security_name = security_name
        self._community_name = community_name
        self._modules = modules or known_modules
 
    def start(self):
        if self._engine is None:
            # Create SNMP engine with autogenernated engineID and pre-bound
            # to socket transport dispatcher
            self._engine = engine.SnmpEngine()
 
            # Setup UDP over IPv4 transport endpoint
        try:
            iface = ('0.0.0.0', self.port)
            self._logger.info("[pid: %d] Starting SNMP server on %s:%d",  os.getpid(), iface[0], iface[1])
            config.addSocketTransport(
            self._engine,
            udp.domainName,
            udp.UdpSocketTransport().openServerMode(iface)
            )
        except CarrierError:
            self._logger.error('Can\'t run SNMP agent on port %d: Address already in use', self.port)
            raise
 
        mibBuilder = self._engine.msgAndPduDsp.mibInstrumController.mibBuilder
 
        MibSources = mibBuilder.getMibPath()
        sources =  ['/mibs','/mibs/instances']
        for source in sources:
            MibSources += ((os.path.realpath(os.path.dirname(__file__) + source), ))
        apply(mibBuilder.setMibPath, MibSources)
 
        try:
            mibBuilder.loadModules(*self._modules)
        except SmiError:
            self._logger.warn('Can\'t load modules')
            raise
 
        config.addV1System(self._engine, self._security_name, self._community_name)
 
        # VACM setup
        config.addContext(self._engine, '')
        config.addRwUser(self._engine, 1, self._security_name, 'noAuthNoPriv', (1,3,6)) # v1
        config.addRwUser(self._engine, 2, self._security_name, 'noAuthNoPriv', (1,3,6)) # v2c
 
        # SNMP context
        snmpContext = context.SnmpContext(self._engine)
        # Apps registration
        cmdrsp.GetCommandResponder(self._engine, snmpContext)
        cmdrsp.SetCommandResponder(self._engine, snmpContext)
        cmdrsp.NextCommandResponder(self._engine, snmpContext)
        cmdrsp.BulkCommandResponder(self._engine, snmpContext)
 
        # Start server
        self._logger.debug('Starting transport dispatcher')
        self._engine.transportDispatcher.jobStarted(1)
        try:
            self._logger.debug('Run transport dispatcher')
            self._engine.transportDispatcher.runDispatcher()
        except select.error, e:
            if e.args[0] == 9:
                # 'Bad file descriptor'
                # Throws when dispatcher closed from another thread
                pass
            else:
                raise
        except KeyboardInterrupt:
            pass
 
 
    def stop(self):
        if self._engine:
            self._engine.transportDispatcher.closeDispatcher()
 
