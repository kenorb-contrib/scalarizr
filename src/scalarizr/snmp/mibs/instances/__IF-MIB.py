from __future__ import with_statement
from sys import version
from time import time
from pysnmp import majorVersionId
import os, re
from pyasn1.type import constraint, namedval
from scalarizr.snmp.mibs import validate
 
( Integer, OctetString, ) = mibBuilder.importSymbols("ASN1", "Integer", "OctetString")
( DisplayString,) = mibBuilder.importSymbols("SNMPv2-TC", "DisplayString")
(Counter32,) = mibBuilder.importSymbols("SNMPv2-SMI", "Counter32")
 
 
( MibScalarInstance,) = mibBuilder.importSymbols(
    'SNMPv2-SMI',
    'MibScalarInstance'
        )
 
(
interfaces,
ifTable,
ifEntry,
ifIndex,
ifDescr,
ifInOctets,
ifOutOctets
     ) = mibBuilder.importSymbols(
    'IF-MIB',
        'interfaces',
        'ifTable',
        'ifEntry',
        'ifIndex',
        'ifDescr',
        'ifInOctets',
        'ifOutOctets'
    )
 
 
directions = {'in' : 1, 'out' : 9}
 
class GetOctets():
    def __init__(self, iface=None, direction=None):
        self.iface = iface
        self.direction = direction
    def clone(self):
        if self.iface != None and self.direction !=None:
            file = open('/proc/net/dev', "r")
            list = file.readlines()
            file.close()
            for row in list:
                row = re.sub(':', ' ', row)
                if re.match('^\s+' + self.iface, row):
                    values = row.split()
                    return validate(Counter32(), values[directions[self.direction]])
 
 
file = open('/proc/net/dev', "r")
ifacesList = file.readlines()
file.close()
ifaces = []
 
for row in ifacesList:
    row = re.sub(':', ' ', row)
    if re.match('^\s+\w+(\s+\d+){16}', row):
        values = row.split()
        ifaces.append((values[0], values[1], values[9],))
 
 
for i in range(len(ifaces)):
    ifIndexInst = MibScalarInstance(ifIndex.getName(), (i+1,), ifIndex.getSyntax().clone(i+1))
    ifDescrInst = MibScalarInstance(ifDescr.getName(), (i+1,), ifDescr.getSyntax().clone(ifaces[i][0]))
    ifInOctetsInst = MibScalarInstance(ifInOctets.getName(), (i+1,), GetOctets(ifaces[i][0], 'in'))
    ifOutOctetsInst = MibScalarInstance(ifOutOctets.getName(), (i+1,), GetOctets(ifaces[i][0], 'out'))
 
    namedSyms = {
            'ifIndex' + str(i) : ifIndexInst,
            'ifDescr' + str(i) : ifDescrInst,
            'ifInOctets' + str(i)  : ifInOctetsInst,
            'ifOutOctets' + str(i) : ifOutOctetsInst
    }
 
    mibBuilder.exportSymbols("__IF-MIB", **namedSyms)
 
 
mibBuilder.exportSymbols(
    "__IF-MIB",
        interfaces = interfaces,
        ifTable = ifTable,
        ifEntry = ifEntry
    )
 
