from __future__ import with_statement
'''
Created on Feb 14th, 2011
 
@author: Dmytro Korsakov
'''
 
class MachineImage(object):
    '''
    classdocs
    '''
    name = None
    file = None
    attributes = None
    account = None
 
 
    def __init__(self, name=None, file=None, attributes=None, account = None, from_response=None):
        if from_response:
            self._deserialize(from_response)
        if name:
            self.name = name
        if file:
            self.file = file
        if attributes:
            self.attributes = attributes or {}
        if account:
            self.account = account
 
    def _deserialize(self, entry):
        self.name = entry['name']
        self.file = entry['file']
        self.attributes = entry['attributes']
        self.account = entry['account']
 
    def __eq__(self, other):
        return self.name == other.name
 
    def __repr__(self):
        return '<nimbula:MachineImage name=%s, file=%s, attributes=%s>' % \
                        (self.name, self.file, self.attributes)
 
 
class Snapshot(object):
    conn = None
    instance = None
    name = None
    machineimage = None
    state = None
    account = None
    site = None
 
    def __init__(self, conn, **kwargs):
        self.conn = conn
        self.set_data(**kwargs)
 
    def set_data(self, **kwargs):
        for k, v in kwargs.items():
            if hasattr(self, k):
                setattr(self, k, v)
        return self
 
    def update(self):
        return self.conn.get_snapshot(snap=self)
 
    def __repr__(self):
        return '<nimbula:Snapshot name=%s instance=%s machineimage=%s state=%s>' % \
                        (self.name, self.instance, self.machineimage, self.state)
 
