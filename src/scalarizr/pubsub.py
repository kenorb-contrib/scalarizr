from __future__ import with_statement
'''
Created on Nov 21, 2011
 
@author: marat
 
Simple Publish-Subscribe message system on the top of 0MQ
'''
 
 
class PubSub(object):
 
    data = None
 
    def __init__(self, address, publisher_data=None):
        pass
 
    def add_publisher(self, address):
        pass
 
    def remove_publisher(self, address):
        pass
 
    def publish(self, subject, message):
        pass
 
    def subscribe(self,  subject, handler):
        pass
 
    def unsubscribe(self, subject, handler):
        pass
 
    def serve_forever(self):
        pass
 
    def stop(self):
        pass
 
 
class Barrier(object):
    '''
    with Barrier('mongodb.configuration', 4, 600):
            # This will never executes until 4 nodes join barrier 'mongodb.configuration'
            # within 600 seconds timeout
            pass
    '''
 
    def __init__(self, name, size, timeout=None):
        pass
 
    def __enter__(self):
        return self
 
    def __exit__(self, *args):
        pass
 
