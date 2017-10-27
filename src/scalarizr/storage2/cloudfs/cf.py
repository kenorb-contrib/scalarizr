from scalarizr.node import __node__
from scalarizr.storage2 import cloudfs
from scalarizr.storage2.cloudfs import swift
 
 
class CloudfilesFileSystem(swift.SwiftFileSystem):
 
    def _get_connection(self):
        return __node__['rackspace']['new_swift_connection']
 
 
cloudfs.cloudfs_types["cf"] = CloudfilesFileSystem
 
