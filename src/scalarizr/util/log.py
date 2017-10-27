'''
Created on 22.01.2010
 
@author: marat
@author: Dmytro Korsakov
'''
 
import logging
import logging.handlers
import os
 
from scalarizr import linux
 
 
class RotatingFileHandler(logging.handlers.RotatingFileHandler):
    def __init__(self, filename, mode, maxBytes, backupCount, chmod=0600):
        logging.handlers.RotatingFileHandler.__init__(self, filename, mode, maxBytes, backupCount)
        try:
            if not linux.os.windows_family:
                os.chown(self.baseFilename, os.getuid(), os.getgid())
                os.chmod(self.baseFilename, chmod)
        except OSError:
            pass
 
 
class NoStacktraceFormatter(logging.Formatter):
 
    def formatException(self, exc_info):
        # pylint: disable=W0613
        return ''
 
