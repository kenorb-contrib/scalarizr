'''
Created on 22.01.2010

@author: marat
@author: Dmytro Korsakov
'''

import os
import time
import logging
import logging.handlers
from datetime import datetime

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


class UtcOffsetFormatter(logging.Formatter):
    """time.strftime("%Y-%m-%d %H:%M:%S %z", time.localtime()) doesn't work in Python < 3.3. See:

    http://bugs.python.org/issue1493676
    http://bugs.python.org/issue1667546

    So we cant just specify a custom `datefmt` parameter with the `%z` option in the logging config,
    but need to create a separate formatter and compute the offset themselves,
    taking into account various edge cases:

    http://bz.selenic.com/show_bug.cgi?id=2511"""
    def formatTime(self, record, datefmt=None):
        # logging.Formatter is old-style in python 2.6, cant use super
        original = logging.Formatter.formatTime(self, record, datefmt)

        delta = (datetime.utcfromtimestamp(record.created) -
                 (datetime.fromtimestamp(record.created)))
        offset_seconds = delta.days * 86400 + delta.seconds

        sign = '-' if offset_seconds > 0 else '+'
        utc_offset = time.strftime("%H:%M", time.gmtime(abs(offset_seconds)))

        return ''.join([original, sign, utc_offset])


class DebugFormatter(UtcOffsetFormatter):
    pass


class UserFormatter(UtcOffsetFormatter, NoStacktraceFormatter):
    pass
