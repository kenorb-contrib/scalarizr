from __future__ import with_statement
'''
Created on Aug 10, 2011

@author: Spike
'''
import os
import re
import sys

from . import FormatProvider
from .ini_pvd import IniFormatProvider
from .. import MetaconfError
from ..utils import quote, unquote

if sys.version_info[0:2] >= (2, 7):
    from xml.etree import ElementTree as ET 
else:
    from scalarizr.externals.etree import ElementTree as ET

class RedisFormatProvider(IniFormatProvider):
    
    _opt_re_string = r'(?P<option>[^\s]+)\s+(?P<value>.+)\s*$'
    
    def __init__(self):
        FormatProvider.__init__(self)
        self._readers = (self.read_blank,
                        self.read_comment,
                        self.read_option)
        self._writers = (self.write_blank,
                        self.write_comment,
                        self.write_option)
                    
    def create_element(self, etree, path, value):
        el = FormatProvider.create_element(self, etree, path, value)
        if not value:
            raise MetaconfError("Redis config format doesn't support empty values")
        if os.path.dirname(path) not in ('.', ''):
            raise MetaconfError("Redis config format doesn't support nesting")
        el.attrib['mc_type'] = 'option'
        return el
    
    def read_option(self, line, root):
        if not hasattr(self, "_opt_re"):
            self._opt_re = re.compile(self._opt_re_string)
        if self._opt_re.match(line):
            new_opt = ET.SubElement(self._cursect, quote(self._opt_re.match(line).group('option').strip()))
            value = self._opt_re.match(line).group('value')
            new_opt.text = value
            new_opt.attrib['mc_type'] = 'option'
            return True
        return False
    
    def write_option(self, fp, node):
        if node.attrib.has_key('mc_type') and node.attrib['mc_type'] == 'option':
            value = node.text
            fp.write(unquote(node.tag)+" "+value+'\n')
            return True
        return False
