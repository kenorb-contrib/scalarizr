from __future__ import with_statement
'''
Created on Feb 7, 2011

@author: spike
'''
from .ini_pvd import IniFormatProvider
from . import FormatProvider
from .. import MetaconfError
from ..utils import quote, unquote

import os
import re
import sys

if sys.version_info[0:2] >= (2, 7):
    from xml.etree import ElementTree as ET
else:
    from scalarizr.externals.etree import ElementTree as ET


class MysqlFormatProvider(IniFormatProvider):

    _opt_re_string          = r'(?P<option>[^:=\s][^:=]*)\s*(?P<vi>[:=])\s*(?P<value>.*?)\s*(?P<comment>#(.*))?$'

    def __init__(self):
        IniFormatProvider.__init__(self)
        self._readers  = (self.read_statement,
                                           self.read_include) + self._readers

        self._writers  = (self.write_statement,
                                           self.write_include) + self._writers


    def create_element(self, etree, path, value):
        el = FormatProvider.create_element(self, etree, path, value)

        parent_path = os.path.dirname(path)

        if os.path.dirname(parent_path) not in ('.', ''):
            raise MetaconfError('Maximum nesting level for ini format is 2')
        elif parent_path in ('.', '') and not '!include' in el.tag:
            if etree.find(path) is not None:
                raise MetaconfError("Mysql file can't contain two identical sections")
            el.attrib['mc_type'] = 'section'
        else:
            if value:
                el.attrib['mc_type'] = 'include' if '!include' in el.tag else 'option'
            else:
                el.attrib['mc_type'] = 'statement'
        return el

    def read_statement(self, line, root):
        if not hasattr(self, "_stat_re"):
            self._stat_re = re.compile(r'\s*([^#=\s\[\]]+)\s*$')
        if self._stat_re.match(line):
            new_statement = ET.SubElement(self._cursect, quote(self._stat_re.match(line).group(1)))
            new_statement.attrib['mc_type'] = 'statement'
            return True
        return False

    def read_include(self, line, root):
        if not hasattr(self, "_inc_re"):
            self._inc_re = re.compile(r'\s*(!include(dir)?)\s+(.+)$')
        if self._inc_re.match(line):
            new_include = ET.SubElement(self._cursect, quote(self._inc_re.match(line).group(1)))
            new_include.text = self._inc_re.match(line).group(3).strip()
            new_include.attrib['mc_type'] = 'include'
            return True
        return False


    def write_statement(self, fp, node):
        if node.attrib.has_key('mc_type') and node.attrib['mc_type'] == 'statement':
            fp.write(unquote(node.tag)+'\n')
            return True
        return False

    def write_include(self, fp, node):
        if node.attrib.has_key('mc_type') and node.attrib['mc_type'] == 'include':
            fp.write(unquote(node.tag)+" "+node.text.strip()+'\n')
            return True
        return False
