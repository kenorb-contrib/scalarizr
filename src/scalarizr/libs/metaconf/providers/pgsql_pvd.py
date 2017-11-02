from __future__ import with_statement
'''
Created on Apr 26, 2011

@author: spike
'''

from .ini_pvd import IniFormatProvider
from . import FormatProvider
from .. import MetaconfError
from ..utils import unquote
import os

class PgsqlFormatProvider(IniFormatProvider):

    def __init__(self):
        IniFormatProvider.__init__(self)
        self._comment_re_string = '\s*#(.*)$'

    def write_section(self, fp, node):
        return False

    def read_section(self, line, root):
        return False

    def create_element(self, etree, path, value):
        el = FormatProvider.create_element(self, etree, path, value)
        parent_path = os.path.dirname(path)
        if parent_path not in ('.', ''):
            raise MetaconfError('Maximum nesting level for postgresql format is 1')
        else:
            if etree.find(path) is not None:
                raise MetaconfError("Postgresql configuration file can't contain two identical options")
            el.attrib['mc_type'] = 'option'
        return el

    def write_option(self, fp, node):
        if node.attrib.has_key('mc_type') and node.attrib['mc_type'] == 'option':
            value = node.text if node.text else ''
            value = "'" + value + "'"
            fp.write(unquote(node.tag)+"\t= "+value+'\n')
            return True
        return False
