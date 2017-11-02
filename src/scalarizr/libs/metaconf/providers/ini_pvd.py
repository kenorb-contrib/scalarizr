from __future__ import with_statement
'''
Created on Feb 7, 2011

@author: spike
'''
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

class IniFormatProvider(FormatProvider):

    _readers = None
    _writers = None
    _comment_re_string = '\s*[#;](.*)$'
    _opt_re_string          = r'(?P<option>[^:=\s][^:=]*)\s*(?P<vi>[:=])\s*(?P<value>.*?)\s*(?P<comment>[#;](.*))?$'

    def __init__(self):
        FormatProvider.__init__(self)
        self._readers = (
                self.read_blank,
                self.read_comment,
                self.read_section,
                self.read_option
        )
        self._writers = (
                self.write_blank,
                self.write_comment,
                self.write_section,
                self.write_option
        )

    def create_element(self, etree, path, value):
        el = FormatProvider.create_element(self, etree, path, value)
        parent_path = os.path.dirname(path)
        if os.path.dirname(parent_path) not in ('.', ''):
            raise MetaconfError('Maximum nesting level for ini format is 2')
        elif parent_path in ('.', ''):
            if etree.find(path) is not None:
                raise MetaconfError("Ini file can't contain two identical sections")
            el.attrib['mc_type'] = 'section'
        else:
            el.attrib['mc_type'] = 'option'
        return el


    def read_comment(self, line, root):
        if not hasattr(self, "_comment_re"):
            self._comment_re = re.compile(self._comment_re_string)
        if self._comment_re.match(line):
            comment = ET.Comment(self._comment_re.match(line).group(1))
            self._cursect.append(comment)
            return True
        return False

    def read_section(self, line, root):
        if not hasattr(self, "_sect_re"):
            self._sect_re = re.compile(r'\[(?P<header>[^]]+)\]')
        if self._sect_re.match(line):
            self._cursect = ET.SubElement(root, quote(self._sect_re.match(line).group('header')))
            self._cursect.attrib['mc_type'] = 'section'
            return True
        return False


    def read_blank(self, line, root):
        if '' == line.strip():
            ET.SubElement(self._cursect, '')
            return True
        return False


    def read_option(self, line, root):
        if not hasattr(self, "_opt_re"):
            self._opt_re = re.compile(self._opt_re_string)
        if self._opt_re.match(line):
            if self._opt_re.match(line).group('comment'):
                comment = ET.Comment(self._opt_re.match(line).group('comment')[1:])
                self._cursect.append(comment)
            new_opt = ET.SubElement(self._cursect, quote(self._opt_re.match(line).group('option').strip()))
            value = self._opt_re.match(line).group('value')
            if len(value) > 1 and value[0] in ('"', "'") and value[-1] in ('"', "'") and value[0] == value[-1]:
                value = value[1:-1]
            new_opt.text = value
            new_opt.attrib['mc_type'] = 'option'
            return True
        return False


    def write_comment(self, fp, node):
        if callable(node.tag):
            comment_lines  = str(node.text).split('\n')
            for line in comment_lines:
                fp.write('#'+line+'\n')
            return True
        return False


    def write_section(self, fp, node):
        if node.attrib.has_key('mc_type') and node.attrib['mc_type'] == 'section':
            fp.write('['+unquote(node.tag)+']\n')
            self.write(fp, node, False)
            return True
        return False


    def write_option(self, fp, node):
        if node.attrib.has_key('mc_type') and node.attrib['mc_type'] == 'option':
            value = str(node.text if node.text else '')
            if re.search('\s', value) or value == '':
                value = '"' + value + '"'
            fp.write(unquote(node.tag)+"\t= "+value+'\n')
            return True
        return False


    def write_blank(self, fp, node):
        if not node.tag and not callable(node.tag):
            fp.write('\n')
            return True
        return False
