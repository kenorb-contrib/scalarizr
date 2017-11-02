from __future__ import with_statement
'''
Created on Feb 7, 2011

@author: spike
'''
from .ini_pvd import IniFormatProvider
from . import FormatProvider
from ..utils import quote, unquote

import re
import os
import sys

if sys.version_info[0:2] >= (2, 7):
    from xml.etree import ElementTree as ET
else:
    from scalarizr.externals.etree import ElementTree as ET

try:
    from  cStringIO import StringIO
except ImportError:
    from StringIO import StringIO

class NginxFormatProvider(IniFormatProvider):

    def __init__(self):
        IniFormatProvider.__init__(self)
        self._readers += (self.read_statement,)
        self._writers += (self.write_statement,)
        self._nesting  = 0
        self._pad = '   '

    def create_element(self, etree, path, value):
        el = FormatProvider.create_element(self, etree, path, value)
        parent_path = os.path.dirname(path)
        if parent_path not in  ('.', ''):
            parent = etree.find(parent_path)
            # We are sure that parent element exists, because Configuration calls private method '_find' first
            if parent.attrib.has_key('mc_type') and parent.attrib['mc_type'] != 'section':
                parent.attrib['mc_type'] = 'section'
                if parent.text and parent.text.strip():
                    parent.attrib['value'] = parent.text.strip()
                    parent.text = ''

        el.attrib['mc_type'] = 'option' if value else 'statement'
        return el

    def read_comment(self, line, root):
        if not hasattr(self, "_comment_re"):
            self._comment_re = re.compile('\s*#(.*)$')
        if self._comment_re.match(line):
            comment = ET.Comment(self._comment_re.match(line).group(1))
            self._cursect.append(comment)
            return True
        return False


    def read_option(self, line, root):
        if not hasattr(self, "_multi_re"):
            self._multi_re = re.compile("\s*(?P<statement>[^\s]+)\s+(?P<value>.+?)(?P<multi_end>;)?\s*(#(?P<comment>.*))?$")

        result = self._multi_re.match(line)

        if result:
            new_multi = ET.Element(quote(result.group('statement').strip()))
            new_multi.attrib['mc_type'] = 'option'
            multi_value = quote(result.group('value').strip())
            if result.group('comment'):
                comment = ET.Comment(result.group('comment').strip())
                self._cursect.append(comment)
            if result.group('multi_end'):
                new_multi.text = multi_value
                self._cursect.append(new_multi)
                return True
            else:
                opened = 1
                if not hasattr(self, "_multi_block"):
                    self._multi_block = re.compile("\s*(?P<value>[^#]+?)(?P<multi_end>;)?\s*(#(?P<comment>.*))?$")
                while opened != 0:
                    new_line = self._fp.readline()
                    if not new_line:
                        return False
                    result = self._multi_block.match(new_line)
                    if not result:
                        return False
                    self._lineno += 1
                    if result.group('comment'):
                        comment = ET.Comment(result.group('comment'))
                        self._cursect.append(comment)
                    multi_value += quote('\n'+self._pad+result.group('value').strip())
                    if result.group('multi_end'):
                        opened -= 1
                new_multi.text = multi_value
                self._cursect.append(new_multi)
                return True
        return False

    def read_statement(self, line, root):
        if not hasattr(self, "_stat_re"):
            self._stat_re = re.compile(r'\s*([^\s\[\]]*)\s*;\s*$')
        if self._stat_re.match(line):
            new_statement = ET.SubElement(self._cursect, quote(self._stat_re.match(line).group(1)))
            new_statement.attrib['mc_type'] = 'statement'
            return True
        return False

    def read_section(self, line, root):
        if not hasattr(self, "_sect_re"):
            self._sect_re = re.compile('\s*(?P<option>[^\s]+)\s*(?P<value>.*?)\s*{\s*(?P<comment>#(.*))?\s*')

        result = self._sect_re.match(line)
        if result:
            new_section = ET.SubElement(self._cursect, quote(result.group('option').strip()))
            new_section.attrib['mc_type'] = 'section'
            if result.group('value'):
                new_section.attrib['value'] = quote(result.group('value').strip())
            if result.group('comment'):
                new_section.append(ET.Comment(result.group(4)))
            opened = 1 if '}' not in line.split('#')[0] else 0

            while opened != 0:
                new_line = self._fp.readline()
                if not new_line:
                    return False
                line += new_line

                line_wo_comment = new_line.split('#')[0]
                if '{' in line_wo_comment:
                    opened += 1
                if '}' in line_wo_comment:
                    opened -= 1

            self._sections.append(self._cursect)
            self._cursect = new_section
            old_fp = self._fp
            content = re.search(re.compile('{(.*)}',re.S), line).group(1).strip()
            self.read(StringIO(content), self._lineno)
            self._fp = old_fp
            self._cursect = self._sections.pop()
            self._lineno += 1
            return True
        return False

    def write_comment(self, fp, node):
        if callable(node.tag):
            comment_lines  = node.text.split('\n')
            for line in comment_lines:
                fp.write(self._pad*self._nesting + '#'+line+'\n')
            return True
        return False

    def write_statement(self, fp, node):
        if node.attrib.has_key('mc_type') and node.attrib['mc_type'] == 'statement':
            fp.write(self._pad*self._nesting + unquote(node.tag)+';\n')
            return True
        return False

    def write_section(self, fp, node):
        if node.attrib.has_key('mc_type') and node.attrib['mc_type'] == 'section':
            value = unquote(node.attrib['value']) if node.attrib.has_key('value') else ''
            fp.write(self._pad*self._nesting + unquote(node.tag) + ' ' + value + ' {\n')
            self._nesting +=1
            try:
                self.write(fp, node, False)
            finally:
                self._nesting -=1
            fp.write(self._pad*self._nesting + '}\n')
            return True
        return False

    def write_option(self, fp, node):
        if node.attrib.has_key('mc_type') and node.attrib['mc_type'] == 'option':
            values = node.text.split('\n') if getattr(node, 'text') else ['']
            fp.write (self._pad*self._nesting + unquote(node.tag) + self._pad + unquote(values.pop(0)))
            if len(values):
                tag_len = len(node.tag)
                for value in values:
                    fp.write('\n'+self._pad*self._nesting + ' '*tag_len + unquote(value))
            fp.write(';\n')
            return True
        return False
