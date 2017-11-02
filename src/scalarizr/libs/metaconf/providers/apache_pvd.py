from __future__ import with_statement
'''
Created on Feb 7, 2011

@author: spike
'''

from ..utils import quote, unquote
from .ini_pvd import IniFormatProvider, FormatProvider

import os
import re
import sys

if sys.version_info[0:2] >= (2, 7):
    from xml.etree import ElementTree as ET
else:
    from scalarizr.externals.etree import ElementTree as ET

try:
    from  cStringIO import StringIO
except ImportError:
    from StringIO import StringIO

class ApacheFormatProvider(IniFormatProvider):

    _readers = None
    _writers = None

    def create_element(self, etree, path, value):
        el = FormatProvider.create_element(self, etree, path, value)
        parent_path = os.path.dirname(path)
        if parent_path not in ('.', ''):
            parent = etree.find(parent_path)
            # We are sure that parent element exists, because Configuration calls private method '_find' first
            if parent.attrib.has_key('mc_type') and parent.attrib['mc_type'] != 'section':
                parent.attrib['mc_type'] = 'section'
                if parent.text and parent.text.strip():
                    parent.attrib['value'] = parent.text.strip()
                    parent.text = ''

        el.attrib['mc_type'] = 'option'
        return el

    def __init__(self):
        IniFormatProvider.__init__(self)
        self._nesting  = 0
        self._pad = '   '

    def read_option(self, line, root):
        if not hasattr(self, "_opt_re"):
            self._opt_re = re.compile(r'\s*(?P<option>[^<].*?)\s+(?P<value>.*?)\s*?(?P<backslash>\\?)$')
        result = self._opt_re.match(line)
        if result:
            new_opt = ET.SubElement(self._cursect, quote(result.group('option').strip()))
            new_opt.attrib['mc_type'] = 'option'
            value = result.group('value')
            if result.group('backslash'):
                while True:
                    new_line = self._fp.readline()
                    if not new_line:
                        return False
                    raw_line = new_line.strip()
                    if raw_line.endswith('\\'):
                        value += ' ' + raw_line[:-1]
                    else:
                        value += ' ' + raw_line
                        break
            new_opt.text = value
            return True
        return False


    def write_option(self, fp, node):
        if node.attrib.has_key('mc_type') and node.attrib['mc_type'] == 'option':
            fp.write(self._pad*self._nesting + unquote(node.tag)+"\t"+node.text+'\n')
            return True
        return False

    def write_comment(self, fp, node):
        if callable(node.tag):
            comment_lines  = node.text.split('\n')
            for line in comment_lines:
                fp.write(self._pad*self._nesting + '#'+line+'\n')
            return True
        return False

    def read_section(self, line, root):
        if not hasattr(self, "_sect_re"):
            self._sect_re = re.compile('\s*<(?P<option>[^\s]+)\s*(?P<value>.*?)\s*>\s*$')

        result = self._sect_re.match(line)
        if result:
            tag = result.group('option').strip()
            new_section = ET.SubElement(self._cursect, quote(tag))
            new_section.attrib['mc_type'] = 'section'
            value = result.group('value').strip()
            if value:
                new_section.attrib['value'] = value

            opened = 1

            while opened != 0:
                new_line = self._fp.readline()
                if not new_line:
                    return False

                line += new_line
                stripped = new_line.strip()
                lower = stripped.lower()
                tag_lower = tag.lower()
                if lower.startswith('</'+tag_lower+'>'):
                    opened -= 1
                if lower.startswith('<'+tag_lower):
                    opened += 1

            self._sections.append(self._cursect)
            self._cursect = new_section
            old_fp = self._fp
            content = re.search(re.compile('.*?>\s*\n(.*)<.*?>',re.S), line).group(1).strip()
            self.read(StringIO(content), self._lineno)
            self._fp = old_fp
            self._cursect = self._sections.pop()
            self._lineno += 1
            return True
        return False

    def write_section(self, fp, node):
        if node.attrib.has_key('mc_type') and node.attrib['mc_type'] == 'section':
            value = ' ' + node.attrib['value'] if node.attrib.has_key('value') else ''
            tag = unquote(node.tag)
            fp.write(self._pad*self._nesting + '<' + tag + value + '>\n')
            self._nesting +=1
            try:
                self.write(fp, node, False)
            finally:
                self._nesting -=1
            fp.write(self._pad*self._nesting + '</'+ tag +'>\n')
            return True
        return False
