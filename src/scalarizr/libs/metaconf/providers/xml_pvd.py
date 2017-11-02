from __future__ import with_statement
'''
Created on Feb 7, 2011

@author: spike
'''
from .. import ParseError
from ..utils import CommentedTreeBuilder, indent, quote
from . import FormatProvider

import os
import sys

if sys.version_info[0:2] >= (2, 7):
    from xml.etree import ElementTree as ET
else:
    from scalarizr.externals.etree import ElementTree as ET

class XmlFormatProvider(FormatProvider):

    def create_element(self, etree, path, value):
        return ET.Element(quote(os.path.basename(path)))

    def read(self, fp):
        try:
            etree = ET.parse(fp, parser=CommentedTreeBuilder())
        except Exception:
            raise ParseError(())

        indent(etree.getroot())
        return [etree.getroot()]

    def write(self, fp, etree, close):
        try:
            new_tree = ET.ElementTree(list(etree.getroot())[0])
            indent(new_tree.getroot())
            new_tree.write(fp)
        finally:
            if close:
                fp.close()
