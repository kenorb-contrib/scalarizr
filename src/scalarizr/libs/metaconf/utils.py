from __future__ import with_statement
'''
Created on Feb 7, 2011

@author: spike
'''
import re
import sys

if sys.version_info[0:2] >= (2, 7):
    from xml.etree import ElementTree as ET
else:
    from scalarizr.externals.etree import ElementTree as ET


def indent(elem, level=0):
    i = "\n" + level*"      "
    if len(elem):
        if not elem.text or not elem.text.strip():
            elem.text = i + "       "
        if not elem.tail or not elem.tail.strip():
            elem.tail = i
        for elem in elem:
            indent(elem, level+1)
        if not elem.tail or not elem.tail.strip():
            elem.tail = i
    else:
        if level and (not elem.tail or not elem.tail.strip()):
            elem.tail = i

def quote(line):
    line = re.sub(' ', '%20', line)
    return re.sub('"', '%22', line)

def unquote(line):
    line = re.sub('%20', ' ', line)
    return re.sub('%22', '"', line)


class CommentedTreeBuilder ( ET.XMLTreeBuilder ):
    def __init__ ( self, html = 0, target = None ):
        ET.XMLTreeBuilder.__init__( self, html, target )
        self._parser.CommentHandler = self.handle_comment

    def handle_comment ( self, data ):
        self._target.start( ET.Comment, {} )
        self._target.data( data.strip() )
        self._target.end( ET.Comment )

def strip_quotes(s):
    return re.sub(r'^["\'](.+)["\']$', r'\1', s)
