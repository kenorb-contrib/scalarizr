from __future__ import with_statement
from .. import ParseError, MetaconfError
from ..utils import quote, unquote, indent

import sys
import os

if sys.version_info[0:2] >= (2, 7):
    from xml.etree import ElementTree as ET
else:
    from scalarizr.externals.etree import ElementTree as ET


class FormatProvider:
    _readers = None
    _writers = None

    def __init__(self):
        self._readers = ()
        self._writers = ()
        self._sections = []
        self._errors = []

    def create_element(self, etree, path, value):
        return ET.Element(quote(os.path.basename(path)))

    def read(self, fp, baseline = 0):
        """
        @return: xml.etree.ElementTree
        """
        self._lineno = baseline
        if not hasattr(self, '_sections') and not hasattr(self, '_errors'):
            self._sections = []
            self._errors = []
        self._fp = fp
        root = ET.Element("configuration")

        toplevel = False
        if not hasattr(self, '_cursect'):
            toplevel = True
            self._cursect = root

        try:
            while True:
                line = self._fp.readline()
                if not line:
                    break
                self._lineno += 1
                for reader in self._readers:
                    if reader(line, root):
                        break
                else:
                    self._errors.append((self._lineno, line.strip()))

            indent(root)
            if self._errors and not self._sections:
                raise ParseError(self._errors)
            else:
                return list(root)
        finally:
            if toplevel:
                del(self._cursect)

    def write(self, fp, etree, close = True):
        """
        Write ElementTree <etree> to filepointer <fp>. If <close> is True - close <fp>
        """
        try:
            if not (isinstance(etree, ET._ElementInterface) or isinstance(etree, ET.ElementTree)):
                raise MetaconfError("etree param must be instance of _ElementInterface or ElementTree. %s passed" % (etree,))
            errors = []
            toplevel = list(etree.find('.'))
            for section in toplevel:
                for writer in self._writers:
                    if writer(fp, section):
                        break
                else:
                    errors.append(unquote(section.tag))
            if errors:
                raise MetaconfError(errors)
        finally:
            if close:
                fp.close()
