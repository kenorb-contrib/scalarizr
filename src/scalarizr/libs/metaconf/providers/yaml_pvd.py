from __future__ import with_statement
'''
Created on Feb 7, 2011

@author: spike
'''

from scalarizr.externals.collections import OrderedDict
from scalarizr.libs.metaconf import ParseError
from scalarizr.libs.metaconf.utils import indent
from yaml.nodes import MappingNode
from yaml.constructor import SafeConstructor, ConstructorError
from yaml.reader import Reader
from yaml.scanner import Scanner
from yaml.parser import Parser
from yaml.composer import Composer
from yaml.resolver import Resolver
from yaml.representer import Representer, SafeRepresenter
from yaml.emitter import Emitter
from yaml.serializer import Serializer
from yaml.nodes import ScalarNode
from yaml import load, dump

import sys
import inspect
import re
from . import FormatProvider

if sys.version_info[0:2] >= (2, 7):
    from xml.etree import ElementTree as ET
else:
    from scalarizr.externals.etree import ElementTree as ET

class YamlFormatProvider(FormatProvider):

    def create_element(self, etree, path, value):
        el = FormatProvider.create_element(self, etree, path, value)
        existed_el = etree.find(path)
        if existed_el is not None:
            existed_el.attrib['mc_type'] = 'list_element'
            el.attrib['mc_type'] = 'list_element'
        return el

    def read(self, fp):
        raw_cfg = fp.read()
        try:
            dict_cfg = load(raw_cfg, Loader = DoublesafeLoader)
        except (Exception, BaseException), e:
            raise ParseError([e])

        root = ET.Element('mc_conf')
        convert_dict_to_xml(root, dict_cfg)

        indent(root)
        return [node for node in root]

    def write(self, fp, etree, close):
        dict_cfg = convert_xml_to_dict(etree.getroot())
        raw_cfg = dump(dict_cfg, Dumper=DoublesafeDumper, default_flow_style=False)
        fp.write(raw_cfg)
        if close:
            fp.close()

class DoublesafeConstructor(SafeConstructor):
    def construct_mapping(self, node, deep=False):
        if not isinstance(node, MappingNode):
            raise ConstructorError(None, None,
                            "expected a mapping node, but found %s" % node.id,
                            node.start_mark)
        mapping = OrderedDict()
        for key_node, value_node in node.value:
            key = self.construct_object(key_node, deep=deep)
            try:
                hash(key)
            except TypeError, exc:
                raise ConstructorError("while constructing a mapping", node.start_mark,
                                "found unacceptable key (%s)" % exc, key_node.start_mark)
            value = self.construct_object(value_node, deep=deep)
            mapping[key] = value
        return mapping

    def construct_yaml_map(self, node):
        data = OrderedDict()
        yield data
        value = self.construct_mapping(node)
        data.update(value)

    def construct_yaml_float(self, node):
        value = str(self.construct_scalar(node))
        value = value.replace('_', '').lower()
        sign = +1
        if value[0] == '-':
            sign = -1
        if value[0] in '+-':
            value = value[1:]
        if value == '.inf':
            return sign*self.inf_value
        elif value == '.nan':
            return self.nan_value
        elif ':' in value:
            digits = [float(part) for part in value.split(':')]
            digits.reverse()
            base = 1
            value = 0.0
            for digit in digits:
                value += digit*base
                base *= 60
            return sign*value
        else:
            return '%s%g' % (str(sign)[0], float(value))

DoublesafeConstructor.add_constructor(
            u'tag:yaml.org,2002:map',
            DoublesafeConstructor.construct_yaml_map)

DoublesafeConstructor.add_constructor(
        u'tag:yaml.org,2002:float',
        DoublesafeConstructor.construct_yaml_float)

class DoublesafeLoader(Reader, Scanner, Parser, Composer, DoublesafeConstructor, Resolver):
    def __init__(self, stream):
        Reader.__init__(self, stream)
        Scanner.__init__(self)
        Parser.__init__(self)
        Composer.__init__(self)
        DoublesafeConstructor.__init__(self)
        Resolver.__init__(self)

class DoublesafeRepresenter(Representer):
    def represent_mapping(self, tag, mapping, flow_style=None):
        value = []
        node = MappingNode(tag, value, flow_style=flow_style)
        if self.alias_key is not None:
            self.represented_objects[self.alias_key] = node
        best_style = True
        if hasattr(mapping, 'items'):
            mapping = mapping.items()
        for item_key, item_value in mapping:
            node_key = self.represent_data(item_key)
            node_value = self.represent_data(item_value)
            if not (isinstance(node_key, ScalarNode) and not node_key.style):
                best_style = False
            if not (isinstance(node_value, ScalarNode) and not node_value.style):
                best_style = False
            value.append((node_key, node_value))
        if flow_style is None:
            if self.default_flow_style is not None:
                node.flow_style = self.default_flow_style
            else:
                node.flow_style = best_style
        return node

    def represent_none(self, data):
        return self.represent_scalar(u'tag:yaml.org,2002:null',
                        u'')

DoublesafeRepresenter.add_representer(OrderedDict, SafeRepresenter.represent_dict)
DoublesafeRepresenter.add_representer(type(None), DoublesafeRepresenter.represent_none)


class DoublesafeEmitter(Emitter):
    def write_single_quoted(self, text, split=True):
        self.write_indicator(u'', True)
        spaces = False
        breaks = False
        start = end = 0
        while end <= len(text):
            ch = None
            if end < len(text):
                ch = text[end]
            if spaces:
                if ch is None or ch != u' ':
                    if start+1 == end and self.column > self.best_width and split   \
                                    and start != 0 and end != len(text):
                        self.write_indent()
                    else:
                        data = text[start:end]
                        self.column += len(data)
                        if self.encoding:
                            data = data.encode(self.encoding)
                        self.stream.write(data)
                    start = end
            elif breaks:
                if ch is None or ch not in u'\n\x85\u2028\u2029':
                    if text[start] == u'\n':
                        self.write_line_break()
                    for br in text[start:end]:
                        if br == u'\n':
                            self.write_line_break()
                        else:
                            self.write_line_break(br)
                    self.write_indent()
                    start = end
            else:
                if ch is None or ch in u' \n\x85\u2028\u2029' or ch == u'\'':
                    if start < end:
                        data = text[start:end]
                        self.column += len(data)
                        if self.encoding:
                            data = data.encode(self.encoding)
                        self.stream.write(data)
                        start = end
            if ch == u'\'':
                data = u'\'\''
                self.column += 2
                if self.encoding:
                    data = data.encode(self.encoding)
                self.stream.write(data)
                start = end + 1
            if ch is not None:
                spaces = (ch == u' ')
                breaks = (ch in u'\n\x85\u2028\u2029')
            end += 1
        #self.write_indicator(u'\'', False)

class DoublesafeDumper(DoublesafeEmitter, Serializer, DoublesafeRepresenter, Resolver):

    def __init__(self, stream,
                    default_style=None, default_flow_style=None,
                    canonical=None, indent=None, width=None,
                    allow_unicode=None, line_break=None,
                    encoding=None, explicit_start=None, explicit_end=None,
                    version=None, tags=None):
        DoublesafeEmitter.__init__(self, stream, canonical=canonical,
                        indent=indent, width=width,
                        allow_unicode=allow_unicode, line_break=line_break)
        Serializer.__init__(self, encoding=encoding,
                        explicit_start=explicit_start, explicit_end=explicit_end,
                        version=version, tags=tags)
        DoublesafeRepresenter.__init__(self, default_style=default_style,
                        default_flow_style=default_flow_style)
        Resolver.__init__(self)

def convert_dict_to_xml(parent, dict_cfg):

    if isinstance(dict_cfg, dict):
        for (key, value) in dict_cfg.iteritems():
            if type(value) == list:
                for listchild in value:
                    elem = ET.Element(key)
                    elem.attrib['mc_type'] = 'list_element'
                    parent.append(elem)
                    convert_dict_to_xml(elem, listchild)
            else:
                elem = ET.Element(key)
                parent.append(elem)
                if type(value) == dict:
                    convert_dict_to_xml(elem, value)
                else:
                    elem.text = value
    else:
        parent.text = dict_cfg

def convert_xml_to_dict(root):
    if not len(root):
        return root.text

    res = OrderedDict()

    for node in root:
        if 'mc_type' in node.attrib and node.attrib['mc_type'] == 'list_element':
            if not node.tag in res:
                res[node.tag] = []
            res[node.tag].append(convert_xml_to_dict(node))
        else:
            res[node.tag] = convert_xml_to_dict(node)

    return res
