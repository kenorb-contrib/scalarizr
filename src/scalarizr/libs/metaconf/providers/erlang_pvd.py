from __future__ import with_statement
'''
Created on Sep 6, 2011

@author: Spike
'''
import string

from . import FormatProvider, ET
from .. import MetaconfError, ParseError
from ..utils import quote, unquote

try:
    from cStringIO import StringIO
except:
    from StringIO import StringIO

class ErlangFormatProvider(FormatProvider):
        
    def create_element(self, etree, path, value):
        el = FormatProvider.create_element(self, etree, path, value)
        
        return el

    def read(self, fp):

        def parse_list(fp):
            nodes = []
            while True:
                sym = fp.read(1)
                if sym in string.whitespace:
                    continue
                if sym == '{':
                    node = parse_hash(fp)
                    nodes.append(node)
                    while True:
                        sym = fp.read(1)
                        if sym in string.whitespace:
                            continue
                        if sym == ',':
                            break
                        if sym == ']':
                            return nodes
                else:
                    list_el = sym
                    while True:
                        sym = fp.read(1)
                        if sym == ',':                                                  
                            node = ET.Element(quote(list_el.strip()))
                            nodes.append(node)
                            list_el = ''
                            continue
                        elif sym == ']':
                            if list_el:
                                node = ET.Element(quote(list_el.strip()))
                                nodes.append(node)                                                      
                            return nodes
                        elif sym in string.whitespace:
                            continue
                        else:
                            list_el += sym

                    continue                                
                                                    
                raise ParseError('Unknown symbol: %s' % sym)                    

        def parse_hash(fp):
            node_name = ''
            while True:
                sym = fp.read(1)
                if sym == ',':
                    break
                node_name += sym
            node = ET.Element(quote(node_name))
            
            value = None
            while True:
                sym = fp.read(1)
                if sym == '[':
                    value = parse_list(fp)
                    for child in value:
                        node.append(child)
                    break
                            
                elif sym not in string.whitespace:
                    value = sym
                    while True:
                        letter = fp.read(1)
                        if letter == '}':
                            break
                        value += letter
                    node.text = value
                    break
            return node             

        """ Read whole config in single string """
        cfg_str = ''.join(line.strip() for line in fp.readlines())
        
        """ Check for erlang configuration format """ 
        if not cfg_str.startswith('[') or not cfg_str.endswith('].'):
            raise ParseError('No valid erlang configuration was found.')
    
        """ Strip main quotes """
        cfg_fp = StringIO(cfg_str[1:-1])
        return parse_list(cfg_fp)


    def write(self, fp, etree, close=True):
            
        """ Open configuration """
        self.ident = '    '
        fp.write('[\n')
        
        def write_hash(node, ident=None):
            if ident is None:
                ident = self.ident
            fp.write(ident)
            fp.write('{')
            fp.write(unquote(node.tag))
            fp.write(', ')
            if len(node):
                shift = 4 + len(node.tag)
                self.ident += ' '*shift
                write_list(list(node))
                self.ident = self.ident[:-shift]
                if len(node) > 1:
                    fp.write('\n%s}' % self.ident)
                else:
                    fp.write('}')
            else:
                fp.write(node.text)
                fp.write('}')                                   
        
        def write_list(nodes):
            fp.write('[')
            count = len(nodes)
            if count:
                if len(nodes[0]):
                    ''' list of dicts '''
                    for i in range(count):
                        if i == 0:
                            write_hash(nodes[i], '')
                        else:
                            write_hash(nodes[i])
                        if i+1 != count:
                            fp.write(',\n')
                            
            
                else:
                    ''' list of simple values '''
                    list_val = ', '.join([unquote(el.tag) for el in nodes])
                    fp.write(list_val)
                    
                    
            fp.write(']')
    
        root = etree.getroot()
        count = len(list(root))
        for i in range(count):
            write_hash(root[i])
            if not i+1 == count:
                fp.write(',\n')
        fp.write('\n].')
        if close:
            fp.close()
                                    
            

            
    
                    
                            
