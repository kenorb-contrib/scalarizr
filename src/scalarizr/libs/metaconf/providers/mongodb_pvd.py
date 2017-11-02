from __future__ import with_statement
'''
Created on Oct 6, 2011

@author: Spike
'''

from .redis_pvd import RedisFormatProvider
from ..utils import unquote


class MongodbFormatProvider(RedisFormatProvider):
    _opt_re_string      = r'(?P<option>[^:=\s][^:=]*)\s*(?P<vi>[:=])\s*(?P<value>.*?)\s*(?P<comment>[#;](.*))?$'
    
    def write_option(self, fp, node):
        if node.attrib.has_key('mc_type') and node.attrib['mc_type'] == 'option':
            value = str(node.text if node.text else '')
            fp.write(unquote(node.tag)+" = "+value+'\n')
            return True
        return False
