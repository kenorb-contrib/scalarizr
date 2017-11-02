import os

from .redis_pvd import RedisFormatProvider
from ..utils import unquote
from . import FormatProvider
from .. import MetaconfError

class KeyvalueFormatProvider(RedisFormatProvider):
    _opt_re_string = r'(?P<option>[^\s]+)=(?P<value>.*)(?P<comment>#(.*))?$'
    
    def create_element(self, etree, path, value):
        el = FormatProvider.create_element(self, etree, path, value)
        if os.path.dirname(path) not in ('.', ''):
            raise MetaconfError("key-value config format doesn't support nesting")
        el.attrib['mc_type'] = 'option'
        return el

    def write_option(self, fp, node):
        if node.attrib.has_key('mc_type') and node.attrib['mc_type'] == 'option':
            value = str(node.text if node.text else '')
            fp.write(unquote(node.tag)+"="+value+'\n')
            return True
        return False
