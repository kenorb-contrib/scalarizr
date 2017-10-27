__author__ = 'spike'
 
import sys
 
if sys.version_info[0:2] >= (2, 7):
    from xml.etree import ElementTree as ET
else:
    from scalarizr.externals.etree import ElementTree as ET
 
def dict2xml(sdict, root_name='root'):
    root = ET.Element(root_name)
    for k, v in sdict.iteritems():
        el = ET.Element(k)
        if isinstance(v, dict):
            el.extend(dict2xml(v))
        else:
            el.text = str(v)
 
        root.append(el)
 
