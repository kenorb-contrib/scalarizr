from __future__ import with_statement
'''
Created on Jun 18, 2010

@author: Marat Komarov
'''

import hashlib
import time
import urllib2
from urllib import urlencode
from scalarizr.util import xml_strip
from xml.dom.minidom import parseString

class GoGridError(BaseException):
    pass

class GoGridService:
    _api_key = None
    _secret = None
    _version = None
    _url = None
    
    def __init__(self, api_key, secret, version="1.4", url="https://api.gogrid.com/api"):
        self._key = api_key
        self._secret = secret
        self._version = version
        self._url = url
    
    def _request(self, uri, params, resp_reader):
        post_data = dict(params) 
        post_data["api_key"] = self._api_key
        post_data["v"] = self._version
        post_data["format"] = "xml"
        post_data["sig"] = hashlib.md5(self._api_key + self._secret + str(int(time.time())))
        
        try:
            r = urllib2.urlopen(self._url + uri, urlencode(post_data))
        except urllib2.URLError, e:
            if isinstance(e, urllib2.HTTPError):
                resp_body = e.read() if e.fp is not None else ""
                raise GoGridError("Request failed. %s. URL: %s. Service message: %s" % (e, self.url, resp_body))
            else:
                raise GoGridError("Cannot connect to GoGrid service. %s" % (e,))
        else:
            try:
                xml = parseString(r.read())
            except (Exception, BaseException), e:
                raise GoGridError("Response couldn't be parsed as XML. %s" % (e,))
            else:
                return resp_reader(xml_strip(xml))

            
    def list_servers(self, num_items=None, page=None, server_type=None, is_sandbox=None):
        p = dict()
        for k, v in locals().items():
            if v:
                if k == "is_sandbox":
                    k = "isSandbox"
                    v = "true"
                elif k == "server_type":
                    k = "server.type"
                p[k] = v
        
        return self._request("/grid/server/list", p, self._list_servers_reader)
    
    def _list_servers_reader(self, xml):
        (xml)
        return tuple(Server(el) for el in xml.getElementsByTagName("list")[0].childNodes)


def _value(el):
    return el.nodeValue if el else None 

class Server:
    id, name, description, ip, ram, image, state, type, os, is_sandbox = None
    
    def __init__(self, xml):
        for el in xml.childNodes:
            n = el.getAttribute("name")
            if n in ("id", "name", "description"):
                setattr(self, n, _value(el.firstChild))
            elif n in ("ram", "state", "type", "os"):
                setattr(self, n, Option(el.firstChild))
            elif n == "ip":
                self.ip = IpAddress(el.firstChild)
            elif n == "image":
                self.image = ServerImage(el.firstChild)
            elif n == "isSandbox":
                self.is_sandbox = _value(el.firstChild) == "true"

class ServerImage:
    id, name, friendly_name, owner_customer_id, owner_name, description, location, \
    price, is_active, is_public, created_time, updated_time  = None

    def __init__(self, xml):
        
        pass
        
class IpAddress:
    id, ip, state, subnet, public = None
    
    def __init__(self, xml):
        pass

class Option:
    id, name, description = None
    
    def __init__(self, xml):
        self.id = _value(xml.childNodes[0].firstChild)
        self.name = _value(xml.childNodes[1].firstChild)
        self.description = _value(xml.childNodes[2].firstChild)
