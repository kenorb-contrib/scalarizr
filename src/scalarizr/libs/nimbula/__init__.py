from __future__ import with_statement
'''
Created on Feb 14th, 2011
 
@author: Dmytro Korsakov
'''
import os
import urlparse
import httplib
import urllib
import urllib2
import json
import string
import random
import cookielib
import socket
 
from types import MachineImage
import sys
from scalarizr.libs.nimbula.types import Snapshot
from scalarizr.util import dicts
 
ENV_NIMBULA_URL                 = 'NIMBULA_URL'
ENV_NIMBULA_USERNAME    = 'NIMBULA_USERNAME'
ENV_NIMBULA_PASSWORD    = 'NIMBULA_PASSWORD'
 
cj = cookielib.CookieJar()
last_cookies = None
authenticated = False
 
EOL = '\r\n'
 
def authenticate(login=None, pwd=None):
 
    login = login or os.environ[ENV_NIMBULA_USERNAME]
    pwd = pwd or os.environ[ENV_NIMBULA_PASSWORD]
 
    if not (login or pwd):
        raise NimbulaError('No login inforation found')
 
    auth_basename = '/authenticate/'
 
    opener = urllib2.build_opener(urllib2.HTTPCookieProcessor(cj))
    uri = urlparse.urljoin(os.environ[ENV_NIMBULA_URL], auth_basename)
    request = urllib2.Request(uri)
 
    request.add_header('Accept', 'application/nimbula-v1+json')
    request.add_header('Content-Type', 'application/nimbula-v1+json')
 
    raw_data = dict(user=login, password=pwd)
    data = json.dumps(raw_data)
 
    urllib2.install_opener(opener)
    response = opener.open(request, data=data)
 
    retcode = response.code
 
    if not str(retcode).startswith('2'):
        raise NimbulaError('Auth failed')
 
    info = response.info()
    headers = info.headers
 
    global last_cookies, authenticated
    last_cookies = ''.join(headers[-1][12:].split(';')[0])
    authenticated = True
 
    return headers
 
 
class NimbulaError(BaseException):
    pass
 
 
class NimbulaConnection:
    username = None
    password = None
    api_url = None
 
 
    def __init__(self, api_url=None, username=None, password=None):
        self.api_url = api_url or os.environ[ENV_NIMBULA_URL]
        self.username = username or os.environ[ENV_NIMBULA_USERNAME]
        self.password = password or os.environ[ENV_NIMBULA_PASSWORD]
 
        if not (self.username or self.password):
            raise NimbulaError('No login inforation found')
 
 
    def _get_object_URI(self, objname, basename=None):
        basename = basename or 'machineimage'
        if not objname.startswith('/'):
            objname = '%s/%s' % (self.username, objname)
        return urlparse.urljoin(self.api_url,basename+objname)
 
 
    def get_machine_image(self, name):
        uri = self._get_object_URI(name)
        f = self._request(uri)
        response = f.read()
        return MachineImage(from_response=json.loads(response))
 
 
    def delete_machine_image(self, name):
        '''
        for image_name = '/nimbula/public/default'
        NimbulaError: HTTP Error 401: Unauthorized
        '''
        uri = self._get_object_URI(name)
        try:
            f = self._request(uri, query_method='DELETE')
        except NimbulaError, e:
            err_msg='Not enough privelegies to delete image'
            raise NimbulaError(err_msg if 'Unauthorized' in str(e) else e)
        response = f.read()
        return response
 
 
    def discover_machine_image(self, container=None):
        uri = self._get_object_URI(container or self.username)
        f = self._request(uri)
        response = f.read()
        return [MachineImage(from_response=img) for img in json.loads(response)['result']]
 
 
    def delete_instance(self, name):
        uri = self._get_object_URI(name, 'instance')
        try:
            f = self._request(uri, query_method='DELETE')
        except NimbulaError, e:
            err_msg='Not enough privelegies to delete instance'
            raise NimbulaError(err_msg if 'Unauthorized' in str(e) else e)
        response = f.read()
        return response
 
    def add_snapshot(self, instance, machineimage=None):
        if machineimage:
            machineimage = os.path.join(self.username, machineimage)
        try:
            f = self._request(os.path.join(self.api_url, 'snapshot/'),
                                    data=json.dumps(dict(instance=instance, machineimage=machineimage)),
                                    query_method='POST')
        except:
            err_msg='Not enough privelegies to add snapshot'
            exc_info = sys.exc_info()
            raise NimbulaError, err_msg if 'Unauthorized' in exc_info[1] else exc_info[1], exc_info[2]
 
        return Snapshot(self, **dicts.encode(json.load(f), 'ascii'))
 
    def get_snapshot(self, name=None, snap=None):
        assert name or snap
        name = name or snap.name
        uri = self._get_object_URI(name, 'snapshot')
        try:
            f = self._request(uri, query_method='GET')
        except:
            err_msg='Not enough privelegies to get snapshot'
            exc_info = sys.exc_info()
            raise NimbulaError, err_msg if 'Unauthorized' in exc_info[1] else exc_info[1], exc_info[2]
        data = dicts.encode(json.load(f), 'ascii')
        if snap:
            return snap.set_data(**data)
        return Snapshot(self, **data)
 
    def _request(self, uri, data=None, headers=None, query_method=None, force=True):
 
        if not authenticated:
            authenticate(self.username, self.password)
 
        headers = headers or {
                'Accept':'application/nimbula-v1+json',
                'Content-Type':'application/nimbula-v1+json'
        }
        request = urllib2.Request(uri, data, headers)
 
        if query_method:
            request.get_method = lambda: query_method
 
        try:
            f = urllib2.urlopen(request)
        except urllib2.HTTPError, e:
            if e.code==401 and force==True:
                authenticate(self.username, self.password)
                return self._request(uri, headers, query_method, force=False)
            else:
                exc = sys.exc_info()
                raise NimbulaError, exc[1], exc[2]
 
        return f
 
 
    def _send_data(self, data, connection):
        if isinstance(data, basestring):
            # Is it a string?
            try:
                connection.send(data)
            except socket.error, v:
                if v[0] == 104:
                    return
                raise
 
        elif hasattr(data, 'read'):
            # Is it a file?
            while 1:
                part = data.read(100000)
                if part == '': break
 
                try:
                    connection.send(part)
                except socket.error, v:
                    if v[0] == 104:
                        return
                    raise
 
 
    def add_machine_image(self, name, file=None, fp=None, attributes=None, account=None):
        '''
        @param name: base name (ex: apache-deb5-20110217), autocomplete customer/user from self.username
        @param file: file name
        @param fp: file-like object. One of `file` or `fp` should be provided
        '''
 
        #host = 'serverbeach.demo.nimbula.com'
        host = urlparse.urlparse(self.api_url)[1]
 
        def _post(form_data, boundary):
            '''
            generates additional form data entries all in one
            for calculating Content-Length and sending through httplib
            '''
            for name, data in form_data:
                yield '--%s%s' % (boundary, EOL)
                content = 'Content-Type: application/nimbula-v1+json%s' % EOL if name=='attributes' else ''
                content +='Content-Disposition: form-data; name="%s"%s' % (name, EOL)
                yield content
                yield EOL
                if data:
                    yield data
                yield EOL
 
        if not file and not fp:
            raise NimbulaError
 
        if not authenticated:
            authenticate(self.username, self.password)
 
        #preparing additional form data
        form_data = []
        form_data.append(('uri', ''))
        form_data.append(('name',  self.username+'/'+name))
        form_data.append(('attributes', '{}'))
        form_data.append(('account', self.username))
 
        boundary = "".join([random.choice(string.ascii_lowercase+string.digits) for x in xrange(31)])
 
        #calculating Content-length:
        file_length = os.path.getsize(file) if file else os.fstat(fp.fileno())[6]
        full_length = file_length
        for entry in _post(form_data, boundary): full_length += len(entry)
 
        cl_entry = 'Content-Length: %s' % file_length
        cl_entry += '%sContent-Disposition: form-data' % EOL
        cl_entry += '; name="file"; filename="%s"%s' % (file or fp.name, EOL)
 
        closing_boundary = '--%s--' % boundary+EOL
 
        add = len(cl_entry)+len('--%s' % boundary+EOL)+ 2*len(EOL)+len(closing_boundary)
        full_length += add
 
        #preparing headers
        headers = []
        headers.append(('Content-Length',full_length))
        headers.append(('AcceptEncoding', 'gzip;q=1.0, identity; q=0.5'))
        headers.append(('Accept', 'application/nimbula-v1+json'))
        headers.append(('Host', host))
        headers.append(('Content-Type', 'multipart/nimbula-v1+form-data; boundary=%s' % boundary))
        headers.append(('Cookie', last_cookies))
 
        connection  = httplib.HTTPSConnection(host, timeout=300)
 
        try:
            if connection.sock is None:
                connection.connect()
            #connection.set_debuglevel(10)
            connection.putrequest('POST', '/machineimage/', skip_host=True)
 
            for (k, v) in headers:
                connection.putheader(k, v)
            connection.endheaders()
 
            for content in _post(form_data, boundary):
                self._send_data(content, connection)
 
            self._send_data('--%s' % boundary+EOL, connection)      #
            self._send_data(cl_entry, connection)   #
            self._send_data(EOL, connection)        #
 
            data = fp or open(file, 'rb')
            if data:
                self._send_data(data, connection)
 
            self._send_data(EOL, connection)        #
            self._send_data(closing_boundary, connection)#
 
            response = connection.getresponse().read()
            entry = json.loads(response)
 
            if entry.has_key('message'):
                msg = entry['message']
 
                if msg == 'Conflict':
                    raise NimbulaError('Image already exists')
                else:
                    raise NimbulaError(msg)
 
            return MachineImage(from_response=entry)
 
        except KeyboardInterrupt:
            raise
 
        except BaseException, e:
            raise NimbulaError(e)
 
