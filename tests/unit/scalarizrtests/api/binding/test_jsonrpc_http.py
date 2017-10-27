'''
Created on May 15, 2012
 
@author: marat
'''
 
from wsgi_intercept.urllib2_intercept import install_opener
import sys
install_opener()
 
import binascii
import os
import tempfile
import shutil
 
from scalarizr import rpc
from scalarizr.api.binding import jsonrpc_http
from scalarizr.util import cryptotool
 
import wsgi_intercept
from nose.tools import raises, assert_equals
import mock
 
 
class MyService(object):
 
    @rpc.service_method
    def foo(self):
        return 'bar'
 
 
class TestWsgiApplication(object):
 
    def setup(self):
        self.tmp = tempfile.mkdtemp()
 
        self.crypto_key_path = os.path.join(self.tmp, 'crypto_key')
        with open(self.crypto_key_path, 'w') as fp:
            fp.write(cryptotool.keygen())
 
 
        def app_creator():
            return jsonrpc_http.WsgiApplication(
                                    rpc.RequestHandler({'myservice': MyService()}),
                                    self.crypto_key_path)
 
        self.app = app_creator()
        self.app_creator = app_creator
 
 
    def teardown(self):
        shutil.rmtree(self.tmp)
 
 
    def _read_crypto_key(self):
        return binascii.a2b_base64(open(self.crypto_key_path).read().strip())
 
 
    def test_check_signature(self):
        data = 'ABCDEF'
        sig, date = self.app.sign(data, self._read_crypto_key())
        self.app.check_signature(sig, data, date)
 
 
    @raises(AssertionError)
    def test_check_signature_invalid(self):
        self.app.check_signature('4ycrRqph560YsgK/HTT5zKeYrQ8=', 'ABC', 'Tue 15 May 2012 16:05:32 UTC')
 
 
    def test_decrypt_data(self):
        s = 'ABC'
        encrypted = cryptotool.encrypt(s, self._read_crypto_key())
        assert self.app.decrypt_data(encrypted) == s
 
 
    @raises(rpc.InvalidRequestError)
    def test_decrypt_data_invalid(self):
        self.app.decrypt_data('invalid')
 
 
    def test_call(self):
        wsgi_intercept.add_wsgi_intercept('localhost', 8011, self.app_creator)
        client = jsonrpc_http.HttpServiceProxy('http://localhost:8011', self.crypto_key_path)
        assert_equals(client.myservice.foo(), 'bar')
 
 
    def test_error_in_request_handler(self):
        def app_creator():
            app = jsonrpc_http.WsgiApplication(
                                    rpc.RequestHandler({'myservice': MyService()}),
                                    self.crypto_key_path)
            app.req_handler.handle_request = mock.Mock(side_effect=Exception('error in handle request'))
            return app
        wsgi_intercept.add_wsgi_intercept('localhost', 8011, app_creator)
        client = jsonrpc_http.HttpServiceProxy('http://localhost:8011', self.crypto_key_path)
        try:
            client.myservice.foo()
            assert 0, 'Exception expected, but statement passed'
        except:
            assert '500' in str(sys.exc_info()[1])
 
