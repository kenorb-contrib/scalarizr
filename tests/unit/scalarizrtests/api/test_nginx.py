'''
Created on Nov 2, 2012
 
@author: uty
'''
import os
import mock
import StringIO
 
from scalarizr.api import nginx
 
 
###############################################################################
# ~*~*~*~*~*~*~*~*~*~*~*~*~*~*~*~*~*~*~*~*~*~*~*~*~*~*~*~*~*~*~*~*~*~*~*~*~*~*~
# OUTDATED: this test is
# ~*~*~*~*~*~*~*~*~*~*~*~*~*~*~*~*~*~*~*~*~*~*~*~*~*~*~*~*~*~*~*~*~*~*~*~*~*~*~
###############################################################################
 
 
class TestNginxAPI(object):
 
    def setup(self):
        self.api = nginx.NginxAPI()
 
    def test_normalize_roles_arg(self):
        roles = [123, '456', {'id': '098', 'down': True}]
        parsed_roles = self.api._normalize_roles_arg(roles)
        assert parsed_roles == [
            {'id': '123', 'servers': []},
            {'id': '456', 'servers': []},
            {'id': '098', 'servers': [], 'down': True}], "%s" % parsed_roles
 
    def test_normalize_servers_arg(self):
        servers = ['123.123.132.321', {'host': '10.10.10.10', 'backup': True}]
        parsed_servers = self.api._normalize_servers_arg(servers)
        assert parsed_servers == [{'servers': ['123.123.132.321']},
                                  {'host': '10.10.10.10', 'servers': ['10.10.10.10'], 'backup': True}], \
            "%s" % parsed_servers
 
    def test_make_backend_conf(self):
        destinations = [{'servers': ['123.231.0.1', '122.232.0.9'], 'max_fails': 5},
                        {'servers': ['0.0.0.1', '1.1.1.9'], 'down': True}]
        desired_config = '''upstream ppp {
\tiphash;
\tserver\t123.231.0.1 max_fails=5;
\tserver\t122.232.0.9 max_fails=5;
\tserver\t0.0.0.1 down;
\tserver\t1.1.1.9 down;
}
'''
        conf = self.api._make_backend_conf('ppp', destinations)
        str_fp = StringIO.StringIO()
        conf.write_fp(str_fp, close=False)
        assert desired_config == str_fp.getvalue(), '%s' % str_fp.getvalue()
 
    def test_group_destinations(self):
        destinations = [
            {'id': '123', 'location': '/'},
            {'id': '234'},
            {'id': '1232', 'location': '/something'},
            {'id': '0158', 'location': '/something'},
            {'id': '756', 'location': '/alocation'},
            {'id': '951', 'location': '/alocation'}
        ]
        grouped_destinations = self.api._group_destinations(destinations)
        assert grouped_destinations == [[{'id': '1232', 'location': '/something'},
                                         {'id': '0158', 'location': '/something'}],
                                        [{'id': '756', 'location': '/alocation'},
                                         {'id': '951', 'location': '/alocation'}],
                                        [{'id': '123', 'location': '/'},
                                         {'id': '234', 'location': '/'}]], \
            '%s' % grouped_destinations
 
    # @mock.patch('self.api._get_ssl_cert')
    def test_make_server_conf(self):
        # get_ssl_cert.return_value = ('keys/https.crt', 'keys/https.key')
        # TODO: rewrite test for using regexp
        desired_config = """server  {
   listen   80;
   server_name   ytu.com;
   location test/ {
      proxy_pass   http://ytu.com_test_backend;
      proxy_set_header   Host $host;
      proxy_set_header   X-Real-IP $remote_addr;
      proxy_set_header   Host $host;
      client_max_body_size   10m;
      client_body_buffer_size   128k;
      proxy_buffering   on;
      proxy_connect_timeout   15;
      proxy_intercept_errors   on;
   }
   location / {
      proxy_pass   http://ytu.com_backend;
      proxy_set_header   Host $host;
      proxy_set_header   X-Real-IP $remote_addr;
      proxy_set_header   Host $host;
      client_max_body_size   10m;
      client_body_buffer_size   128k;
      proxy_buffering   on;
      proxy_connect_timeout   15;
      proxy_intercept_errors   on;
   }
}"""
        locations_and_backends = (('test/', 'ytu.com_test_backend'),
                                  ('/', 'ytu.com_backend'))
        conf = self.api._make_server_conf('ytu.com', locations_and_backends)
        str_fp = StringIO.StringIO()
        conf.write_fp(str_fp, close=False)
        assert desired_config == str_fp, '%s' % str_fp.getvalue()
 
 
