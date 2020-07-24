# -*- coding: utf-8 -*-
import BaseHTTPServer
import urllib2
from urllib2 import urlopen
from httplib import HTTPSConnection
import time
from threading import Thread
from mock import patch
import os
from shutil import copyfile
from scalarizr.util import initdv2
 
 
from lettuce import step
from lettuce import world
from lettuce import before
from lettuce import after
from scalarizr.api import nginx
 
 
TEST_SSL_KEY = \
"""-----BEGIN RSA PRIVATE KEY-----
MIIEpAIBAAKCAQEAwOhlIzUdJfEPwz9+Exp1D4xspHtxvx8O5QK0Q2+Faau0sgJf
cOjxTi3D9yF1FMp9vI8F/xwTYodtPYCQGSH4zlO8BFdFjzXVwNmdjBiE76t5jc6N
bzc6WjRDZzUzMICSg+NLMn+i/F6Hnb4MDzyUxTt+mVVzqpSi4vDSFYOYCJIiNz5s
IjdkOhOA0F4CGL6d51LOLPXGzaVE9CddQIMhX3oR0MJyByMMF2YXahgGo0snGQy7
fQ0RmoTyqaMk0VB5AK22G5X/whCCY6r40L6eS0ZtXtfCZ8jyeL5ipdSjSXCqQKUZ
7F2OBViD/cCpaj9GztEInB9clTjtviKz9OuhewIDAQABAoIBAQCyhR47Y8bSuvA0
ZwicUyqrte9AlECidvKIumTp10WmkdFJvupmChxtleq5RAwernmHyu/osym5T8yn
UteHeqnO/yyK89yaeF6U9o5W/MXhKlX2BoVau8hTe/Q4icISi5mLVgfb9sR1OmHU
d/CfFRg0Iie5bJv660yGKgixAjPrEPyU6rttUkDiFAgtYi5b62AfXbIOstiJlyAA
Mb2TX7qR1EcZHrwD6LExQWEqDyiQGbvYfVsVSxWfUqYjxP0yn4AypIPv3DO0kPa1
ONa4eFAq/k7y+F7Ki/zXGaUHGXrHiBdwiEIWTfaaQxqxytAk90dUbGsNwHr47A+4
CfMIAzHpAoGBAPKkZTgzY0T7PDMBB5fVZABbBtBi3ewopl2iCLGG+1ljTpHpz5Ip
JGRN5qX26533pGhmFgsw13esIH4Fb15khETExLq/NO1U4O8IpIyBSWsfAxpmPMsr
fT2iugqJM4UMVJ2Nfl4tCJz/afB4GdcZIsmdcp9pijdTl3zZWkGbZ1VXAoGBAMuH
FUqs7WWHMZcbIflvYmppXmbQnohnnX9BMdS3+/Xi22+RwtJqk6KxS959SAugLn4o
raRE8KoECV5XDeeCylfke0VrzVr5VmDRDyCGa1/6RYTvHGZZiAgLXUAyWTn1QYC0
A399d4qf78vbKbYKjinuM3BkyPfQxOYj5rGy4Pp9AoGBAI6CBTzj2YrbL7kZAren
Sct3quHbH1IjccqObyKtD5SpYa0LMLE8XrZWln+lLS8jEjmKs6mw7uvHeXHqiUVc
Ld8h3hV8VX6Kmm1pmxM2n8M9fJoVr+D30t/PYgrsGAkte8jpIG35bxSeYj8smqid
h7P4OCjuWJI0E0XtdjgQKLmTAoGAW0P73DynR7vUFPppxbyY2TbeyiQKswgjrAjE
G6tVJPHRjLpELq7z/SSb7O0o/W2a65+6Hct8UAD3YoKPDZ2strUSQhMRRxZAEbIt
olwkkilcOzwt9Ad55IGUE1GAiWjdMqWGXAkbLeKCWVux3JvnHA5gqqnHJLlNUhYP
QOgB0tECgYB7Z9dA0LhloKcirIjOjMoHxJQRkgTAI+AJaSn9WH03JdNclkcGf5XD
AL7qQy12GQcu+1VpYieRoydJ1JsHVSHpOELnOR67dh4IigjkLSMWbHcl4W3VIfda
ACqsDtC2Eyr1JvKadcWI4zGw6qDXW38tDrf8NapFxX4JjByOmLwIVg==
-----END RSA PRIVATE KEY-----"""
 
TEST_SSL_CERT = \
"""-----BEGIN CERTIFICATE-----
MIIC9zCCAd+gAwIBAgIJAJGpAeUq8dZFMA0GCSqGSIb3DQEBBQUAMBIxEDAOBgNV
BAMMB3V0eS5jb20wHhcNMTMwNDE5MTYzNDQwWhcNMjMwNDE3MTYzNDQwWjASMRAw
DgYDVQQDDAd1dHkuY29tMIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEA
wOhlIzUdJfEPwz9+Exp1D4xspHtxvx8O5QK0Q2+Faau0sgJfcOjxTi3D9yF1FMp9
vI8F/xwTYodtPYCQGSH4zlO8BFdFjzXVwNmdjBiE76t5jc6Nbzc6WjRDZzUzMICS
g+NLMn+i/F6Hnb4MDzyUxTt+mVVzqpSi4vDSFYOYCJIiNz5sIjdkOhOA0F4CGL6d
51LOLPXGzaVE9CddQIMhX3oR0MJyByMMF2YXahgGo0snGQy7fQ0RmoTyqaMk0VB5
AK22G5X/whCCY6r40L6eS0ZtXtfCZ8jyeL5ipdSjSXCqQKUZ7F2OBViD/cCpaj9G
ztEInB9clTjtviKz9OuhewIDAQABo1AwTjAdBgNVHQ4EFgQUxIFfj5Paa9PfeHFS
T5hayHHhMQEwHwYDVR0jBBgwFoAUxIFfj5Paa9PfeHFST5hayHHhMQEwDAYDVR0T
BAUwAwEB/zANBgkqhkiG9w0BAQUFAAOCAQEAg0cR1KXGmXuIitzJowoqnkdMGa3o
YhxR97p8qRjOw0rcIQburgbGWL2OA+P+1rBqGJxJ5xBhOVvQYDOIbPjq3Cv+OLGn
Mo4hhOSWDnQOfuIdjBoU9WCaoKs/HYOXJ1KFI5xGl+PKEdvJ8taKelpZwqJ/rYUc
uINsVMKrRiOgmQMFk+xCjGcu9mseEk3LSCy1GDUdAfEf1qwTAlzJSdN//qafi/bL
XudkNgs3PflVNnLa34czdKWCNgo8816/LZynxxiO/cQs3dWMYC/RhPnYTtBX+gR3
2P1eqWWsQDD0WA9sYZEJCGn6Gp++KN7HH+wWmQKY4+ycFnujwcLKL14u2A==
-----END CERTIFICATE-----"""
 
 
class Server(BaseHTTPServer.HTTPServer):
    """
    Server creates simple HTTP listener on given port on localhost
    """
 
    def __init__(self, port, get_response=None):
        "process with running server should be creaded"
        if type(port) is str:
            port = int(port)
        if not get_response:
            get_response = "<html><head><title>Something</title></head></html>"
 
        class _Handler(BaseHTTPServer.BaseHTTPRequestHandler):
 
            def do_HEAD(self):
                self.send_response(200)
                self.send_header("Content-type", "text/html")
                self.end_headers()
 
            def do_GET(self):
                """Respond to a GET request."""
                self.send_response(200)
                self.send_header("Content-type", "text/html")
                self.end_headers()
                self.wfile.write(get_response)
 
            def log_message(self, format, *args):
                return
 
        self.port = port
 
        super_cls = BaseHTTPServer.HTTPServer
        super_cls.__init__(self, ('localhost', port), _Handler)
 
        def serve_job():
            super_cls.serve_forever(self)
 
        self.serve_thread = Thread(target=serve_job)
        self.serve_thread.daemon = True
 
    def serve_forever(self):
        self.serve_thread.start()
 
    def go_down(self):
        self.shutdown()
        self.server_close()
 
 
def clear_nginx_includes():
    with open('/etc/nginx/app-servers.include', 'w') as fp:
        fp.write('')
    with open('/etc/nginx/proxies.include', 'w') as fp:
        fp.write('')
 
 
def read_nginx_includes():
    result = None
    with open('/etc/nginx/app-servers.include', 'r') as fp:
        result = fp.read() + '\n\n'
    with open('/etc/nginx/proxies.include', 'r') as fp:
        result += fp.read() + '\n\n'
    return result
 
 
def get_responses(qty):
    return [urlopen('http://localhost:8008').read() for _ in xrange(qty)]
 
 
@before.each_feature
def patch_nginx_conf(feature):
    current_dir = os.path.dirname(__file__)
    patch_conf_path = os.path.join(current_dir, 'nginx_fixtures/nginx.conf')
    world.nginx_conf_backup = '/etc/nginx/nginx.conf.%s' % time.time()
    copyfile('/etc/nginx/nginx.conf', world.nginx_conf_backup)
    copyfile(patch_conf_path, '/etc/nginx/nginx.conf')
 
@after.each_feature
def unpatch_nginx_conf(feature):
    copyfile(world.nginx_conf_backup, '/etc/nginx/nginx.conf')
    os.remove(world.nginx_conf_backup)
 
@before.each_feature
def patch_node(feature):
    patcher = patch.object(nginx, 
                           '__node__',
                           new={'nginx': {'binary_path': '/usr/sbin/nginx',
                                          'app_include_path': '/etc/nginx/app-servers.include',
                                          # 'https_include_path': '/etc/nginx/https.include',
                                          'app_port': '80',
                                          'main_handler': 'nginx'},
                                'behavior': ['nginx']})
    patcher.start()
    world.patchers = [patcher]
    patcher = patch.object(nginx, 
                           '__nginx__',
                           new={'binary_path': '/usr/sbin/nginx',
                                'app_include_path': '/etc/nginx/app-servers.include',
                                # 'https_include_path': '/etc/nginx/https.include',
                                'app_port': '80',
                                'main_handler': 'nginx'})
    patcher.start()
    world.patchers.append(patcher)
 
    open('/etc/nginx/error-pages.include', 'w').close()
    patcher = patch.object(nginx.NginxAPI, '_make_error_pages_include')
    patcher.start()
    world.patchers.append(patcher)
 
 
@after.each_feature
def unpatch_node(feature):
    for patcher in world.patchers:
        patcher.stop()
 
 
@before.each_scenario
def create_api(feature=None):
    world.api = nginx.NginxAPI()
    world.api._add_noapp_handler = lambda x: None
    world.api.error_pages_inc = '/etc/nginx/error-pages.include'
 
 
@before.each_scenario
def clear_make_proxy_world_parms(scenario):
    world.http = True
    world.port = 8008
    world.destinations = None
    world.ssl = None
    world.ssl_port = None
    world.ssl_cert_id = None
    world.backend_port = None
    world.backend_ip_hash = None
    world.backend_max_fails = None
    world.backend_fail_timeout = None
 
 
@after.each_scenario
def shutdown_servers(scenario):
    for world_attr in dir(world):
        if not world_attr.startswith('_'):
            obj = world.__getattribute__(world_attr)
            if isinstance(obj, Server):
                obj.go_down()
 
 
###############################################################################
# Scenario 1
 
@step(u'Given I have a server')
def given_i_have_a_server(step):
    world.expected_response = 'KUKU'
    world.server = Server(8000, world.expected_response)
    world.server.serve_forever()
    world.destinations = [{'host': 'localhost', 'port': '8000'}]
 
 
@step(u'When I add proxy')
def when_i_add_proxy(step):
    templates = [{'content': '''server {
proxy_set_header Host $host;
proxy_set_header X-Real-IP $remote_addr;
proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
client_max_body_size 10m;
client_body_buffer_size 128k;
proxy_buffering on;
proxy_connect_timeout 15;
proxy_intercept_errors on;
}''',
                  'server': True}]
    world.api.make_proxy(hostname='uty.com',
                         backends=world.destinations,
                         port=world.port,
                         http=world.http,
                         ssl=world.ssl,
                         ssl_port=world.ssl_port,
                         ssl_certificate_id=world.ssl_cert_id,
                         templates=templates,
                         backend_port=world.backend_port,
                         backend_ip_hash=world.backend_ip_hash,
                         backend_max_fails=world.backend_max_fails,
                         backend_fail_timeout=world.backend_fail_timeout)
    time.sleep(1)
 
 
@step(u'Then I expect proxying to server')
def then_i_expect_proxying_to_server(step):
    response = get_responses(1)[0]
 
    clear_nginx_includes()
 
    assert response == world.expected_response
 
###############################################################################
# Scenario 2
 
 
@step(u'Given I have a role')
def given_i_have_a_role(step):
    server1_port = 8001
    server2_port = 8002
    # Mocking up get role servers to return our Server adresses
    world.api.get_role_servers = lambda x: ['localhost:%s' % server1_port,
                                             'localhost:%s' % server2_port]
 
    world.expected_response1 = 'Test1'
    world.expected_response2 = 'Test2'
    world.server1 = Server(server1_port, world.expected_response1)
    world.server1.serve_forever()
    world.server2 = Server(server2_port, world.expected_response2)
    world.server2.serve_forever()
 
    world.role_id = 123
    world.destinations = [{'farm_role_id': world.role_id}]
 
 
@step(u'Then I expect proxying to role')
def then_i_expect_proxying_to_role(step):
    responses = get_responses(2)
 
    clear_nginx_includes()
    create_api()
 
    assert world.expected_response1 in responses, '%s' % responses
    assert world.expected_response2 in responses, '%s' % responses
 
 
###############################################################################
# Scenario 3
 
 
@step(u'Given I have a proxy to a role')
def given_i_have_a_proxy_to_a_role(step):
    given_i_have_a_role(step)
    when_i_add_proxy(step)
 
 
@step(u'When I launch new server of this role')
def when_i_launch_new_server_of_this_role(step):
    world.expected_response3 = 'Test3'
    world.server3 = Server('8003', world.expected_response3)
    world.server3.serve_forever()
 
    world.api.add_server_to_role('localhost:8003', world.role_id)
    time.sleep(1)
 
 
@step(u'Then server appears in backend')
def then_server_appears_in_backend(step):
    responses = get_responses(3)
 
    clear_nginx_includes()
    create_api()
 
    assert world.expected_response3 in responses, '%s' % responses
 
 
###############################################################################
# Scenario 4
 
 
@step(u'When I terminate one server of this role')
def when_i_terminate_one_server_of_this_role(step):
    world.api.remove_server_from_role('localhost:8002', world.role_id)
    time.sleep(1)
 
 
@step(u'Then server removed from backend')
def then_server_removed_from_backend(step):
    responses = get_responses(2)
 
    clear_nginx_includes()
    create_api()
 
    assert world.expected_response2 not in responses, '%s' % responses
 
 
###############################################################################
# Scenario 5
 
 
@step(u'And I have SSL keypair')
def and_i_have_ssl_keypair(step):
    def get_ssl_cert(id_):
        if id_ != 123:
            return None
 
        cert_path = '/home/vagrant/https.crt'
        with open(cert_path, 'w') as fp:
            fp.write(TEST_SSL_CERT)
 
        key_path = '/home/vagrant/https.key'
        with open(key_path, 'w') as fp:
            fp.write(TEST_SSL_KEY)
 
        return (cert_path, key_path)
 
    world.api._fetch_ssl_certificate = get_ssl_cert
    world.ssl = True
    world.ssl_cert_id = 123
 
    world.port = None
 
 
@step(u'Then I expect proxying https -> http')
def then_i_expect_proxying_https_http(step):
    c = HTTPSConnection('localhost')
    c.request("GET", "/")
    response = c.getresponse()
    data = response.read()
 
    clear_nginx_includes()
    create_api()
 
    assert data == world.expected_response, data
 
 
###############################################################################
# Scenario 6
 
 
@step(u'And I have HTTP disabled')
def and_i_have_http_disabled(step):
    world.port = 8008
    world.ssl_port = 443
    world.http = False
 
 
@step(u'And I expect redirect http -> https')
def and_i_expect_redirect_http_https(step):
    time.sleep(1)
    class TestRedirectHandler(urllib2.HTTPRedirectHandler):
        redirect_occured = False
        super = urllib2.HTTPRedirectHandler
 
        def http_error_302(self, req, fp, code, msg, headers):
            TestRedirectHandler.redirect_occured = True
            return self.super.http_error_302(self, req, fp, code, msg, headers)
 
        http_error_301 = http_error_303 = http_error_307 = http_error_302
 
    cookieprocessor = urllib2.HTTPCookieProcessor()
    opener = urllib2.build_opener(TestRedirectHandler, cookieprocessor)
    opener.open('http://localhost:%s' % world.port).read()
 
    assert TestRedirectHandler.redirect_occured, 'No http to https redirect'
 
 
###############################################################################
# Scenario 7
 
 
@step(u'Given I have a proxy to two roles: main and backup')
def given_i_have_a_proxy_to_two_roles_main_and_backup(step):
    server1_port = 8001
    server2_port = 8002
    server3_port = 8003
 
    # Mocking up get role servers to return our Server adresses
    def get_role_servers(role):
        if role == '123':
            return ['localhost:%s' % server1_port, 'localhost:%s' % server2_port]
        else:
            return ['localhost:%s' % server3_port]
 
    world.api.get_role_servers = get_role_servers
 
    world.expected_response1 = 'Test1'
    world.expected_response2 = 'Test2'
    world.expected_response3 = 'Test3123'
    world.server1 = Server(server1_port, world.expected_response1)
    world.server1.serve_forever()
    world.server2 = Server(server2_port, world.expected_response2)
    world.server2.serve_forever()
    world.server3 = Server(server3_port, world.expected_response3)
    world.server3.serve_forever()
 
    world.destinations = [{'farm_role_id': 123},
                          {'farm_role_id': 321, 'backup': True}]
 
    world.api.make_proxy(hostname='uty.com',
                         backends=world.destinations,
                         port=8008)
    time.sleep(1)
 
    responses = get_responses(3)
    assert world.expected_response3 not in responses
 
 
@step(u'When I terminate main servers')
def when_i_terminate_main_servers(step):
    world.server1.go_down()
    world.server2.go_down()
 
 
@step(u'Then I expect proxying to backup servers')
def then_i_expect_proxying_to_backup_servers(step):
    responses = get_responses(3)
 
    clear_nginx_includes()
    create_api()
 
    assert world.expected_response1 not in responses, responses
    assert world.expected_response2 not in responses, responses
    assert world.expected_response3 in responses, responses
 
 
###############################################################################
# Scenario 8
 
 
@step(u'Given I have a proxy to two servers')
def given_i_have_a_proxy_to_two_servers(step):
    world.expected_response1 = 'Test1'
    world.server1 = Server(8001, world.expected_response1)
    world.server1.serve_forever()
 
    world.expected_response2 = 'Test2'
    world.server2 = Server(8002, world.expected_response2)
    world.server2.serve_forever()
    world.destinations = [{'host': 'localhost', 'port': '8001'},
                          {'host': 'localhost', 'port': '8002'}]
 
    world.api.make_proxy(hostname='uty.com',
                         backends=world.destinations,
                         port=8008)
    time.sleep(1)
 
    responses = get_responses(2)
    assert world.expected_response1 in responses, '%s' % responses
    assert world.expected_response2 in responses, '%s' % responses
 
 
@step(u'When I update proxy marking one server as down')
def when_i_update_proxy_marking_one_server_as_down(step):
    server_to_update = world.destinations[1]
    server_to_update['down'] = True
    world.api.make_proxy(hostname='uty.com',
                         backends=world.destinations,
                         port=8008)
    time.sleep(1)
 
 
@step(u'Then I expect proxying to remaining server')
def then_i_expect_proxying_to_remaining_server(step):
    responses = get_responses(2)
    clear_nginx_includes()
 
    assert world.expected_response2 not in responses, '%s' % responses
 
 
###############################################################################
# Scenario 9
 
 
@step(u'Given I have a regular server S')
def given_i_have_a_regular_server_s(step):
    world.S_response = 'Server S'
    world.server_S = Server(8001, world.S_response)
    world.server_S.serve_forever()
 
    world.destinations = [{'host': 'localhost', 'port': '8001'}]
 
 
@step(u'And I have a down server SD')
def and_i_have_a_down_server_sd(step):
    world.SD_response = 'Server SD'
    world.server_SD = Server(8002, world.SD_response)
    world.server_SD.serve_forever()
 
    world.destinations.append({'host': 'localhost', 'port': '8002', 'down': True})
 
 
@step(u'And I have a backup server SB')
def and_i_have_i_backup_server_sb(step):
    world.SB_response = 'Server SB'
    world.server_SB = Server(8003, world.SB_response)
    world.server_SB.serve_forever()
 
    world.destinations.append({'host': 'localhost', 'port': '8003', 'backup': True})
 
 
@step(u'And I have a regular role R')
def and_i_have_a_regular_role_r(step):
    server1_port = 8004
    server2_port = 8005
 
    def get_role_servers(role):
        if role == '123':
            return ['localhost:%s' % 8004, 'localhost:%s' % 8005]
        elif role == '321':
            return ['localhost:%s' % 8006]
        elif role == '890':
            return ['localhost:%s' % 8007]
 
    # Mocking up get role servers to return our Server adresses
    world.api.get_role_servers = get_role_servers
 
    world.R_role_response1 = 'R Role server 1'
    world.R_role_response2 = 'R Role server 2'
    world.R_role_server1 = Server(server1_port, world.R_role_response1)
    world.R_role_server1.serve_forever()
    world.R_role_server2 = Server(server2_port, world.R_role_response2)
    world.R_role_server2.serve_forever()
 
    world.destinations.append({'farm_role_id': 123})
 
 
@step(u'And I have a backup role RB')
def and_i_have_a_backup_role_rb(step):
    server1_port = 8006
 
    world.RB_role_response1 = 'RB Role server 1'
    world.RB_role_server1 = Server(server1_port, world.RB_role_response1)
    world.RB_role_server1.serve_forever()
 
    world.destinations.append({'farm_role_id': 321, 'backup': True})
 
 
@step(u'And I have a down role RD')
def and_i_have_a_down_role_rd(step):
    server1_port = 8007
 
    world.RD_role_response1 = 'RD Role server 1'
    world.RD_role_server1 = Server(server1_port, world.RD_role_response1)
    world.RD_role_server1.serve_forever()
 
    world.destinations.append({'farm_role_id': 890, 'down': True})
 
 
@step(u'Then I expect S and R servers are regular in backend')
def then_i_expect_s_and_r_servers_are_regular_in_backend(step):
    responses = get_responses(10)
 
    # regular destination's responses are in responses list
    assert world.S_response in responses, responses
    assert world.R_role_response1 in responses, responses
    assert world.R_role_response2 in responses, responses
 
    # backup and down destination's responses are not
    assert world.SB_response not in responses, responses
    assert world.SD_response not in responses, responses
    assert world.RB_role_response1 not in responses, responses
    assert world.RD_role_response1 not in responses, responses
 
 
@step(u'And I expect SB and RB servers are backup in backend')
def and_i_expect_sb_and_rb_servers_are_backup_in_backend(step):
    world.server_S.go_down()
    world.R_role_server1.go_down()
    world.R_role_server2.go_down()
 
    responses = get_responses(10)
 
    # as regular destinations went down, backups started to respond
    assert world.SB_response in responses, responses
    assert world.RB_role_response1 in responses, responses
 
    assert world.S_response not in responses, responses
    assert world.R_role_response1 not in responses, responses
    assert world.R_role_response2 not in responses, responses
 
    # down destinations still quiet
    assert world.SD_response not in responses, responses
    assert world.RD_role_response1 not in responses, responses
 
 
@step(u'And I expect SD and RD servers are down in backend')
def and_i_expect_sd_and_rd_servers_are_down_in_backend(step):
    # as previous steps checks that serponses list don't have responses from
    # down destinations, we simply pass this step
    clear_nginx_includes()
    create_api()
 
 
###############################################################################
# Scenario 10
 
 
@step(u'Given I have proxy to server')
def given_i_have_proxy_to_server(step):
    given_i_have_a_server(step)
    when_i_add_proxy(step)
 
 
@step(u'When I reconfigure proxy to another server')
def when_i_reconfigure_proxy_to_another_server(step):
    world.expected_response2 = 'hi there'
    world.server2 = Server(8001, world.expected_response2)
    world.server2.serve_forever()
    world.destinations = [{'host': 'localhost', 'port': '8001'}]
 
    proxies = [dict(hostname='uty.com',
                         backends=world.destinations,
                         port=world.port)]
    world.api.reconfigure(proxies)
    time.sleep(1)
 
 
@step(u'Then I expect proxying to another server')
def then_i_expect_proxying_to_another_server(step):
    response = get_responses(1)[0]
 
    clear_nginx_includes()
    create_api()
 
    assert response == world.expected_response2, response
 
 
###############################################################################
# Scenario 11
 
 
@step(u'When I reconfigure proxy with bad configuration')
def when_i_reconfigure_proxy_with_bad_configuration(step):
    world.expected_response2 = 'hi there'
    world.server = Server(8001, world.expected_response2)
    world.server.serve_forever()
    world.destinations = [{'host': 'localhost', 'port': '::::'}]
 
    bad_proxies = [dict(hostname='uty.com',
                         backends=world.destinations,
                         port=world.port)]
    try:
        world.api.reconfigure(bad_proxies)
    except initdv2.InitdError, e:
        world.exception_thrown = True
    else:
        world.exception_thrown = False
 
 
@step(u'Then I expect exception to be thrown')
def then_i_expect_exception_to_be_thrown(step):
    assert world.exception_thrown, 'No exception has been thrown'
 
 
@step(u'And proxy should remain unchanged')
def and_proxy_should_remain_unchanged(step):
    response = get_responses(1)[0]
 
    clear_nginx_includes()
    create_api()
 
    assert response == world.expected_response, response
 
