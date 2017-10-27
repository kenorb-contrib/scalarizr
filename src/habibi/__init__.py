# pylint: disable=R0902, W0613, R0913, R0914, R0201, R0904
 
"""
Habibi is a testing tool which scalarizr team uses to mock scalr's side of communication.
It allowes to test scalarizr behavior on real virtual machines, 
without implementing it by scalr team. Habibi uses lxc containers as instances, which 
makes habibi tests incredibly fast and totally free (no cloud providers involved).
 
Habibi consists of several modules:
 
- Habibi - represents scalr farm, could contain zero or more Roles.
- Storage - persistent storage service, based on lvm. Replaces EBS' and similar services.
- Events system, which connects framework parts and test code together.
 
Prerequisites:
    Ubuntu 12.04 or higher as host machine
    lvm2
    M2Crypto
 
 
For usage information, see tests/acceptance/block_device test
 
"""
 
import os
import sys
import cgi
import json
import copy
import uuid
import glob
import time
import shutil
import random
import string
import urllib2
import logging
import binascii
import itertools
import threading
import subprocess
import BaseHTTPServer
import xml.dom.minidom as dom
import xml.etree.ElementTree as etree
 
from wsgiref import simple_server
 
from habibi import crypto, storage, events
 
 
logging.basicConfig(
        stream=sys.stderr, 
        level=logging.DEBUG, 
        format='%(asctime)s %(name)-20s %(levelname)-8s - %(message)s')
LOG = logging.getLogger('habibi')
 
VAGRANT_FILE = '''
Vagrant.configure("2") do |config|
  config.vm.box = "mongo-248"
  #config.vm.box = "ubuntu1204"
  config.vm.box_url = "http://scalr-labs.s3.amazonaws.com/ubuntu1204-lxc_devel_20130814.box"
  config.vm.synced_folder "../..", "/vagrant0"
  config.berkshelf.enabled = true
  config.vm.provision :chef_solo do |chef|
    #chef.cookbooks_path = ["../../cookbooks/cookbooks", "../../public_cookbooks/cookbooks"]
 
$recipes_for_behaviors
 
    chef.add_recipe "vagrant_boxes::scalarizr_lxc"
    chef.json = { :user_data => "$user_data" }
  end
end
'''
 
BERKSHELF_FILE = """
site :opscode
 
repo_int = "git@github.com:Scalr/int-cookbooks.git"
repo_pub = "git@github.com:Scalr/cookbooks.git"
 
cookbook "vagrant_boxes", git: repo_int, rel: "cookbooks/vagrant_boxes", ref: "HEAD"
"""
 
ROUTER_IP = '10.0.3.1'
ROUTER_PORT = 10001
 
 
class Server(object):
 
    # REVIEW: please document server events
 
    def __init__(self,
                 behaviors,
                 role,
                 sid=None,
                 index=0,
                 crypto_key=None,
                 farm_hash=None,
                 public_ip=None,
                 private_ip=None,
                 status='pending',
                 zone=None):
        """
        @param role: Role object
        @type role: FarmRole
        """
        self.id = sid or str(uuid.uuid4())
        self.index = index
        self.role = role
        self.crypto_key = crypto_key or crypto.keygen()
        self.farm_hash = farm_hash or crypto.keygen(10)
        self.public_ip = public_ip
        self.private_ip = private_ip
        self._status = status
        self.behaviors = behaviors
        self.zone = zone
        self._rootfs_path = None
 
    @property
    def rootfs_path(self):
        if not self._rootfs_path:
            lxc_containers_path = '/var/lib/lxc'
 
            # REVIEW: why loop? server-id -> lxc mapping is 1-1
            # Container directory name and server id are not the same. We need to find
            # exact path using loop or glob.
            # Rewrited with glob
            try:
                server_dir_path = glob.glob(os.path.join(lxc_containers_path, str(self.id) + '*'))[0]
                self._rootfs_path = os.path.join(server_dir_path, 'rootfs')
            except KeyError:
                raise BaseException("Can't find server with id: %s" % self.id)
 
        return self._rootfs_path
 
    def set_status(self, new_status):
        old_status = self._status
        self._status = new_status
        self.role.farm.event_mgr.notify(events.Event(event='server_status_changed',
                                        old_status=old_status,
                                        new_status=new_status,
                                        server=self))
 
    def get_status(self):
        return self._status
 
    status = property(get_status, set_status)
 
    def terminate(self):
        if self.status != 'terminated':
            p = subprocess.Popen('vagrant destroy -f', shell=True, cwd=self.server_dir)
            p.communicate()
            self.status = 'terminated'
 
    def stop(self):
        p = subprocess.Popen('vagrant halt', shell=True, cwd=self.server_dir)
        p.communicate()
 
    def block_network(self):
        self.execute('iptables -A INPUT -p tcp --dport 22 -j ACCEPT')
        self.execute('iptables -A OUTPUT -p tcp --sport 22 -j ACCEPT')
        self.execute('iptables -P INPUT DROP')
        self.execute('iptables -P OUTPUT DROP')
        self.execute('iptables -P FORWARD DROP')
 
    def execute(self, command):
        p = subprocess.Popen('vagrant ssh -c "sudo %s"' % command,
                             shell=True, cwd=self.server_dir,
                             stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        return p.communicate()
 
    @property
    def server_dir(self):
        return  os.path.join(self.role.farm.base_dir, self.id)
 
    def send(self, msg, event_data=None):
        """
        @param msg: Message to send to the server
        @param event_data: additional fields for 'outgoing_message' event
        """
        event = {'event': 'outgoing_message',
                 'message': msg,
                 'target_server': self}
 
        if event_data:
            event.update(event_data)
        self.role.farm.event_mgr.notify(events.Event(**event))
 
        LOG.debug('Outgoing message %s to %s', msg.name, self.id)
        xml_data = msg.toxml()
        LOG.debug(' * data: %s', xml_data)
        crypto_key = binascii.a2b_base64(self.crypto_key)
        encrypted_data = crypto.encrypt(xml_data, crypto_key)
        signature, timestamp = crypto.sign(encrypted_data, crypto_key)
 
        url = 'http://{0}:8013/control'.format(self.public_ip)
        req = urllib2.Request(url, encrypted_data, {
            'Content-type': 'application/xml',
            'Date': timestamp,
            'X-Signature': signature,
            'X-Server-Id': self.id})
        opener = urllib2.build_opener(urllib2.HTTPRedirectHandler())
        opener.open(req)
 
 
class FarmRole(object):
 
    chef_recipes = None
    def __init__(self, name, behaviors, farm):
        """
        @param farm: Farm object (habibi)
        @type farm: Habibi
        """
        if not isinstance(behaviors, (list, tuple)):
            behaviors = [behaviors]
        self.behaviors = behaviors
        self.name = name
        self.farm = farm
        self.id = random.randint(1, 1000000)
        self.servers = []
 
    def _next_server_index(self):
        last_server_index = 1
        for server in self.servers:
            if server.status == 'terminated':
                continue
            if server.index > last_server_index:
                return last_server_index
            last_server_index += 1
        return last_server_index
 
    def _pack_user_data(self, user_data):
        return ';'.join(['{0}={1}'.format(k, v) for k, v in user_data.items()])
 
    def run_server(self, zone='lxc-zone'):
        if not self.farm.started:
            raise Exception("You should start your farm first.")
        server = Server(behaviors=self.behaviors,
                        index=self._next_server_index(),
                        zone=zone,
                        role=self)
 
        self.servers.append(server)
        t = threading.Thread(target=self._run_lxc_container, args=(server,))
        t.daemon = True
        t.start()
        return server
 
    def _run_lxc_container(self, server):
        server_dir = self.farm.base_dir + '/' + server.id
        os.makedirs(server_dir)
 
        server.user_data = self._user_data(server)
 
        recipes_for_behaviors_list = self.chef_recipes or self.behaviors
        with open(server_dir + '/Vagrantfile', 'w+') as fp:
            tpl = string.Template(VAGRANT_FILE)
            recipes_subst = "\n".join(["    chef.add_recipe '%s'" % recipe for recipe in recipes_for_behaviors_list])
 
            fp.write(tpl.substitute(
                user_data=self._pack_user_data(server.user_data),
                recipes_for_behaviors=recipes_subst
                ))
 
        with open(server_dir + '/Berksfile', 'w+') as fp:
            cookbooks_strs = []
            for recipe in recipes_for_behaviors_list:
                cookbook = '::' in recipe and recipe.split('::')[0] or recipe
                cookbook_str = 'cookbook "{0}", git: repo_pub, rel: "cookbooks/{0}", ref: "HEAD"'.format(cookbook)
                cookbooks_strs.append(cookbook_str)
 
            fp.write(BERKSHELF_FILE + '\n' + '\n'.join(cookbooks_strs))
 
        #subprocess.Popen('vagrant init ubuntu1210', shell=True, cwd=server_dir).communicate()
        lxc_start = subprocess.Popen('vagrant up --provider lxc', shell=True, cwd=server_dir)
        lxc_start.communicate()
        if lxc_start.returncode:
            raise Exception('Container start or provisioning failed. ret code: %s' % lxc_start.returncode)
 
    def _user_data(self, server):
        return {'szr_key': server.crypto_key,
                'hash': server.farm_hash,
                'serverid': server.id,
                'p2p_producer_endpoint': 'http://{0}:{1}/messaging'.format(ROUTER_IP, ROUTER_PORT),
                'queryenv_url': 'http://{0}:{1}/query-env'.format(ROUTER_IP, ROUTER_PORT),
                'behaviors': ','.join(server.behaviors),
                'farm_roleid': '1',
                'roleid': '1',
                'env_id': '1',
                'platform': 'lxc',
                'region': server.zone,
                'server_index': str(server.index),
                'storage_service_url': 'http://{0}:{1}'.format(ROUTER_IP, storage.port)}
 
 
class ServerSet(object):
    """
    Represents list of servers. Every action on this object will be performed consequently.
    Also supports iteration, if you want to get one server or subset:
 
        s = ServerSet([server1, server2, server3])
        s.block_network() # kill network to all servers in set
        s.terminate() # terminate all servers in set
 
        s[0] # first server in set
        s[:2] # ServerSet object with first and second servers of current ServerSet
    """
 
    def __init__(self, servers):
        self._servers = servers
 
    def __getattr__(self, item):
        return self._wrapper(self._servers, item)
 
    def __iter__(self):
        for server in self._servers:
            yield server
 
    def __getitem__(self, item):
        if not isinstance(item, (int, slice)):
            raise TypeError('Indicies must be of int type, not %s' % type(item))
        ret = self._servers[item]
        if isinstance(ret, list):
            return ServerSet(ret)
        else:
            return ret
 
    class _wrapper(object):
 
        def __init__(self, servers, attr):
            self.attr_name = attr
            self.servers = servers
 
        def __call__(self, *args, **kwargs):
            ret = []
            for server in self.servers:
                attr = getattr(server, self.attr_name)
                ret.append(attr(*args, **kwargs))
            return ret
 
 
class Habibi(object):
 
    comminucation_srv = None
    communication_thread = None
    storage_manager = None
    storage_server = None
 
    def __init__(self, base_dir=None):
        self._servers = []
        self.event_mgr = events.EventMgr()
        self.queryenv_version = '2012-07-01'
        self.base_dir = base_dir or '.habibi'
        self.RequestHandler.habibi = self
        self.queryenv = QueryEnv(self)
        self.roles = []
        self.farm_crypto_key = crypto.keygen()
        self.started = False
 
    def add_role(self, name, behaviors):
        role = FarmRole(name, behaviors, self)
        self.roles.append(role)
        return role
 
    def spy(self, spy):
        for attr_name in dir(spy):
            if not attr_name.startswith('_'):
                attr = getattr(spy, attr_name)
                if callable(attr) and hasattr(attr, '_events'):
                    self.event_mgr.add_listener(attr._events, attr)
 
    def remove_role(self, role):
        if role not in self.roles:
            raise Exception('Role %s not found' % role.name)
        self.roles.remove(role)
        for server in self.servers(role_name=role.name):
            try:
                server.destroy()
            except:
                LOG.debug('Failed to terminate server %s' % server.id, exc_info=sys.exc_info())
 
    def start(self):
        # comminucation_srv manages queryenv requests and handles incoming messages
        self.comminucation_srv = BaseHTTPServer.HTTPServer(('', ROUTER_PORT), self.RequestHandler)
        self.communication_thread = threading.Thread(name='Communication server', 
                                                target=self.comminucation_srv.serve_forever)
        self.communication_thread.setDaemon(True)
        self.communication_thread.start()
 
        self.storage_manager = storage.StorageMgr(self)
        self.storage_server = simple_server.make_server('0.0.0.0', storage.port, self.storage_manager)
        storage_thread = threading.Thread(target=self.storage_server.serve_forever, name='Storage server')
        storage_thread.setDaemon(True)
        storage_thread.start()
 
        storage.lvm2.lvremove(storage.vg_name)
 
        if os.path.exists(self.base_dir):
            shutil.rmtree(self.base_dir)
        os.makedirs(self.base_dir)
        self.started = True
 
    def stop(self):
        if self.started:
            self.comminucation_srv.shutdown()
            self.storage_manager.cleanup()
            self.storage_server.shutdown()
            self.started = False
        else:
            raise Exception('Habibi has not been started yet')
 
    def servers(self, sid=None, role_name=None, **kwds):
        """
        @param sid: find server with specified id
        @param role: role name to search servers in
        @param kwds: filter found servers by attribute values (see examples)
 
        keyword arguments are additional filters for servers, where key is server's attribute name,
        and value either attribute value or validator function, which accepts single argument (attribute value):
 
            # find pending and initilizing servers across all roles
            servers(status=lambda s: s.status in ('pending', 'initializing'))
 
            # find server with index=3 in percona55 role
            third = servers(role_name='percona55', index=3)[0]
        """
        search_res = []
        if role_name is not None:
            for role in self.roles:
                if role.name == role_name:
                    search_res = copy.copy(role.servers)
                    break
        else:
            search_res = list(itertools.chain(*[r.servers for r in self.roles]))
 
        if sid is not None:
            search_res = filter(lambda x: x.id == sid, search_res)
 
        if kwds:
            def filter_by_attrs(server):
                for find_attr, find_value in kwds.iteritems():
                    real_value = getattr(server, find_attr)
                    if callable(find_value):
                        if not find_value(real_value):
                            return False
                    else:
                        if real_value != find_value:
                            return False
                else:
                    return True
 
            search_res = filter(filter_by_attrs, search_res)
 
        if search_res:
            return ServerSet(search_res)
        else:
            raise LookupError('No servers were found')
 
    def on_message(self, msg):
        server_id = msg.meta['server_id']
        LOG.debug('Incoming message  "%s" from %s', msg.name, server_id)
        server = self.servers(sid=server_id)[0]
 
        self.event_mgr.notify(events.Event(event='incoming_message',
                                        message=msg, server=server))
 
        kwds = dict(trigger_message=msg, trigger_server=server)
 
        if msg.name == 'HostInit':
            server.status = 'initializing'
            server.public_ip = msg.body['remote_ip']
            server.private_ip = msg.body['local_ip']
            server.crypto_key = msg.body['crypto_key'].strip()
            time.sleep(1)  # It's important gap for Scalarizr
 
            server.send(Message('HostInitResponse'), event_data=kwds)
 
        if msg.name in ('HostInit', 'BeforeHostUp', 'HostUp'):
            if msg.name == 'HostUp':
                server.status = 'running'
            # Send copy of original message to every server in the farm
            msg_copy = msg.copy()
            msg_copy.scripts = []
            self.servers(status=lambda s: s not in ('pending', 'terminated')).send(msg_copy, event_data=kwds)
 
        elif msg.name == 'HostDown':
            pass
        elif msg.name == 'BeforeHostDown':
            pass
        elif msg.name in ('OperationDefinition', 'OperationProgress', 'OperationResult'):
            pass
 
    class RequestHandler(BaseHTTPServer.BaseHTTPRequestHandler):
        LOG = logging.getLogger('habibi.web')
 
        def do_POST(self):
            try:
                server_id = self.headers['X-Server-Id']
                server = self.habibi.servers(sid=server_id)[0]
                if self.path.startswith('/messaging'):
                    self.do_messaging(server)
                elif self.path.startswith('/query-env'):
                    self.do_queryenv(server)
                else:
                    msg = 'Unknown route: {0}'.format(self.path)
                    raise Exception(msg)
            except:
                LOG.exception('POST exception')
                self.render_html(500)
 
        def do_HEAD(self):
            try:
                self.do_static()
            except:
                LOG.exception('GET exception')
                self.render_html(500)
 
        def do_GET(self):
            try:
                self.do_static()
            except:
                LOG.exception('HEAD exception')
                self.render_html(500)
 
        def do_messaging(self, server):
            if os.path.basename(self.path) != 'control':
                self.render_html(201)
                return
 
            encrypted_data = self.rfile.read(int(self.headers['Content-length']))
            xml_data = crypto.decrypt(encrypted_data, binascii.a2b_base64(server.crypto_key))
 
            msg = Message()
            msg.fromxml(xml_data)
            self.render_html(201)
 
            hdlr_thread = threading.Thread(target=self.habibi.on_message, args=(msg, ))
            hdlr_thread.setDaemon(True)
            hdlr_thread.start()
 
        def do_queryenv(self, server):
            operation = self.path.rsplit('/', 1)[-1]
            fields = cgi.FieldStorage(
                fp=self.rfile,
                keep_blank_values=True,
                environ = {'REQUEST_METHOD':'POST'},
                headers=self.headers
            )
            response = self.habibi.queryenv.run(operation, fields, server)
            self.render_html(200, response)
 
        def do_static(self, head=False):
            base = os.path.dirname(__file__)
            static_dir = base + '/static'
            filename = static_dir + self.path
            if os.path.exists(filename):
                with open(filename) as fp:
                    self.render_html(200, fp.read())
            else:
                self.render_html(404)
 
        def render_html(self, http_code, http_body=None, http_headers=None):
            if http_code >= 400:
                if not http_body:
                    exc_info = sys.exc_info()
                    http_body = '{0}: {1}'.format(exc_info[0].__class__.__name__, exc_info[1])
                self.send_error(http_code)
            else:
                self.send_response(http_code)
 
            http_body = http_body or ''
            if http_headers:
                for header, value in http_headers:
                    self.send_header(header, value)
            if not http_headers or (http_headers and not 'Content-length' in http_headers):
                self.send_header('Content-length', len(http_body))
            self.end_headers()
            self.wfile.write(http_body) 
 
        def log_message(self, format_, *args):
            self.LOG.debug(format_, *args)
 
 
class QueryEnv(object):
    """
    Queryenv mock, produces only one event:
        event: 'queryenv',
        method_name: method_name,    # e.g. list_ebs_mountpoints
        response: response,          # etree.Element, response that you can modify
        server: server               # habibi.Server, server who initiated request
    """
    habibi = None
    LOG = logging.getLogger('habibi.queryenv')
 
    def __init__(self, habibi):
        self.habibi = habibi
        self.fields = None
 
    def run(self, operation, fields, server):
        try:
            method_name = operation.replace('-', '_')
            self.LOG.debug('run %s', operation)
            response = etree.Element('response')
            if hasattr(self, method_name):
                response.append(getattr(self, method_name)(fields, server))
 
            self.habibi.event_mgr.notify(events.Event(event='queryenv', method_name=method_name,
                                         response=response, server=server))
 
            return etree.tostring(response)
        except:
            exc_info = sys.exc_info()
            LOG.error('Queryenv error', exc_info=exc_info)
            raise
 
    def get_latest_version(self, fields, server):
        ret = etree.Element('version')
        ret.text = self.habibi.queryenv_version
        return ret
 
    def list_global_variables(self, fields, server):
        return etree.Element('variables')
 
    def get_global_config(self, fields, server):
        ret = etree.Element('settings')
        settings = {'dns.static.endpoint': 'scalr-dns.com',
                    'scalr.version': '4.5.0',
                    'scalr.id': '884c7c0'}
        for key, val in settings.items():
            setting = etree.Element('setting', key=key)
            setting.text = val
            ret.append(setting)
        return ret
 
    def list_roles(self, fields, server):
        with_init = fields.getvalue('showInitServers')
        if with_init:
            with_init = int(with_init) == 1
        ret = etree.Element('roles')
        for role in self.habibi.roles:
 
            role_el = etree.Element('role')
            role_el.attrib.update({'id': str(role.id),
                            'role-id': '1',
                            'behaviour': ','.join(role.behaviors),
                            'name': role.name})
            hosts = etree.Element('hosts')
            role_el.append(hosts)
 
            for server in self.habibi.servers(role_name=role.name):
                if server.status == 'running' or (with_init and server.status == 'initializing'):
 
                    host = etree.Element('host')
                    host.attrib.update({'internal-ip': server.private_ip,
                                        'external-ip': server.public_ip,
                                        'status': server.status,
                                        'index': str(server.index),
                                        'cloud-location': server.zone})
                    hosts.append(host)
            ret.append(role_el)
        return ret
 
    def get_service_configuration(self, fields, server):
        return etree.Element('settings')
 
    def get_server_user_data(self, fields, server):
        ret = etree.Element('user-data')
        for k, v in server.user_data.iteritems():
            key = etree.Element('key', attrib=dict(name=k))
            val = etree.Element('value')
            val.text = '<![CDATA[%s]]>' % v
            key.append(val)
            ret.append(key)
        return ret
 
 
class Message(object):
    def __init__(self, name=None, meta=None, body=None, id=None):
        self.id = id or str(uuid.uuid4())
        self.name = name
        self.meta = meta or {}
        self.body = body or {}
 
    def fromjson(self, json_str):
        if isinstance(json_str, str):
            json_str = json_str.decode('utf-8')
 
        json_obj = json.loads(json_str)
        for attr in  ('id', 'name', 'meta', 'body'):
            assert attr in json_obj, 'Attribute required: %s' % attr
            setattr(self, attr, copy.copy(json_obj[attr]))
 
    def fromxml(self, xml):
        if isinstance(xml, str):
            xml = xml.decode('utf-8')
        doc = dom.parseString(xml.encode('utf-8'))
        xml_strip(doc)
 
        root = doc.documentElement
        self.id = root.getAttribute("id")
        self.name = root.getAttribute("name")
 
        for ch in root.firstChild.childNodes:
            self.meta[ch.nodeName] = self._walk_decode(ch)
        for ch in root.childNodes[1].childNodes:
            self.body[ch.nodeName] = self._walk_decode(ch)
 
    def tojson(self):
        result = dict(id=self.id, name=self.name, body=self.body, meta=self.meta)
        return json.dumps(result, ensure_ascii=True)
 
    def _walk_decode(self, el):
        if el.firstChild and el.firstChild.nodeType == 1:
            if all((ch.nodeName == "item" for ch in el.childNodes)):
                return [self._walk_decode(ch) for ch in el.childNodes]
            else:
                return {ch.nodeName: self._walk_decode(ch) for ch in el.childNodes}
        else:
            return el.firstChild and el.firstChild.nodeValue or None
 
    def __str__(self):
        impl = dom.getDOMImplementation()
        doc = impl.createDocument(None, "message", None)
 
        root = doc.documentElement;
        root.setAttribute("id", str(self.id))
        root.setAttribute("name", str(self.name))
 
        meta = doc.createElement("meta")
        root.appendChild(meta)
        self._walk_encode(self.meta, meta, doc)
 
        body = doc.createElement("body")
        root.appendChild(body)
        self._walk_encode(self.body, body, doc)
 
        return doc.toxml('utf-8')
 
    toxml = __str__
 
    def _walk_encode(self, value, el, doc):
        if getattr(value, '__iter__', False):
            if getattr(value, "keys", False):
                for k, v in value.items():
                    itemEl = doc.createElement(str(k))
                    el.appendChild(itemEl)
                    self._walk_encode(v, itemEl, doc)
            else:
                for v in value:
                    itemEl = doc.createElement("item")
                    el.appendChild(itemEl)
                    self._walk_encode(v, itemEl, doc)
        else:
            if value is not None and not isinstance(value, unicode):
                value = str(value).decode('utf-8')
            el.appendChild(doc.createTextNode(value or ''))
 
    def copy(self):
        return Message(name=self.name, meta=copy.deepcopy(self.meta),
                                            body=copy.deepcopy(self.body))
 
 
def xml_strip(el):
    for child in list(el.childNodes):
        if child.nodeType == child.TEXT_NODE and child.nodeValue.strip() == '':
            el.removeChild(child)
        else:
            xml_strip(child)
