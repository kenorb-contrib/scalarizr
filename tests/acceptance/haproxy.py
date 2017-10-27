import socket
import threading
import logging
import time
import errno
import sys
from pprint import pformat
 
from lettuce import step, world, before, after
import mock
 
from scalarizr.libs.bases import Task
from scalarizr.api import haproxy as haproxy_api
 
 
haproxy_api.iptables = mock.MagicMock()
 
 
LOG = logging.getLogger(__name__)
logging.getLogger().setLevel(logging.DEBUG)
 
 
class IdDict(dict):
    """
    Allows adding multiple items with '' as the key, uses an int instead.
    """
 
    def __init__(self, *args, **kwargs):
        super(IdDict, self).__init__(*args, **kwargs)
 
        self._id = 0
 
    def __setitem__(self, key, val):
        if key == '':
            self._id += 1
            key = self._id
 
        super(IdDict, self).__setitem__(key, val)
 
 
class SocketServer(Task):
 
    def __init__(self, port):
        super(SocketServer, self).__init__()
 
        self.address = ('127.0.0.1', port)
 
        self.on(start=lambda: LOG.info("[%s] Starting up", self))
        self.on(complete=lambda r: LOG.info("[%s] Went down", self))
        self.on(error=lambda exc_info: LOG.info("[%s] Crashed", self,
            exc_info=exc_info))
 
    def _run(self):
        self._killed = False
 
        sock = socket.socket()
        # allow restarting right after a crash
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind(self.address)
        sock.listen(5)
 
        while not self._killed:
            self._handle(*sock.accept())
 
    def __str__(self):
        return ':'.join(map(str, self.address))
 
    def _handle(self, client_sock, address):
        """ The server sends back it's own address """
        address_str = ':'.join(map(str, address))
 
        if not self._killed:
            LOG.debug("[%s] Got connection from %s", self, address_str)
            client_sock.send(str(self))
        else:
            # we expect client_sock to be the one created by self._kill()
            LOG.debug("[%s] Recieved kill connection from %s", self, 
                address_str)
            client_sock.send("debug: being killed")
 
    def _kill(self):
        self._killed = True
        # make sock.accept() return
        try:
            socket.socket().connect(self.address)
        except socket.error, e:
            if e.errno == errno.ECONNREFUSED:
                pass
 
 
class Server(SocketServer, threading.Thread):
    """
    SocketServer + Thread + server registration for test setup and teardown
    """
 
    _port = 27000
    _servers = []
 
    def __init__(self):
        cls = self.__class__
 
        cls._port += 1
        super(Server, self).__init__(cls._port)
        threading.Thread.__init__(self)  # bases.Task breaks super() chain
 
        cls._servers.append(self)
 
        self.as_arg = {"host": self.address[0], "port": self.address[1]}
 
    @classmethod
    def setup(cls):
        cls.teardown()
        cls._servers = []
 
    @classmethod
    def teardown(cls):
        map(lambda server: server.kill(), cls._servers)
        for server in cls._servers:
            try:
                server.join()
            except RuntimeError:
                pass
 
 
def communicate(target):
    """
    Get response from a running :class:`Server` instance.
 
    :param target: port(int), netloc(str) or address for socket as tuple
 
    """
 
    if isinstance(target, int):
        address_str = "127.0.0.1:%s" % target
        address = ("127.0.0.1", target)
    elif isinstance(target, str):
        address_str = target
        ip, port = target.split(':')
        address = (ip, int(port))
    elif isinstance(target, tuple):
        address_str = ':'.join(map(str, self.address))
        address = target
 
    sock = socket.socket()
    try:
        sock.connect(address)
    except socket.error, e:
        if e.errno == errno.ECONNREFUSED:
            response = 'debug: connection refused'
        else:
            raise
    else:
        response = sock.recv(1024)  # FIXME: indefinite block if _handle has
                                    # crashed; using communicate from main 
                                    # thread allows to CTRL+C this
    LOG.info("[%s] %s replied: \"%s\"", "Communicate".ljust(15), address_str,
        response)
    return response
 
 
class Role(object):
 
    _id = 10
    _instances = {}
 
    def __init__(self, servers=None):
        cls = self.__class__
        cls._id += 1
 
        self.id = cls._id
        self.as_arg = {"farm_role_id": self.id}
        self.servers = servers or []
 
        cls._instances[self.id] = self
 
    @classmethod
    def setup(cls):
        cls._instances = {}
 
    @classmethod
    def get_servers(cls, role_id):
        return map(str, cls._instances[role_id].servers)
 
    get_role_servers = get_servers
 
 
haproxy_api.get_role_servers = Role.get_servers
 
 
PARAMS = {
    '': {},
    "down": {"down": True},
    "backup": {"backup": True},
    "regular": {},
}
 
 
HC_PARAMS = {
    None: {},
    "C1": {
        "fall_threshold": '3',
        "rise_threshold": '6',
        "check_interval": '7s',
    },
    "C2": {
        "fall_threshold": '4',
        "rise_threshold": '7',
        "check_interval": '8s',
    },
}
 
 
def minimal_haproxy_conf(path="/etc/haproxy/haproxy.cfg"):
    contents = \
"""
defaults
    timeout connect 5000ms
    timeout client 5000ms
    timeout server 5000ms
    option log-health-checks
 
listen init
    bind *:26998
 
"""
 
    # healthcheck scenario fails when there is no 'global' section
    contents = "\nglobal\n\n" + contents
 
    with open(path, 'w') as f:
        f.write(contents)
 
 
def dont_fail(f):
    def wrapper(*args, **kwargs):
        try:
            return f(*args, **kwargs)
        except:
            # after.each_scenario doesn't execute for some reason if
            # we let this exception pop out
            LOG.error("STEP FAILED", exc_info=sys.exc_info())
    return wrapper
 
 
def acceptable_responses():
 
    responses = map(str, world.servers.values())
 
    role_ids = map(lambda role: role.id, world.roles.values())
    [responses.extend(map(str, Role.get_servers(role_id))) for role_id in role_ids]
 
    return responses
 
def log_acceptable_responses():
    responses = world.acceptable_responses
    LOG.info("Acceptable responses: %s", ', '.join(['"' + response + '"'
                                                    for response in responses]))
 
 
@before.each_scenario
def setup(scenario):
    Server.setup()
    Role.setup()
 
    world.servers = IdDict()
    world.roles = IdDict()
    world.proxy_port = 27000
    world.acceptable_responses = []
 
    # for 'new server up'
    world.new_server = None
    # for 'server goes down' scenario
    world.terminated = []
    # for 'get health'
    world.health = None
 
    minimal_haproxy_conf()
    world.api = haproxy_api.HAProxyAPI()
    world.api.svc.start()
    LOG.info("Started haproxy service")
 
 
@after.each_scenario
def teardown(scenario):
    Server.teardown()
 
    world.api.svc.stop()
    LOG.info("Stopped haproxy service")
 
    LOG.info("-" * 17)
 
 
@step("i have a ?(\w*) server ?(\w*) ?(with custom healthcheck (\w+))?")
def i_have_a_server(step, desc, name, _, hc_name):
    server = Server()
    server.start()
 
    server.as_arg.update(PARAMS[desc])
    server.as_arg.update(HC_PARAMS[hc_name])
 
    # if server.as_arg.keys() == ["host"]:
    #     server.as_arg = server.as_arg["host"]
 
    world.servers[name] = server
 
 
@step("i have a ?(\w*) role ?(\w*) ?(with custom healthcheck (\w+))?")
def i_have_a_role(step, desc, name, _, hc_name):
    servers = [Server() for i in range(2)]
    [server.start() for server in servers]
    role = Role(servers)
 
    role.as_arg.update(PARAMS[desc])
    role.as_arg.update(HC_PARAMS[hc_name])
 
    # if id is the only key, test the short form
    # if role.as_arg.keys() == ["id"]:
    #     role.as_arg = role.as_arg["id"]
 
    world.roles[name] = role
 
 
@step("i add proxy")
@dont_fail
def i_add_proxy(step):
    servers = map(lambda server: server.as_arg, world.servers.values())
    roles = map(lambda role: role.as_arg, world.roles.values())
    backends = servers + roles
 
    world.api.make_proxy(port=world.proxy_port,
                        backends=backends)
    world.acceptable_responses = acceptable_responses()
    log_acceptable_responses()
 
 
@step("i have a proxy to a role")
def i_have_a_proxy_to_a_role(step):
    step.given("i have a role")
    step.given("i add proxy")
 
 
@step("i expect proxying")
def i_expect_proxying(step):
    for i in range(10):
        assert communicate(world.proxy_port) in world.acceptable_responses
 
 
@step("i launch new server of this role")
def i_launch_new_server_of_this_role(step):
    role = world.roles.values()[0]
    server = Server()
    server.start()
    role.servers.append(server)
 
    world.new_server = server
 
    world.api.add_server(
        {
            'port': server.address[1],
            'host': server.address[0]
        },
        "tcp:27000")  #? how do we associate role with a backend?
 
 
@step("i terminate one server of this role")
def i_terminate_one_server_of_this_role(step):
    role = world.roles.values()[0]
    server = role.servers.pop()
 
    world.terminated.append(server)
    world.acceptable_responses.remove(str(server))
 
    world.api.remove_server({
        'port': server.address[1],
        'host': server.address[0],
    })
 
 
@step("server appears in the backend")
def server_appears_in_the_backend(step):
    assert world.new_server, world.new_server
    server_name = str(world.new_server).replace('.', '-')
    assert server_name in world.api.cfg.backends["scalr:backend:tcp:27000"]['server']
 
 
@step("server is removed from the backend")
def server_is_removed_from_the_backend(step):
    assert len(world.terminated) == 1, world.terminated
    server = world.terminated[-1]
    server_name = str(server).replace('.', '-')
    assert not server_name in world.api.cfg.backends["scalr:backend:tcp:27000"]['server']
 
 
@step("i have a proxy to two roles: master and backup")
def i_have_a_proxy_to_two_roles(step):
    step.given("i have a role master")
    step.given("i have a backup role backup")
    step.given("i add proxy")
 
 
@step("i have a proxy to two servers")
def i_have_a_proxy_to_two_servers(step):
    step.given("i have a server")
    step.given("i have a server")
    step.given("i add proxy")
 
 
@step("i terminate master servers")
def i_terminate_master_servers(step):
    master_role = world.roles["master"]
 
    for server in master_role.servers:
        world.api.remove_server({
            'port': server.address[1],
            'host': server.address[0],
        })
        world.acceptable_responses.remove(str(server))
    log_acceptable_responses()
 
 
@step("i update proxy marking one server as down")
def i_update_proxy_marking_one_server_as_down(step):
    server = world.servers.values()[0]
 
    server_spec = {
        'port': server.address[1],
        'host': server.address[0],
    }
    server_spec.update(PARAMS["down"])
 
    world.api.add_server(server_spec, "tcp:27000")  #?
 
    world.acceptable_responses.remove(str(server))
    log_acceptable_responses()
 
 
def get_servers(name):
    if name is None:
        return []
    elif name in world.servers:
        return [world.servers[name]]
    elif name in world.roles:
        return world.roles[name].servers
    else:
        raise Exception("No server or role with the name %s" % name)
 
def server_params(server):
    server_name = str(server).replace('.', '-')
 
    record = world.api.cfg.backends["scalr:backend:tcp:27000"]['server'][server_name]
    record.pop("check", None)
    assert record.pop("host") == server.address[0]
    assert record.pop("port") == str(server.address[1])
 
    #? should we translate param names in the api?
    if "disabled" in record:
        record["down"] = record["disabled"]
        del record["disabled"]
 
    return record
 
 
@step("i expect (\w+) and (\w+) servers are (\w+) in the backend")
def i_expect_servers_in_backend(step, name1, name2, desc):
 
    servers = get_servers(name1) + get_servers(name2)
 
    records = map(server_params, servers)
 
    for record in records:
        assert record == PARAMS[desc], "%s != %s" % (record, PARAMS[desc])
 
 
@step("i expect (\w+)( and (\w+))? servers? having (default|custom) healthcheck ?(\w+)?")
def i_expect_servers_having_healthcheck(step, name1, _, name2, __, hc_name):
 
    servers = get_servers(name1) + get_servers(name2)
 
    records = map(server_params, servers)
 
    for record in records:
        assert record == haproxy_api.rename(HC_PARAMS[hc_name]), str(record) + '  !=  ' + str(HC_PARAMS[hc_name])
 
 
@step("i have a proxy (\w+) to (running|broken) servers")
def i_have_a_proxy_to_servers(step, name, status):
 
    #haproxy_api.HEALTHCHECK_DEFAULTS['default-server']['inter'] = 2000
    #haproxy_api.HEALTHCHECK_DEFAULTS['default-server']['fall'] = 2
 
    for i in range(1, 3):
        server_name = name + str(i)
        step.given("i have a server %s" % server_name)
        #world.servers[server_name] = type("X", (object, ), {"__str__": lambda *args: server_addr, "as_arg": {"address": server_addr}})()
        if status == "broken":
            LOG.info("Killing %s" % str(world.servers[server_name]))
            world.servers[server_name].kill()
 
 
@step("i get health")
def i_get_health(step):
    time.sleep(30)
    world.health = world.api.health()
    LOG.info(pformat(world.health))
 
 
@step("i expect (\w+) servers are (UP|DOWN)")
def i_expect_servers_are(step, name, status):
 
    for key in filter(lambda key: key.startswith(name), world.servers):
        server = world.servers[key]
        server_name = world.api._server_name(str(server))
 
        # get servers health
        for health in world.health:
            if health["svname"] == server_name:
                server_health = health
                break
        else:
            assert False, "Haven't got healthcheck for %s" % server_name
 
        assert health["status"] == status, "%s != %s" % (health["status"], status)
 
 
@step("i expect down explanation")
def i_expect_fail_explanation(step):
    for health in filter(lambda health: health["status"] == "DOWN", world.health):
        health["check_status"]  # TODO: finish this test
 
 
 
