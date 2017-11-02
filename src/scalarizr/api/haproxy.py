'''
Created on Nov 25, 2011

@author: marat
'''

from pprint import pformat
import logging
from collections import OrderedDict

from scalarizr import exceptions
from scalarizr.libs import validate
from scalarizr.services import haproxy
from scalarizr.linux import iptables
from scalarizr.linux import pkgmgr
from scalarizr import rpc
from scalarizr import linux
from scalarizr.handlers import get_role_servers
from scalarizr.util import Singleton
from scalarizr.util.initdv2 import Status
from scalarizr.util import PopenError
from scalarizr import exceptions
from scalarizr.api import BehaviorAPI
from scalarizr.api.nginx import open_port
from scalarizr.api.nginx import close_port
from scalarizr.api import operation


LOG = logging.getLogger(__name__)
HEALTHCHECK_DEFAULTS = {
    'timeout_check': '3s',
    'default-server': OrderedDict((('fall', 2), ('inter', '30s'), ('rise', 10)))
}


def normalize_params(params):
    # translate param names from readable form to config param names
    server_param_names_map = {
        "check_interval": "inter",
        "fall_threshold": "fall",
        "rise_threshold": "rise",
        "server_maxconn": "maxconn",
        "down": "disabled",
        "host": "address"
    }
    return dict([
        (server_param_names_map.get(key, key), val)
            for key, val in params.items()
    ])


_rule_protocol = validate.rule(choises=['tcp', 'http', 'TCP', 'HTTP'])
_rule_backend = validate.rule(re=r'^role:\d+$')
_rule_hc_target = validate.rule(re='^[tcp|http]+:\d+$')


class SectionNamingMgr(object):

    main_sep = ':'
    roles_sep = '_'
    name_format = '{prefix}{sep}{roles}{sep}{type}{sep}{port}'

    def __init__(self, name_format=None, main_sep=None, roles_sep=None):
        if name_format != None:
            self.name_format = name_format
        if main_sep != None:
            self.main_sep = main_sep
        if roles_sep != None:
            self.roles_sep = roles_sep
        self.name_format = self.name_format.replace('{sep}', self.main_sep)

    def make_name(self, port, type_, roles=None):
        if roles == None:
            roles = []
        role_namepart = self.roles_sep.join(map(str, roles))
        return self.name_format.format(prefix='scalr',
            port=port,
            type=type_,
            roles=role_namepart)

    def get_data(self, section_name):
        name_parts = section_name.split(self.main_sep)
        parts_len = len(name_parts)
        result = {'prefix': name_parts[0]}
        if parts_len > 1:
            result['roles'] = name_parts[1].split(self.roles_sep)
        if parts_len > 2:
            result['type'] = name_parts[2]
        if parts_len > 3:
            result['port'] = name_parts[3]
        return result

    def get_pattern(self, search_filter):
        """
        Produces search pattern string to find section by its name
        search_filter is dict of name parts as keys and their values as search keys.
        Available keys:
            - port
            - roles
        Example:
            {'port': '8080'} -> 'scalr:8080:.*'
        """
        pattern_dict = {'prefix': 'scalr', 'port': '.*', 'roles': '.*', 'type': '.*'}
        pattern_dict.update(search_filter)
        roles = pattern_dict['roles']
        if hasattr(roles, '__iter__'):
            pattern_dict['roles'] = self.roles_sep.join(map(str, roles))
        return self.name_format.format(**pattern_dict)

    def get_pattern_single_role(self, search_filter):
        """
        Like get_pattern() produces search pattern string, but with single role
        Available keys:
            - port
            - role
        Example:
            {'role': '123456'} -> 'scalr:.*:.*123456.*'
        """
        search_filter['roles'] = '.*%s.*' % search_filter.pop('role')
        return self.get_pattern(search_filter)


class HAProxyAPI(BehaviorAPI):
    """
    Basic API for configuring HAProxy settings and controlling service status.

    Namespace::

        haproxy
    """
    __metaclass__ = Singleton

    behavior = 'haproxy'

    def __init__(self, path=None):
        self.cfg = haproxy.HAProxyConfManager() #haproxy.HAProxyCfg(path)
        open(self.cfg.conf_path, 'w').close()  # clear conf file
        self.svc = haproxy.HAProxyInitScript(path)
        self.naming_mgr = SectionNamingMgr()
        self._op_api = operation.OperationAPI()
        self._proxies_table = {}

    def _server_name(self, server):
        if isinstance(server, basestring):
            return server.replace('.', '-')
        elif isinstance (server, dict):
            return self._server_name(':'.join([server["address"], str(server["port"])]))
        else:
            raise TypeError("server must be a dict or a string")

    @rpc.command_method
    def start_service(self):
        """
        Starts HAProxy service.

        Example::

            api.haproxy.start_service()
        """
        self.svc.start()

    @rpc.command_method
    def stop_service(self):
        """
        Stops HAProxy service.

        Example::

            api.haproxy.stop_service()
        """
        self.svc.stop()

    @rpc.command_method
    def reload_service(self):
        """
        Reloads HAProxy configuration.

        Example::

            api.haproxy.reload_service()
        """
        self.svc.reload()

    @rpc.command_method
    def restart_service(self):
        """
        Restarts HAProxy service.

        Example::

            api.haproxy.restart_service()
        """
        self.svc.restart()

    @rpc.command_method
    def get_service_status(self):
        """
        Checks Chef service status.

        RUNNING = 0
        DEAD_PID_FILE_EXISTS = 1
        DEAD_VAR_LOCK_EXISTS = 2
        NOT_RUNNING = 3
        UNKNOWN = 4

        :return: Status num.
        :rtype: int


        Example::

            >>> api.haproxy.get_service_status()
            0
        """
        return self.svc.status()

    def do_reconfigure(self, op, proxies, template=None):
        """
        template is a raw part of haproxy.conf which is inserted at the beginning
        of file
        """
        LOG.debug("Recreating haproxy conf at %s", self.cfg.conf_path)
        self._proxies_table = proxies
        self.cfg.remove('*')

        if template:
            template = '##### main template start #####\n' + \
                template + \
                '\n##### main template end #####'
            self.cfg.add_conf(template)
        else:
            defaults = {'timeout_connect': '5000ms',
                'timeout_client': '5000ms',
                'timeout_server': '5000ms'}
            self.cfg.add('defaults', defaults)
            
        for proxy in proxies:       
            LOG.debug("Calling make_proxy port=%s, backends=%s, %s", proxy["port"],
                    pformat(proxy["backends"]), pformat(proxy["healthcheck_params"]))
            self.make_proxy(port=proxy["port"],
                backends=proxy["backends"],
                template=proxy.get('template'),
                **proxy["healthcheck_params"])

        # start
        if self.svc.status() != 0:
            try:
                self.svc.start()
            except PopenError, e:
                if "no <listen> line. Nothing to do" in e.err or \
                    "No enabled listener found" in e.err:
                    LOG.debug("Not starting haproxy daemon: nothing to do")
                else:
                    raise
    
    @rpc.service_method
    def reconfigure(self, proxies, template=None, async=True):
        self._op_api.run('api.haproxy.reconfigure',
                         func=self.do_reconfigure,
                         func_kwds={'proxies': proxies, 'template': template},
                         async=async,
                         exclusive=True)

    def _ordered_server_params(self, server_dict):
        """
        Makes OrderedDict server configuration out of regular dict
        """
        server_dict.setdefault("check", True)
        server_dict = normalize_params(server_dict)

        ordered_server_params = OrderedDict((('name', None), ('address', None), ('port', None)))
        ordered_server_params.update(server_dict)
        return ordered_server_params

    @rpc.command_method
    def make_proxy(self, port, backend_port=None, backends=None, template=None,
                check_timeout=None, maxconn=None, **default_server_params):
        """
        Add listen and backend sections to the haproxy conf and restart the
        service.

        :param port: listener port
        :type port: int
        :param backend_port: port for backend server to listen on
        :type backend_port: int
        :param backends: list of dicts, each dict represents role or server
        :type backends: list
        # :param roles: role ids (ints) or dicts with "id" key
        # :type roles: list
        # :param servers: server ips
        # :type servers: list
        :param check_timeout: ``timeout check`` - additional read timeout,
                              e.g. "3s"
        :type check_timeout: str
        :param maxconn: set ``maxconn`` of the frontend
        :type maxconn: str
        :param **default_server_params: following kwargs will be applied to
                                        the ``default-server`` key of the
                                        backend
        :param check_interval: value for ``inter``, e.g. "3s"
        :type check_interval: str
        :param fall_threshold: value for ``fall``
        :type fall_threshold: int
        :param rise_threshold: value for ``rise``
        :type rise_threshold: int
        :param server_maxconn: value for ``maxconn``, not to confuse with
                               the frontend's ``maxconn``
        :type server_maxconn: str
        :param down: value for ``disabled``
        :type down: bool
        :param backup: value for ``backup``
        :type backup: bool

        :returns: ?

        .. note:: official documentation on the global parameters and server \
        options can be found at \
        http://cbonte.github.com/haproxy-dconv/configuration-1.4.html

        """

        # args preprocessing: default values and short forms
        if not backend_port:
            backend_port = port

        # new: backends instead of separate roles/hosts args
        if not backends:
            backends = []
        roles = filter(lambda spec: "farm_role_id" in spec, backends)
        servers = filter(lambda spec: "host" in spec, backends)

        roles = map(lambda x: dict(farm_role_id=x) if isinstance(x, int) else dict(x), roles)
        servers = map(lambda x: dict(host=x) if isinstance(x, str) else dict(x), servers)

        role_ids = [role['farm_role_id'] for role in roles]

        for server in servers:
            server['name'] = None
            server['address'] = server.pop('host')

        # create a single server list with proper params for each server
        # 1. extract servers from the roles and apply role params to them
        roles_servers = []
        for role in roles:
            role_id, role_params = role.pop("farm_role_id"), role
            role.pop('farm_role_alias', None)

            role_servers = map(lambda ip: {"address": ip},
                get_role_servers(role_id, network=role_params.pop('network', None)))
            LOG.debug("get_role_servers response: %s", pformat(role_servers))

            # for testing on a single machine purposes / get_servers returns "address:port"
            for server in role_servers:
                if ':' in server["address"]:
                    address, port_ = server["address"].split(':')
                    server["address"] = address
                    server["port"] = port_
            #/
            for server in role_servers:
                server.update(role_params)

            roles_servers.extend(role_servers)
        # 2. get all servers together, enable healthchecks, ensure `port` and
        # convert some keys
        servers.extend(roles_servers)
        for server in servers:
            server.setdefault('check', True)
            server.setdefault('port', backend_port)
        servers = map(normalize_params, servers)

        LOG.debug(" Backend servers:\n" + pformat(servers))

        # construct listener and backend sections for the conf
        # listener_name = haproxy.naming('listen', "tcp", port)
        backend_name = self.naming_mgr.make_name(port, 'backend', role_ids)
        bind_addr = OrderedDict((('address', '*'), ('port', str(port))))
        listener = OrderedDict((
            ('name', self.naming_mgr.make_name(port, 'listen', role_ids)),
            ('mode', 'tcp'),
            ('balance', {'algorithm': 'roundrobin'}),
            ('bind', {'bind_addr': bind_addr}),
            ('default_backend', backend_name),
        ))
        if maxconn:
            listener['maxconn'] = maxconn

        backend = OrderedDict((
            ('name', backend_name),
            ('mode', 'tcp'),
            ('server', {}),
        ))

        backend.update(HEALTHCHECK_DEFAULTS)
        if check_timeout:
            backend["timeout_check"] = check_timeout
        backend["default-server"].update(normalize_params(default_server_params))

        _servers = []
        for server in servers:
            server['name'] = self._server_name(server)
            _servers.append(self._ordered_server_params(server))

        backend['server'] = _servers

        # update the cfg
        listener_xpath = self.cfg.find_one_xpath('listen', 'name', listener['name'])
        if listener_xpath is None:
            self.cfg.add('listen', listener)
        else:
            self.cfg.set(listener_xpath, listener)

        backend_xpath = self.cfg.find_one_xpath('backend', 'name', backend['name'])
        if backend_xpath is None:
            self.cfg.add('backend', backend)

        self.cfg.save()

        if template:
            if template.endswith('\n'):
                template = template[:-1]
            template = '##### listen template start #####\n' + \
                template + \
                '\n##### listen template end #####'
            self.cfg.extend_section(template, listener['name'])

        if iptables.enabled():
            open_port(port)

        if self.svc.status() == Status.RUNNING:
            self.svc.reload()

    def remove_proxy(self, port):
        """
        Removes listen and backend sections from haproxy.cfg and restarts service
        """
        backend_name = self.naming_mgr.get_pattern({'port': port, 'type': 'backend'})
        listener_name = self.naming_mgr.get_pattern({'port': port, 'type': 'backend'})

        listener_xpath = self.cfg.find_one_xpath('listen', 'name', listener_name)
        backend_xpath = self.cfg.find_one_xpath('backend', 'name', backend_name)

        self.cfg.remove(listener_xpath)
        self.cfg.remove(backend_xpath)

        self.cfg.save()
        self.svc.reload()

        if iptables.enabled():
            close_port(port)
        
    @rpc.command_method
    def add_server(self, server=None, backend=None):
        """
        Adds server with ipaddr to backend section.

        :param server: Server configuration.
        :type server: dict

        :param backend: Backend name.
        :type backend: str

        Example::

            TBD.
        """
        self.cfg.load()

        if backend:
            backend = backend.strip()

        LOG.debug('HAProxyAPI.add_server')

        backend_xpaths = self.cfg.get_all_xpaths('backend')
        if backend_xpaths is None:
            if backend:
                raise exceptions.NotFound('Backend not found: %s' % (backend, ))
            else:
                raise exceptions.Empty('No listeners to add server to')

        server.setdefault("check", True)
        server['name'] = self._server_name(server)
        server = normalize_params(server)
        server = self._ordered_server_params(server)

        for backend_xpath in backend_xpaths:
            self.cfg.add(backend_xpath+'/server', server)

        self.cfg.save()
        self.svc.reload()

    @rpc.command_method
    def add_server_to_role(self, server, role_id):
        """
        Adds server to each backend which contains role_id
        """

        backend_server_pairs = []
        for proxy in self._proxies_table:
            for backend in proxy["backends"]:
                if backend["farm_role_id"] == role_id:
                    if not isinstance(server, dict):
                        server = {
                            "address": server,
                            "port": backend.get("port", proxy["port"]),
                            "backup": backend.get("backup", False),
                            "down": backend.get("down", False),
                            "check": backend.get("check", True)}
                    server['name'] = self._server_name(server)
                    server = self._ordered_server_params(server)
                    backend_pattern = self.naming_mgr.get_pattern({'port': proxy["port"],
                        'type': 'backend'})
                    backend_xpath = self.cfg.find_one_xpath('backend', 'name', backend_pattern)
                    backend_server_pairs.append((backend_xpath, server))

        if not backend_server_pairs:
            return

        LOG.debug("Adding servers to backends: %s", backend_server_pairs)

        for backend_xpath, server in backend_server_pairs:
            self.cfg.add(backend_xpath+'/server', server)

        self.cfg.save()
        self.svc.reload()

    @rpc.command_method
    def remove_server(self, server, backend=None):
        """
        Removes server with ipaddr from backend section.

        :param server: Server configuration.
        :type server: dict

        :param backend: Backend name.
        :type backend: str

        Example::

            TBD.
        """

        if backend: 
            backend = backend.strip()
        if isinstance(server, dict) and 'host' in server:
            server['address'] = server.pop('host')
        srv_name = self._server_name(server)

        backend_xpaths = self.cfg.get_all_xpaths('backend')
        for backend_xpath in backend_xpaths:
            found_servers = self.cfg.find_all_xpaths(backend_xpath+'/server',
                'name',
                srv_name.replace('*', '\*')+'.*')
            for server_xpath in found_servers:
                self.cfg.remove(server_xpath)
                self.cfg.save() # DBG:

        self.cfg.save()
        if self.svc.status() == Status.RUNNING:
            self.svc.reload()

    def health(self):
        if self.cfg.get('globals/stats_socket') != '/var/run/haproxy-stats.sock':
            self.cfg.set('globals/stats_socket', '/var/run/haproxy-stats.sock')
            self.cfg.set('globals/spread-checks', '5')
            self.cfg.save()
            self.svc.reload()

        stats = haproxy.StatSocket().show_stat()

        # filter the stats
        relevant_keys = [
            "pxname",
            "svname",
            "status",
            "act",
            "bck",
            "chkfail",
            "chkdown",
            "downtime",
            "check_status",
            "check_duration",
        ]
        
        stats = filter(lambda health: health["svname"] not in ("FRONTEND", "BACKEND"), stats)
        for health in stats:
            for key in health.keys():
                if key not in relevant_keys:
                    del health[key]

        # TODO: return data in different format
        return stats

    @rpc.query_method
    @validate.param('ipaddr', type='ipv4', optional=True)
    def get_servers_health(self, ipaddr=None):
        """
        APIDOC TBD.
        """

        if self.cfg.get('globals/stats_socket') != '/var/run/haproxy-stats.sock' \
            or not self.cfg.get('defaults/stats_enable'):
            self.cfg.set('globals/stats_socket', '/var/run/haproxy-stats.sock')
            self.cfg.set('defaults/stats_enable')
            self.cfg.save()
            self.svc.reload()

        #TODO: select parameters what we need with filter by ipaddr
        stats = haproxy.StatSocket().show_stat()
        return stats

    @rpc.command_method
    @validate.param('target', required=_rule_hc_target)
    def reset_healthcheck(self, target):  # TODO: figure out what target is
        """
        Return to defaults for `target` backend sections
        """
        target = target.strip()
        # backend_name = self.naming_mgr.get_pattern({}) haproxy.naming('backend', backend=target) + '*'
        backend_xpaths = self.cfg.find_all_xpaths('backend', '/name', target)
        if backend_xpaths == None:
            raise exceptions.NotFound('Backend `%s` not found' % target)
        for backend_xpath in backend_xpaths:
            self.cfg.set(backend_xpath, HEALTHCHECK_DEFAULTS)

        self.cfg.save()
        self.svc.reload()

    @rpc.query_method
    def list_listeners(self):
        """
        :returns: Listeners list
        :rtype: list

        Method returns object of the following structure::

            [{
                    <port>,
                    <protocol>,
                    <server_port>,
                    <server_protocol>,
                    <backend>,
                    <servers>: [<ipaddr>, ...]
            }, ...]

        """
        self.cfg.load()
        res = []
        for listener_xpath in self.cfg.get_all_xpaths('listen'):
            bnd_name = self.cfg.get(listener_xpath+'/default_backend')
            bnd_data = self.naming_mgr.get_data(bnd_name)
            bnd_role = bnd_data['roles']  # TODO: parse roles?
            backend_xpath = self.cfg.find_one_xpath('backend', 'name', bnd_name)

            # TODO: make method to return str representation of bind
            port = self.cfg.get(listener_xpath+'/bind/bind_addr/port')
            listener_mode = self.cfg.get(listener_xpath+'/mode')
            backend_mode = self.cfg.get(backend_xpath+'/mode')
            res.append({
                'port': port,
                'protocol': listener_mode,
                'server_port': bnd_name.split(':')[-1],
                'server_protocol': backend_mode,
                'backend': bnd_role})
        return res


    @rpc.query_method
    def list_servers(self, backend=None):
        """
        Lists all servers or servers from particular backend.

        :returns: List of IP addresses.
        :rtype: [<ipaddr>, ...]
        """
        if backend:
            backend = backend.strip()

        backend_xpaths = self.cfg.find_all_xpaths('backend', 'name', backend)

        res = []
        for backend_xpath in backend_xpaths:
            server_xpaths = self.cfg.find_all_xpaths(backend_xpath+'/server',
                'name',
                backend_name)
            for server_xpath in server_xpaths:
                res.append(self.cfg.get(server_xpath+'/name'))

        res = list(set(res))
        return res

    @classmethod
    def do_check_software(cls, system_packages=None):
        return pkgmgr.check_software(['haproxy'], system_packages)[0]

