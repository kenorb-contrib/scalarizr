from __future__ import with_statement
'''
Created on Nov 25, 2011
 
@author: marat
'''
 
from pprint import pformat
 
from scalarizr import exceptions
from scalarizr.libs import validate
from scalarizr.services import haproxy
from scalarizr.linux import iptables
from scalarizr import rpc
from scalarizr.handlers import get_role_servers
 
import logging
LOG = logging.getLogger(__name__)
HEALTHCHECK_DEFAULTS = {
    'timeout': {'check': '3s'},
    'default-server': {'inter': '30s', 'fall': 2, 'rise': 10}
}
 
 
def rename(params):
    # translate param names from readable form to config param names
    server_param_names_map = {
        "check_interval": "inter",
        "fall_threshold": "fall",
        "rise_threshold": "rise",
        "server_maxconn": "maxconn",
        "down": "disabled",
    }
    return dict([
        (server_param_names_map.setdefault(key, key), val)
            for key, val in params.items()
    ])
 
 
_rule_protocol = validate.rule(choises=['tcp', 'http', 'TCP', 'HTTP'])
_rule_backend = validate.rule(re=r'^role:\d+$')
_rule_hc_target = validate.rule(re='^[tcp|http]+:\d+$')
 
 
class HAProxyAPI(object):
    """
    Placeholder
    """
 
    def __init__(self, path=None):
        self.path_cfg = path
        self.cfg = haproxy.HAProxyCfg(path)
        self.svc = haproxy.HAProxyInitScript(path)
 
    def _server_name(self, server):
        if isinstance(server, basestring):
            return server.replace('.', '-')
        elif isinstance (server, dict):
            return self._server_name(':'.join([server["host"], str(server["port"])]))
        else:
            raise TypeError("server must be a dict or a string")
 
 
    def make_proxy(self, port, backend_port=None, backends=None,
                check_timeout=None, maxconn=None, **default_server_params):
        """
        Add listener and backend sections to the haproxy conf and restart the
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
 
 
        roles = map(lambda x: {"farm_role_id": x} if isinstance(x, int) else dict(x), roles)
        servers = map(lambda x: {"host": x} if isinstance(x, str) else dict(x), servers)
 
        # create a single server list with proper params for each server
        # 1. extract servers from the roles and apply role params to them
        roles_servers = []
        for role in roles:
            role_id, role_params = role.pop("farm_role_id"), role
 
            role_servers = map(lambda ip: {"host": ip}, get_role_servers(role_id))
            LOG.debug("get_role_servers response: %s", pformat(role_servers))
 
            # for testing on a single machine purposes / get_servers retunrs "host:port"
            for server in role_servers:
                if ':' in server["host"]:
                    host, port_ = server["host"].split(':')
                    server["host"] = host
                    server["port"] = port_
            #/
 
            [server.update(role_params) for server in role_servers]
 
            roles_servers.extend(role_servers)
        # 2. get all servers together, enable healthchecks, ensure `port` and
        # convert some keys
        servers.extend(roles_servers)
        [server.setdefault("check", True) for server in servers]
        [server.setdefault("port", backend_port) for server in servers]
        servers = map(rename, servers)
 
        LOG.debug(" Backend servers:\n" + pformat(servers))
 
        # construct listener and backend sections for the conf
        listener_name = haproxy.naming('listen', "tcp", port)
        backend_name = haproxy.naming('backend', "tcp", port)
 
        listener = {
            'mode': "tcp",
            'balance': 'roundrobin',
            'bind': '*:%s' % port,
            'default_backend': backend_name,
        }
        if maxconn:
            listener["maxconn"] = maxconn
 
        backend = {
            "mode": "tcp",
            "server": {},
        }
        backend.update(HEALTHCHECK_DEFAULTS)
        if check_timeout:
            backend["timeout"]["check"] = check_timeout
        backend["default-server"].update(rename(default_server_params))
 
        for server in servers:
            backend['server'][self._server_name(server)] = server
 
        # update the cfg
        self.cfg['listen'][listener_name] = listener
        if not self.cfg.backend or not backend_name in self.cfg.backend:
            self.cfg['backend'][backend_name] = backend
 
        iptables.FIREWALL.ensure(
            [{"jump": "ACCEPT", "protocol": "tcp", "match": "tcp", "dport": port}]
        )
 
        self.cfg.save()
        if self.svc.status() == 0:
            self.svc.reload()
 
 
    def recreate_conf(self):
        LOG.debug("Recreating haproxy conf at %s", self.cfg.cnf_path)
        with open(self.cfg.cnf_path, 'w') as f:
            f.write("defaults\n")
        self.cfg.reload()
 
        self.cfg.defaults['timeout']['connect'] = '5000ms'
        self.cfg.defaults['timeout']['client'] = '5000ms'
        self.cfg.defaults['timeout']['server'] = '5000ms'
 
        self.cfg.save()
        self.cfg.reload()
 
 
    def reset_conf(self):
        self.cfg.reload()
        # TODO: remove all iptables rules as well?
 
        backends = map(lambda listener: listener["backend"], self.list_listeners())
 
        for backend in backends:
            for server in self.list_servers(backend=backend):
                self.remove_server(server=server, backend=backend)
 
 
    @rpc.command_method
    def add_server(self, server=None, backend=None):
        '''Add server with ipaddr in backend section'''
        self.cfg.reload()
 
        if backend:
            backend = backend.strip()
 
        LOG.debug('HAProxyAPI.add_server')
        LOG.debug('     %s' % haproxy.naming('backend', backend=backend))
 
 
        bnds = self.cfg.sections(haproxy.naming('backend', backend=backend))
        if not bnds:
            if backend:
                raise exceptions.NotFound('Backend not found: %s' % (backend, ))
            else:
                raise exceptions.Empty('No listeners to add server to')
 
        #with self.svc.trans(exit='running'):
            #with self.cfg.trans(exit='working'):
 
        server.setdefault("check", True)
 
        server = rename(server)
        for bnd in bnds:
            self.cfg.backends[bnd]['server'][self._server_name(server)] = server
 
        self.cfg.save()
        self.svc.reload()
 
 
    @rpc.command_method
    def remove_server(self, server, backend=None):
        '''Remove server from backend section with ipaddr'''
        if backend: 
            backend = backend.strip()
 
        srv_name = self._server_name(server)
        for bd in self.cfg.sections(haproxy.naming('backend', backend=backend)):
            if ':' in srv_name:
                if srv_name in self.cfg.backends[bd]['server']:
                    del self.cfg.backends[bd]['server'][srv_name]
            else:
                for srv_name_ in list(self.cfg.backends[bd]['server']):
                    if srv_name_.startswith(srv_name):
                        del self.cfg.backends[bd]['server'][srv_name_]
 
        self.cfg.save()
        if self.svc.status() == 0:
            self.svc.reload()
 
 
    def health(self):
        try:
            # if self.cfg.defaults['stats'][''] == 'enable' and \
            if self.cfg.globals['stats']['socket'] == '/var/run/haproxy-stats.sock':
                pass
        except:
            self.cfg.globals['stats']['socket'] = '/var/run/haproxy-stats.sock'
            self.cfg.globals['spread-checks'] = 5
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
 
 
# ---
 
 
    @rpc.command_method
    @validate.param('port', 'server_port', type=int)
    @validate.param('protocol', required=_rule_protocol)
    @validate.param('server_port', optional=True, type=int)
    @validate.param('backend', optional=_rule_backend)
    def create_listener(self, port=None, protocol=None, server_port=None,
                                    server_protocol=None, backend=None):
        ''' '''
        LOG.debug('create_listener: %s, %s, %s, %s, %s, %s', self, port, protocol, server_port, server_protocol, backend)
        if protocol:
            protocol = protocol.lower()
        ln = haproxy.naming('listen', protocol, port)
        bnd = haproxy.naming('backend', server_protocol or protocol, server_port or port, backend=backend)
        return ln, bnd
        listener = backend = None
        LOG.debug('HAProxyAPI.create_listener: listener = `%s`, backend = `%s`', ln, bnd)
 
        try:
            if self.cfg.listener[ln]:
                raise 'Duplicate'
        except Exception, e:
            if 'Duplicate' in e:
                raise exceptions.Duplicate('Listener %s:%s already exists' % (protocol, port))
        if protocol == 'tcp':
            listener = {'balance': 'roundrobin'}
        elif protocol == 'http':
            listener = {'option': {'forwardfor': True}}
        else:
            raise ValueError('Unexpected protocol: %s' % (protocol, ))
            #TODO: not correct for https or ssl...
 
        # listen config:
        listener.update({
                'bind': '*:%s' % port,
                'mode': protocol,
                'default_backend': bnd
        })
 
        backend_protocol = server_protocol or protocol
        if backend_protocol == 'tcp':
            backend = {}
        elif backend_protocol == 'http':
            backend = {'option': {'httpchk': True}}
        else:
            raise ValueError('Unexpected protocol: %s' % (backend_protocol, ))
            #TODO: not correct for https or ssl...
 
        # backend config:
        backend.update({'mode': backend_protocol})
        backend.update(HEALTHCHECK_DEFAULTS)
 
        # apply changes
        #with self.svc.trans(exit='running'):
        #       with self.cfg.trans(enter='reload', exit='working'):
        #TODO: change save() and reload(),`if True` condition to `with...` enter, exit
        if True:
            self.cfg['listen'][ln] = listener
            if not self.cfg.backend or not bnd in self.cfg.backend:
                self.cfg['backend'][bnd] = backend
            try:
                iptables.FIREWALL.ensure(
                    {"jump": "ACCEPT", "protocol": "tcp", "match": "tcp", "dport": port}
                )
            except Exception, e:
                raise exceptions.Duplicate(e)
 
            self.cfg.save()
            self.svc.reload()
 
            return listener
 
 
    @rpc.command_method
    @validate.param('unhealthy_threshold', 'healthy_threshold', type=int)
    @validate.param('target', optional=_rule_hc_target)
    @validate.param('interval', 'timeout', re=r'(^\d+[smhd]$)|^\d')
    def configure_healthcheck(self, target=None, interval=None, timeout=None,
                                                    unhealthy_threshold=None, healthy_threshold=None):
        ''' '''
        try:
            if interval == 'None': interval=None
            int(interval)
            interval = '%ss' % interval
        except:
            pass
        try:
            if timeout == 'None': timeout=None
            int(timeout)
            timeout = '%ss' % timeout
        except:
            pass
 
        bnds = haproxy.naming('backend', backend=target)
        if not self.cfg.sections(bnds):
            raise exceptions.NotFound('Backend `%s` not found' % bnds)
 
        for bnd in self.cfg.sections(bnds):
            if timeout:
                if isinstance(timeout, dict):
                    self.cfg['backend'][bnd]['timeout'] = timeout
                else:
                    self.cfg['backend'][bnd]['timeout'] = {'check': str(timeout)}
            default_server = {
                    'inter': interval,
                    'fall': unhealthy_threshold,
                    'rise': healthy_threshold
            }
            self.cfg['backend'][bnd]['default-server'] = default_server
            for srv in self.cfg['backend'][bnd]['server']:
                server = self.cfg['backend'][bnd]['server'][srv]
                server.update({'check' : True})
                self.cfg['backend'][bnd]['server'][srv] = server
        #with self.svc.trans(exit='running'):
            #       with self.cfg.trans(enter='reload', exit='working'):
        self.cfg.save()
        self.svc.reload()
 
 
 
 
 
    @rpc.query_method
    @validate.param('ipaddr', type='ipv4', optional=True)
    def get_servers_health(self, ipaddr=None):
        try:
            if self.cfg.defaults['stats'][''] == 'enable' and \
                            self.cfg.globals['stats']['socket'] == '/var/run/haproxy-stats.sock':
                pass
        except:
            self.cfg.globals['stats']['socket'] = '/var/run/haproxy-stats.sock'
            self.cfg.defaults['stats'][''] = 'enable'
            self.cfg.save()
            self.svc.reload()
 
        #TODO: select parameters what we need with filter by ipaddr
        stats = haproxy.StatSocket().show_stat()
        return stats
 
 
    @rpc.command_method
    @validate.param('port', type=int)
    @validate.param('protocol', required=_rule_protocol)
    def delete_listener(self, port=None, protocol=None):
        ''' Delete listen section(s) by port (and)or protocol '''
 
        ln = haproxy.naming('listen', protocol, port)
        if not self.cfg.sections(ln):
            raise exceptions.NotFound('Listen `%s` not found can`t remove it' % ln)
        try:
            default_backend = self.cfg.listener[ln]['default_backend']
        except:
            default_backend = None
 
        for path in self.cfg.sections(ln):
            del self.cfg['listen'][ln]
            LOG.debug('HAProxyAPI.delete_listener: removed listener `%s`' % ln)
 
        if default_backend:
            has_ref = False
            for ln in self.cfg.listener:
                try:
                    if self.cfg.listener[ln]['default_backend'] == default_backend:
                        has_ref = True
                        break
                except:
                    pass
            if not has_ref:
                #it not used in other section, so will be deleting
                del self.cfg.backends[default_backend]
 
        try:
            iptables.FIREWALL.remove({
                    "jump": "ACCEPT", "protocol": "tcp", "match": "tcp", "dport": port
            })
        except Exception, e:
            raise exceptions.NotFound(e)
 
        self.cfg.save()
        self.svc.reload()
 
 
    @rpc.command_method
    @validate.param('target', required=_rule_hc_target)
    def reset_healthcheck(self, target):
        '''Return to defaults for `tartget` backend sections'''
        target = target.strip()
        bnds = haproxy.naming('backend', backend=target)
        if not self.cfg.sections(bnds):
            raise exceptions.NotFound('Backend `%s` not found' % target)
        for bnd in self.cfg.sections(bnds):
            backend = self.cfg['backend'][bnd]
            backend.update(HEALTHCHECK_DEFAULTS)
            self.cfg['backend'][bnd] = backend
 
        #with self.svc.trans(exit='running'):
            #       with self.cfg.trans(enter='reload', exit='working'):
            #TODO: with...
        self.cfg.save()
        self.svc.reload()
 
 
    @rpc.query_method
    def list_listeners(self):
        '''
        @return: Listeners list
        @rtype: [{
                <port>,
                <protocol>,
                <server_port>,
                <server_protocol>,
                <backend>,
                <servers>: [<ipaddr>, ...]
        }, ...]'''
        self.cfg.reload()
        res = []
        for ln in self.cfg.sections(haproxy.naming('listen')):
            listener = self.cfg.listener[ln]
            bnd_name = listener['default_backend']
            bnd_role = ':'.join(bnd_name.split(':')[2:4]) #example`role:1234`
            bnd = self.cfg.backends[bnd_name]
 
            res.append({
                            'port': listener['bind'].replace('*:',''),
                            'protocol': listener['mode'],
                            'server_port': bnd_name.split(':')[-1],
                            'server_protocol': bnd['mode'],
                            'backend': bnd_role,
                    })
        return res
 
 
    @rpc.query_method
    def list_servers(self, backend=None):
        '''
        List all servers, or servers from particular backend
        @rtype: [<ipaddr>, ...]
        '''
        if backend:
            backend = backend.strip()
        list_section = self.cfg.sections(haproxy.naming('backend', backend=backend))
 
        res = []
        for bnd in list_section:
            for srv_name in self.cfg.backends[bnd]['server']:
                res.append(srv_name)
                #res.append(self.cfg.backends[bnd]['server'][srv_name]['host'])
        res = list(set(res))
        return res
 
