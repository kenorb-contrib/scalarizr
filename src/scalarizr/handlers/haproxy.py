from __future__ import with_statement
 
from scalarizr.bus import bus
from scalarizr.handlers import Handler, HandlerError
from scalarizr.api import haproxy as haproxy_api
from scalarizr.services import haproxy as haproxy_svs
from scalarizr.config import ScalarizrState
from scalarizr.messaging import Messages
from scalarizr.config import ScalarizrCnf
from scalarizr.queryenv import QueryEnvService
from scalarizr.node import __node__
from scalarizr.util import PopenError
 
import os
import sys
import logging
from pprint import pformat
from copy import deepcopy
import hashlib
 
 
def get_handlers():
    return [HAProxyHandler()]
 
LOG = logging.getLogger(__name__)
 
 
def _result_message(name):
    def result_wrapper(fn):
        LOG.debug('result_wrapper')
        def fn_wrapper(self, *args, **kwds):
            LOG.debug('fn_wrapper name = `%s`', name)
            result = self.new_message(name, msg_body={'status': 'ok'})
            try:
                fn_return = fn(self, *args, **kwds)
                result.body.update(fn_return or {})
            except:
                result.body.update({'status': 'error', 'last_error': str(sys.exc_info)})
            self.send_message(result)
        return fn_wrapper
    return result_wrapper
 
 
class HAProxyHandler(Handler):
 
    def __init__(self):
        LOG.debug("HAProxyHandler __init__")
        self.api = haproxy_api.HAProxyAPI()
        self.on_reload()
        bus.on(init=self.on_init, reload=self.on_reload)
 
    def _remove_add_servers_from_queryenv(self):
        cnf = ScalarizrCnf(bus.etc_path)
        cnf.bootstrap()
        globals()['ini'] = cnf.rawini
        key_path = os.path.join(bus.etc_path, ini.get('general', 'crypto_key_path'))
        server_id = ini.get('general', 'server_id')
        url = ini.get('general','queryenv_url')
        queryenv = QueryEnvService(url, server_id, key_path)
        result = queryenv.list_roles()
        running_servers = []
 
        bnds = []
        for elem in self.api.list_listeners():
            bnds.append(elem['backend'])
        bnds = list(set(bnds))
 
        for bnd in bnds:
            for srv in self.api.list_servers(backend=bnd):
                self.api.remove_server(ipaddr=srv, backend=bnd)
 
        for d in result:
            behaviour=', '.join(d.behaviour)
            for host in d.hosts:
                try:
                    if 'role:%s' % d.farm_role_id in bnds:
                        self.api.add_server(ipaddr=host.internal_ip,
                                backend='role:%s' % d.farm_role_id)
                except:
                    LOG.warn('HAProxyHandler.on_before_host_up.Failed add_server `%s` in'
                                    ' backend=`role:%s`, details: %s' %     (
                                    host.internal_ip.replace('.', '-'),
                                    d.farm_role_id, sys.exc_info()[1]))
                running_servers.append([d.farm_role_id, host.internal_ip])
        LOG.debug('running_servers: `%s`', running_servers)
 
 
    def accept(self, message, queue, behaviour=None, platform=None, os=None, dist=None):
        accept_res = haproxy_svs.BEHAVIOUR in behaviour and message.name in (
            Messages.HOST_INIT_RESPONSE,
            # Messages.BEFORE_HOST_UP,
            Messages.HOST_UP, 
            Messages.HOST_DOWN, 
            # Messages.BEFORE_HOST_TERMINATE,
            # 'HAProxy_AddServer',
            # 'HAProxy_ConfigureHealthcheck',
            # 'HAProxy_GetServersHealth',
            # 'HAProxy_ListListeners',
            # 'HAProxy_ListServers',
            # 'HAProxy_RemoveServer',
            # 'HAProxy_ResetHealthcheck'
        )
        return accept_res
 
    def on_init(self, *args, **kwds):
        bus.on(
            start=self.on_start,
            host_init_response=self.on_host_init_response,
            # before_host_up=self.on_before_host_up,
        )
 
    def on_reload(self, *args):
        LOG.debug("HAProxyHandler on_reload")
        self.cnf = bus.cnf
        self.svs = haproxy_svs.HAProxyInitScript()
 
    def on_start(self):
        healthcheck_names = {
            "healthcheck.fallthreshold": "fall_threshold",
            "healthcheck.interval": "check_interval",
            "healthcheck.risethreshold": "rise_threshold",
        }
 
        LOG.debug("HAProxyHandler on_start")
        queryenv = bus.queryenv_service
        role_params = queryenv.list_farm_role_params(__node__['farm_role_id'])
        haproxy_params = role_params["params"]["haproxy"]
        LOG.debug("Haproxy params from queryenv: %s", pformat(haproxy_params))
 
        # convert haproxy params to more suitable form for the api
        if haproxy_params["proxies"] is None:
            haproxy_params["proxies"] = []
        for proxy in haproxy_params["proxies"]:
            for backend in proxy["backends"]:
                for name in ("backup", "down"):
                    if name in backend:
                        backend[name] = bool(int(backend[name]))
            proxy["healthcheck_params"] = {}
            for name in healthcheck_names:
                if name in proxy:
                    proxy["healthcheck_params"][healthcheck_names[name]] = proxy[name]
 
        # useful for on_hostup
        self.haproxy_params = haproxy_params
 
        # if we have a sample conf, recreate
        with open(self.api.cfg.cnf_path) as f:
            conf_md5 = hashlib.md5(f.read()).hexdigest()
        LOG.debug("%s md5 sum: %s", self.api.cfg.cnf_path, conf_md5)
        if conf_md5 == "c3bfb0c86138552475dea458e8ab36f3":  # TODO: remove actual sum
            LOG.debug("Creating new haproxy conf")
            self.api.recreate_conf()
 
        self.api.reset_conf()
 
        # add the proxies
        for proxy in haproxy_params["proxies"]:       
            LOG.debug("make_proxy args: port=%s, backends=%s, %s", proxy["port"],
                    pformat(proxy["backends"]), pformat(proxy["healthcheck_params"]))
            self.api.make_proxy(port=proxy["port"],
                                backends=proxy["backends"],
                                **proxy["healthcheck_params"])
 
        # start
        if self.svs.status() != 0:
            try:
                self.svs.start()
            except PopenError, e:
                if "no <listen> line. Nothing to do" in e.err:
                    LOG.debug("Not starting haproxy daemon: nothing to do")
                else:
                    raise
 
    """
    def on_start(self):
        LOG.debug("HAProxyHandler on_start")
        if bus.cnf.state == ScalarizrState.INITIALIZING:
            # todo: Repair data from HIR
            pass
        if bus.cnf.state == ScalarizrState.RUNNING:
            #remove all servers from backends and add its from queryenv
            self._remove_add_servers_from_queryenv()
    """
 
    def on_host_init_response(self, msg):
        LOG.debug('HAProxyHandler.on_host_init_response')
        return
 
        """
        if not 'haproxy' in msg.body:
            raise HandlerError('HostInitResponse message for HAProxy behaviour must \
                            have `haproxy` property')
        data = msg.haproxy.copy()
        self._data = data
        LOG.debug("data for add proxy %s", pformat(data))
 
        self._listeners = data.get('listeners', [])
        self._healthchecks = data.get('healthchecks', [])
        LOG.debug('listeners = `%s`', self._listeners)
        LOG.debug('healthchecks = `%s`', self._healthchecks)
        """
 
    def on_HostInitResponse(self, msg):
        LOG.debug("HAProxyHandler on_HostInitResponse")
        # self._data = deepcopy(msg.haproxy)
        # LOG.debug("data for add proxy %s", pformat(self._data))
 
 
        """
    def on_before_host_up(self, msg):
        try:
            if self.svs.status() != 0:
                self.svs.start()
        except:
            LOG.warn('Can`t start `haproxy`. Details: `%s`', sys.exc_info()[1],
                            exc_info=sys.exc_info())
 
        data = {'listeners': [], 'healthchecks': []}
 
        if isinstance(self._listeners, list):
            for ln in self._listeners:
                try:
                    ln0 = self.api.create_listener(**ln)
                    data['listeners'].append(ln0)
                except Exception, e:
                    LOG.error('HAProxyHandler.on_before_host_up. Failed to add listener'\
                            ' `%s`. Details: %s', str(ln), e, exc_info=sys.exc_info())
                    #raise Exception, sys.exc_info()[1], sys.exc_info()[2]
 
        if isinstance(self._healthchecks, list):
            for hl in self._healthchecks:
                try:
                    hl0 = self.api.configure_healthcheck(**hl)
                    data['healthchecks'].append(hl0)
                except Exception, e:
                    LOG.error('HAProxyHandler.on_before_host_up. Failed to configure'\
                            ' healthcheck `%s`. Details: %s', str(hl), e, exc_info=sys.exc_info())
                    #raise Exception, sys.exc_info()[1], sys.exc_info()[2]
        msg.haproxy = data
 
        self._remove_add_servers_from_queryenv()
        """
 
 
    def on_HostUp(self, msg):
        LOG.debug('HAProxyHandler on_HostUp')
        local_ip = msg.body.get('local_ip')
        if local_ip == __node__["private_ip"]:
            LOG.debug("My HostUp, doing nothing")
            return
 
        farm_role_id = msg.body.get('farm_role_id')
 
        calls = []
        for proxy in self.haproxy_params["proxies"]:
            for backend in proxy["backends"]:
                if backend["farm_role_id"] == farm_role_id:
                    kwargs = {}
                    calls.append(kwargs)
                    kwargs["backend"] = "tcp:%s" % proxy["port"]
                    kwargs["server"] = {
                        "host": local_ip,
                        "port": backend.get("port", proxy["port"]),
                    }
                    for name in ("backup", "down"):
                        if name in backend:
                            kwargs["server"][name] = backend[name]
                    break
 
        for kwargs in calls:
            try:
                LOG.debug("adding server: %s", pformat(kwargs))
                self.api.add_server(**kwargs)
            except:
                LOG.error('HAProxyHandler.on_HostUp. Failed to add_server `%s`, details:'
                        ' %s' % (local_ip, sys.exc_info()[1]), exc_info=sys.exc_info())
 
 
    def on_HostDown(self, msg):
        LOG.debug('HAProxyHandler on_HostDown')
        local_ip = msg.body.get('local_ip')
        try:
            LOG.debug("removing server: %s", local_ip)
            self.api.remove_server(local_ip)
        except:
            LOG.error('HAProxyHandler.on_HostDown. Failed to remove server `%s`, '
                    'details: %s' % (local_ip, sys.exc_info()[1]), exc_info=sys.exc_info())
 
    on_BeforeHostTerminate = on_HostDown
 
    @_result_message('HAProxy_AddServerResult')
    def on_HAProxy_AddServer(self, msg):
        return self.api.add_server(**msg.body)
 
 
    @_result_message('HAProxy_RemoveServerResult')
    def on_HAProxy_RemoveServer(self, msg):
        return self.api.remove_server(**msg.body)
 
 
    @_result_message('HAProxy_ConfigureHealthcheckResult')
    def on_HAProxy_ConfigureHealthcheck(self, msg):
        return self.api.configure_healthcheck(**msg.body)
 
 
    @_result_message('HAProxy_GetServersHealth')
    def on_HAProxy_GetServersHealth(self, msg):
        return {'health': self.api.get_servers_health()}
 
 
    @_result_message('HAProxy_ResetHealthcheckResult')
    def on_HAProxy_ResetHealthcheck(self, msg):
        return self.api.reset_healthcheck(msg.target)
 
 
    @_result_message('HAProxy_ListListenersResult')
    def on_HAProxy_ListListeners(self, msg):
        return {'listeners': self.api.list_listeners()}
 
 
    @_result_message('HAProxy_ListServersResult')
    def on_HAProxy_ListServers(self, msg):
        return {'servers': self.api.list_servers(msg.backend)}
 
