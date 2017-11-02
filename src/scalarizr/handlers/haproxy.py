from copy import deepcopy
from pprint import pformat
import hashlib
import logging
import os
import sys

from scalarizr import linux
from scalarizr.api import haproxy as haproxy_api
from scalarizr.bus import bus
from scalarizr.config import ScalarizrCnf
from scalarizr.config import ScalarizrState
from scalarizr.handlers import Handler, HandlerError
from scalarizr.messaging import Messages
from scalarizr.node import __node__
from scalarizr.services import haproxy as haproxy_svs
from scalarizr.util import PopenError


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
        self._proxies = None
        self.on_reload()
        bus.on(
            init=self.on_init, 
            reload=self.on_reload
        )

    def accept(self, message, queue, behaviour=None, platform=None, os=None, dist=None):
        accept_res = haproxy_svs.BEHAVIOUR in behaviour and message.name in (
            # Messages.BEFORE_HOST_UP,
            Messages.HOST_UP, 
            Messages.HOST_DOWN, 
            # Messages.BEFORE_HOST_TERMINATE,
        )
        return accept_res

    def on_init(self, *args, **kwds):
        bus.on(
            start=self.on_start,
            host_init_response=self.on_host_init_response
        )

    def on_reload(self, *args):
        LOG.debug("HAProxyHandler on_reload")
        self.cnf = bus.cnf
        self.svs = haproxy_svs.HAProxyInitScript()

    def _norm_haproxy_params(self, haproxy_params):
        healthcheck_names = {
            "healthcheck.fallthreshold": "fall_threshold",
            "healthcheck.interval": "check_interval",
            "healthcheck.risethreshold": "rise_threshold",
        }
        if not haproxy_params:
            haproxy_params = {}
        # convert haproxy params to more suitable form for the api
        if haproxy_params.get("proxies") is None:
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
        return haproxy_params

    def on_start(self):
        if __node__['state'] != 'running':
            return

        LOG.debug("HAProxyHandler on_start")
        queryenv = bus.queryenv_service
        role_params = queryenv.list_farm_role_params(__node__['farm_role_id'])
        haproxy_params = role_params["params"]["haproxy"]
        LOG.debug("Haproxy params from queryenv: %s", pformat(haproxy_params))
        # useful for on_hostup
        self.haproxy_params = self._norm_haproxy_params(haproxy_params)

        LOG.debug("Creating new haproxy conf")
        self.api.reconfigure(self.haproxy_params["proxies"],
            template=self.haproxy_params.get('template'),
            async=False)

    def on_host_init_response(self, msg):
        LOG.debug('on_host_init_response')
        if linux.os.debian_family:
            LOG.info('Updating file /etc/default/haproxy')
            with open('/etc/default/haproxy', 'w+') as fp:
                fp.write('ENABLED=1\n')

        LOG.debug("Creating new haproxy conf")

        self.haproxy_params = self._norm_haproxy_params(msg.body.get('haproxy', {}))
        self.api.reconfigure(self.haproxy_params["proxies"],
            template=self.haproxy_params.get('template'),
            async=False)

    def on_HostUp(self, msg):
        LOG.debug('HAProxyHandler on_HostUp')
        local_ip = msg.body.get('local_ip')
        if local_ip == __node__["private_ip"]:
            LOG.debug("My HostUp, doing nothing")
            return

        farm_role_id = msg.body.get('farm_role_id')

        LOG.debug('self.haproxy_params["proxies"]: %s', self.haproxy_params["proxies"])
        try:
            LOG.debug("adding server: %s to role: %s" % (local_ip, farm_role_id))
            self.api.add_server_to_role(local_ip, farm_role_id)
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
