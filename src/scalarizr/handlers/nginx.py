'''
Created on Jan 6, 2010

@author: marat
@author: Dmytro Korsakov
@author: spike
@author: uty
'''

from __future__ import with_statement

# Core components
from scalarizr.bus import bus
from scalarizr.config import Configurator, BuiltinBehaviours, ScalarizrState
from scalarizr.service import CnfController
from scalarizr.handlers import HandlerError, ServiceCtlHandler
from scalarizr.messaging import Messages
from scalarizr.api import service as preset_service
from scalarizr.node import __node__
from scalarizr.api import nginx as nginx_api
from scalarizr.api.nginx import NginxAPI
from scalarizr.api.nginx import NginxInitScript
from scalarizr.api.nginx import update_ssl_certificate
from scalarizr.api.nginx import get_all_app_roles
import StringIO

# Libs
from scalarizr.libs.metaconf import Configuration, NoPathError
from scalarizr.util import cached, firstmatched, validators, software, initdv2
from scalarizr.linux import iptables
from scalarizr.services import BaseConfig, PresetProvider

# Stdlibs
import os, logging, shutil
import time
from datetime import datetime
import ConfigParser
import cStringIO


BEHAVIOUR = SERVICE_NAME = BuiltinBehaviours.WWW
CNF_NAME = BEHAVIOUR
CNF_SECTION = BEHAVIOUR

__nginx__ = nginx_api.__nginx__


initdv2.explore('nginx', NginxInitScript)


def get_handlers():
    return [NginxHandler()]


class NginxCnfController(CnfController):
    def __init__(self):
        nginx_conf_path = __nginx__['nginx.conf']
        CnfController.__init__(self, BEHAVIOUR, nginx_conf_path, 'nginx', {"on":'1',"'off'":'0','off':'0'})

    @property
    def _software_version(self):
        return software.software_info('nginx').version


class NginxHandler(ServiceCtlHandler):

    def __init__(self):
        self._cnf = bus.cnf
        self._nginx_v2_flag_filepath = os.path.join(bus.etc_path, "private.d/nginx_v2")
        ServiceCtlHandler.__init__(self, BEHAVIOUR, initdv2.lookup('nginx'), NginxCnfController())

        self._logger = logging.getLogger(__name__)
        self.preset_provider = NginxPresetProvider()
        self.api = NginxAPI()
        self.api.init_service()
        self._terminating_servers = []

        bus.define_events("nginx_upstream_reload")
        bus.on(init=self.on_init, reload=self.on_reload)
        self.on_reload()

    def on_init(self):
        bus.on(start=self.on_start,
               before_host_up=self.on_before_host_up,
               host_init_response=self.on_host_init_response)

        self._insert_iptables_rules()

        if __node__['state'] == ScalarizrState.BOOTSTRAPPING:
            self._stop_service('Configuring')

    def on_reload(self):
        self._queryenv = bus.queryenv_service

        self._nginx_binary = __nginx__['binary_path']
        self._app_inc_path = __nginx__['app_include_path']
        self._app_port = __nginx__['app_port']
        try:
            self._upstream_app_role = __nginx__['upstream_app_role']
        except ConfigParser.Error:
            self._upstream_app_role = None

    def on_host_init_response(self, message):
        self._logger.debug('Handling on_host_init_response message')
        if hasattr(message, BEHAVIOUR):
            data = getattr(message, BEHAVIOUR)
            if not data and hasattr(message, 'nginx'):
                data = getattr(message, 'nginx')

            self._logger.debug('message data: %s' % data)
            if data and 'preset' in data:
                self.initial_preset = data['preset'].copy()
            if data and data.get('proxies'):
                self._set_nginx_v2_mode_flag(True)
                self._proxies = list(data.get('proxies', []))
            else:
                self._proxies = None
            self._logger.debug('proxies: %s' % self._proxies)


    def accept(self, message, queue, behaviour=None, platform=None, os=None, dist=None):
        return (BEHAVIOUR in behaviour or 'nginx' in behaviour) and \
            message.name in (Messages.HOST_UP,
                             Messages.HOST_DOWN,
                             Messages.BEFORE_HOST_TERMINATE,
                             Messages.VHOST_RECONFIGURE,
                             Messages.UPDATE_SERVICE_CONFIGURATION,
                             Messages.SSL_CERTIFICATE_UPDATE)

    def _set_nginx_v2_mode_flag(self, on):
        if on and not self._get_nginx_v2_mode_flag():
            open(self._nginx_v2_flag_filepath, 'w').close()
        elif not on and self._get_nginx_v2_mode_flag():
            os.remove(self._nginx_v2_flag_filepath)

    def _get_nginx_v2_mode_flag(self):
        return os.path.exists(self._nginx_v2_flag_filepath)

    def on_start(self):
        self._logger.debug('Handling on_start message')
        if __node__['state'] == 'running':
            role_params = self._queryenv.list_farm_role_params(__node__['farm_role_id'])['params']
            nginx_params = role_params.get(BEHAVIOUR)
            v2_mode = (nginx_params and nginx_params.get('proxies')) \
                or self._get_nginx_v2_mode_flag()

            self._logger.debug('Updating main config')
            self.api._update_main_config(remove_server_section=v2_mode, reload_service=False)

            if v2_mode:
                self._set_nginx_v2_mode_flag(True)
                proxies = nginx_params.get('proxies', []) if nginx_params else []
                self._logger.debug('Recreating proxying with proxies:\n%s' % proxies)
                self.api.recreate_proxying(proxies)
            else:
                self.api._recreate_compat_mode()

    def on_before_host_up(self, message):
        self._logger.debug('Handling on_before_host_up message')
        log = bus.init_op.logger
        self._init_script.stop()

        log.info('Copy default html error pages')
        self._copy_error_pages()

        log.info('Setup proxying')
        self._logger.debug('Updating main config')
        v2_mode = bool(self._proxies) or self._get_nginx_v2_mode_flag()
        self.api._update_main_config(remove_server_section=v2_mode,
                                 reload_service=False)

        if v2_mode:
            self._logger.debug('Recreating proxying with proxies:\n%s' % self._proxies)
            self.api.recreate_proxying(self._proxies)
        else:
            # default behaviour
            roles_for_proxy = []
            if __nginx__['upstream_app_role']:
                roles_for_proxy = [__nginx__['upstream_app_role']]
            else:
                roles_for_proxy = get_all_app_roles()
            self.api.make_default_proxy(roles_for_proxy)

        bus.fire('service_configured',
                 service_name=SERVICE_NAME,
                 preset=self.initial_preset)

    def on_HostUp(self, message):
        server = ''
        role_id = message.farm_role_id
        role_name = message.role_name
        behaviours = message.behaviour
        if message.cloud_location == __node__['cloud_location']:
            server = message.local_ip
        else:
            server = message.remote_ip

        self._logger.debug('on host up backend table is %s' % self.api.backend_table)
        # Assuming backend `backend` can be only in default behaviour mode
        if not self._get_nginx_v2_mode_flag():
            self.api._recreate_compat_mode()
            # upstream_role = __nginx__['upstream_app_role']
            # self._logger.debug('upstream app role is %s and server is up in role %s',
            #     __nginx__['upstream_app_role'],
            #     role_name)
            # if (upstream_role and upstream_role == role_name) or \
            #     (not upstream_role and BuiltinBehaviours.APP in behaviours):

            #     for default_backend in ['backend', 'backend.ssl']:
            #         if default_backend not in self.api.backend_table:
            #             continue

            #         server_list = []
            #         for dest in self.api.backend_table[default_backend]:
            #             server_list.extend(dest['servers'])
            #         if server in server_list:
            #             continue

            #         self.api.remove_server(default_backend,
            #                                '127.0.0.1',
            #                                reload_service=False,
            #                                update_backend_table=True)
            #         self._logger.info('adding new app server %s to default backend', server)
            #         self.api.add_server(default_backend, server,
            #                              update_backend_table=True)

        else:
            self._logger.info('adding new app server %s to role %s backend(s)', server, role_id)
            # self.api.add_server_to_role(server, role_id)
            role_params = self._queryenv.list_farm_role_params(__node__['farm_role_id'])['params']
            nginx_params = role_params.get(BEHAVIOUR)
            proxies = nginx_params.get('proxies', []) if nginx_params else []
            self._logger.debug('Recreating proxying with proxies:\n%s' % proxies)
            self.api.recreate_proxying(proxies)

        self._logger.info('After %s host up backend table is %s' % (server, self.api.backend_table))


    def _remove_shut_down_server(self,
                                 server,
                                 role_id,
                                 role_name,
                                 behaviours,
                                 cache_remove=False):
        if server in self._terminating_servers:
            self._terminating_servers.remove(server)
            return

        self._logger.debug('on host down backend table is %s' % self.api.backend_table)
        self._logger.debug('removing server %s from backends' % server)
        # Assuming backend `backend` can be only in default behaviour mode
        if not self._get_nginx_v2_mode_flag():
            self.api._recreate_compat_mode()
            # upstream_role = __nginx__['upstream_app_role']
            # if (upstream_role and upstream_role == role_name) or \
            #     (not upstream_role and BuiltinBehaviours.APP in behaviours):

            #     self._logger.info('removing server %s from default backend' %
            #                        server)

            #     for default_backend in ['backend', 'backend.ssl']:
            #         if default_backend not in self.api.backend_table:
            #             continue
            #         server_list = []
            #         for dest in self.api.backend_table[default_backend]:
            #             server_list.extend(dest['servers'])
            #         if server not in server_list:
            #             continue

            #         if len(server_list) == 1:
            #             self._logger.debug('adding localhost to default backend')
            #             self.api.add_server(default_backend, '127.0.0.1',
            #                                 reload_service=False,
            #                                 update_backend_table=True)
            #         self._logger.info('Removing %s server from backend' % server)
            #         self.api.remove_server(default_backend, server, 
            #                                update_backend_table=True)

        else:
            # self._logger.info('removing server %s from role %s backend(s)', server, role_id)
            # self.api.remove_server_from_role(server, role_id)
            # self._logger.info('adding new app server %s to role %s backend(s)', server, role_id)
            # self.api.add_server_to_role(server, role_id)
            role_params = self._queryenv.list_farm_role_params(__node__['farm_role_id'])['params']
            nginx_params = role_params.get(BEHAVIOUR)
            proxies = nginx_params.get('proxies', []) if nginx_params else []
            self._logger.debug('Recreating proxying with proxies:\n%s' % proxies)
            self.api.recreate_proxying(proxies)
        self._logger.debug('After %s host down backend table is %s' %
                           (server, self.api.backend_table))

        if cache_remove:
            self._terminating_servers.append(server)

    def on_HostDown(self, message):
        server = ''
        role_id = message.farm_role_id
        role_name = message.role_name
        behaviours = message.behaviour
        if message.cloud_location == __node__['cloud_location']:
            server = message.local_ip
        else:
            server = message.remote_ip

        self._remove_shut_down_server(server, role_id, role_name, behaviours)

    def on_BeforeHostTerminate(self, message):
        server = ''
        role_id = message.farm_role_id
        role_name = message.role_name
        behaviours = message.behaviour
        if message.cloud_location == __node__['cloud_location']:
            server = message.local_ip
        else:
            server = message.remote_ip

        self._remove_shut_down_server(server, role_id, role_name, behaviours, True)

    def on_VhostReconfigure(self, message):
        if not self._get_nginx_v2_mode_flag():
            self._logger.debug('updating certificates')
            cert, key, cacert = self._queryenv.get_https_certificate()
            update_ssl_certificate('', cert, key, cacert)

            self._logger.debug('before vhost reconf backend table is %s' % self.api.backend_table)
            roles_for_proxy = []
            if __nginx__['upstream_app_role']:
                roles_for_proxy = [__nginx__['upstream_app_role']]
            else:
                roles_for_proxy = get_all_app_roles()
            self.api.make_default_proxy(roles_for_proxy)
            self._logger.debug('after vhost reconf backend table is %s' % self.api.backend_table)

    def on_SSLCertificateUpdate(self, message):
        ssl_cert_id = message.body['id']
        private_key = message.private_key
        certificate = message.certificate
        cacertificate = message.cacertificate
        update_ssl_certificate(ssl_cert_id,
                               certificate,
                               private_key,
                               cacertificate)
        self.api._reload_service()

    def _copy_error_pages(self):
        pages_source = __node__['share_dir'] + '/nginx/html/'
        pages_destination = '/usr/share/nginx/html/'

        current_dir = ''
        for d in pages_destination.split(os.path.sep)[1:-1]:
            current_dir = current_dir + '/' + d
            if not os.path.exists(current_dir):
                os.makedirs(current_dir)

        if not os.path.exists(pages_destination + '500.html'):
            shutil.copy(pages_source + '500.html', pages_destination)
        if not os.path.exists(pages_destination + '502.html'):
            shutil.copy(pages_source + '502.html', pages_destination)
        if not os.path.exists(pages_destination + 'noapp.html'):
            shutil.copy(pages_source + 'noapp.html', pages_destination)

    def _https_config_exists(self):
        config_dir = os.path.dirname(self.api.app_inc_path)
        conf_path = os.path.join(config_dir, 'https.include')

        config = None
        try:
            config = Configuration('nginx')
            config.read(conf_path)
        except (Exception, BaseException), e:
            raise HandlerError('Cannot read/parse nginx main configuration file: %s' % str(e))

        return config.get('server') != None

    def _test_config(self):
        self._logger.debug("Testing new configuration")
        try:
            self._init_script.configtest()
        except initdv2.InitdError, e:
            self._logger.error("Configuration error detected: %s Reverting configuration." % str(e))

            if os.path.isfile(self._app_inc_path):
                shutil.move(self._app_inc_path, self._app_inc_path+".junk")
            else:
                self._logger.debug('%s does not exist', self._app_inc_path)
            if os.path.isfile(self._app_inc_path+".save"):
                shutil.move(self._app_inc_path+".save", self._app_inc_path)
            else:
                self._logger.debug('%s does not exist', self._app_inc_path+".save")
        else:
            self.api._reload_service()

    def _insert_iptables_rules(self, *args, **kwargs):
        if iptables.enabled():
            iptables.FIREWALL.ensure([
                    {"jump": "ACCEPT", "protocol": "tcp", "match": "tcp", "dport": "80"},
                    {"jump": "ACCEPT", "protocol": "tcp", "match": "tcp", "dport": "443"},
            ])


class NginxConf(BaseConfig):

    config_type = 'www'
    config_name = 'nginx.conf'


class NginxPresetProvider(PresetProvider):

    def __init__(self):

        nginx_conf_path = __nginx__['nginx.conf']
        config_mapping = {'nginx.conf':NginxConf(nginx_conf_path)}
        service = initdv2.lookup('nginx')
        PresetProvider.__init__(self, service, config_mapping)
