'''
Created on Oct 24, 2011

@author: marat
'''

import os
import re
import sys
import uuid
import time
import json
import signal
import shutil
import logging
import tempfile
import subprocess
import binascii

from scalarizr import linux
from scalarizr.api import chef as chef_api
from scalarizr.node import __node__
from scalarizr.bus import bus
from scalarizr.util import system2, initdv2, deploy
from scalarizr.util.software import which
from scalarizr.handlers import Handler, HandlerError

if linux.os.windows_family:
    import win32service
    import win32serviceutil
    import pywintypes


def get_handlers():
    if linux.os.windows_family or chef_api.ChefAPI.software_supported:
        return [ChefHandler()]
    else:
        return []

WIN_SERVICE_NAME = 'chef-client'
LOG = logging.getLogger(__name__)
CLIENT_CONF_TPL = '''
log_level        :%(log_level)s
log_location     STDOUT
chef_server_url  '%(server_url)s'
environment      '%(environment)s'
validation_client_name '%(validator_name)s'
node_name        '%(node_name)s'
%(ssl_verify_mode)s
'''

SOLO_CONF_TPL = '''
cookbook_path "{0}"
file_cache_path "{1}"
log_level :{2}
'''

if linux.os.windows_family:
    CLIENT_CONF_PATH = r'C:\chef\client.rb'
    VALIDATOR_KEY_PATH = r'C:\chef\validation.pem'
    CLIENT_KEY_PATH = r'C:\chef\client.pem'
    JSON_ATTRIBUTES_PATH = r'C:\chef\json_attributes.json'
    CHEF_CLIENT_BIN = r'C:\opscode\chef\bin\chef-client.bat'
    CHEF_SOLO_BIN = r'C:\opscode\chef\bin\chef-solo.bat'
else:
    CLIENT_CONF_PATH = '/etc/chef/client.rb'
    VALIDATOR_KEY_PATH =  '/etc/chef/validation.pem'
    CLIENT_KEY_PATH = '/etc/chef/client.pem'
    JSON_ATTRIBUTES_PATH = '/etc/chef/json_attributes.json'
    CHEF_CLIENT_BIN = which('chef-client')
    CHEF_SOLO_BIN = which('chef-solo')


PID_FILE = '/var/run/chef-client.pid'

def extract_json_attributes(chef_data):
    """
    Extract json attributes dictionary from scalr formatted structure
    """
    try:
        json_attributes = json.loads(chef_data.get('json_attributes') or "{}")
    except ValueError, e:
        raise HandlerError("Chef attributes is not a valid JSON: {0}".format(e))

    if chef_data.get('run_list'):
        try:
            json_attributes['run_list'] = json.loads(chef_data['run_list'])
        except ValueError, e:
            raise HandlerError("Chef runlist is not a valid JSON: {0}".format(e))

    elif chef_data.get('role'):
        json_attributes['run_list'] = ["role[%s]" % chef_data['role']]

    return json_attributes


class ChefInitScript(initdv2.ParametrizedInitScript):
    _default_init_script = '/etc/init.d/chef-client'

    def __init__(self):
        self._env = None
        super(ChefInitScript, self).__init__('chef', None, PID_FILE)


    def start(self, env=None):
        self._env = env or os.environ
        super(ChefInitScript, self).start()


    # Uses only pid file, no init script involved
    def _start_stop_reload(self, action):
        chef_client_bin = which('chef-client')
        if action == "start":
            if not self.running:
                # Stop default chef-client init script
                if os.path.exists(self._default_init_script):
                    system2(
                        (self._default_init_script, "stop"), 
                        close_fds=True, 
                        preexec_fn=os.setsid, 
                        raise_exc=False
                    )

                cmd = (chef_client_bin, '--daemonize', '--logfile', 
                        '/var/log/chef-client.log', '--pid', PID_FILE)
                out, err, rcode = system2(cmd, close_fds=True, 
                            preexec_fn=os.setsid, env=self._env,
                            stdout=open(os.devnull, 'w+'), 
                            stderr=open(os.devnull, 'w+'), 
                            raise_exc=False)
                if rcode == 255:
                    LOG.debug('chef-client daemon already started')
                elif rcode:
                    msg = (
                        'Chef failed to start daemonized. '
                        'Return code: %s\nOut:%s\nErr:%s'
                        )
                    raise initdv2.InitdError(msg % (rcode, out, err))

        elif action == "stop":
            if self.running:
                with open(self.pid_file) as f:
                    pid = int(f.read().strip())
                try:
                    os.getpgid(pid)
                except OSError:
                    os.remove(self.pid_file)
                else:
                    os.kill(pid, signal.SIGTERM)

    def restart(self):
        self._start_stop_reload("stop")
        self._start_stop_reload("start")

initdv2.explore('chef', ChefInitScript)

class ChefHandler(Handler):

    def __init__(self):
        super(ChefHandler, self).__init__()
        bus.on(init=self.on_init)
        self._chef_data = None
        self._hir_data = None
        self._run_list = None

        self._with_json_attributes = None
        self._platform = bus.platform
        self._global_variables = {}
        self._init_script = initdv2.lookup('chef')

    def on_init(self, *args, **kwds):
        bus.on(
            host_init_response=self.on_host_init_response,
            before_host_up=self.on_before_host_up,
            start=self.on_start
        )

    def on_start(self):
        if 'running' == __node__['state']:
            queryenv = bus.queryenv_service
            farm_role_params = queryenv.list_farm_role_params(__node__['farm_role_id'])
            params_dict = farm_role_params['params'].get('chef')
            if params_dict:
                daemonize = int(params_dict.get('daemonize', False))
                if daemonize:
                    self.daemonize()

    def on_host_init_response(self, message):
        self._global_variables = message.body.get('global_variables') or []
        # global_variables = message.body.get('global_variables') or []
        # for kv in global_variables:
        #     self._global_variables[kv['name']] = kv['value'].encode('utf-8') if kv['value'] else ''

        if 'chef' in message.body and message.body['chef']:
            self._hir_data = message.body.copy()
            self._chef_data = message.chef.copy()
            if not self._chef_data.get('node_name'):
                self._chef_data['node_name'] = self.get_node_name()
            self._chef_data['first_bootstrap'] = True

            self._with_json_attributes = extract_json_attributes(self._chef_data)

            if linux.os.windows_family:
                # TODO: why not doing the same on linux?
                try:
                    # Set startup type to 'manual' for chef-client service
                    hscm = win32service.OpenSCManager(None, None, 
                                win32service.SC_MANAGER_ALL_ACCESS)
                    try:
                        hs = win32serviceutil.SmartOpenService(hscm, WIN_SERVICE_NAME, 
                                win32service.SERVICE_ALL_ACCESS)
                        try:
                            snc = win32service.SERVICE_NO_CHANGE
                            # change only startup type
                            win32service.ChangeServiceConfig(hs, snc, 
                                    win32service.SERVICE_DEMAND_START,
                                    snc, None, None, 0, None, None, None, None)
                        finally:
                            win32service.CloseServiceHandle(hs)
                    finally:
                        win32service.CloseServiceHandle(hscm)

                    win32serviceutil.StopService(WIN_SERVICE_NAME)
                except:
                    e = sys.exc_info()[1]
                    self._logger.warning('Could not stop chef service: %s' % e)

    def on_before_host_up(self, msg):
        if not self._chef_data:
            return

        from scalarizr.api.system import SystemAPI
        system_api = SystemAPI()
        log = bus.init_op.logger if bus.init_op else LOG
        try:
            event_name = self._chef_data.pop('event_name', 'HostInit')
            chef_script = {
                'name': self._chef_data.pop('script_name', '[Scalr built-in] Chef bootstrap'),
                'execution_id': self._chef_data.pop('execution_id', str(uuid.uuid4())),
                'exec_timeout': sys.maxint,   
                'chef': self._chef_data}

            prod = bus.messaging_service.get_producer()
            out_messages = []
            def append_message(queue, message):
                out_messages.append(message)
            prod.on('before_send', append_message)
            try:
                # Create client configuration
                if self._chef_data.get('server_url'):
                    # Delete client.pem
                    if os.path.exists(CLIENT_KEY_PATH):
                        os.remove(CLIENT_KEY_PATH)

                    self.send_message('HostUpdate', dict(chef=self._chef_data))

                    system_api.execute_scripts(
                        [chef_script], 
                        msg_body={
                            'event_id': self._hir_data.get('event_id'),
                            'server_id': self._hir_data.get('server_id'),
                        },
                        event_name=event_name, 
                        global_variables=self._global_variables, 
                        async=False)

                    # chef_client = ChefClient(chef_server_url=self._chef_data['server_url'],
                    #                          json_attributes=self._with_json_attributes,
                    #                          node_name=self._chef_data['node_name'],
                    #                          validator_name=self._chef_data['validator_name'],
                    #                          validation_pem=self._chef_data['validator_key'],
                    #                          environment=self._chef_data['environment'],
                    #                          environment_variables=self._environ_variables,
                    #                          log_level=self._chef_data.get('log_level', 'auto'))
                    # try:
                    #     chef_client.prepare()
                    #     self.send_message('HostUpdate', dict(chef=self._chef_data))
                    #     chef_client.run()
                    # finally:
                    #     chef_client.cleanup()

                    daemonize = self._chef_data.get('daemonize')
                    if daemonize and int(daemonize):
                        log.info('Daemonizing chef-client')
                        self.daemonize()

                elif self._chef_data.get('cookbook_url'):
                    system_api.execute_scripts(
                        [chef_script], 
                        event_name=event_name, 
                        global_variables=self._global_variables, 
                        async=False)

                    # solo = ChefSolo(cookbook_url=self._chef_data['cookbook_url'],
                    #                 cookbook_url_type=self._chef_data['cookbook_url_type'],
                    #                 json_attributes=self._with_json_attributes,
                    #                 relative_path=self._chef_data.get('relative_path'),
                    #                 environment=self._environ_variables,
                    #                 ssh_private_key=self._chef_data.get('ssh_private_key'),
                    #                 binary_path=CHEF_SOLO_BIN)
                    # try:
                    #     solo.prepare()
                    #     solo.run()
                    # finally:
                    #     solo.cleanup()

                else:
                    raise HandlerError('Neither chef server nor cookbook url were specified')
            finally:
                prod.un('before_send', append_message)

            for msg in out_messages:
                if msg.name == 'ExecScriptResult':
                    self._raise_from_exec_script_result(msg)

            msg.chef = self._chef_data
        finally:
            self._chef_data = None


    def _raise_from_exec_script_result(self, msg):
        if msg.body.get('return_code') == 0:
            return
        raise HandlerError('{} failed: {}'.format(
            msg.body['script_name'], 
            binascii.a2b_base64(msg.body['stdout'])))


    def daemonize(self):
        if linux.os.windows_family:
            self._logger.info('Starting chef-client service')
            try:
                win32serviceutil.StartService(WIN_SERVICE_NAME)
            except pywintypes.error, e:
                if e.args[0] == 1060:
                    err = ("Can't daemonize Chef cause 'chef-client', "
                            "cause 'chef-client' is not a registered Windows Service.\n"
                            "Most likely you haven't selected Chef Service option in Chef installer.")
                    raise HandlerError(err)
        else:
            self._init_script.start(env=self._environ_variables)


    @property
    def _environ_variables(self):
        environ = {
            'SCALR_INSTANCE_INDEX': __node__['server_index'],
            'SCALR_FARM_ID': __node__['farm_id'],
            'SCALR_ROLE_ID': __node__['role_id'],
            'SCALR_FARM_ROLE_ID': __node__['farm_role_id'],
            'SCALR_BEHAVIORS': ','.join(__node__['behavior']),
            'SCALR_SERVER_ID': __node__['server_id']
        }
        environ.update(os.environ)
        gv = {}
        for kv in self._global_variables:
            gv[kv['name']] = kv['value'].encode('utf-8') if kv['value'] else ''
        environ.update(gv)
        if linux.os.windows_family:
            # Windows env should contain only strings, unicode is not an option
            environ = dict((str(x), str(y)) for x, y in environ.items())
        return environ

    def get_node_name(self):
        hostname = __node__['base'].get('hostname')
        return hostname and re.sub('\s+', '-', hostname) or \
                '{0}-{1}-{2}'.format(
                    self._platform.name, 
                    self._platform.get_public_ip(), 
                    time.time())

class ChefClient(object):

    def __init__(self,
                 chef_server_url=None,
                 json_attributes=None,
                 node_name=None,
                 validator_name=None,
                 validation_pem=None,
                 environment=None,
                 environment_variables=None,
                 log_level='auto',
                 run_as='root',
                 override_runlist=False,
                 ssl_verify_mode='chef_auto'):

        self.chef_server_url = chef_server_url
        self.validation_pem = validation_pem

        self.json_attributes = json_attributes or dict()
        self.run_list = self.json_attributes.get("run_list", list())

        self.node_name = node_name
        self.validator_name = validator_name
        self.environment = environment
        self.environment_variables = environment_variables or dict()
        self.log_level = log_level
        self.run_as = run_as
        self.ssl_verify_mode = ssl_verify_mode

        self.override_runlist = override_runlist
        # Remove runlist from attributes to preserve node's run list
        if self.override_runlist:
            self.json_attributes.pop("run_list", None)

    def prepare(self):
        if os.path.exists(CLIENT_KEY_PATH) and os.path.exists(CLIENT_CONF_PATH):
            if self.chef_server_url:
                with open(CLIENT_CONF_PATH) as f:
                    for line in f:
                        if line.strip().startswith("chef_server_url"):
                            splitted_line = line.strip().split(None, 1)
                            if len(splitted_line) != 2:
                                break
                            server_url = splitted_line[1].strip("'\"")
                            if server_url == self.chef_server_url:
                                break
                            raise Exception("Can not configure chef to use {0} as server url, because it's"
                                ' already configured to use {1}'.format(self.chef_server_url, server_url))
        else:
            assert self.node_name
            assert self.chef_server_url
            assert self.environment

            _dir = os.path.dirname(CLIENT_CONF_PATH)
            if not os.path.exists(_dir):
                os.makedirs(_dir)
            with open(CLIENT_CONF_PATH, 'w+') as fp:
                if self.ssl_verify_mode and self.ssl_verify_mode != 'chef_auto':
                    ssl_verify_mode = 'ssl_verify_mode :{}'.format(self.ssl_verify_mode)
                else:
                    ssl_verify_mode = ''
                fp.write(CLIENT_CONF_TPL % dict(server_url=self.chef_server_url, environment=self.environment,
                                            validator_name=self.validator_name, node_name=self.node_name,
                                            log_level=self.log_level, ssl_verify_mode=ssl_verify_mode))
            os.chmod(CLIENT_CONF_PATH, 0644)

            if not os.path.exists(CLIENT_KEY_PATH):
                assert self.validation_pem
                assert self.validator_name
                # Write validation cert
                with open(VALIDATOR_KEY_PATH, 'w+') as fp:
                    fp.write(self.validation_pem)

        if self.json_attributes:
            with open(JSON_ATTRIBUTES_PATH, 'w+') as fp:
                json.dump(self.json_attributes, fp)


    def _run_chef_client(self):
        system2(self.get_cmd(),
            close_fds=not linux.os.windows_family,
            log_level=logging.INFO,
            preexec_fn=not linux.os.windows_family and os.setsid or None,
            env=self.environment_variables
        )

    def get_cmd(self):
        cmd = [CHEF_CLIENT_BIN]

        if self.override_runlist and self.run_list:
            cmd += ["-o", ",".join(self.run_list)]

        if self.json_attributes:
            cmd += ['--json-attributes', JSON_ATTRIBUTES_PATH]

        if self.run_as != 'root':
            cmd = ['sudo', '-u', self.run_as] + cmd

        return cmd


    def run(self):
        LOG.info('Starting chef-client. Run list: {0}'.format(self.run_list))
        self._run_chef_client()


    def cleanup(self):
        if os.path.exists(JSON_ATTRIBUTES_PATH):
            os.remove(JSON_ATTRIBUTES_PATH)

        if os.path.exists(VALIDATOR_KEY_PATH):
            os.remove(VALIDATOR_KEY_PATH)


class ChefSolo(object):

    stdout = None
    stacktrace = None
    temp_dir = None

    def __init__(self, cookbook_url, cookbook_url_type, json_attributes,
                 relative_path=None, environment=None, ssh_private_key=None,
                 binary_path=None, run_as=None, log_level='auto', temp_dir=None):
        """
        @param cookbook_url:
        @param cookbook_url_type:
        @param json_attributes: dictionary to pass to -j argument of chef-solo, contains run_list
        @param relative_path:
        @param environment:
        @param ssh_private_key:
        @param binary_path:
        """
        self.cookbook_url = cookbook_url
        self.cookbook_url_type = cookbook_url_type
        self.relative_path = relative_path
        self.json_attributes = json_attributes
        self.environment = environment or dict()
        self.ssh_private_key = ssh_private_key
        self.binary_path = binary_path or CHEF_SOLO_BIN
        if not self.binary_path or not os.path.exists(self.binary_path):
            raise Exception('Could not find chef-solo binary')

        self.run_as = run_as or 'root'
        self.log_level = log_level
        self.temp_dir = temp_dir or tempfile.mkdtemp()

    def prepare(self):
        if self.cookbook_url_type == 'git':
            downloader = deploy.GitSource(self.cookbook_url, ssh_private_key=self.ssh_private_key)
            downloader.update(self.temp_dir)
        elif self.cookbook_url_type == 'http':
            downloader = deploy.HttpSource(self.cookbook_url)
            downloader.update(self.temp_dir)
        else:
            raise HandlerError('Unknown cookbook source type: %s' % self.cookbook_url_type)
        cookbook_path = os.path.join(self.temp_dir, self.relative_path or '')

        with open(self.chef_solo_cfg_path, 'w') as f:
            f.write(SOLO_CONF_TPL.format(cookbook_path, self.temp_dir, self.log_level))

        with open(self.attrs_path, 'w') as f:
            json.dump(self.json_attributes, f)

    @property
    def chef_solo_cfg_path(self):
        return os.path.join(self.temp_dir, 'solo.rb')

    @property
    def attrs_path(self):
        return os.path.join(self.temp_dir, 'runlist.json')


    def get_cmd(self):
        cmd = [self.binary_path, '-c', self.chef_solo_cfg_path, '-j', self.attrs_path]
        if self.run_as != 'root':
            cmd = ['sudo', '-u', self.run_as] + cmd
        return cmd

    def cleanup(self):
        if self.temp_dir:
            try:
                shutil.rmtree(self.temp_dir)
            except:
                pass

    def get_stacktrace(self):
        chef_stacktrace_path = os.path.join(self.temp_dir, 'chef-stacktrace.out')
        if os.path.exists(chef_stacktrace_path):
            with open(chef_stacktrace_path) as f:
                return f.read()
        return None


    def run(self):
        try:
            system2(self.get_cmd(),
                     close_fds=not linux.os.windows_family,
                     preexec_fn=not linux.os.windows_family and os.setsid or None,
                     env=self.environment,
                     stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        except:
            e_type, e, tb = sys.exc_info()
            stacktrace = self.get_stacktrace() or ''
            e = e_type(str(e) + '\n' + stacktrace)
            raise e_type, e, tb
