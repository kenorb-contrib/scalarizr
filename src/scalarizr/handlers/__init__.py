from __future__ import with_statement

from scalarizr import config, util, linux, api, exceptions
from scalarizr.bus import bus
from scalarizr.api import operation
from scalarizr.node import __node__
from scalarizr.config import ScalarizrState, STATE
from scalarizr.messaging import Queues, Message, Messages
from scalarizr.util import initdv2, software, system2, PopenError
from scalarizr.linux import iptables, pkgmgr
from scalarizr.service import CnfPresetStore, CnfPreset, PresetType

import os
import logging
import threading
import pprint
import sys
import traceback
import uuid
import codecs
import distutils.version
import platform as platform_module

LOG = logging.getLogger(__name__)


class Handler(object):
    _service_name = behaviour = None
    _logger = logging.getLogger(__name__)

    def __init__(self):
        pass

    def new_message(self, msg_name, msg_body=None, msg_meta=None, broadcast=False, include_pad=False, srv=None):
        srv = srv or bus.messaging_service
        pl = bus.platform

        msg = srv.new_message(msg_name, msg_meta, msg_body)
        if broadcast:
            self._broadcast_message(msg)
        if include_pad:
            msg.body['platform_access_data'] = pl.get_access_data()
        return msg

    def send_message(self, msg_name, 
            msg_body=None, 
            msg_meta=None, 
            broadcast=False,
            queue=Queues.CONTROL, 
            handle_host_init=False,
            handle_before_host_up=False,
            new_crypto_key=None):
        srv = bus.messaging_service
        msg = msg_name if isinstance(msg_name, Message) else \
                        self.new_message(msg_name, msg_body, msg_meta, broadcast)
        srv.get_producer().send(queue, msg)
        cons = srv.get_consumer()

        if new_crypto_key:
            cnf = bus.cnf
            cnf.write_key(cnf.DEFAULT_KEY, new_crypto_key)

        if handle_host_init:
            return cons.handle_host_init(msg)
        elif handle_before_host_up:
            return cons.handle_before_host_up(msg)


    def send_int_message(self, host, msg_name, msg_body=None, msg_meta=None, broadcast=False,
                                            include_pad=False, queue=Queues.CONTROL):
        srv = bus.int_messaging_service
        msg = msg_name if isinstance(msg_name, Message) else \
                                self.new_message(msg_name, msg_body, msg_meta, broadcast, include_pad, srv)
        srv.new_producer(host).send(queue, msg)


    def send_result_error_message(self, msg_name, error_text=None, exc_info=None, body=None):
        body = body or {}
        if not exc_info:
            exc_info = sys.exc_info()
        body['status'] = 'error'
        body['last_error'] = ''
        if error_text:
            body['last_error'] += error_text + '. '
        body['last_error'] += str(exc_info[1])
        body['trace'] = ''.join(traceback.format_tb(exc_info[2]))

        LOG.error(body['last_error'], exc_info=exc_info)
        self.send_message(msg_name, body)


    def _broadcast_message(self, msg):
        cnf = bus.cnf
        platform = bus.platform

        msg.local_ip = platform.get_private_ip()
        msg.remote_ip = platform.get_public_ip()
        msg.behaviour = config.split(cnf.rawini.get(config.SECT_GENERAL, config.OPT_BEHAVIOUR))
        msg.role_name = cnf.rawini.get(config.SECT_GENERAL, config.OPT_ROLE_NAME)


    def accept(self, message, queue, behaviour=None, platform=None, os=None, dist=None):
        return False


    def __call__(self, message):
        fn = "on_" + message.name
        if hasattr(self, fn) and callable(getattr(self, fn)):
            getattr(self, fn)(message)
        else:
            raise HandlerError("Handler %s has no method %s", self.__class__.__name__, fn)


    def get_ready_behaviours(self):
        LOG.info('Detecting supported behaviors...')        
        if linux.os['family'] != 'Windows':
            system_packages = pkgmgr.package_mgr().list()
            possible_behaviors = config.BuiltinBehaviours.values()
        else:
            system_packages = []
            possible_behaviors = ('base', 'chef')

        msg = (
                "Scalr built-in automation: checking for supported software.\n"
                "If installed software isn't detected, "
                "review the Scalr Wiki: https://scalr-wiki.atlassian.net/wiki/x/IoB1")
        LOG.info(msg)

        ready_behaviors = list()
        for behavior in possible_behaviors:
            if behavior in ['base', 'mongodb'] or behavior not in api.api_routes.keys():
                continue
            try:
                api_cls = util.import_class(api.api_routes[behavior])
                installed = api_cls.check_software(system_packages)
                ready_behaviors.append(behavior)
                LOG.info('%s: Available. Installed version: %s', behavior, installed[1])
            except (exceptions.NotFound, exceptions.UnsupportedBehavior, ImportError), e:
                if isinstance(e, exceptions.UnsupportedBehavior):
                    LOG.info('%s: %s', behavior, e.args[1])
                else:
                    LOG.info('%s: %s', behavior, e)
                continue
        return ready_behaviors


class HandlerError(BaseException):
    pass


class MessageListener:
    _accept_kwargs = {}

    def __init__(self):
        self._logger = logging.getLogger(__name__)
        self._handlers_chain = None
        cnf = bus.cnf
        platform = bus.platform

        LOG.debug("Initializing message listener")
        self._accept_kwargs = dict(
                behaviour = config.split(cnf.rawini.get(config.SECT_GENERAL, config.OPT_BEHAVIOUR)),
                platform = platform.name,
                os = platform_module.uname(),
                dist = platform_module.dist()
        )
        LOG.debug("Keywords for each Handler::accept\n%s", pprint.pformat(self._accept_kwargs))

        self.get_handlers_chain()


    def get_handlers_chain (self):
        if self._handlers_chain is None:
            hds = []
            LOG.debug("Collecting message handlers...")

            cnf = bus.cnf
            for _, module_str in cnf.rawini.items(config.SECT_HANDLERS):
                __import__(module_str)
                try:
                    hds.extend(sys.modules[module_str].get_handlers())
                except:
                    LOG.error("Can't get module handlers (module: %s)", module_str)
                    raise

            def cls_weight(obj):
                cls = obj.__class__.__name__
                if cls == 'IpListBuilder':
                    return 20
                elif cls in ('EbsHandler', 'BlockDeviceHandler'):
                    return 10
                elif cls == 'DeploymentHandler':
                    return 1
                else:
                    return 0

            def sort_fn(a, b):
                return cmp(cls_weight(a), cls_weight(b))

            self._handlers_chain = list(reversed(sorted(hds, sort_fn)))
            bus._listeners['init'] = list(reversed(sorted(bus._listeners['init'], sort_fn)))
            bus._listeners['start'] = list(reversed(sorted(bus._listeners['start'], sort_fn)))
            LOG.debug("Message handlers chain:\n%s", pprint.pformat(self._handlers_chain))


        return self._handlers_chain

    def __call__(self, message, queue):
        LOG.debug("Handle '%s'" % (message.name))

        cnf = bus.cnf
        pl = bus.platform
        platform_access_data_on_me = False
        try:
            # Each message can contains secret data to access platform services.
            # Scalarizr assign access data to platform object and clears it when handlers processing finished
            if message.body.has_key("platform_access_data"):
                platform_access_data_on_me = True
                pl.set_access_data(message.platform_access_data)

            if message.body.get('global_variables'):    
                global_variables = message.body.get('global_variables') or []
                glob_vars = {}
                glob_vars['public'] = dict((kv['name'], kv['value'].encode('utf-8') if kv['value'] else '') 
                                        for kv in global_variables 
                                        if not kv.get('private'))
                glob_vars['private'] = dict((kv['name'], kv['value'].encode('utf-8') if kv['value'] else '') 
                                        for kv in global_variables
                                        if kv.get('private'))
                sync_globals(glob_vars)

            if 'scalr_version' in message.meta:
                try:
                    ver = tuple(map(int, message.meta['scalr_version'].strip().split('.')))
                    if ver != bus.scalr_version:
                        # Refresh QueryEnv version
                        queryenv = bus.queryenv_service
                        queryenv.api_version = queryenv.get_latest_version()
                        bus.queryenv_version = tuple(map(int, queryenv.api_version.split('-')))
                    LOG.debug('Scalr version: %s', ver)
                except:
                    pass
                else:
                    with open(cnf.private_path('.scalr-version'), 'w') as fp:
                        fp.write('.'.join(map(str, ver)))
                    bus.scalr_version = ver

            accepted_any = False
            for handler in self.get_handlers_chain():
                hnd_name = handler.__class__.__name__
                accepted = False
                try:
                    accepted = handler.accept(message, queue, **self._accept_kwargs)
                except (BaseException, Exception), e:
                    LOG.error("%s accept() method failed with exception", hnd_name)
                    LOG.exception(e)
                if accepted:
                    LOG.debug("Call handler %s" % hnd_name)
                    accepted_any = True
                    try:
                        handler(message)
                    except (BaseException, Exception), e:
                        if message.name == 'BeforeHostUp' \
                                and message.local_ip == __node__['private_ip']:
                            raise
                        else:
                            LOG.exception(e)
            if not accepted_any:
                LOG.warning("No one could handle '%s'", message.name)
        finally:
            #if platform_access_data_on_me:
            #       pl.clear_access_data()
            # XXX(marat): I've commented this cause multithreaded and defered message handling failed
            # without credentials. We need a better secret data passing mechanism
            pass

def async(fn):
    def decorated(*args, **kwargs):
        t = threading.Thread(target=fn, args=args, kwargs=kwargs)
        t.start()

    return decorated


class ServiceCtlHandler(Handler):
    _logger = None
    _cnf_ctl = None
    _init_script = None
    _preset_store = None
    _service_name = None
    initial_preset = None

    def __init__(self, service_name, init_script=None, cnf_ctl=None):
        '''
        XXX: When migrating to the new preset system
        do not forget that self._service_name is essential for
        Handler.get_ready_behaviours() and should be overloaded
        in every ServiceCtlHandler child.

        '''
        self._service_name = service_name
        self._cnf_ctl = cnf_ctl
        self._init_script = init_script
        self._logger = logging.getLogger(__name__)
        self._preset_store = CnfPresetStore(self._service_name)

        Handler.__init__(self)

        self._queryenv = bus.queryenv_service
        bus.on('init', self.sc_on_init)
        bus.define_events(
                self._service_name + '_reload',
                'before_' + self._service_name + '_configure',
                self._service_name + '_configure'
        )


    def on_UpdateServiceConfiguration(self, message):
        if self._service_name != message.behaviour:
            return

        result = self.new_message(Messages.UPDATE_SERVICE_CONFIGURATION_RESULT)
        result.behaviour = message.behaviour

        # Obtain current configuration preset
        if message.reset_to_defaults == '1':
            new_preset = self._preset_store.load(PresetType.DEFAULT)
        else:
            new_preset = self._obtain_current_preset()
        if new_preset:
            result.preset = new_preset.name

        # Apply current preset
        try:
            LOG.info("Applying preset '%s' to %s %s service restart",
                                            new_preset.name, self._service_name,
                                            'with' if message.restart_service == '1' else 'without')
            self._cnf_ctl.apply_preset(new_preset)
            if message.restart_service == '1' or message.reset_to_defaults == '1':
                self._stop_service(reason="Applying preset '%s'" % new_preset.name)
                self._start_service_with_preset(new_preset)
            result.status = 'ok'
        except (BaseException, Exception), e:
            result.status = 'error'
            result.last_error = str(e)

        # Send result
        self.send_message(result)

    def _start_service(self):
        if not self._init_script.running:
            LOG.info("Starting %s" % self._service_name)
            try:
                self._init_script.start()
            except BaseException, e:
                if not self._init_script.running:
                    raise
                LOG.warning(str(e))
            LOG.debug("%s started" % self._service_name)

    def _stop_service(self, reason=None):
        if self._init_script.running:
            LOG.info("Stopping %s%s", self._service_name, '. (%s)' % reason if reason else '')
            try:
                self._init_script.stop()
            except:
                if self._init_script.running:
                    raise
            LOG.debug("%s stopped", self._service_name)

    def _restart_service(self, reason=None):
        LOG.info("Restarting %s%s", self._service_name, '. (%s)' % reason if reason else '')
        self._init_script.restart()
        LOG.debug("%s restarted", self._service_name)

    def _reload_service(self, reason=None):
        LOG.info("Reloading %s%s", self._service_name, '. (%s)' % reason if reason else '')
        try:
            self._init_script.reload()
            bus.fire(self._service_name + '_reload')
        except initdv2.InitdError, e:
            if e.code == initdv2.InitdError.NOT_RUNNING:
                LOG.debug('%s not running', self._service_name)
            else:
                raise
        LOG.debug("%s reloaded", self._service_name)

    def _obtain_current_preset(self):
        service_conf = self._queryenv.get_service_configuration(self._service_name)
        if service_conf.new_engine:
            '''New sheriff in town. No need to calculate or apply old preset'''
            return None

        cur_preset = CnfPreset(service_conf.name, service_conf.settings)
        if cur_preset.name == 'default':
            try:
                cur_preset = self._preset_store.load(PresetType.DEFAULT)
            except IOError, e:
                if e.errno == 2:
                    cur_preset = self._cnf_ctl.current_preset()
                    self._preset_store.save(cur_preset, PresetType.DEFAULT)
                else:
                    raise
        return cur_preset

    def _start_service_with_preset(self, preset):
        '''
        TODO: Revise method carefully
        '''
        try:
            if self._init_script.running:
                self._restart_service('applying new service settings from configuration preset')
            else:
                self._start_service()
        except BaseException, e:
            LOG.error('Cannot start %s with current configuration preset. ' % self._service_name
                            + '[Reason: %s] ' % str(e)
                            + 'Rolling back to the last successful preset')
            preset = self._preset_store.load(PresetType.LAST_SUCCESSFUL)
            self._cnf_ctl.apply_preset(preset)
            self._start_service()

        LOG.debug("Set %s configuration preset '%s' as last successful", self._service_name, preset.name)
        self._preset_store.save(preset, PresetType.LAST_SUCCESSFUL)

    def sc_on_init(self):
        bus.on(
                start=self.sc_on_start,
                service_configured=self.sc_on_configured,
                before_host_down=self.sc_on_before_host_down
        )

    def sc_on_start(self):
        szr_cnf = bus.cnf
        if szr_cnf.state == ScalarizrState.RUNNING:
            if self._cnf_ctl:
                # Obtain current configuration preset
                cur_preset = self._obtain_current_preset()
                if not cur_preset:
                    LOG.info('New configuration preset engine is used. Skipping old presets.')
                    return
                # Apply current preset
                my_preset = self._cnf_ctl.current_preset()
                if not self._cnf_ctl.preset_equals(cur_preset, my_preset):
                    if not STATE['global.start_after_update']:
                        LOG.info("Applying '%s' preset to %s", cur_preset.name, self._service_name)
                        self._cnf_ctl.apply_preset(cur_preset)
                        # Start service with updated configuration
                        self._start_service_with_preset(cur_preset)
                    else:
                        LOG.debug('Skiping apply configuration preset whereas Scalarizr was restarted after update')
                        self._start_service()

                else:
                    LOG.debug("%s configuration satisfies current preset '%s'", self._service_name, cur_preset.name)
                    self._start_service()

            else:
                self._start_service()


    def sc_on_before_host_down(self, msg):
        self._stop_service('instance goes down')

    def sc_on_configured(self, service_name, **kwargs):
        if self._service_name != service_name:
            return

        # Fetch current configuration preset
        service_conf = self._queryenv.get_service_configuration(self._service_name)

        if service_conf.new_engine:
            LOG.debug('New configuration presets engine is available.')
            response = None
            settings = {}
            LOG.debug('Initial preset from HostInitResponse: %s' % self.initial_preset)

            if self.initial_preset:
                LOG.debug('initial_preset = %s' % self.initial_preset)
                for preset in self.initial_preset:
                    for f, data in preset.items():
                        kv = {}
                        for setting in data['settings']:
                            k = setting['setting']['name']
                            v = setting['setting']['value']
                            kv[k] = v
                        settings.update({data['name']:kv})
                LOG.debug('Got settings from initial preset: %s' % settings)

            else:
                cnf = bus.cnf
                ini = cnf.rawini
                farm_role_id = ini.get('general', 'farm_role_id')
                response = self._queryenv.list_farm_role_params(farm_role_id)
            LOG.debug('list_farm_role_params: %s' %  response)
            if response and service_name in response and 'preset' in response[service_name]:
                settings = response[service_name]['preset']
                LOG.debug('list_farm_role_params returned settings: %s' % settings)
            if settings:
                manifest = self.preset_provider.get_manifest(service_name)
                if manifest:
                    LOG.debug('Applying configuration preset')
                    self.preset_provider.set_preset(settings, manifest)
                    LOG.debug('Configuration preset has been successfully applied.')
                else:
                    LOG.WARNING('Cannot apply preset: Manifest not found.')

        else:
            log = bus.init_op.logger if bus.init_op else LOG
            if self._cnf_ctl:
                log.info('Applying configuration preset')

                # Backup default configuration
                my_preset = self._cnf_ctl.current_preset()
                self._preset_store.save(my_preset, PresetType.DEFAULT)

                # Stop service if it's already running
                self._stop_service('Applying configuration preset')

                cur_preset = CnfPreset(service_conf.name, service_conf.settings, self._service_name)
                self._preset_store.copy(PresetType.DEFAULT, PresetType.LAST_SUCCESSFUL, override=False)

                if cur_preset.name == 'default':
                    # Scalr respond with default preset
                    LOG.debug('%s configuration is default', self._service_name)
                    #self._preset_store.copy(PresetType.DEFAULT, PresetType.LAST_SUCCESSFUL)
                    self._start_service()
                    return

                elif self._cnf_ctl.preset_equals(cur_preset, my_preset):
                    LOG.debug("%s configuration satisfies current preset '%s'", self._service_name, cur_preset.name)
                    self._start_service()
                    return

                else:
                    LOG.info("Applying '%s' preset to %s", cur_preset.name, self._service_name)
                    self._cnf_ctl.apply_preset(cur_preset)

                log.info('Start %s with configuration preset', service_name)
                self._start_service_with_preset(cur_preset)
            else:
                log.info('Start %s' % service_name)
                self._start_service()

        bus.fire(self._service_name + '_configure', **kwargs)


    def _get_preset(self, preset_data, config_fname):
        p = {}
        for preset in preset_data:
            file = preset['file']
            if 'name' in file and file['name'] == config_fname and 'settings' in file:
                settings = file['settings']
                if settings:
                    for setting in settings:
                        variable = setting['setting']['name']
                        value = setting['setting']['value']
                        p[variable] = value
                break
        return {config_fname : p}


class DbMsrMessages:
    DBMSR_CREATE_DATA_BUNDLE = "DbMsr_CreateDataBundle"
    DBMSR_CANCEL_DATA_BUNDLE = "DbMsr_CancelDataBundle"

    DBMSR_CREATE_DATA_BUNDLE_RESULT = "DbMsr_CreateDataBundleResult"
    '''
    @ivar: db_type: postgresql|mysql
    @ivar: status: Operation status [ ok | error ]
    @ivar: last_error: errmsg if status = error
    @ivar: snapshot_config: snapshot configuration
    @ivar: current_xlog_location:  pg_current_xlog_location() on main after snap was created
    '''

    DBMSR_CREATE_BACKUP = "DbMsr_CreateBackup"
    DBMSR_CANCEL_BACKUP = "DbMsr_CancelBackup"

    DBMSR_CREATE_BACKUP_RESULT = "DbMsr_CreateBackupResult"
    '''
    @ivar: db_type: postgresql|mysql
    @ivar: status: Operation status [ ok | error ]
    @ivar: last_error:  errmsg if status = error
    @ivar: backup_parts: URL List (s3, cloudfiles)
    '''

    DBMSR_PROMOTE_TO_MASTER = "DbMsr_PromoteToMain"

    DBMSR_PROMOTE_TO_MASTER_RESULT = "DbMsr_PromoteToMainResult"
    '''
    @ivar: db_type: postgresql|mysql
    @ivar: status: ok|error
    @ivar: last_error: errmsg if status=error
    @ivar: volume_config: volume configuration
    @ivar: snapshot_config?: snapshot configuration
    @ivar: current_xlog_location_?:  pg_current_xlog_location() on main after snap was created
    '''

    DBMSR_NEW_MASTER_UP = "DbMsr_NewMainUp"
    '''
    @ivar: db_type:  postgresql|mysql
    @ivar: local_ip
    @ivar: remote_ip
    @ivar: snapshot_config
    @ivar: current_xlog_location:  pg_current_xlog_location() on main after snap was created
    '''

    DBMSR_NEW_MASTER_UP_RESULT = "DbMsr_NewMainUpResult"

    """
    Also Postgresql behaviour adds params to common messages:

    = HOST_INIT_RESPONSE =
    @ivar db_type: postgresql|mysql
    @ivar postgresql=dict(
            replication_main:      1|0
            root_user
            root_password:                   'scalr' user password                                          (on subordinate)
            root_ssh_private_key
            root_ssh_public_key
            current_xlog_location
            volume_config:                  Main storage configuration                    (on main)
            snapshot_config:                Main storage snapshot                                 (both)
    )

    = HOST_UP =
    @ivar db_type: postgresql|mysql
    @ivar postgresql=dict(
            replication_main: 1|0
            root_user
            root_password:                  'scalr' user password                                   (on main)
            root_ssh_private_key
            root_ssh_public_key
            current_xlog_location
            volume_config:                  Current storage configuration                   (both)
            snapshot_config:                Main storage snapshot                                 (on main)
    )
    """


class FarmSecurityMixin(object):
    def __init__(self):
        self.__enabled = False

    def init_farm_security(self, ports):
        self._logger = logging.getLogger(__name__)
        self._ports = ports
        self._iptables = iptables
        if self._iptables.enabled():
            bus.on(
                reload=self.__on_reload
            )
            self.__on_reload()
            self.__insert_iptables_rules()
            self.__enabled = True 
    

    def __on_reload(self):
        self._queryenv = bus.queryenv_service
        self._platform = bus.platform


    def on_HostInit(self, message):
        if not self.__enabled:
            return
        # Append new server to allowed list
        rules = []
        for port in self._ports:
            rules += self.__accept_host(message.local_ip, message.remote_ip, port)
        self._iptables.FIREWALL.ensure(rules)


    def on_HostDown(self, message):
        if not self.__enabled:
            return
        # Remove terminated server from allowed list
        rules = []
        for port in self._ports:
            rules += self.__accept_host(message.local_ip, message.remote_ip, port)
        for rule in rules:
            try:
                self._iptables.FIREWALL.remove(rule)
            except: #?
                if 'does a matching rule exist in that chain' in str(sys.exc_info()[1]):
                    # When HostDown comes from a server that didn't send HostInit
                    pass
                else:
                    raise


    def __create_rule(self, source, dport, jump):
        rule = {"jump": jump, 
                "protocol": "tcp", 
                "match": "tcp", 
                "dport": str(dport)}
        if source:
            rule["source"] = source
        return rule


    def __create_accept_rule(self, source, dport):
        return self.__create_rule(source, dport, 'ACCEPT')


    def __create_drop_rule(self, dport):
        return self.__create_rule(None, dport, 'DROP')


    def __accept_host(self, local_ip, public_ip, dport):
        ret = []
        if local_ip == self._platform.get_private_ip():
            ret.append(self.__create_accept_rule('127.0.0.1', dport))
        if local_ip:
            ret.append(self.__create_accept_rule(local_ip, dport))
        ret.append(self.__create_accept_rule(public_ip, dport))
        return ret


    def __insert_iptables_rules(self, *args, **kwds):
        # Collect farm servers IP-s
        hosts = []
        for role in self._queryenv.list_roles(with_init=True):
            for host in role.hosts:
                hosts.append((host.internal_ip, host.external_ip))

        rules = []
        for port in self._ports:
            # TODO: this will be duplicated, because current host is in the
            # hosts list too
            # TODO: this also duplicates the rules, inserted in on_HostInit
            # for the current host
            rules += self.__accept_host(self._platform.get_private_ip(),
                                    self._platform.get_public_ip(), port)
            for local_ip, public_ip in hosts:
                rules += self.__accept_host(local_ip, public_ip, port)

        # Deny from all
        drop_rules = []
        for port in self._ports:
            drop_rules.append(self.__create_drop_rule(port))

        self._iptables.FIREWALL.ensure(rules)
        self._iptables.FIREWALL.ensure(drop_rules, append=True)


def build_tags(purpose=None, state=None, set_owner=True, **kwargs):
    tags = dict()

    if purpose:
        tags['scalr-purpose'] = purpose

    if state:
        tags['scalr-status'] = state

    if set_owner:
        for tag, node_opt in {
            'scalr-farm-id': 'farm_id',
            'scalr-farm-role-id': 'farm_role_id',
            'scalr-env-id': 'env_id'
        }.items():
            try:
                tags[tag] = __node__[node_opt]
            except KeyError:
                tags[tag] = None

        try:
            tags['scalr-owner'] = __node__['owner_email']
        except KeyError:
            tags['scalr-owner'] = None

    if kwargs:
        # example: tmp = 1
        tags.update(kwargs)

    excludes = []
    for k, v in tags.items():
        if not v:
            excludes.append(k)
            del tags[k]
        else:
            try:
                tags[k] = str(v)
            except:
                excludes.append(k)

    LOG.debug('Prepared tags: %s. Excluded empty tags: %s' % (tags, excludes))
    return tags


def transfer_result_to_backup_result(mnf):
    base = os.path.dirname(mnf.cloudfs_path)
    files_sizes = list((os.path.join(base, chunk[0]), chunk[2])
                                    for file_ in mnf['files']
                                    for chunk in file_['chunks'])
    return list(dict(path=path, size=size) for path, size in files_sizes)


def _choose_host_ip(host, network=None):
    this_server_location = __node__['cloud_location']
    if network == None:
        use_internal_ip = this_server_location == host.cloud_location
    else:
        use_internal_ip = network == 'private'
    return host.internal_ip if use_internal_ip else host.external_ip


def get_role_servers(role_id=None, role_name=None, network=None):
    """ Method is used to get role servers from scalr """
    if type(role_id) is int:
        role_id = str(role_id)

    _queryenv = bus.queryenv_service
    roles = _queryenv.list_roles(farm_role_id=role_id, role_name=role_name)
    servers = []
    for role in roles:
        ips = [_choose_host_ip(h, network) for h in role.hosts]
        servers.extend(ips)

    return servers


def sync_globals(glob_vars=None):
    if linux.os.windows:
        return
    if not glob_vars:
        queryenv = bus.queryenv_service
        glob_vars = queryenv.list_global_variables()
    globals_path = '/etc/profile.d/scalr_globals.sh'
    with codecs.open(globals_path, 'w+', encoding='utf-8') as fp:
        for k, v in glob_vars['public'].items():
            v = v.replace('"', '\\"')
            if isinstance(v, str):
                v = v.decode('utf-8')
            fp.write(u'export %s="%s"\n' % (k, v))
    os.chmod(globals_path, 0644)


def fail_init():
    '''
    raises OperationResult that will mark server as Failed in Scalr
    '''
    op = operation.Operation('system.init', lambda op: None)
    op._failed()
    __node__['messaging'].send('OperationResult', body=op.serialize())    
