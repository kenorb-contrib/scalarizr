'''
Created on Mar 3, 2010

@author: marat
'''

from __future__ import with_statement

# Core
import scalarizr.handlers
from scalarizr.bus import bus
from scalarizr import config, linux
from scalarizr.api import operation
from scalarizr.api import system as system_api
from scalarizr import config, storage2
from scalarizr.node import __node__
from scalarizr import node 
from scalarizr.config import ScalarizrState
from scalarizr.messaging import Messages, MessageServiceFactory
from scalarizr.messaging.p2p import P2pConfigOptions
from scalarizr.util import system2, port_in_use, parse_bool
from scalarizr.util.flag import Flag
from scalarizr.util import metadata

# Libs
from scalarizr.util import cryptotool, software
from scalarizr.linux import iptables, os as os_dist

# Stdlibs
import logging, os, sys, threading
from scalarizr.config import STATE
import time
import re


_lifecycle = None
def get_handlers():
    if not _lifecycle:
        globals()["_lifecycle"] = LifeCycleHandler()
    return [_lifecycle]


class LifeCycleHandler(scalarizr.handlers.Handler):
    _logger = None
    _bus = None
    _msg_service = None
    _producer = None
    _platform = None
    _cnf = None
    
    _new_crypto_key = None
    boot_id_file = '/proc/sys/kernel/random/boot_id'
    saved_boot_id_file = os.path.join(node.private_dir, 'boot_id')

    def __init__(self):
        super(LifeCycleHandler, self).__init__()
        self._logger = logging.getLogger(__name__)
        self._op_api = operation.OperationAPI()
        self._system_api = system_api.SystemAPI()
        self._hostname_assigned = False

        bus.define_events(
            # Fires before HostInit message is sent
            # @param msg 
            "before_host_init",
            
            # Fires after HostInit message is sent
            "host_init",
            
            # Fires when HostInitResponse received
            # @param msg
            "host_init_response",
            
            # Fires before HostUp message is sent
            # @param msg
            "before_host_up",
            
            # Fires after HostUp message is sent
            "host_up",
            
            # Fires before RebootStart message is sent
            # @param msg
            "before_reboot_start",
            
            # Fires after RebootStart message is sent
            "reboot_start",
            
            # Fires before RebootFinish message is sent
            # @param msg
            "before_reboot_finish",
            
            # Fires after RebootFinish message is sent
            "reboot_finish",
            
            # Fires before Restart message is sent
            # @param msg: Restart message
            "before_restart",
            
            # Fires after Restart message is sent
            "restart",
            
            # Fires before Hello message is sent
            # @param msg
            "before_hello",
            
            # Fires after Hello message is sent
            "hello",
            
            # Fires after HostDown message is sent
            # @param msg
            "before_host_down",
            
            # Fires after HostDown message is sent
            "host_down",
            
            # 
            # Service events
            #
            
            # Fires when behaviour is configured
            # @param service_name: Service name. Ex: mysql
            "service_configured"
        )
        bus.on(
            init=self.on_init, 
            start=self.on_start, 
            reload=self.on_reload, 
            shutdown=self.on_shutdown
        )
        self.on_reload()



    def accept(self, message, queue, **kwds):
        return message.name == Messages.INT_SERVER_REBOOT \
            or message.name == Messages.INT_SERVER_HALT \
            or message.name == Messages.HOST_INIT \
            or message.name == Messages.HOST_INIT_RESPONSE \
            or message.name == Messages.BEFORE_HOST_TERMINATE \
            or message.name == Messages.SCALARIZR_UPDATE_AVAILABLE


    def on_init(self):
        bus.on(
            host_init_response=self.on_host_init_response
        )

        # Add internal messages to scripting skip list
        try:
            for m in (Messages.INT_SERVER_REBOOT, 
                      Messages.INT_SERVER_HALT, 
                      Messages.HOST_INIT_RESPONSE):
                scalarizr.handlers.script_executor.skip_events.add(m)
        except AttributeError:
            pass

        # Mount all filesystems
        if os_dist['family'] != 'Windows':
            system2(('mount', '-a'), raise_exc=False)

        # cloud-init scripts may disable root ssh login
        for path in ('/etc/ec2-init/ec2-config.cfg', '/etc/cloud/cloud.cfg'):
            if os.path.exists(path):
                c = None
                with open(path, 'r') as fp:
                    c = fp.read()
                c = re.sub(re.compile(r'^disable_root[^:=]*([:=]).*', re.M), r'disable_root\1 0', c)
                with open(path, 'w') as fp:
                    fp.write(c)

        # Add firewall rules
        #if self._cnf.state in (ScalarizrState.BOOTSTRAPPING, ScalarizrState.IMPORTING):
        self._insert_iptables_rules()
        #if __node__['state'] !=  ScalarizrState.IMPORTING:
        if __node__['state'] == 'running':
            scalarizr.handlers.sync_globals()


    def _assign_hostname(self):
        if not __node__['base'].get('hostname') or self._hostname_assigned:
            # hostname should be assigned only once: either in HI or HIR
            return
        try:
            __node__['base']['hostname'] = __node__['base']['hostname'].replace(' ', '')
            self._system_api.set_hostname(__node__['base']['hostname'])
            self._hostname_assigned = True
        except:
            msg = ("The following error was thrown due to the hostname format you "
                    "configured for this instance (note that the hostname format might be "
                    "defined in hostname Governance):\n\n{}\n\n"
                    "To fix this error, review and correct the hostname format for this "
                    "Farm Role. The hostname format is found under the Farm Role's Advanced "
                    "Tab in the Farm Designer.").format(sys.exc_info()[1])
            raise Exception(msg)


    def on_start(self):
        optparser = bus.optparser

        """
        [SCALARIZR-1564]
        if iptables.enabled():
            iptables.save()
        """
        
        if os_dist['family'] != 'Windows':
            if os.path.exists(self.saved_boot_id_file):
                saved_boot_id = None
                current_boot_id = None
                with open(self.boot_id_file, 'r') as fp:
                    current_boot_id = fp.read()
                with open(self.saved_boot_id_file, 'r') as fp:
                    saved_boot_id = fp.read()

                if saved_boot_id and saved_boot_id != current_boot_id \
                    and not Flag.exists(Flag.HALT):
                    Flag.set(Flag.REBOOT)

            with open(self.boot_id_file, 'r') as fp:
                current_boot_id = fp.read()
                with open(self.saved_boot_id_file, 'w') as saved_fp:
                    saved_fp.write(current_boot_id)

        if Flag.exists(Flag.REBOOT):
            self._logger.info("Scalarizr resumed after reboot")
            Flag.clear(Flag.REBOOT)
            self._check_control_ports()
            self._assign_hostname()
            self._start_after_reboot()


        elif Flag.exists(Flag.HALT):
            self._logger.info("Scalarizr resumed after server stop")
            Flag.clear(Flag.HALT)
            self._check_control_ports()
            self._assign_hostname()

            queryenv = bus.queryenv_service
            farm_role_params = queryenv.list_farm_role_params(farm_role_id=__node__['farm_role_id'])
            try:
                resume_strategy = farm_role_params['params']['base']['resume_strategy']
            except KeyError:
                resume_strategy = 'reboot'

            if resume_strategy == 'reboot':
                self._start_after_reboot()

            elif resume_strategy == 'init':
                __node__['state'] = ScalarizrState.BOOTSTRAPPING
                self._logger.info('Scalarizr will re-initialize server due to resume strategy')
                self._start_init()

        elif optparser and optparser.values.import_server:
            self._logger.info('Server will be imported into Scalr')
            self._start_import()

        elif self._cnf.state == ScalarizrState.IMPORTING:
            self._logger.info('Server import resumed. Awaiting Rebundle message')

        elif self._cnf.state == ScalarizrState.BOOTSTRAPPING:
            self._logger.info("Starting initialization")
            self._start_init()

        else:
            self._logger.info("Normal start")
            self._check_control_ports()
            self._assign_hostname()


    def _start_after_reboot(self):
        if __node__['state'] != 'running':
            self._logger.info('Skipping RebootFinish firing, server state is: {}'.format(
                __node__['state']))
            return
        msg = self.new_message(
            Messages.REBOOT_FINISH, 
            msg_body={
                'base': {
                    'hostname': self._system_api.get_hostname()}},
            broadcast=True)
        bus.fire("before_reboot_finish", msg)
        self.send_message(msg)
        bus.fire("reboot_finish")       


    def _start_after_stop(self):
        msg = self.new_message(Messages.RESTART)
        bus.fire("before_restart", msg)
        self.send_message(msg)
        bus.fire("restart")


    def _start_init(self):
        # Regenerage key
        new_crypto_key = cryptotool.keygen()

        # Prepare HostInit
        msg = self.new_message(Messages.HOST_INIT, 
            dict(
                seconds_since_start=float('%.2f' % (time.time() - __node__['start_time'], )),
                seconds_since_boot=float('%.2f' % (time.time() - metadata.boot_time(), )),
                #operation_id = bus.init_op.operation_id,
                crypto_key = new_crypto_key
            ), 
            broadcast=True)
        bus.fire("before_host_init", msg)

        result_msg = self.send_message(msg, 
            new_crypto_key=new_crypto_key, 
            handle_host_init=True)

        bus.cnf.state = ScalarizrState.INITIALIZING
        bus.fire("host_init")

        if result_msg and \
            parse_bool(result_msg.body.get('base', {}).get('reboot_after_hostinit_phase')):
            # apply setting from HostInit
            self._system_api.reboot()
            threading.Event().wait(600)


    def _start_import(self):
        data = software.system_info()
        data['architecture'] = self._platform.get_architecture()
        data['server_id'] = self._cnf.rawini.get(config.SECT_GENERAL, config.OPT_SERVER_ID)

        # Send Hello
        msg = self.new_message(Messages.HELLO, data,
            broadcast=True # It's not really broadcast but need to contain broadcast message data 
        )
        behs = self.get_ready_behaviours()
        if 'mysql2' in behs or 'percona' in behs:
            # only mysql2 should be returned to Scalr
            try:
                behs.remove('mysql')
            except (IndexError, ValueError):
                pass
        msg.body['behaviour'] = behs
        bus.fire("before_hello", msg)
        self.send_message(msg)
        bus.fire("hello")


    def on_reload(self):
        self._msg_service = bus.messaging_service
        self._producer = self._msg_service.get_producer()
        self._cnf = bus.cnf
        self._platform = bus.platform
        
        if self._cnf.state == ScalarizrState.RUNNING and self._cnf.key_exists(self._cnf.FARM_KEY):
            self._start_int_messaging()

    def _insert_iptables_rules(self, *args, **kwargs):
        self._logger.debug('Adding iptables rules for scalarizr ports')

        if iptables.enabled():
            # Scalarizr ports
            iptables.FIREWALL.ensure([
                {"jump": "ACCEPT", "protocol": "tcp", "match": "tcp", "dport": "8008"},
                {"jump": "ACCEPT", "protocol": "tcp", "match": "tcp", "dport": "8010"},
                {"jump": "ACCEPT", "protocol": "tcp", "match": "tcp", "dport": "8012"},
                {"jump": "ACCEPT", "protocol": "tcp", "match": "tcp", "dport": "8013"},
                {"jump": "ACCEPT", "protocol": "udp", "match": "udp", "dport": "8014"},
            ])


    def on_shutdown(self):
        self._logger.debug('Calling %s.on_shutdown', __name__)
        # Shutdown internal messaging
        int_msg_service = bus.int_messaging_service
        if int_msg_service:
            self._logger.debug('Shutdowning internal messaging')            
            int_msg_service.get_consumer().shutdown()
        bus.int_messaging_service = None


    def on_host_init_response(self, message):
        farm_crypto_key = message.body.get('farm_crypto_key', '')
        if farm_crypto_key:
            self._cnf.write_key(self._cnf.FARM_KEY, farm_crypto_key)
            if not port_in_use(8012):
                # This cond was added to avoid 'Address already in use' 
                # when scalarizr reinitialized with `szradm --reinit` 
                self._start_int_messaging()
        else:
            self._logger.warning("`farm_crypto_key` doesn't received in HostInitResponse. " 
                    + "Cross-scalarizr messaging not initialized")

        # Not necessary, cause we've got fresh GV in HIR
        # scalarizr.handlers.sync_globals()
        self._assign_hostname()


    def _start_int_messaging(self):
        if 'mongodb' in __node__['behavior'] or 'rabbitmq' in __node__['behavior']:
            srv = IntMessagingService()
            bus.int_messaging_service = srv
            t = threading.Thread(name='IntMessageConsumer', target=srv.get_consumer().start)
            t.start()


    def _check_control_ports(self):
        defaults = __node__['defaults']['base']
        ports_changed = __node__['base']['api_port'] != defaults['api_port'] \
                or __node__['base']['messaging_port'] != defaults['messaging_port']
        if ports_changed:
            # @deprecated. expires 2014/04
            self.send_message(Messages.UPDATE_CONTROL_PORTS, {
                'api': __node__['base']['api_port'],
                'messaging': __node__['base']['messaging_port']
            })


    def on_IntServerReboot(self, message):
        # Scalarizr must detect that it was resumed after reboot
        Flag.set(Flag.REBOOT)
        # Send message 
        msg = self.new_message(Messages.REBOOT_START, broadcast=True)
        try:
            bus.fire("before_reboot_start", msg)
        finally:
            #mesage now send in scripts/reboot.py
            #self.send_message(msg)
            pass
        bus.fire("reboot_start")
        
    
    def on_IntServerHalt(self, message):
        Flag.set(Flag.HALT)
        msg = self.new_message(Messages.HOST_DOWN, broadcast=True)
        try:
            bus.fire("before_host_down", msg)
        finally:
            self.send_message(msg)
        bus.fire("host_down")


    def on_HostInit(self, message):
        if message.body.get('base') and message.local_ip == __node__["private_ip"]:
            try:
                self._logger.debug('HI.body.base: %s', message.body.get('base', {}))
                __node__['base'].update(message.body.get('base', {}))
                self._assign_hostname()
            except:
                scalarizr.handlers.fail_init()


    def on_HostInitResponse(self, message):
        if bus.cnf.state == ScalarizrState.RUNNING:
            self._logger.info("Ignoring 'HostInitResponse' message, cause state is '%s'", 
                    bus.cnf.state)
            return
        if Flag.exists(Flag.HIR):
            msg = ('Panic! Host Initialization sequence (HostInit -> HostUp) '
                    'was interrupted by Scalarizr restart or server reboot. '
                    'Scalarizr cannot continue. Exiting.')
            self._logger.error(msg)
            sys.exit(1)


        def handler(*args):
            Flag.set(Flag.HIR)
            self._check_control_ports()

            # FIXME: how about apply all HIR configuration here?
            self._logger.debug('HIR.body.base: %s', message.body.get('base', {}))
            __node__['base'].update(message.body.get('base', {}))  # update node with 'base' settings
            bus.fire("host_init_response", message)

            hostup_msg = self.new_message(Messages.HOST_UP, broadcast=True)
            hostup_msg.body['base'] = {
                'hostname': self._system_api.get_hostname()
            }
            bus.fire("before_host_up", hostup_msg)
            if bus.scalr_version >= (2, 2, 3):
                self.send_message(Messages.BEFORE_HOST_UP, broadcast=True, handle_before_host_up=True)

            self.send_message(hostup_msg)
            bus.cnf.state = ScalarizrState.RUNNING
            bus.fire("host_up")

        bus.init_op = op = self._op_api.create('system.init', handler)
        try:
            self._logger.debug('bus.init_op: %s', bus.init_op)
            op.run()
            Flag.clear(Flag.HIR)
        finally:
            bus.init_op = None


    def on_BeforeHostTerminate(self, message):
        if message.local_ip != __node__['private_ip']:
            return

        if __node__['platform'] == 'cloudstack':
            # Important! 
            # After following code run, server will loose network for some time
            # Fixes: SMNG-293
            conn = __node__['cloudstack'].connect_cloudstack()
            vm = conn.listVirtualMachines(id=__node__['cloudstack']['instance_id'])[0]
            result = conn.listPublicIpAddresses(ipAddress=vm.publicip)
            if result:
                try:
                    conn.disableStaticNat(result[0].id)
                except:
                    self._logger.warn('Failed to disable static NAT: %s', 
                            str(sys.exc_info()[1]))

        suspend = message.body.get('suspend')
        suspend = suspend and int(suspend) or False

        if suspend:
            return

        volumes = message.body.get('volumes', [])
        volumes = volumes or []

        for volume in volumes:
            try:
                volume = storage2.volume(volume)
                volume.umount()
                volume.detach()
            except:
                self._logger.warn('Failed to detach volume %s: %s',
                        volume.id, sys.exc_info()[1])

        if __node__['platform'] == 'openstack':
            conn = __node__['openstack'].connect_nova()
            sid = __node__['openstack']['server_id']
            for vol in conn.volumes.get_server_volumes(sid):
                try:
                    conn.volumes.delete_server_volume(sid, vol.id)
                except:
                    self._logger.warn('Failed to detach volume %s: %s', 
                            vol.id, str(sys.exc_info()[1]))


    def on_ScalarizrUpdateAvailable(self, message):
        self._update_package()


    def _update_package(self):
        up_script = self._cnf.rawini.get(config.SECT_GENERAL, config.OPT_SCRIPTS_PATH) + '/update'
        system2([sys.executable, up_script], close_fds=True)
        Flag.set('update')


class IntMessagingService(object):

    _msg_service = None
    
    def __init__(self):
        cnf = bus.cnf
        f = MessageServiceFactory()
        self._msg_service = f.new_service("p2p", **{
            P2pConfigOptions.SERVER_ID : cnf.rawini.get(config.SECT_GENERAL, config.OPT_SERVER_ID),
            P2pConfigOptions.CRYPTO_KEY_PATH : cnf.key_path(cnf.FARM_KEY),
            P2pConfigOptions.CONSUMER_URL : 'http://0.0.0.0:8012',
            P2pConfigOptions.MSG_HANDLER_ENABLED : False
        })


    def get_consumer(self):
        return self._msg_service.get_consumer()

    
    def new_producer(self, host):
        return self._msg_service.new_producer(endpoint="http://%s:8012" % host)


    def new_message(self, *args, **kwargs):
        return self._msg_service.new_message(*args, **kwargs)
