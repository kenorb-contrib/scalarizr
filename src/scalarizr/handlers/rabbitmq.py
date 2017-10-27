from __future__ import with_statement
'''
Created on Sep 7, 2011
 
@author: Spike
'''
from __future__ import with_statement
 
import os
import re
import sys
import pwd
import logging
 
 
from scalarizr.bus import bus
from scalarizr.messaging import Messages
from scalarizr import storage2
from scalarizr.handlers import HandlerError, ServiceCtlHandler, build_tags
from scalarizr.config import BuiltinBehaviours
from scalarizr.util import initdv2, software, dns, cryptotool
from scalarizr.node import __node__
from scalarizr.linux import iptables
import scalarizr.services.rabbitmq as rabbitmq_svc
 
 
 
__rabbitmq__ = __node__['rabbitmq']
 
 
BEHAVIOUR = SERVICE_NAME = BuiltinBehaviours.RABBITMQ
OPT_VOLUME_CNF = 'volume_config'
OPT_SNAPSHOT_CNF = 'snapshot_config'
DEFAULT_STORAGE_PATH = '/var/lib/rabbitmq/mnesia'
STORAGE_VOLUME_CNF = 'rabbitmq.json'
RABBITMQ_MGMT_PLUGIN_NAME = 'rabbitmq_management'
RABBITMQ_MGMT_AGENT_PLUGIN_NAME = 'rabbitmq_management_agent'
RABBITMQ_ENV_CFG_PATH = '/etc/rabbitmq/rabbitmq-env.conf'
 
 
 
 
class RabbitMQMessages:
    RABBITMQ_RECONFIGURE = 'RabbitMq_Reconfigure'
    RABBITMQ_SETUP_CONTROL_PANEL = 'RabbitMq_SetupControlPanel'
    RABBITMQ_RECONFIGURE_RESULT = 'RabbitMq_ReconfigureResult'
    RABBITMQ_SETUP_CONTROL_PANEL_RESULT = 'RabbitMq_SetupControlPanelResult'
 
    INT_RABBITMQ_HOST_INIT = 'RabbitMq_IntHostInit'
 
 
def get_handlers():
    return (RabbitMQHandler(), )
 
 
class RabbitMQHandler(ServiceCtlHandler):
 
    def __init__(self):
        try:
            software.which('rabbitmqctl')
        except LookupError:
            raise HandlerError("Rabbitmqctl binary was not found. Check your installation.")
 
        bus.on("init", self.on_init)
 
        self._logger = logging.getLogger(__name__)
        self.rabbitmq = rabbitmq_svc.rabbitmq
        self.service = initdv2.lookup(BuiltinBehaviours.RABBITMQ)
        self._service_name = BEHAVIOUR
        self.on_reload()
 
        if 'ec2' == self.platform.name:
            self._logger.debug('Setting hostname_as_pubdns to 0')
            __ec2__ = __node__['ec2']
            __ec2__['hostname_as_pubdns'] = 0
 
 
    def on_init(self):
        bus.on("host_init_response", self.on_host_init_response)
        bus.on("before_host_up", self.on_before_host_up)
        bus.on("before_hello", self.on_before_hello)
        bus.on("start", self.on_start)
 
        if bus.event_defined('rebundle_cleanup_image'):
            bus.on("rebundle_cleanup_image", self.cleanup_hosts_file)
        bus.on("before_host_down", self.on_before_host_down)
 
 
    def on_start(self):
        self._insert_iptables_rules()
 
        if 'running' == __node__['state']:
            self._set_nodename_in_env()
            rabbitmq_vol = __rabbitmq__['volume']
 
            if not __rabbitmq__['volume'].mounted_to():
                self.service.stop()
                rabbitmq_vol.ensure()
            self.service.start()
 
            __rabbitmq__['volume'] = rabbitmq_vol
 
 
    def on_reload(self):
        self.queryenv = bus.queryenv_service
        self.platform = bus.platform
 
    def accept(self, message, queue, behaviour=None, platform=None, os=None, dist=None):
        return BEHAVIOUR in behaviour and message.name in (Messages.HOST_INIT,
                                                           Messages.HOST_DOWN,
                                                           Messages.UPDATE_SERVICE_CONFIGURATION,
                                                           Messages.BEFORE_HOST_TERMINATE,
                                                           RabbitMQMessages.RABBITMQ_RECONFIGURE,
                                                           RabbitMQMessages.RABBITMQ_SETUP_CONTROL_PANEL,
                                                           RabbitMQMessages.INT_RABBITMQ_HOST_INIT)
 
 
    def _insert_iptables_rules(self):
        if iptables.enabled():
            iptables.FIREWALL.ensure([
                {"jump": "ACCEPT", "protocol": "tcp", "match": "tcp", "dport": '5672'},
                {"jump": "ACCEPT", "protocol": "tcp", "match": "tcp", "dport": '15672'},
                {"jump": "ACCEPT", "protocol": "tcp", "match": "tcp", "dport": '55672'},
                {"jump": "ACCEPT", "protocol": "tcp", "match": "tcp", "dport": '4369'}
            ])
 
 
    def cleanup_hosts_file(self, rootdir):
        """ Clean /etc/hosts file """
        hosts_path = os.path.join(rootdir, 'etc', 'hosts')
        if os.path.isfile(hosts_path):
            try:
                dns.ScalrHosts.HOSTS_FILE_PATH = hosts_path
                for hostname in dns.ScalrHosts.hosts().keys():
                    dns.ScalrHosts.delete(hostname=hostname)
            finally:
                dns.ScalrHosts.HOSTS_FILE_PATH = '/etc/hosts'
 
    def on_before_hello(self, message):
        try:
            rabbit_version = software.rabbitmq_software_info()
        except:
            raise HandlerError("Can't find rabbitmq on this server.")
 
        if rabbit_version.version < (2, 7, 0):
            self._logger.error("Unsupported RabbitMQ version. Assertion failed: %s >= 2.7.0",
                                            '.'.join(rabbit_version.version))
            sys.exit(1)
 
 
    def on_RabbitMq_SetupControlPanel(self, message):
        try:
            if not 'running' == __node__['state']:
                raise HandlerError('Server is not in RUNNING state yet')
            try:
                self.service.stop()
                self.rabbitmq.enable_plugin(RABBITMQ_MGMT_PLUGIN_NAME)
            finally:
                self.service.start()
 
            panel_url = 'http://%s:55672/mgmt/' % self.platform.get_public_ip()
            msg_body = dict(status='ok', cpanel_url=panel_url)
        except:
            error = str(sys.exc_info()[1])
            msg_body = dict(status='error', last_error=error)
        finally:
            self.send_message(RabbitMQMessages.RABBITMQ_SETUP_CONTROL_PANEL_RESULT, msg_body)
 
 
    def on_RabbitMq_Reconfigure(self, message):
        try:
            if not 'running' == __node__['state']:
                raise HandlerError('Server is not in RUNNING state yet')
 
            if message.node_type != __rabbitmq__['node_type']:
                self._logger.info('Changing node type to %s' % message.node_type)
 
                disk_node = message.node_type == rabbitmq_svc.NodeTypes.DISK
 
                hostname_ip_pairs = self._get_cluster_nodes()
                nodes_to_cluster_with = []
 
                for hostname, ip in hostname_ip_pairs:
                    nodes_to_cluster_with.append(hostname)
                    dns.ScalrHosts.set(ip, hostname)
 
                if nodes_to_cluster_with or disk_node:
                    self_hostname = __rabbitmq__['hostname']
                    self.rabbitmq.change_node_type(self_hostname,
                                            nodes_to_cluster_with, disk_node)
                else:
                    raise HandlerError('At least 1 disk node should'
                                            'present in cluster')
 
 
                __rabbitmq__['node_type'] = message.node_type
            else:
                raise HandlerError('Node type is already %s' % message.node_type)
 
            msg_body = dict(status='ok', node_type=message.node_type)
        except:
            error = str(sys.exc_info()[1])
            msg_body = dict(status='error', last_error=error)
        finally:
            self.send_message(RabbitMQMessages.RABBITMQ_RECONFIGURE_RESULT, msg_body)
 
 
    def on_HostInit(self, message):
        if not BuiltinBehaviours.RABBITMQ in message.behaviour:
            return
 
        if message.local_ip != self.platform.get_private_ip():
            hostname = rabbitmq_svc.RABBIT_HOSTNAME_TPL % message.server_index
            self._logger.info("Adding %s as %s to hosts file", message.local_ip, hostname)
            dns.ScalrHosts.set(message.local_ip, hostname)
 
 
    on_RabbitMq_IntHostInit = on_HostInit
 
 
    def on_HostDown(self, message):
        if not BuiltinBehaviours.RABBITMQ in message.behaviour:
            return
        dns.ScalrHosts.delete(message.local_ip)
 
 
    def on_before_host_down(self, msg):
        self.service.stop()
 
 
    def on_BeforeHostTerminate(self, msg):
        if msg.remote_ip == self.platform.get_public_ip() and \
                                int(__rabbitmq__['server_index']) != 1:
            self.service.stop()
 
 
    def _set_nodename_in_env(self):
        node_name = rabbitmq_svc.NODE_HOSTNAME_TPL % __rabbitmq__['hostname']
        os.environ.update(dict(RABBITMQ_NODENAME=node_name))
 
        env_cfg = ''
        if os.path.exists(RABBITMQ_ENV_CFG_PATH):
            with open(RABBITMQ_ENV_CFG_PATH) as f:
                env_cfg = f.read()
 
        if 'RABBITMQ_NODENAME' in env_cfg:
            env_cfg = re.sub(re.compile('^(RABBITMQ_NODENAME=(?!%s).*)$' % node_name, re.M), '#\g<0>', env_cfg)
        if not re.search(re.compile('^RABBITMQ_NODENAME=%s' % node_name, re.M), env_cfg):
            env_cfg += '\nRABBITMQ_NODENAME=%s' % node_name
 
        with open(RABBITMQ_ENV_CFG_PATH, 'w') as f:
            f.write(env_cfg)
 
 
    def on_host_init_response(self, message):
        log = bus.init_op.logger
        log.info('Accept Scalr configuration')
 
        if not message.body.has_key("rabbitmq"):
            raise HandlerError("HostInitResponse message for RabbitMQ behaviour must have 'rabbitmq' property")
 
        rabbitmq_data = message.rabbitmq.copy()
 
        if not rabbitmq_data['password']:
            rabbitmq_data['password'] = cryptotool.pwgen(10)
 
        self.cleanup_hosts_file('/')
 
        if os.path.exists(RABBITMQ_ENV_CFG_PATH):
            os.remove(RABBITMQ_ENV_CFG_PATH)
 
        if not os.path.isdir(DEFAULT_STORAGE_PATH):
            os.makedirs(DEFAULT_STORAGE_PATH)
 
        rabbitmq_user = pwd.getpwnam("rabbitmq")
        os.chown(DEFAULT_STORAGE_PATH, rabbitmq_user.pw_uid, rabbitmq_user.pw_gid)
 
        self._logger.info('Performing initial cluster reset')
 
        self.service.stop()
 
        hostname = rabbitmq_svc.RABBIT_HOSTNAME_TPL % int(message.server_index)
        __rabbitmq__['hostname'] = hostname
        dns.ScalrHosts.set('127.0.0.1', hostname)
        self._set_nodename_in_env()
 
        self.service.start()
        self.rabbitmq.stop_app()
        self.rabbitmq.reset()
        self.service.stop()
 
        # Use RABBITMQ_NODENAME instead of setting actual hostname
        #with open('/etc/hostname', 'w') as f:
        #    f.write(hostname)
        #system2(('hostname', '-F', '/etc/hostname'))
 
        volume_config = rabbitmq_data.pop('volume_config')
        volume_config['mpoint'] = DEFAULT_STORAGE_PATH
        rabbitmq_data['volume'] = storage2.volume(volume_config)
        rabbitmq_data['volume'].tags = self.rabbitmq_tags
 
        __rabbitmq__.update(rabbitmq_data)
 
 
    def _is_storage_empty(self, storage_path):
        for subdir in os.listdir(storage_path):
            if subdir.startswith('rabbit'):
                return False
        return True
 
 
    def on_before_host_up(self, message):
        log = bus.init_op.logger
 
        log.info('Create storage')
        hostname_ip_pairs = self._get_cluster_nodes()
        nodes_to_cluster_with = []
        server_index = __node__['server_index']
        msg_body = dict(server_index=server_index)
 
        for hostname, ip in hostname_ip_pairs:
            nodes_to_cluster_with.append(hostname)
            dns.ScalrHosts.set(ip, hostname)
            try:
                self.send_int_message(ip, 
                    RabbitMQMessages.INT_RABBITMQ_HOST_INIT,
                    msg_body, 
                    broadcast=True)
            except:
                e = sys.exc_info()[1]
                self._logger.warning("Can't deliver internal message"
                                " to server %s: %s" % (ip, e))
 
        rabbitmq_volume = __rabbitmq__['volume']
        rabbitmq_volume.ensure(mkfs=True, mount=True)
        __rabbitmq__['volume'] = rabbitmq_volume
 
        rabbitmq_user = pwd.getpwnam("rabbitmq")
        os.chown(DEFAULT_STORAGE_PATH, rabbitmq_user.pw_uid, rabbitmq_user.pw_gid)
 
 
        log.info('Patch configuration files')
        # Check if it's first run here, before rabbit starts
        init_run = self._is_storage_empty(DEFAULT_STORAGE_PATH)
        if init_run:
            self._logger.debug("Storage is empty. Assuming it's "
                                    "initial run.")
 
        do_cluster = True if nodes_to_cluster_with else False
        self._logger.debug('Nodes to cluster with: %s' %
                                        nodes_to_cluster_with)
 
        is_disk_node = self.rabbitmq.node_type == rabbitmq_svc.NodeTypes.DISK
 
        self_hostname = __rabbitmq__['hostname']
 
        self._logger.debug('Enabling management agent plugin')
        self.rabbitmq.enable_plugin(RABBITMQ_MGMT_AGENT_PLUGIN_NAME)
 
        cookie = __rabbitmq__['cookie']
        self._logger.debug('Setting erlang cookie: %s' % cookie)
        self.rabbitmq.set_cookie(cookie)
 
        self.service.start()
 
        log.info('Join cluster')
        if do_cluster and (not is_disk_node or init_run):
            self._logger.info('Joining cluster with other nodes.')
            self.rabbitmq.cluster_with(self_hostname,
                                    nodes_to_cluster_with, is_disk_node)
 
        self.rabbitmq.delete_user('guest')
        scalr_user_password = __rabbitmq__['password']
        self.rabbitmq.check_scalr_user(scalr_user_password)
 
        master_user_password = __rabbitmq__['password']
        self.rabbitmq.check_master_user(master_user_password)
 
        cluster_nodes = self.rabbitmq.cluster_nodes()
        if not all([node in cluster_nodes for node in nodes_to_cluster_with]):
            raise HandlerError('Cannot cluster with all role servers')
 
        log.info('Collect HostUp data')
        # Update message
        msg_data = dict()
        msg_data['volume_config'] = dict(__rabbitmq__['volume'])
        msg_data['node_type'] = self.rabbitmq.node_type
        msg_data['password'] = scalr_user_password
        self._logger.debug('Updating HostUp message with %s' % msg_data)
        message.rabbitmq = msg_data
 
 
    @property
    def rabbitmq_tags(self):
        return build_tags(BEHAVIOUR, 'active')
 
 
    def _get_cluster_nodes(self):
        nodes = []
        for role in self.queryenv.list_roles(behaviour = BEHAVIOUR):
            for host in role.hosts:
                ip = host.internal_ip
                hostname = rabbitmq_svc.RABBIT_HOSTNAME_TPL % host.index
                nodes.append((hostname, ip))
        return nodes
 
