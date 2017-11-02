from __future__ import with_statement
import copy
import logging
import xml.dom.minidom as dom
try:
    import json
except ImportError:
    import simplejson as json

from scalarizr.libs.bases import Observable
from scalarizr.util import xml_strip

LOG = logging.getLogger(__name__)

class MessagingError(BaseException):
    pass

class MessageServiceFactory(object):
    _adapters = {}

    def new_service (self, name, **params):
        if not self._adapters.has_key(name):
            adapter =  __import__("scalarizr.messaging." + name,
                                                      globals(), locals(), ["new_service"])
            self._adapters[name] = adapter
        return self._adapters[name].new_service(**params)


class MessageService(object):
    def new_message(self, name=None, meta=None, body=None):
        pass

    def new_consumer(self, **params):
        pass

    def new_producer(self, **params):
        pass


class MetaOptions(object):
    SERVER_ID       = "server_id"
    PLATFORM        = "platform" # ec2, vps, rs
    OS                      = "os" # linux, win, sunos
    DIST            = "dist"
    REQUEST_ID      = "request_id"
    TIMESTAMP       = "timestamp"
    SZR_VERSION = "szr_version"


class Message(object):

    id = None
    name = None
    meta = None
    body = None

    def __init__(self, name=None, meta=None, body=None):
        self.id = None
        self.name = name
        self.meta = meta or {}
        self.body = body or {}

    def __setattr__(self, name, value):
        if name in ("id", "name", "meta", "body"):
            object.__setattr__(self, name, value)
        else:
            self.body[name] = value

    def __getattr__(self, name):
        return self.body[name] if name in self.body else None

    def is_handled(self):
        pass

    def is_delivered(self):
        pass

    def is_responce_received(self):
        pass

    def get_response(self):
        pass

    def fromjson(self, json_str):
        if isinstance(json_str, str):
            json_str = json_str.decode('utf-8')

        json_obj = json.loads(json_str)
        for attr in  ('id', 'name', 'meta', 'body'):
            assert attr in json_obj, 'Attribute required: %s' % attr
            setattr(self, attr, copy.copy(json_obj[attr]))

    def fromxml (self, xml):
        if isinstance(xml, str):
            xml = xml.decode('utf-8')
        doc = dom.parseString(xml.encode('utf-8'))
        xml_strip(doc)

        root = doc.documentElement
        self.id = root.getAttribute("id")
        self.name = root.getAttribute("name")

        for ch in root.firstChild.childNodes:
            self.meta[ch.nodeName] = self._walk_decode(ch)
        for ch in root.childNodes[1].childNodes:
            self.body[ch.nodeName] = self._walk_decode(ch)


    def tojson(self, indent=None):
        result = dict(id=self.id, name=self.name,
                                  body=self.body, meta=self.meta)

        return json.dumps(result, ensure_ascii=True, indent=indent)


    def _walk_decode(self, el):
        if el.firstChild and el.firstChild.nodeType == 1:
            if all((ch.nodeName == "item" for ch in el.childNodes)):
                return list(self._walk_decode(ch) for ch in el.childNodes)
            else:
                return dict(tuple((ch.nodeName, self._walk_decode(ch)) for ch in el.childNodes))
        else:
            return el.firstChild and el.firstChild.nodeValue or None

    def __str__(self):
        from xml.dom.minidom import getDOMImplementation
        impl = getDOMImplementation()
        doc = impl.createDocument(None, "message", None)

        root = doc.documentElement;
        root.setAttribute("id", str(self.id))
        root.setAttribute("name", str(self.name))

        meta = doc.createElement("meta")
        root.appendChild(meta)
        self._walk_encode(self.meta, meta, doc)

        body = doc.createElement("body")
        root.appendChild(body)
        self._walk_encode(self.body, body, doc)

        return doc.toxml('utf-8')

    toxml = __str__

    def _walk_encode(self, value, el, doc):
        if getattr(value, '__iter__', False):
            if getattr(value, "keys", False):
                for k, v in value.items():
                    itemEl = doc.createElement(str(k))
                    el.appendChild(itemEl)
                    self._walk_encode(v, itemEl, doc)
            else:
                for v in value:
                    itemEl = doc.createElement("item")
                    el.appendChild(itemEl)
                    self._walk_encode(v, itemEl, doc)
        else:
            '''
            if not isinstance(value, unicode):
                    if value is not None:
                            value = str(value)
                    else:
                            value = ''
            el.appendChild(doc.createTextNode(value))
            '''
            if value is not None and not isinstance(value, unicode):
                value = str(value).decode('utf-8')
            el.appendChild(doc.createTextNode(value or ''))


class MessageProducer(Observable):
    filters = None
    """
    Out message filter
    Filter is a callable f(producer, queue, message, headers)
    """

    def __init__(self):
        Observable.__init__(self)
        self.filters = {
                'data' : [],
                'protocol' : []
        }
        self.define_events(
                # Fires before message is send
                "before_send",
                # Fires after message is send
                "send",
                # Fires when error occures
                "send_error"
        )

    def send(self, queue, message):
        pass

    def shutdown(self):
        pass

class MessageConsumer(object):
    filters = None
    """
    In message filters
    Filter is a callable f(consumer, queue, message)
    """

    listeners = None
    running = False

    def __init__(self):
        self.listeners = []
        self.filters = {
                'data' : [],
                'protocol' : []
        }

    def start(self):
        pass

    def shutdown(self):
        pass

class Queues:
    CONTROL = "control"
    LOG = "log"

class Messages:
    ###
    # Scalarizr events
    ###

    HELLO = "Hello"
    """
    Fires when Scalarizr wants to remind Scalr of himself
    @ivar behaviour
    @ivar local_ip
    @ivar remote_ip
    @ivar role_name
    """

    HOST_INIT = "HostInit"
    """
    @broadcast
    Fires when scalarizr is initialized and ready to be configured
    @ivar behaviour
    @ivar local_ip
    @ivar remote_ip
    @ivar role_name
    """

    BEFORE_HOST_UP = "BeforeHostUp"
    """
    @broadcast
    @blocking
    Fires before HostUp
    @ivar behaviour
    @ivar local_ip
    @ivar remote_ip
    @ivar role_name
    """

    HOST_UP = "HostUp"
    """
    @broadcast
    Fires when server is ready to play it's role
    """

    HOST_DOWN = "HostDown"
    """
    @broadcast
    Fires when server is terminating
    """

    REBOOT_START = "RebootStart"
    """
    Fires when scalarizr is going to reboot
    """

    REBOOT_FINISH = "RebootFinish"
    """
    @broadcast
    Fires when scalarizr is resumed after reboot
    """

    RESTART = "Restart"
    """
    @broadcast
    Fires when server is resumed after stop
    """

    BLOCK_DEVICE_ATTACHED = "BlockDeviceAttached"
    """
    Fires when block device was attached
    """

    BLOCK_DEVICE_DETACHED = "BlockDeviceDetached"
    """
    Fires when block device was detached
    """

    BLOCK_DEVICE_MOUNTED = "BlockDeviceMounted"
    """
    Fires when block device was mounted
    """

    EXEC_SCRIPT_RESULT = "ExecScriptResult"
    """
    Fires after script execution
    """

    REBUNDLE_RESULT = "RebundleResult"
    """
    Fires after rebundle task finished
    """

    DEPLOY_RESULT = 'DeployResult'
    '''
    Fires after deployment finished
    '''

    OPERATION_DEFINITION = 'OperationDefinition'
    '''
    Fires before long timed operation
    '''

    OPERATION_PROGRESS = 'OperationProgress'
    '''
    Log message described operation progress
    '''

    OPERATION_RESULT = 'OperationResult'
    '''
    Operation result message
    '''

    UPDATE_CONTROL_PORTS = "UpdateControlPorts"

    REBUNDLE_LOG = "RebundleLog"

    DEPLOY_LOG = "DeployLog"

    LOG = "Log"

    ###
    # Scalr events
    ###

    VHOST_RECONFIGURE = "VhostReconfigure"

    MOUNTPOINTS_RECONFIGURE = "MountPointsReconfigure"

    HOST_INIT_RESPONSE = "HostInitResponse"

    REBUNDLE = "Rebundle"

    DEPLOY = 'Deploy'

    SCALARIZR_UPDATE_AVAILABLE = "ScalarizrUpdateAvailable"

    BEFORE_HOST_TERMINATE = "BeforeHostTerminate"

    BEFORE_INSTANCE_LAUNCH = "BeforeInstanceLaunch"

    DNS_ZONE_UPDATED = "DNSZoneUpdated"

    IP_ADDRESS_CHANGED = "IPAddressChanged"

    SCRIPTS_LIST_UPDATED = "ScriptsListUpdated"

    EXEC_SCRIPT = "ExecScript"

    UPDATE_SERVICE_CONFIGURATION = "UpdateServiceConfiguration"

    UPDATE_SERVICE_CONFIGURATION_RESULT = "UpdateServiceConfigurationResult"

    SSL_CERTIFICATE_UPDATE = "SSLCertificateUpdate"

    ###
    # Internal events
    ###

    INT_BLOCK_DEVICE_UPDATED = "IntBlockDeviceUpdated"
    """
    Fired by scripts/udev.py when block device was added/updated/removed
    """

    INT_SERVER_REBOOT = "IntServerReboot"
    """
    Fired by scripts/reboot.py when server is going to reboot
    """

    INT_SERVER_HALT = "IntServerHalt"
    """
    Fired by scripts/halt.py when server is going to halt
    """

    UPDATE_SSH_AUTHORIZED_KEYS = "UpdateSshAuthorizedKeys"

    WIN_PREPARE_BUNDLE = "Win_PrepareBundle"

    WIN_PREPARE_BUNDLE_RESULT = "Win_PrepareBundleResult"

    WIN_HOST_DOWN = "Win_HostDown"
