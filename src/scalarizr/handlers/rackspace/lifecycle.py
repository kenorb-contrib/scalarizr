from __future__ import with_statement
 
from scalarizr.bus import bus
from scalarizr.handlers import Handler
from scalarizr.config import ScalarizrState
from scalarizr.linux import iptables
from scalarizr.node import __node__
import logging
 
 
def get_handlers ():
    return [RackspaceLifeCycleHandler()]
 
class RackspaceLifeCycleHandler(Handler):
    _logger = None
    _platform = None
    _cnf = None
 
    def __init__(self):
        self._logger = logging.getLogger(__name__)
        bus.on(init=self.on_init, reload=self.on_reload)
        self.on_reload()
 
    def on_init(self, *args, **kwargs):
        self._insert_iptables_rules()
 
    def on_reload(self):
        self._cnf = bus.cnf
        self._platform = bus.platform
 
 
    def _insert_iptables_rules(self):
        self._logger.debug('Adding iptables rules for scalarizr ports')
 
        if iptables.enabled():
            rules = [
                    {"jump": "ACCEPT", "protocol": "tcp", "match": "tcp", "dport": "8008"},
                    {"jump": "ACCEPT", "protocol": "tcp", "match": "tcp", "dport": "8012"},
                    {"jump": "ACCEPT", "protocol": "tcp", "match": "tcp", "dport": "8013"},
                    {"jump": "ACCEPT", "protocol": "udp", "match": "udp", "dport": "8014"},
            ]
            iptables.FIREWALL.ensure(rules)
 
