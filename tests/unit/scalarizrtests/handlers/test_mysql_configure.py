'''
Created on Sep 12, 2010
 
@author: shaitanich
'''
import unittest
from scalarizr.handlers.mysql import MysqlCnfController
from scalarizr.handlers.mysql import _spawn_mysql
from scalarizr.util import init_tests, initdv2
from scalarizr.handlers import ServiceCtlHandler
from scalarizr.service import CnfController, CnfPreset
from scalarizr.messaging import Queues, Message
from scalarizr.bus import bus
from scalarizr.queryenv import Preset
 
class _EmptyQueryEnv:
    def get_service_configuration(self):
        return []
 
class _MysqlCnfController(MysqlCnfController):
 
    ROOT_USER = 'root'
    root_password = ''
 
    def _get_connection(self):
        return _spawn_mysql(self.ROOT_USER, self.root_password)
 
 
class _QueryEnv:
 
    def get_service_configuration(self, behaviour):
        return Preset(
                name = "test_preset1",
                settings = {'1':'new one', '2':'new two'}
                        )
 
class _ServiceCtlHandler(ServiceCtlHandler):
 
    def new_message(self, msg_name, msg_body=None, msg_meta=None, broadcast=False, include_pad=False, srv=None):
        msg = Message(name = msg_name, meta = msg_meta, body = msg_body)
        return msg
 
    def send_message(self, msg_name, msg_body=None, msg_meta=None, broadcast=False,
            queue=Queues.CONTROL):
        msg = msg_name if isinstance(msg_name, Message) else \
                self.new_message(msg_name, msg_body, msg_meta, broadcast)
        print "Status:", msg.status
 
class MockCnfController(CnfController):
 
    preset = None
 
    def __init__(self):
        self.preset = CnfPreset(name='current', settings={'1':'old_value'})
 
    def current_preset(self):
        print "current preset:", self.preset
        return self.preset
 
    def apply_preset(self, preset):
        self.preset = preset
        print "Applying preset:", self.preset
 
class MockHandler(_ServiceCtlHandler):
 
    SERVICE_NAME = 'Mock'
    initd = None
 
    def __init__(self):
        self._initd = initdv2.lookup('mysql')
        #_ServiceCtlHandler.__init__(self,self.SERVICE_NAME, self._initd, MockCnfController())
        super(MockHandler, self).__init__(self.SERVICE_NAME, self._initd, MockCnfController())
 
class MockMessage():
 
    behaviour = None
    restart_service = None
    reset_to_defaults = None
 
    def __init__(self, behaviour=None, reset_to_defaults=False, restart_service=False):
        self.behaviour = behaviour
        self.reset_to_defaults = reset_to_defaults
        self.restart_service = restart_service
 
class TestMysqlCnfController(unittest.TestCase):
 
    def setUp(self):
        self.ctl = _MysqlCnfController()
        self.default_preset = self.ctl.current_preset()
        pass
 
    def tearDown(self):
        self.ctl.apply_preset(self.default_preset)
        pass
 
    def test_current_preset(self):
 
        preset = self.ctl.current_preset()
        self.assertEqual(preset.settings['log_warnings'], '1')
 
        preset.settings['ololo'] = 'trololo'
 
        preset.settings['log_warnings'] = '0'
        self.ctl.apply_preset(preset)
        new_preset = self.ctl.current_preset()
        self.assertEqual(new_preset.settings['log_warnings'],preset.settings['log_warnings'])
 
    def test_comparator(self):
        ctl = ServiceCtlHandler(None, None)
        self.assertFalse(ctl.preset_changed({'1':'one', '2':'two'}, {'1':'one', '2':'two', '3':'three'}))
        self.assertFalse(ctl.preset_changed({'1':'one', '2':'two', 'key_cache_age_threshold':''}, {'1':'one', '2':'two', 'join_buffer_size':''}))
 
    def test_ServiceCtlHandler(self):
        bus.queryenv_service = _QueryEnv()
        handler = MockHandler()
        handler.sc_on_configured(MockHandler.SERVICE_NAME)
        handler.sc_on_start()
        handler.on_UpdateServiceConfiguration(MockMessage(MockHandler.SERVICE_NAME))
        handler.on_UpdateServiceConfiguration(MockMessage(MockHandler.SERVICE_NAME, reset_to_defaults=True))
        handler.on_UpdateServiceConfiguration(MockMessage(MockHandler.SERVICE_NAME, restart_service=True))
        handler.on_UpdateServiceConfiguration(MockMessage(MockHandler.SERVICE_NAME, True, True))
 
 
    def test_get_mysql_version(self):
        self.assertEqual(self.ctl._get_mysql_version(), (5, 1, 41))
 
if __name__ == "__main__":
    init_tests()
    unittest.main()
 
