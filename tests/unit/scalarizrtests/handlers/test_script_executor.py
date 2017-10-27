'''
Created on Apr 11, 2012
 
@author: marat
'''
import mock
 
from scalarizr.bus import bus
from scalarizr import handlers, config as szrconfig
from scalarizr.handlers import script_executor
from scalarizr.config import ScalarizrState
 
import os
import platform
import binascii
import threading
import ConfigParser
 
 
 
class TestScriptExecutor(object):
 
    @classmethod
    def setup_class(cls):
        bus.queryenv_service = mock.Mock()
        #bus.queryenv_service.get_producer.return_value = mock.Mock()
        bus.platform = mock.Mock()
        bus.cnf = mock.Mock()
        bus.cnf.state = ScalarizrState.RUNNING
        handlers.operation = mock.Mock()
 
    def test_fetch_scripts_from_message(self):
        pass
 
    def test_fetch_scripts_from_queryenv(self):
        pass
 
 
class TestScript(object):
    DATA = dict(
            name='mike',
            body='#!/usr/bin/python\nimport platform; print platform.dist()',
            asynchronous=True,
            exec_timeout=30,
            event_name='BeforeMySQLMasterUp'
    )
 
    DATA_TIMEOUTED = dict(
            name='mike.time',
            body='#!/usr/bin/python\nimport time; time.sleep(5)',
            asynchronous=False,
            exec_timeout=2,
            event_name='HostInit'
    )
 
    def setup(self):
        self.old_tmp_stat = os.stat('/tmp')
        os.chmod('/tmp', 0777)
        script_executor.exec_dir_prefix = '/tmp/scalr-scripting.'
        script_executor.logs_dir = '/tmp'
 
 
 
    def teardown(self):
        os.chmod('/tmp', self.old_tmp_stat.st_mode)
 
    def assert_wait(self, script):
        data = script.wait()
        assert data['stderr'] == '\n'
        assert data['stdout'] ==  binascii.b2a_base64(str(platform.dist()) + '\n')
        assert data['time_elapsed']
        assert data['event_name'] == self.DATA['event_name']
        assert data['return_code'] == 0
 
    def test_ok(self):
        script = script_executor.Script(**self.DATA)
        script.start()
        assert type(script.pid) == int
        assert script.interpreter == '/usr/bin/python'
        assert script.exec_path.startswith('/')
        assert script.stderr_path.startswith('/')
        assert script.stdout_path.startswith('/')
 
        self.assert_wait(script)
 
    def test_interrupted_and_running(self):
        script = script_executor.Script(**self.DATA)
        script.start()
 
        state = script.state()
        assert state['id']
        assert state['pid']
        assert type(state['start_time']) == float
        assert state['interpreter']
 
        restored = script_executor.Script(**state)
 
        self.assert_wait(restored)
 
    def test_interrupted_and_terminated(self):
        script = script_executor.Script(**self.DATA)
        script.start()
        state = script.state()
        script.wait() # wait termination
 
        restored = script_executor.Script(**state)
        self.assert_wait(restored)
 
    def test_timeouted(self):
        script = script_executor.Script(**self.DATA_TIMEOUTED)
        script.start()
        data = script.wait()
 
        assert data['return_code'] == -9 # killed
 
    def test_interrupted_and_timeouted(self):
        pass
 
