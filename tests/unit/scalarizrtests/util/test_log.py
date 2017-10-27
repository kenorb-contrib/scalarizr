'''
Created on 01.02.2010
 
@author: Dmytro Korsakov
'''
import unittest
import threading
from scalarizr.bus import bus
from scalarizr.util import log, init_tests
import logging
from scalarizr.messaging import MessageService, Message
try:
    import time
except ImportError:
    import timemodule as time
 
 
class _MessageProducer(object):
 
    last_message = None
 
    def send(self, log,message):
        self.last_message = message
 
class _MessageService(MessageService):
 
    producer = None
 
    def __init__(self):
        self.producer = _MessageProducer()
 
    def new_message(self, name=None, meta={}, body={}):
        return Message(name, meta, body)
 
    def get_producer(self):
        return self.producer
 
 
class Test(unittest.TestCase):
 
    _msg_producer = None
    _handler = None
    _logger = None
 
    def setUp(self):
        msg_service = _MessageService()
        bus.messaging_service = msg_service
        self._msg_producer = msg_service.get_producer()
        self._logger = logging.getLogger()
 
    def tearDown(self):
        if self._handler:
            self._logger.removeHandler(self._handler)
            self._handler.__del__()
            self._handler = None
 
    def test_log_exception(self):
        self._handler = log.MessagingHandler()
        self._logger.addHandler(self._handler)
        # Log exception
        try:
            open("/non/existed/path", "r")
        except IOError, e:
            self._logger.exception(e)
 
        message = self._wait_sender()
        # Assertions
        self.assertTrue(message, "Message was sent")
        entry = message.entries[0]
        self.assertTrue(entry['stack_trace'] is not None
                        and entry['stack_trace'].find('open("/non/existed/path", "r")') != -1,
                        "Entry contains stack trace")
        self.assertEqual(entry['level'], 'ERROR')
 
 
    def test_send_interval(self):
        # To many entries to store but send each second
        self._handler = log.MessagingHandler(num_entries=1000, send_interval='1s')
        self._logger.addHandler(self._handler)
 
        num_it = 3
        num_msg = 5
        # Do `num_it` iterations
        for i in range(num_it):
            start = time.time()
            # Do logging
            for msg in range(num_msg):
                self._logger.info("test_send_interval, iteration %d, entry %d", i, msg)
 
            self._wait_sender()
            # Assertions
            self.assertAlmostEqual(self._handler.send_interval, time.time() - start, 1)
 
 
    def test_num_entries(self):
        self._handler = log.MessagingHandler(num_entries=5, send_interval='1min')
        self._logger.addHandler(self._handler)
 
        num_it = 3
        num_msg = 5
        # Do `num_it` iterations
        for i in range(num_it):
            # Do logging
            for msg in range(num_msg):
                self._logger.info("test_num_entries, iteration %d, entry %d", i, msg)
 
            message = self._wait_sender()
            # Assertions
            self.assertEqual(len(message.entries), num_msg)
 
 
    def _wait_sender(self):
        timeout = 1
        start = time.time()
        while self._msg_producer.last_message is None and time.time() - start < timeout:
            time.sleep(0.05)
        message = self._msg_producer.last_message
        self._msg_producer.last_message = None
        print message
        return message
 
 
if __name__ == "__main__":
    init_tests()
    unittest.main()
 
