'''
Created on Oct 14, 2010
 
@author: marat
'''
from scalarizr.bus import bus
from scalarizr.messaging.p2p import P2pMessageStore, P2pMessage
from scalarizr.messaging.p2p.consumer import P2pMessageConsumer
from scalarizr.messaging.p2p.producer import P2pMessageProducer
from scalarizr.messaging import Queues
 
import unittest, os
import szr_unittest
from szr_unittest import RESOURCE_PATH, switch_db, switch_reset_db, reset_db
import shutil, threading, time, socket
from urlparse import urlparse
 
 
class MessageStoreTest(unittest.TestCase):
    def setUp(self):
        self.dbfile = os.path.join(RESOURCE_PATH, 'messaging/p2p/db-for-rotate.sqlite')
        shutil.copy(self.dbfile, self.dbfile + '.0')
        switch_db(self.dbfile)
 
    def tearDown(self):
        shutil.copy(self.dbfile + '.0', self.dbfile)
 
    def test_rotate(self):
        store = P2pMessageStore()
        db = bus.db
        conn = db.get().get_connection()
        cur = conn.cursor()
        cur.execute('SELECT COUNT(*) FROM p2p_message')
        self.assertEqual(cur.fetchone()[0], 114)
        store.rotate()
        cur.execute('SELECT COUNT(*) FROM p2p_message')
        self.assertEqual(cur.fetchone()[0], 50)
 
class MessagingTest(unittest.TestCase):
    ENDPOINT = 'http://0.0.0.0:8813'
 
 
    consumer = None
    producer = None
    sock_address = None
 
    def setUp(self):
        switch_reset_db()
 
        self.producer = P2pMessageProducer(self.ENDPOINT)
 
        url = urlparse(self.ENDPOINT)
        self.sock_address = (url.hostname, url.port)
        self.consumer = P2pMessageConsumer(self.ENDPOINT)
        t = threading.Thread(target=lambda: self.consumer.start())
        t.start()
        time.sleep(1)
 
    def tearDown(self):
        reset_db()
        self.consumer.shutdown()
        self.producer.shutdown()
 
    def test_start_shutdown(self):
        # Start server
        self.assertTrue(self.consumer.running)
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            s.connect(self.sock_address)
        finally:
            s.close()
 
        # Shutdown server
        self.consumer.shutdown()
        self.assertFalse(self.consumer.running)
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            s.connect(self.sock_address)
            self.fail()
        except:
            pass
        finally:
            s.close()
 
    def test_send_recv(self):
        exp_name = 'TestMessage'
        exp_queue = 'TestQueue'
 
        def listener(message, queue):
            self.assertEqual(message.name, exp_name)
            self.assertEqual(queue, exp_queue)
        self.consumer.listeners.append(listener)
 
        msg = P2pMessage(exp_name)
        self.producer.send(exp_queue, msg)
 
 
 
class MessagingExTest(unittest.TestCase):
    ENDPOINT = 'http://0.0.0.0:8813'
    ENDPOINT2 = 'http://0.0.0.0:8812'
 
    producer = producer2 = None
    consumer = consumer2 = None
 
    def setUp(self):
        switch_reset_db()
 
        self.producer = P2pMessageProducer(self.ENDPOINT)
        self.producer2 = P2pMessageProducer(self.ENDPOINT2)
 
        self.consumer = P2pMessageConsumer(self.ENDPOINT)
        t = threading.Thread(target=lambda: self.consumer.start())
        t.start()
 
        self.consumer2 = P2pMessageConsumer(self.ENDPOINT2)
        t = threading.Thread(target=lambda: self.consumer2.start())
        t.start()
 
        time.sleep(1)
 
    def tearDown(self):
        self.consumer.shutdown()
        self.consumer2.shutdown()
        self.producer.shutdown()
        self.producer2.shutdown()
 
    def test_send_recv(self):
        msg = 'ScalrMessage'
        msg2 = 'InternalMessage'
        expected_msg = None
 
        def listener(message, queue):
            self.assertFalse(expected_msg is None)
            self.assertEqual(message.name, expected_msg)
 
        self.consumer.listeners.append(listener)
        self.consumer2.listeners.append(listener)
 
        expected_msg = msg
        self.producer.send(Queues.CONTROL, P2pMessage(msg))
        expected_msg = msg2
        self.producer2.send(Queues.CONTROL, P2pMessage(msg2))
 
 
 
if __name__ == "__main__":
    szr_unittest.main()
    unittest.main()
 
