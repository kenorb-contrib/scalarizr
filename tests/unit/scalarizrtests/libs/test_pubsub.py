'''
Created on Mar 3, 2010
 
@author: marat
'''
import unittest
from scalarizr.libs.pubsub import Observable
 
class Test(unittest.TestCase):
 
    def test_extends(self):
        class Bus(Observable):
            def __init__(self):
                Observable.__init__(self)
                self.define_events("bus_event")
 
        class MessageService(Observable):
            def __init__(self):
                Observable.__init__(self)
                self.define_events("msg_event")
 
        bus = Bus()
        msg = MessageService()
        bus_events = bus.list_events()
        self.assertEqual(len(bus_events), 1)
        self.assertEqual(bus_events[0], "bus_event")
 
 
    def test_ways_to_add_listeners(self):
        o = Observable()
        o.define_events("add", "remove", "apply")
        o.on(add=None, remove=None)
 
 
    def test_all(self):
        o = Observable()
 
        args = ("undefevent", "param1", "param2")
        self.assertRaises(Exception, o.on, *args)
 
        o.define_events("add", "remove")
 
        # Add listeners
        def handler1(item):
            item["h1"] = True
        def handler2(item):
            item["h2"] = True
 
        o.on("add", handler1, handler2)
        item = {}
        o.fire("add", item)
        self.assertTrue(item["h1"])
        self.assertTrue(item["h2"])
 
 
        # Remove listener
        o.un("add", handler2)
        item = {}
        o.fire("add", item)
        self.assertTrue(item["h1"])
        self.assertFalse(item.has_key("h2"))
 
 
        # Suspend events
        o.suspend_events()
        item = {}
        o.fire("add", item)
        self.assertFalse(item.has_key("h1"))
 
 
        # Resume events
        o.resume_events()
        item = {}
        o.fire("add", item)
        self.assertTrue(item["h1"])
 
 
if __name__ == "__main__":
    unittest.main()
 
