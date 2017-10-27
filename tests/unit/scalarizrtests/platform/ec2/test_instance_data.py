import unittest
from BaseHTTPServer import BaseHTTPRequestHandler, HTTPServer
from scalarizr.platform.ec2 import Ec2Platform
import threading
 
class DummyBus:
    cnf = None
 
bus = DummyBus()
 
class HttpRequestHanler(BaseHTTPRequestHandler):
 
    def do_GET(self):
        if self.path in instance_data:
            self.send_response(200)
            self.end_headers()
            self.wfile.write(instance_data[self.path])
        else:
            self.send_response(404)
 
 
 
instance_data = {"/latest/meta-data/instance-id"        : "i-12345678",
                                 "/latest/meta-data/ami-id"                     : "ami-12345678",
                                 "/latest/user-data"                            : "key=value;another_key=another_value",
                                 "/latest/meta-data/local-ipv4"         : "123.123.123.123",
                                 "/latest/meta-data/public-ipv4"        : "1.1.1.1"}
 
class TestInstanceDataRetrieveing(unittest.TestCase):
 
 
    def test_metadata(self):
        platform = Ec2Platform()
        platform._meta_url = "http://localhost:9999/"
        self.assertEqual('i-12345678', platform.get_instance_id())
        self.assertEqual('ami-12345678', platform.get_ami_id())
        self.assertEqual("value", platform.get_user_data("key"))
        self.assertEqual("another_value", platform.get_user_data("another_key"))
        self.assertEqual("123.123.123.123", platform.get_private_ip())
        self.assertEqual("1.1.1.1", platform.get_public_ip())
        self.assertTrue("1.1.1.1" in platform._metadata.values())
 
class InstanceDataServer(threading.Thread):
 
    def __init__(self):
        threading.Thread.__init__(self)
        self.server = HTTPServer(("localhost", 9999), HttpRequestHanler)
 
    def run(self):
        self.server.serve_forever()
 
    def stop(self):
        self.server.shutdown()
        self.join()
 
 
if __name__ == "__main__":
 
    server = InstanceDataServer()
    server.start()
    try:
        unittest.main()
    finally:
        server.stop()
 
