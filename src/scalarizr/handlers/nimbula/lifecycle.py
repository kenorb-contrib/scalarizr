from __future__ import with_statement
from scalarizr.bus import bus
from scalarizr.handlers import Handler
 
def get_handlers ():
    return [NimbulaLifeCycleHandler()]
 
class NimbulaLifeCycleHandler(Handler):
 
    def __init__(self):
        bus.on("init", self.on_init)
 
    def on_init(self, *args, **kwargs):
        bus.on("before_hello", self.on_before_hello)
 
    def on_before_hello(self, message):
        """
        @param message: Hello message
        """
        pl = bus.platform
        message.instance_id = pl.get_instance_id()
 
