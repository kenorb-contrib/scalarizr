from __future__ import with_statement
'''
Created on Mar 3, 2010
 
@author: marat
'''
 
import sys
from scalarizr.messaging import Messages, Queues
from scalarizr.bus import bus
from scalarizr.app import init_script
import logging
try:
    import time
except ImportError:
    import timemodule as time
 
 
def main ():
    init_script()
    logger = logging.getLogger("scalarizr.scripts.reboot")
    logger.info("Starting reboot script...")
 
    try:
        try:
            action = sys.argv[1]
        except IndexError:
            logger.error("Invalid execution parameters. argv[1] must be presented")
            sys.exit()
 
        if action == "start" or action == "stop":
            msg_service = bus.messaging_service
            producer = msg_service.get_producer()
 
            msg = msg_service.new_message(Messages.INT_SERVER_REBOOT)
            producer.send(Queues.CONTROL, msg)
 
            # 30 seconds for termination
            start = time.time()
            while not msg.is_handled():
                if time.time() - start < 30:
                    time.sleep(1)
                else:
                    break
 
    except (BaseException, Exception), e:
        logger.exception(e)
 
