from __future__ import with_statement
'''
Created on Mar 1, 2010

@author: marat
'''

import os
import time

from scalarizr.messaging import Messages, Queues
from scalarizr.bus import bus
from scalarizr.util import initdv2
from scalarizr.app import init_script
import logging

def main():
    init_script()   
    logger = logging.getLogger("scalarizr.scripts.udev")
    logger.info("Starting udev script...")


    
    try:
        initd = initdv2.lookup('scalarizr')
        if initd.running:
            channel = '/tmp/udev-block-device'
            with open(channel, 'w+') as fp:
                fp.write(os.environ['DEVNAME'])

            msg_service = bus.messaging_service
            producer = msg_service.get_producer()
        
            msg = msg_service.new_message(Messages.INT_BLOCK_DEVICE_UPDATED)
            for k, v in os.environ.items():
                msg.body[k.lower()] = v
            producer.send(Queues.CONTROL, msg)
    
    except (BaseException, Exception), e:
        logger.exception(e)
    finally:
        time.sleep(2)