'''
Created on Mar 3, 2010

@author: marat
'''

import os
import sys
from scalarizr.bus import bus
from scalarizr.messaging import Messages, Queues
from scalarizr.messaging.p2p import P2pMessageService
from scalarizr.app import init_script
from scalarizr.node import __node__
from scalarizr.handlers import Handler
import logging


LOG = logging.getLogger('scalarizr.scripts.reboot')

def main ():
    init_script()
    LOG.info("Starting reboot script...")

    try:
        try:
            action = sys.argv[1]
        except IndexError:
            LOG.error("Invalid execution parameters. argv[1] must be presented")
            sys.exit()

        if action == "start" or action == "stop":
            if __node__['state'] != 'running':
                LOG.debug('Skipping RebootStart firing, server state is: {}'.format(__node__['state']))
                return

            # fire internal reboot start message for Scalarizr
            msg_service = bus.messaging_service
            producer = msg_service.get_producer()
            msg = msg_service.new_message(Messages.INT_SERVER_REBOOT)
            producer.send(Queues.CONTROL, msg)

            # fire RebootStart for Scalr
            # don't use bus.messaging_service, cause it's producer points to Scalarizr endpoint
            msg_service = P2pMessageService(
                server_id=__node__['server_id'],
                crypto_key_path=os.path.join(__node__['etc_dir'], 'private.d/keys/default'),
                producer_url=__node__['producer_url'],
                producer_retries_progression='1,2,5,10,20,30,60')
            hdlr = Handler()
            producer = msg_service.get_producer()
            msg = hdlr.new_message(Messages.REBOOT_START, srv=msg_service)
            producer.send(Queues.CONTROL, msg)

    except:
        LOG.exception('Caught exception')
