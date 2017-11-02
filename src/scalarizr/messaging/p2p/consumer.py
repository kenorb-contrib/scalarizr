from __future__ import with_statement
'''
Created on Dec 5, 2009

@author: marat
'''

from scalarizr.bus import bus
from scalarizr.node import __node__

# Core
from scalarizr.messaging import MessageConsumer, MessagingError
from scalarizr.messaging.p2p import P2pMessageStore, P2pMessage
from scalarizr.config import STATE
from scalarizr.util import wait_until, parse_bool, system2

# Stdlibs
from BaseHTTPServer import BaseHTTPRequestHandler, HTTPServer
from urlparse import urlparse
import threading
import logging
import sys
import os
import copy
import time
import socket
import HTMLParser
from copy import deepcopy


class P2pMessageConsumer(MessageConsumer):
    endpoint = None
    _logger = None
    _server = None
    _handler_thread = None

    #_not_empty = None
    handler_locked = False
    handler_status = 'stopped'
    handing_message_id = None

    def __init__(self, endpoint=None, msg_handler_enabled=True):
        MessageConsumer.__init__(self)
        self._logger = logging.getLogger(__name__)
        self.endpoint = endpoint

        if msg_handler_enabled:
            self._handler_thread = threading.Thread(name='MessageHandler', target=self.message_handler)
        else:
            self._handler_thread = None
        self.message_to_ack = None
        self.result_msg = None
        self.subhandler_exc_info = None
        self.ack_event = threading.Event()
        self.special_case = None
        #self._not_empty = threading.Event()

    def start(self):
        if self.running:
            raise MessagingError('Message consumer is already running')

        r = urlparse(self.endpoint)
        try:
            if self._server is None:
                self._logger.info('Building message consumer server on %s:%s', r.hostname, r.port)
                #server_class = HTTPServer if sys.version_info >= (2,6) else _HTTPServer25
                self._server = HTTPServer((r.hostname, r.port), self._get_request_handler_class())
        except (BaseException, Exception), e:
            self._logger.error("Cannot build server on port %s. %s", r.port, e)
            return

        self._logger.debug('Starting message consumer %s', self.endpoint)
        try:
            self.running = True
            if self._handler_thread:
                self._handler_thread.start()    # start message handler
            self._server.serve_forever()    # start http server
        except (BaseException, Exception), e:
            self._logger.exception(e)

    def _get_request_handler_class(self):
        class RequestHandler(BaseHTTPRequestHandler):
            consumer = None
            '''
            @cvar consumer: Message consumer instance
            @type consumer: P2pMessageConsumer
            '''

            def _msg_without_sensitive_data(self, message):
                msg_copy = P2pMessage(message.name, message.meta.copy(), deepcopy(message.body))
                msg_copy.id = message.id

                if 'platform_access_data' in msg_copy.body:
                    del msg_copy.body['platform_access_data']

                if 'global_variables' in msg_copy.body:
                    glob_vars = msg_copy.body.get('global_variables', []) or []
                    i = 0
                    for v in list(glob_vars):
                        if v.get('private'):
                            del glob_vars[i]
                            i -= 1
                        elif 'private' in v:
                            del glob_vars[i]['private']
                        i += 1

                if 'chef' in msg_copy.body:
                    try:
                        del msg_copy.body['chef']['validator_name']
                        del msg_copy.body['chef']['validator_key']
                    except (KeyError, TypeError):
                        pass
                return msg_copy

            def do_POST(self):
                logger = logging.getLogger(__name__)

                queue = os.path.basename(self.path)
                rawmsg = self.rfile.read(int(self.headers["Content-length"]))
                logger.debug("Received ingoing message in queue: '%s'", queue)

                try:
                    for f in self.consumer.filters['protocol']:
                        rawmsg = f(self.consumer, queue, rawmsg)
                        try:
                            if isinstance(rawmsg, str):
                                h = HTMLParser.HTMLParser()
                                rawmsg = h.unescape(rawmsg).encode('utf-8')
                        except:
                            logger.debug('Caught message parsing error', exc_info=sys.exc_info())

                except (BaseException, Exception), e:
                    err = 'Message consumer protocol filter raises exception: %s' % str(e)
                    logger.exception(err)
                    self.send_response(201, 'Created')
                    return

                try:
                    #logger.debug("Decoding message: %s", rawmsg)
                    message = P2pMessage()

                    mime_type = self.headers.get('Content-Type', 'application/xml')
                    format = ('application/json' in mime_type) and 'json' or 'xml'

                    if 'json' == format:
                        message.fromjson(rawmsg)
                    else:
                        message.fromxml(rawmsg)

                    msg_copy = self._msg_without_sensitive_data(message)
                    logger.debug('Decoding message: %s', msg_copy.tojson(indent=4))


                except (BaseException, Exception), e:
                    err = "Cannot decode message. error: %s; raw message: %s" % (str(e), rawmsg)
                    logger.exception(err)
                    self.send_response(201, 'Created')
                    return


                logger.debug("Received message '%s' (message_id: %s, format: %s)", message.name, message.id, format)
                #logger.info("Received ingoing message '%s' in queue %s", message.name, queue)

                try:
                    store = P2pMessageStore()
                    store.put_ingoing(message, queue, self.consumer.endpoint)
                    #self.consumer._not_empty.set()
                except (BaseException, Exception), e:
                    logger.exception(e)
                    self.send_response(500, str(e))
                    return

                self.send_response(201, 'Created')
                self.end_headers()


            def log_message(self, format, *args):
                logger = logging.getLogger(__name__)
                logger.debug(format % args)

        RequestHandler.consumer = self
        return RequestHandler

    def shutdown(self, force=False):
        self._logger.debug('entring shutdown _server: %s, running: %s', self._server, self.running)
        self.running = False
        if not self._server:
            return

        self._logger.debug('Shutdown message consumer %s ...', self.endpoint)

        self._logger.debug("Shutdown HTTP server")
        self._server.shutdown()
        self._server.socket.shutdown(socket.SHUT_RDWR)
        self._server.socket.close()
        #self._server.server_close()
        self._server = None
        self._logger.debug("HTTP server terminated")

        self._logger.debug("Shutdown message handler")
        self.handler_locked = True
        if not force:
            t = 120
            self._logger.debug('Waiting for message handler to complete it`s task. Timeout: %d seconds', t)
            wait_until(lambda: self.handler_status in ('idle', 'stopped'),
                            timeout=t, error_text='Message consumer is busy', logger=self._logger)

        if self.handing_message_id:
            store = P2pMessageStore()
            store.mark_as_handled(self.handing_message_id)

        if self._handler_thread:
            self._handler_thread.join()
            self._logger.debug("Message handler terminated")

        self._logger.debug('Message consumer %s terminated', self.endpoint)

    def _handle_one_message(self, message, queue, store):
        try:
            self.handler_status = 'running'
            self._logger.debug('Notify message listeners (message_id: %s)', message.id)
            self.handing_message_id = message.id
            for ln in list(self.listeners):
                ln(message, queue)
        except (BaseException, Exception), e:
            if message.name == 'BeforeHostUp' \
                    and message.local_ip == __node__['private_ip']:
                raise
            self._logger.exception(e)
        finally:
            self._logger.debug('Mark message (message_id: %s) as handled', message.id)
            store.mark_as_handled(message.id)
            self.handler_status = 'idle'
            self.handing_message_id = None

    def handle_host_init(self, message):
        self.message_to_ack = message
        self.result_msg = None
        self.special_case = 'HostInit'
        self.ack_event.clear()
        self._logger.debug('Waiting message acknowledge event: %s', message.name)
        self.ack_event.wait()
        self._logger.debug('Fired message acknowledge event: %s', message.name)
        return self.result_msg

    def handle_before_host_up(self, message):
        pl = bus.platform

        saved_access_data = pl.get_access_data()
        if saved_access_data:
            saved_access_data = dict(saved_access_data)

        self.message_to_ack = message
        self.result_msg = None
        self.special_case = 'BeforeHostUp'
        thread = threading.Thread(name='%sHandler' % message.name, target=self.message_handler)
        self._logger.debug('Starting message subhandler thread: %s', thread.getName())
        thread.start()
        self._logger.debug('Waiting message subhandler thread: %s', thread.getName())
        thread.join()
        self._logger.debug('Completed message subhandler thread: %s', thread.getName())
        if self.subhandler_exc_info:
            self._logger.debug('Subhandler completed with exception')
            exc_info = self.subhandler_exc_info
            self.subhandler_exc_info = None
            raise exc_info[0], exc_info[1], exc_info[2]

        if saved_access_data:
            pl.set_access_data(saved_access_data)
        return self.result_msg

    def message_handler (self):
        store = P2pMessageStore()
        self.handler_status = 'idle'

        self._logger.debug('Starting message handler')

        while self.running:
            if not self.handler_locked:
                try:
                    if self.message_to_ack:
                        for queue, message in store.get_unhandled(self.endpoint):
                            sid = self.message_to_ack.meta['server_id']
                            if message.name == self.message_to_ack.name and \
                                            message.body.get('server_id', sid) == sid:
                                self._logger.debug('Going to handle_one_message. Thread: %s', threading.currentThread().getName())
                                try:
                                    self._handle_one_message(message, queue, store)
                                except:
                                    self.subhandler_exc_info = sys.exc_info()
                                    self._logger.debug('Caught exception from _handle_one_message: thread: %s', threading.currentThread())

                                self._logger.debug('Completed handle_one_message. Thread: %s', threading.currentThread().getName())
                                self.message_to_ack = None
                                self.result_msg = message
                                self.ack_event.set()
                                if self.special_case == 'HostInit' and \
                                    parse_bool(self.result_msg.body.get('base', {}).get('reboot_after_hostinit_phase')):
                                    self._logger.debug('Hostinit case and reboot_after_hostinit_phase. Interrupting message handler')
                                    return
                                if self.special_case == 'BeforeHostUp':
                                    self._logger.debug('BeforeHostUp case. Interrupting message handler')
                                    return
                                self._logger.debug('Found a message and continue message handler')
                                break
                        time.sleep(0.1)
                        continue

                    for queue, message in store.get_unhandled(self.endpoint):
                        self._handle_one_message(message, queue, store)

                except (BaseException, Exception), e:
                    self._logger.exception(e)
            time.sleep(0.1)

        self.handler_status = 'stopped'
        self._logger.debug('Message handler stopped')
