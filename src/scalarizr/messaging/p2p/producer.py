from __future__ import with_statement
'''
Created on Dec 5, 2009

@author: marat
'''

import logging
import threading
import time
import uuid
import httplib
import urllib2
import sys
from copy import deepcopy

from scalarizr import messaging, util
from scalarizr.bus import bus
from scalarizr.messaging import p2p
from scalarizr.util import urltool
from scalarizr.node import __node__
from scalarizr.messaging.p2p import P2pMessage


class P2pMessageProducer(messaging.MessageProducer):
    endpoint = None
    retries_progression = None
    no_retry = False
    sender = 'daemon'
    _store = None
    _logger = None
    _stop_delivery = None

    def __init__(self, endpoint=None, retries_progression=None):
        messaging.MessageProducer.__init__(self)
        self.endpoint = endpoint
        if retries_progression:
            self.retries_progression = util.split_ex(retries_progression, ",")
        else:
            self.no_retry = True

        self._logger = logging.getLogger(__name__)
        self._store = p2p.P2pMessageStore()
        self._stop_delivery = threading.Event()

        self._local = threading.local()
        self._local_defaults = dict(interval=None, next_retry_index=0, delivered=False)

    def shutdown(self):
        self._stop_delivery.set()

    def send(self, queue, message):
        self._logger.debug("Sending message '%s' into queue '%s'", message.name, queue)

        if message.id is None:
            message.id = str(uuid.uuid4())
        self.fire("before_send", queue, message)
        self._store.put_outgoing(message, queue, self.sender)

        if not self.no_retry:
            if not hasattr(self._local, "interval"):
                for k, v in self._local_defaults.items():
                    setattr(self._local, k, v)

            self._local.delivered = False
            #while not self._local.delivered and not self._stop_delivery.isSet():
            while not self._local.delivered:
                if self._local.interval:
                    self._logger.debug("Sleep %d seconds before next attempt", self._local.interval)
                    time.sleep(self._local.interval)
                    # FIXME: SIGINT hanged
                    # strace:
                    # --- SIGINT (Interrupt) @ 0 (0) ---
                    # rt_sigaction(SIGINT, {0x36b9210, [], 0}, {0x36b9210, [], 0}, 8) = 0
                    # sigreturn()                             = ? (mask now [])

                    # futex(0xa3f5d78, FUTEX_WAIT_PRIVATE, 0, NUL
                #if not self._stop_delivery.isSet():
                #       self._send0(queue, message, self._delivered_cb, self._undelivered_cb)
                self._send0(queue, message, self._delivered_cb, self._undelivered_cb)
        else:
            self._send0(queue, message, self._delivered_cb, self._undelivered_cb_raises)


    def _undelivered_cb_raises(self, queue, message, ex):
        raise ex

    def _delivered_cb(self, queue, message):
        self._local.next_retry_index = 0
        self._local.interval = None
        self._local.delivered = True

    def _undelivered_cb(self, queue, message, ex):
        self._local.interval = self._get_next_interval()
        if self._local.next_retry_index < len(self.retries_progression) - 1:
            self._local.next_retry_index += 1

    def _get_next_interval(self):
        return int(self.retries_progression[self._local.next_retry_index]) * 60.0

    def _send0(self, queue, message, success_callback=None, fail_callback=None):
        try:
            use_json = __node__['message_format'] == 'json'
            data = message.tojson() if use_json else message.toxml()

            content_type = 'application/%s' % 'json' if use_json else 'xml'
            headers = {'Content-Type': content_type}

            if message.name not in ('Log',
                                    'OperationDefinition',
                                    'OperationProgress',
                                    'OperationResult'):
                msg_copy = P2pMessage(message.name, message.meta.copy(), deepcopy(message.body))
                try:
                    del msg_copy.body['chef']['validator_name']
                    del msg_copy.body['chef']['validator_key']
                except (KeyError, TypeError):
                    pass
                self._logger.debug("Delivering message '%s' %s. Json: %s, Headers: %s",
                                   message.name, msg_copy.body, use_json, headers)

            for f in self.filters['protocol']:
                data = f(self, queue, data, headers)

            url = self.endpoint + "/" + queue
            req = urllib2.Request(url, data, headers)
            opener = urllib2.build_opener(urltool.HTTPRedirectHandler())
            opener.open(req)

            self._message_delivered(queue, message, success_callback)

        except:
            e = sys.exc_info()[1]
            # Python < 2.6 raise exception on 2xx > 200 http codes except
            if isinstance(e, urllib2.HTTPError):
                if e.code == 201:
                    self._message_delivered(queue, message, success_callback)
                    return

            self._logger.warning("Message '%s' not delivered (message_id: %s)", message.name, message.id)
            self.fire("send_error", e, queue, message)

            msg = None
            if isinstance(e, urllib2.HTTPError):
                if e.code == 401:
                    self._logger.warn("Cannot authenticate on message server. %s", e.msg)
                elif e.code == 400:
                    self._logger.warn("Malformed request. %s", e.msg)
                else:
                    self._logger.warn("Cannot post message to %s. %s", url, e)
                if e.code in (400, 403, 509):
                    raise
            elif isinstance(e, urllib2.URLError):
                msg = ("Scalr messaging endpoint '{0}' is unreachable. "
                        "Cause: {1}").format(self.endpoint, e)
            elif isinstance(e, httplib.HTTPException):
                msg = ("Scalr messaging endpoint '{0}' answered with invalid HTTP response. "
                        "Cause: {1}").format(self.endpoint, e)
            else:
                self._logger.warn('Caught exception', exc_info=sys.exc_info())

            if msg:
                self._logger.warn(msg)

            # Call user code
            if fail_callback:
                fail_callback(queue, message, e)


    def _message_delivered(self, queue, message, callback=None):
        if message.name not in ('Log', 'OperationDefinition',
                                                'OperationProgress', 'OperationResult'):
            self._logger.debug("Message '%s' delivered (message_id: %s)",
                                            message.name, message.id)
        self._store.mark_as_delivered(message.id)
        self.fire("send", queue, message)
        if callback:
            callback(queue, message)
