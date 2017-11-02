from __future__ import with_statement
'''
Created on Aug 10, 2010

@author: marat
'''

# Core
from scalarizr.bus import bus
from scalarizr.messaging import MessagingError

# Utils
from scalarizr.util import cryptotool

# Stdlibs
import logging, binascii, sys


class P2pMessageSecurity(object):
    server_id = None
    crypto_key_path = None
    _logger = None
    def __init__(self, server_id=None, crypto_key_path=None):
        self._logger = logging.getLogger(__name__)
        self.server_id = server_id
        self.crypto_key_path = crypto_key_path

    def in_protocol_filter(self, consumer, queue, message):
        try:
            # Decrypt message
            cnf = bus.cnf
            self._logger.debug('Decrypting message')
            crypto_key = binascii.a2b_base64(cnf.read_key(self.crypto_key_path))
            xml = cryptotool.decrypt(message, crypto_key)

            # Remove special chars
            return xml.strip(''.join(chr(i) for i in range(0, 31)))

        except:
            self._logger.debug('Decryption error', exc_info=sys.exc_info())
            self._logger.debug('Crypto key: %s', crypto_key)
            self._logger.debug('Raw message: %s', message)
            raise MessagingError('Cannot decrypt message')

    def out_protocol_filter(self, producer, queue, message, headers):
        try:
            # Encrypt message
            cnf = bus.cnf
            self._logger.debug('Encrypting message')
            crypto_key = binascii.a2b_base64(cnf.read_key(self.crypto_key_path))
            data = cryptotool.encrypt(message, crypto_key)

            # Generate signature
            signature, timestamp = cryptotool.sign_http_request(data, crypto_key)

            # Add extra headers
            headers['Date'] = timestamp
            headers['X-Signature'] = signature
            headers['X-Server-Id'] = self.server_id

            return data
        except (BaseException, Exception), e:
            raise MessagingError('Cannot encrypt message. error: %s' % (e))
