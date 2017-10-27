'''
Created on Jan 20, 2012
 
@author: marat
'''
 
import binascii
import hashlib
import time
import hmac
import logging
 
from M2Crypto.EVP import Cipher
from M2Crypto import Rand
 
CRYPTO_ALGO = dict(name="des_ede3_cbc", key_size=24, iv_size=8)
LOG = logging.getLogger(__name__)
 
 
def _init_cipher(key, op_enc=1):
    skey = key[0:CRYPTO_ALGO["key_size"]]     # Use first n bytes as crypto key
    iv = key[-CRYPTO_ALGO["iv_size"]:]         # Use last m bytes as IV
    return Cipher(CRYPTO_ALGO["name"], skey, iv, op_enc)
 
def encrypt (s, key):
    c = _init_cipher(key, 1)
    ret = c.update(s)
    ret += c.final()
    del c
    return binascii.b2a_base64(ret)
 
def decrypt (s, key):
    c = _init_cipher(key, 0)
    ret = c.update(binascii.a2b_base64(s))
    ret += c.final()
    del c
    return ret
 
def keygen(length=40):
    return binascii.b2a_base64(Rand.rand_bytes(length))
 
def _get_canonical_string (params={}):
    s = ""
    for key, value in sorted(params.items()):
        s = s + str(key) + str(value)
    return s
 
def sign(data, key, time_struct=None):
    #LOG.debug('sign key: %s', key)
    date = time.strftime("%a %d %b %Y %H:%M:%S UTC", time_struct or time.gmtime())
    #LOG.debug('sign date: %s', date)
    if hasattr(data, "__iter__"):
        canonical_string = _get_canonical_string(data)
    else:
        canonical_string = data
    canonical_string += date
    #LOG.debug('sign canonical string: %s', canonical_string)
 
    digest = hmac.new(key, canonical_string, hashlib.sha1).digest()
    sign = binascii.b2a_base64(digest)
    if sign.endswith('\n'):
            sign = sign[:-1]
    #LOG.debug('final signature: %s', sign)
    return sign, date
 
def validate(signature, date, data, key):
    time_struct = time.strptime(date, "%a %d %b %Y %H:%M:%S UTC")
    calc_sign = sign(data, key, time_struct)[0]
    LOG.debug('Validatating signature:\n'
            '  time: %s\n'
            '  data: %s\n'
            '  client signature: %s\n'
            '  expected signature: %s', 
            time_struct, data, signature, calc_sign)
    return calc_sign == signature
 
