from __future__ import with_statement
'''
Created on Apr 7, 2010

@author: marat
'''

import binascii
import hmac
import hashlib
import re
import os
import time

try:
    with_m2crypto = True
    from M2Crypto.EVP import Cipher
except ImportError:
    with_m2crypto = False
    from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
    from cryptography.hazmat.primitives import padding
    from cryptography.hazmat.backends import default_backend


crypto_algo = dict(name="des_ede3_cbc", key_size=24, iv_size=8)


def keygen(length=40):
    return binascii.b2a_base64(os.urandom(length))

if with_m2crypto:
    def _init_cipher(key, op_enc=1):
        skey = key[0:crypto_algo["key_size"]]   # Use first n bytes as crypto key
        iv = key[-crypto_algo["iv_size"]:]              # Use last m bytes as IV
        return Cipher(crypto_algo["name"], skey, iv, op_enc)

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

else:
    def _new_cipher(key):
        skey = key[0:crypto_algo["key_size"]]   # Use first n bytes as crypto key
        iv = key[-crypto_algo["iv_size"]:]      # Use last m bytes as IV
        return Cipher(algorithms.TripleDES(skey), modes.CBC(iv), backend=default_backend())

    def _new_padding():
        return padding.PKCS7(64)

    def encrypt(s, key):
        enc = _new_cipher(key).encryptor()
        pad = _new_padding().padder()
        padded = pad.update(s) + pad.finalize()
        encrypted = enc.update(padded) + enc.finalize()
        return binascii.b2a_base64(encrypted)

    def decrypt(s, key):
        dec = _new_cipher(key).decryptor()
        unpad = _new_padding().unpadder()
        encrypted = binascii.a2b_base64(s)
        padded = dec.update(encrypted) + dec.finalize()
        return unpad.update(padded) + unpad.finalize()


def _get_canonical_string (params=None):
    params = params or {}
    s = ""
    for key, value in sorted(params.items()):
        s = s + str(key) + str(value)
    return s

def sign_http_request(data, key, timestamp=None):
    date = time.strftime("%a %d %b %Y %H:%M:%S %Z", timestamp or time.gmtime())
    canonical_string = _get_canonical_string(data) if hasattr(data, "__iter__") else data
    canonical_string += date

    digest = hmac.new(key, canonical_string, hashlib.sha1).digest()
    sign = binascii.b2a_base64(digest)
    if sign.endswith('\n'):
        sign = sign[:-1]
    return sign, date

def pwgen(size):
    return re.sub('[^\w]', '', keygen(size*2))[:size]

def calculate_md5_sum(path):
    md5_sum = hashlib.md5()
    with open(path, 'rb') as f:
        while True:
            data = f.read(4096)
            if not data:
                break
            md5_sum.update(data)
    return md5_sum.hexdigest()

