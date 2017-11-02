from __future__ import with_statement
'''
Created on Sep 29, 2011

@author: marat
'''

# @xxx: not tested

def mask(n):
    ''' return a mask of n bits as a long integer '''
    return (2L<<n-1) - 1

def ip2num(ip):
    ''' Convert decimal dotted quad string to long integer'''
    parts = ip.split('.')
    parts = map(int, parts)
    if len(parts) != 4:
        parts = (parts + [0, 0, 0, 0])[:4]
    return (parts[0] << 24) | (parts[1] << 16) | (parts[2] << 8) | parts[3]

def network_mask(ip, bits):
    "Convert a network address to a long integer"
    return ip2num(ip) & mask(bits)


class CIDR(object):
    def __init__(self, cidr):
        ip, bits = cidr.split('/')
        self.mask = network_mask(ip, int(bits))

    def __contains__(self, ip):
        return ip2num(ip) & self.mask
