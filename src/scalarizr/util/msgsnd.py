from __future__ import with_statement
'''
Created on Dec 16, 2009

@author: marat
'''
from scalarizr import init_script
from scalarizr.bus import bus
from optparse import OptionParser
import sys


def main():

    parser = OptionParser(usage="Usage: %prog [options] key=value key2=value2 ...")
    parser.add_option("-e", "--endpoint", dest="endpoint", default=None, help="Messaging server URL")
    parser.add_option("-q", "--queue", dest="queue", help="Queue to send message into")
    parser.add_option("-n", "--name", dest="name", help="Message name")
    parser.add_option("-f", "--file", dest="msgfile", help="File with message")

    (options, args) = parser.parse_args()

    if not options.queue or (not options.msgfile and not options.name):
        print parser.format_help()
        sys.exit()


    init_script()

    msg_service = bus.messaging_service
    producer = msg_service.get_producer()
    if options.endpoint:
        producer.endpoint = options.endpoint

    msg = msg_service.new_message()

    if options.msgfile:
        str = None
        with open(options.msgfile, 'r') as fp:
            str = fp.read()
        if str:
            msg.fromxml(str)

    if msg.name:
        msg.name = options.name

    for pair in args:
        k, v = pair.split("=")
        msg.body[k] = v

    producer.send(options.queue, msg)

    print "Done"


"""
<?xml version="1.0" ?>
<message id="037b1864-4539-4201-ac0b-5b1609686c80" name="Rebundle">
        <meta>
                <server_id>ab4d8acc-f001-4666-8f87-0748af52f700</server_id>
        </meta>
        <body>
                <platform_access_data>
                        <account_id>*account_id*</account_id>
                        <key_id>*key_id*</key_id>
                        <key>*key*</key>
                        <cert>*cert*</cert>
                        <pk>*pk*</pk>
                </platform_access_data>
                <role_name>euca-base-1</role_name>
                <bundle_task_id>567</bundle_task_id>
                <excludes><excludes>
        </body>
</message>
"""
