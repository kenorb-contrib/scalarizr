from __future__ import with_statement
'''
Created on Mar 31, 2010

@author: marat
'''

from BaseHTTPServer import BaseHTTPRequestHandler, HTTPServer
from scalarizr.messaging import Messages

def msg_main():
    class HttpRequestHanler(BaseHTTPRequestHandler):
        def do_POST(self):
            self.send_response(201)
    server = HTTPServer(("localhost", 9999), HttpRequestHanler)
    server.serve_forever()


def queryenv_main():
    import cgi

    class HttpRequestHanler(BaseHTTPRequestHandler):
        def do_POST(self):
            form = cgi.FieldStorage(
                    fp=self.rfile,
                    headers=self.headers,
                    environ={
                            'REQUEST_METHOD' : 'POST',
                            'CONTENT_TYPE' : self.headers['Content-Type']
                    }
            )

            op = form["operation"].value

            if op == "get-https-certificate":
                self.send_response(200)
                self.end_headers()

                xml = """<?xml version="1.0" encoding="UTF-8"?>
                        <response>
                                <cert>MIICWjCCAhigAwIBAgIESPX5.....1myoZSPFYXZ3AA9kwc4uOwhN</cert>
                                <pkey>MIICWjCCAhigAwIBAgIESPX5.....1myoZSPFYXZ3AA9kwc4uOwhN</pkey>
                        </response>
                        """
                self.wfile.write(xml)

            elif op == "list-roles":
                self.send_response(200)
                self.end_headers()

                xml = """<?xml version="1.0" encoding="UTF-8"?>
                        <response>
                                        <roles>
                                                        <role behaviour="cassandra" name="szr-ubuntu10-cassandra">
                                                                        <hosts>
                                                                        </hosts>
                                                        </role>
                                        </roles>
                        </response>
                                """
                self.wfile.write(xml)

            elif op == "list-ebs-mountpoints":
                self.send_response(200)
                self.end_headers()
                xml = """<?xml version="1.0" encoding="UTF-8"?>
                        <response>
                        <mountpoints>
                        <mountpoint name='some_name_for_LVM' dir='/mnt/storage1' createfs='1' isarray='0'>
                        <volumes>
                        <volume volume-id='vol-fb8e3492' device='/dev/sdh'></volume>
                        </volumes>
                        </mountpoint>
                        </mountpoints>
                        </response>
                        """
                self.wfile.write(xml)

            elif op == "list-virtualhosts":
                self.send_response(200)
                self.end_headers()

                xml = """<?xml version="1.0" encoding="UTF-8"?>
                        <response>
                                <vhosts>
                                        <vhost hostname="test-example.scalr.net" type="apache">
                                                <raw><![CDATA[
                        <VirtualHost *:80>
                        DocumentRoot /var/www/test/
                        ServerName test-example.scalr.net
                        CustomLog     /var/log/apache2/test-example.scalr.net-access.log combined
                        ErrorLog      /var/log/apache2/test-example.scalr.net-error.log
                        </VirtualHost>
                                                        ]]></raw>
                                        </vhost>

                                        <vhost hostname="test-ssl-example.scalr.net" https="1" type="apache">
                                                <raw><![CDATA[
                        <VirtualHost *:443>
                        DocumentRoot /var/www/test_ssl/
                        ServerName test-ssl-example.scalr.net
                        CustomLog     /var/log/apache2/test-ssl-example.scalr.net-access.log combined
                        ErrorLog      /var/log/apache2/test-ssl-example.scalr.net-error.log
                        </VirtualHost>
                                                        ]]></raw>
                                        </vhost>
                                </vhosts>
                        </response>
                        """
                self.wfile.write(xml)

            elif op == "list-scripts":
                self.send_response(200)
                self.end_headers()

                if 0 and form["event"].value != Messages.EXEC_SCRIPT_RESULT:
                    xml = """<?xml version="1.0" encoding="UTF-8"?>
                            <response>
                            <scripts>
                    <script asynchronous="1" exec-timeout="100" name="phpinfo">
                    <body><![CDATA[#!/usr/bin/php
<? phpinfo() ?>]]></body>
                                    </script>
                                    <script  asynchronous="0" exec-timeout="100" name="python-info">
                                            <body><![CDATA[#!/usr/bin/python
import os
import pkgutil
import pprint
import sys
from cgi import escape

def dl(tuples):
output = u''
output += '<dl>\n'
for title, description in tuples:
if title:
output += '  <dt>%s</dt>\n' % escape(title)
if description:
output += '  <dt>%s</dt>\n' % escape(description)
output += '</dl>\n'
return output

def group(seq):
result = {}
for item, category in seq:
result.setdefault(category, []).append(item)
return result

def get_packages():
return set([modname for importer, modname, ispkg in
       pkgutil.walk_packages(onerror=lambda x:x)
       if ispkg and '.' not in modname])

def format_packages():
packages = group((pkg, pkg[0].lower()) for pkg in get_packages())
# convert ('a',['apackage','anotherapackage]) into ('a', 'apackage, anotherapackage')
packages = [(letter, ', '.join(pkgs)) for letter, pkgs in packages.items()]
return '<h2>Installed Packages</h2>\n%s' % dl(sorted(packages))

def format_environ(environ):
return '<h2>Environment</h2>\n%s' % dl(sorted(environ.items()))

def format_python_path():
# differentiate between eggs and regular paths
eggs = [p for p in sys.path if p.endswith('.egg')]
paths = [p for p in sys.path if p not in eggs]
return dl([('Paths', ',\n'.join(paths)),
   ('Eggs', ',\n'.join(eggs)),
  ])

def format_version():
version, platform = sys.version.split('\n')
sysname, nodename, release, osversion, machine = os.uname()
return '<h2>Version</h2>\n%s' % dl([
('Python Version', version),
('Build Platform', platform),
('OS', sysname),
('OS Version', osversion),
('Machine Type', machine),])

def format():
output = u''
output += '<h1>Python Info</h1>\n'
output += format_version()
output += format_python_path()
output += format_environ(os.environ)
output += format_packages()
return output

def page(html):
print "Content-type: text/html"
print
print '<html>\n<head><title>%s Python configuration</title></head>' % os.uname()[1]
print '<body>\n%s</body>\n</html>' % html

if __name__ == '__main__':
page(format())]]></body>
                                    </script>
                            </scripts>
                      </response>
                            """
                else:
                    xml = """<?xml version="1.0" encoding="UTF-8"?><response><scripts/></response>"""

                self.wfile.write(xml)
            else:
                self.send_response(400)
                self.end_headers()
                self.wfile.write("Unknown operatation '%s'" % (op))


    server = HTTPServer(("localhost", 9998), HttpRequestHanler)
    server.serve_forever()
