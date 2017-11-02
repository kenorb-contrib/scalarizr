import os
import re
import shutil
import logging

from scalarizr import handlers, linux
from scalarizr.bus import bus
from scalarizr.linux import pkgmgr, execute, iptables
from scalarizr.node import __node__
from scalarizr.api import tomcat as tomcat_api
from scalarizr.api.tomcat import augtool


LOG = logging.getLogger(__name__)

__tomcat__ = __node__['tomcat']

def get_handlers():
    return [TomcatHandler()]


class KeytoolExec(execute.BaseExec):
    executable = '{0}/bin/keytool'.format(__tomcat__['java_home'])

    # keytool uses long args with a short prefix
    def _default_handler(self, key, value, cmd_args):
        cmd_args.append('-{0}'.format(key))
        if value is not True:
            cmd_args.append(str(value))

    # last param is a keytool command that should be first
    def _after_all_handlers(self, cmd_args):
        return ['-{0}'.format(cmd_args[-1])] + cmd_args[0:-1]


class TomcatHandler(handlers.Handler):

    def __init__(self):
        handlers.Handler.__init__(self)
        bus.on(
            init=self.on_init, 
            start=self.on_start
        )
        self.api = tomcat_api.TomcatAPI()
        self.service = self.api.service


    def on_init(self):
        bus.on(
            host_init_response=self.on_host_init_response,
            before_host_up=self.on_before_host_up
        )
        self._insert_iptables_rules()

    def on_start(self):
        if __node__['state'] == 'running':
            self.service.start()


    def on_host_init_response(self, hir_message):
        '''
        if not os.path.exists(self.service.initd_script):
            tomcat = 'tomcat{0}'.format(self.tomcat_version)
            pkgs = [tomcat]
            if linux.os.debian_family:
                pkgs.append('{0}-admin'.format(tomcat))
            elif linux.os.redhat_family or linux.os.oracle_family:
                pkgs.append('{0}-admin-webapps'.format(tomcat))
            for pkg in pkgs:
                pkgmgr.installed(pkg)
        '''

        pkgmgr.installed('augeas-tools' if linux.os.debian_family else 'augeas', updatedb=True)

    '''
    def _aug_load_tomcat(self, aug):
        aug.set('/augeas/load/Xml/incl[last()+1]', '{0}/*.xml'.format(__tomcat__['config_dir']))
        aug.load()
        file_ = __tomcat__['config_dir'] + '/server.xml'
        path = '/augeas/files{0}/error'.format(file_)
        if aug.match(path):
            msg = 'AugeasError: {0}. file: {1} line: {2} pos: {3}'.format(
                aug.get(path + '/message'),
                file_,
                aug.get(path + '/line'),
                aug.get(path + '/pos'))
            raise Exception(msg)
        aug.defvar('service', '/files{0}/Server/Service'.format(file_))
    '''


    def on_before_host_up(self, message):
        # Fix XML prolog in server.xml
        config_dir = __tomcat__['config_dir']
        fp = open(config_dir + '/server.xml')
        prolog = fp.readline()
        fp.close()
        if prolog.startswith("<?xml version='1.0'"):
            LOG.info('Making xml prolog in server.xml compatible with augtool')
            shutil.copy(config_dir + '/server.xml', config_dir + '/server.xml.0')
            with open(config_dir + '/server.xml.0') as fpr:
                with open(config_dir + '/server.xml', 'w') as fpw:
                    fpr.readline()  # skip xml prolog
                    fpw.write('<?xml version="1.0" encoding="utf-8"?>\n')
                    for line in fpr:
                        fpw.write(line)
            os.remove(config_dir + '/server.xml.0')


        # Enable SSL
        if not '8443' in augtool(['print $service/Connector/*/port']):
            if __tomcat__['install_type'] == 'binary':
                # catalina.sh shows error when tomcat is not running
                if self.service.running:
                    self.service.stop()
            else:
                self.service.stop()

            keystore_path = config_dir + '/keystore'
            if not os.path.exists(keystore_path):
                LOG.info('Initializing keystore in %s', keystore_path)
                keytool = KeytoolExec()
                keytool.start('genkey', 
                    alias='tomcat', 
                    keystore=keystore_path, 
                    storepass='changeit', 
                    keypass='changeit', 
                    dname='CN=John Smith')

            # Detect keystore type
            keytool = KeytoolExec()
            out = keytool.start('list', 
                keystore=keystore_path, 
                storepass='changeit')[1]
            keystore_type = 'jks'
            for line in out.splitlines():
                m = re.search(r'^Key store type: (.+)$', line)
                if m:
                    keystore_type = m.group(1)
                    break
            LOG.info('Keystore type: %s', keystore_type)

            LOG.info('Enabling HTTPS on 8443')
            augscript = [
                'set $service/Connector[last()+1]/#attribute/port 8443',
                'defvar attrs $service/Connector[last()]/#attribute',
                'set $attrs/protocol org.apache.coyote.http11.Http11NioProtocol',
                'set $attrs/SSLEnabled true',
                'set $attrs/maxThreads 150',
                'set $attrs/scheme https',
                'set $attrs/keystoreFile {0}'.format(keystore_path),
                'set $attrs/keystoreType {0}'.format(keystore_type),
                'set $attrs/secure true',
                'set $attrs/clientAuth false',
                'set $attrs/sslProtocol TLS',
                'save'
            ]
            augtool(augscript)


        # TODO: Import PEM cert/pk into JKS
        # openssl pkcs12 -export -in cert.pem -inkey key.pem > server.p12
        # keytool -importkeystore -srckeystore server.p12 -destkeystore server.jks -srcstoretype pkcs12

        self.service.start()


    def _insert_iptables_rules(self):
        if iptables.enabled():
            for port in (8080, 8443):
                iptables.FIREWALL.ensure([{
                    "jump": "ACCEPT", 
                    "protocol": "tcp", 
                    "match": "tcp", 
                    "dport": str(port)
                }])
