import os
import re
import shutil
import logging
import socket
import glob
 
from scalarizr import handlers, linux
from scalarizr.bus import bus
from scalarizr.linux import pkgmgr, execute
from scalarizr.messaging import Messages
from scalarizr.util import initdv2, firstmatched
from scalarizr.node import __node__
 
 
LOG = logging.getLogger(__name__)
 
__tomcat__ = __node__['tomcat']
__tomcat__.update({
    'catalina_home_dir': None,
    'java_home': firstmatched(lambda path: os.access(path, os.X_OK), [
            linux.system('echo $JAVA_HOME', shell=True)[0].strip(),
            '/usr/java/default'], 
            '/usr'),
    'config_dir': None,
    'install_type': None
})
 
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
 
 
class CatalinaInitScript(initdv2.ParametrizedInitScript):
    def __init__(self):
        initdv2.ParametrizedInitScript.__init__(self, 'tomcat', 
                __tomcat__['catalina_home_dir'] + '/bin/catalina.sh')
        self.server_port = None
 
    def status(self):
        if not self.server_port:
            out = augtool(['print /files{0}/server.xml/Server/#attribute/port'.format(__tomcat__['config_dir'])])
            self.server_port = out.split(' = ')[-1]
 
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            sock.connect(('', self.server_port))
            return initdv2.Status.RUNNING
        except:
            return initdv2.Status.NOT_RUNNING
        finally:
            try:
                sock.close()
            except:
                pass
 
 
def augload():
    path = __tomcat__['config_dir']
    return [
        'set /augeas/load/Xml/incl[last()+1] "{0}/*.xml"'.format(path),
        'load',
        'defvar service /files{0}/server.xml/Server/Service'.format(path)                       
    ]
 
def augtool(script_lines):
    augscript = augload() + script_lines
    augscript = '\n'.join(augscript)
    LOG.debug('augscript: %s', augscript)
    return linux.system(('augtool', ), stdin=augscript)[0].strip()
 
 
class TomcatHandler(handlers.Handler, handlers.FarmSecurityMixin):
 
    def __init__(self):
        handlers.Handler.__init__(self)
        handlers.FarmSecurityMixin.__init__(self, [8080, 8443])
        bus.on(
            init=self.on_init, 
            start=self.on_start
        )
 
        # try to read CATALINA_HOME from environment
        __tomcat__['catalina_home_dir'] = linux.system('echo $CATALINA_HOME', shell=True)[0].strip()
        if not __tomcat__['catalina_home_dir']:
            # try to locate CATALINA_HOME in /opt/apache-tomcat*
            try:
                __tomcat__['catalina_home_dir'] = glob.glob('/opt/apache-tomcat*')[0]
            except IndexError:
                pass
 
        if __tomcat__['catalina_home_dir']:
            __tomcat__['install_type'] = 'binary'
            __tomcat__['config_dir'] = '{0}/conf'.format(__tomcat__['catalina_home_dir'])
            init_script_path = '/etc/init.d/tomcat'
            if os.path.exists(init_script_path):
                self.service = initdv2.ParametrizedInitScript('tomcat', init_script_path)
            else:
                self.service = CatalinaInitScript()
        else:
            __tomcat__['install_type'] = 'package'
            if linux.os.debian_family:
                if (linux.os['name'] == 'Ubuntu' and linux.os['version'] >= (12, 4)) or \
                    (linux.os['name'] == 'Debian' and linux.os['version'] >= (7, 0)):
                    tomcat_version = 7
                else:
                    tomcat_version = 6
            else:
                tomcat_version = 6
            __tomcat__['config_dir'] = '/etc/tomcat{0}'.format(tomcat_version)
            init_script_path = '/etc/init.d/tomcat{0}'.format(tomcat_version)  
            self.service = initdv2.ParametrizedInitScript('tomcat', init_script_path)
 
    def on_init(self):
        bus.on(
            host_init_response=self.on_host_init_response,
            before_host_up=self.on_before_host_up
        )
 
    def on_start(self):
        if __node__['state'] == 'running':
            self.service.start()
 
    def accept(self, message, queue, behaviour=None, **kwds):
        return message.name in (
                Messages.HOST_INIT, 
                Messages.HOST_DOWN) and 'tomcat' in behaviour
 
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
 
        pkgmgr.installed('augeas-tools' if linux.os.debian_family else 'augeas')
 
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
 
 
