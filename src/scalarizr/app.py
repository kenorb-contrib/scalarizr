
import sys

try:
    import httplib2
    httplib2_loaded = True
except ImportError:
    httplib2_loaded = False

# Core
from scalarizr import __version__
from scalarizr import config, rpc, linux, api
from scalarizr.node import __node__
from scalarizr.linux import coreutils, mount

from scalarizr.bus import bus
from scalarizr.config import CmdLineIni, ScalarizrCnf, ScalarizrState, ScalarizrOptions, STATE
from scalarizr.storage import Storage
from scalarizr.handlers import MessageListener
from scalarizr.messaging import MessageServiceFactory, MessageService, MessageConsumer, Queues, Messages
from scalarizr.messaging.p2p import P2pConfigOptions
from scalarizr.platform import meta, PlatformFactory, UserDataOptions
from scalarizr.queryenv import new_queryenv
from scalarizr.api.binding import jsonrpc_http

from scalarizr.linux import pkgmgr
if linux.os.windows:
    import win32com
    import win32com.client
    from scalarizr.util import wintool

if linux.os.windows:
    import win32timezone as os_time
else:
    from datetime import datetime as os_time

# Utils
from scalarizr import util
from scalarizr.util import initdv2, log, PeriodicalExecutor
from scalarizr.util import SqliteLocalObject, daemonize, system2, firstmatched, format_size
from scalarizr.util import wait_until, sqlite_server
from scalarizr.util.flag import Flag

# Stdlibs
import cStringIO
import logging
import logging.config
import os, shutil, time, uuid
import sqlite3 as sqlite
import threading, socket, signal
from optparse import OptionParser
from urlparse import urlparse, urlunparse
import urllib
import urllib2
import pprint
import select
import wsgiref.simple_server
import SocketServer

from scalarizr import exceptions

if not linux.os.windows:
    import pwd
    import ctypes
    libc = ctypes.CDLL('libc.so.6')

    def res_init():
        return libc.__res_init()


class ScalarizrError(BaseException):
    pass

class NotConfiguredError(BaseException):
    pass


EMBED_SNMPD = True
NET_SNMPD = False

SNMP_RESTART_DELAY = 5 # Seconds
SNMP_POLL_INTERVAL = 10 # Seconds

if linux.os.windows_family:
    from scalarizr.util import reg_value
    INSTALL_DIR = reg_value('InstallDir')
else:
    INSTALL_DIR = '/'

PID_FILE = os.path.join(INSTALL_DIR, 'var', 'run', 'scalarizr.pid')

LOGFILES_BASEPATH = os.path.join(INSTALL_DIR, 'var', 'log')
LOG_PATH = os.path.join(LOGFILES_BASEPATH, 'scalarizr.log')
LOG_DEBUG_PATH = os.path.join(LOGFILES_BASEPATH, 'scalarizr_debug.log')

LOGGING_CONFIG = r'''
[loggers]
keys=root,scalarizr

[handlers]
keys=console,user_log,debug_log

[formatters]
keys=debug,user

[logger_root]
level=DEBUG
handlers=console,user_log,debug_log

[logger_scalarizr]
level=DEBUG
qualname=scalarizr
handlers=console,user_log,debug_log
propagate=0

[handler_console]
class=StreamHandler
level=ERROR
formatter=user
args=(sys.stderr,)

[handler_user_log]
class=scalarizr.util.log.RotatingFileHandler
level=INFO
formatter=user
args=(r'LOG_PATH', 'a+', 5242880, 5, 0600)

[handler_debug_log]
class=scalarizr.util.log.RotatingFileHandler
level=DEBUG
formatter=debug
args=(r'LOG_DEBUG_PATH', 'a+', 5242880, 5, 0600)

[formatter_debug]
format=%(asctime)s - %(levelname)s - %(name)s - %(message)s
class=scalarizr.util.log.DebugFormatter

[formatter_user]
format=%(asctime)s - %(levelname)s - %(name)s - %(message)s
class=scalarizr.util.log.UserFormatter
'''
LOGGING_CONFIG = LOGGING_CONFIG.replace('LOG_PATH', LOG_PATH)
LOGGING_CONFIG = LOGGING_CONFIG.replace('LOG_DEBUG_PATH', LOG_DEBUG_PATH)


'''
True when scalarizr daemon should be running
'''

_pid = None
'''
Scalarizr main process PID
'''

_logging_configured = False


class ScalarizrInitScript(initdv2.ParametrizedInitScript):
    def __init__(self):
        initdv2.ParametrizedInitScript.__init__(self, 
            'scalarizr', 
            '/etc/init.d/scalarizr', 
            socks=[initdv2.SockParam(8013)]
        )


class ScalrUpdClientScript(initdv2.Daemon):
    def __init__(self):
        name = 'ScalrUpdClient' if linux.os.windows_family else 'scalr-upd-client'
        super(ScalrUpdClientScript, self).__init__(name)

    if not linux.os.windows:
        def restart(self):
            self.stop()
            pid_file = '/var/run/scalr-upd-client.pid'
            if os.access(pid_file, os.R_OK):
                pid = open(pid_file).read().strip()
                if pid:
                    try:
                        os.kill(int(pid), signal.SIGKILL)
                    except:
                        pass
                    os.unlink(pid_file)
            self.start()

def _init():
    optparser = bus.optparser
    bus.base_path = os.path.realpath(os.path.dirname(__file__) + "/../..")
    
    _init_logging()
    logger = logging.getLogger(__name__)    
    
    # Initialize configuration
    if not bus.etc_path:
        bus.etc_path = __node__['etc_dir']
    cnf = ScalarizrCnf(bus.etc_path)
    if not os.path.exists(cnf.private_path()):
        os.makedirs(cnf.private_path())
    bus.cnf = cnf
    
    # Find shared resources dir
    if not bus.share_path:
        bus.share_path = __node__['share_dir']
    
    # Registering in init.d
    initdv2.explore("scalarizr", ScalarizrInitScript)


DB_NAME = 'db.sqlite'
DB_SCRIPT = 'db.sql'

def _db_connect(file=None):
    logger = logging.getLogger(__name__)
    cnf = bus.cnf
    file = file or cnf.private_path(DB_NAME)
    logger.debug("Open SQLite database (file: %s)" % (file))    
    
    conn = sqlite.connect(file, 5.0)
    conn.row_factory = sqlite.Row
    conn.text_factory = sqlite.OptimizedUnicode
    #conn.executescript("PRAGMA journal_mode=OFF;")    
    return conn

def _init_db(file=None):
    logger = logging.getLogger(__name__)    
    cnf = bus.cnf

    # Check that database exists (after rebundle for example)    
    try:
        db_file = file or cnf.private_path(DB_NAME)
        if not os.path.exists(db_file) or not os.stat(db_file).st_size:
            logger.debug("Database doesn't exist, creating new one from script")
            _create_db(file)
        os.chmod(db_file, 0600)

        # XXX(marat) Added here cause postinst script sometimes failed and we get
        # OperationalError: table p2pmessage has no column named format
        conn = _db_connect()
        cur = conn.cursor()
        cur.execute('pragma table_info(p2p_message)')
        if not any(filter(lambda row: row[1] == 'format', cur.fetchall())):
            cur.execute("alter table p2p_message add column format TEXT default 'xml'")
            conn.commit()
        cur.close()
        conn.close()
    except sqlite.OperationalError, e:
        if 'database schema has changed' not in str(e):
            # This caused by UpdateClient paralled startup.
            #  
            # By initial plan, Scalarizr should be started by UpdateClient, 
            # but old Ubuntu 10.04 roles don't have UpdateClient. 
            # Whereas it's installed during migration, but scalr-upd-client init script added to rc2.d 
            # in run time never executed and migration to latest Scalarizr fails.
            raise

        
    # Configure database connection pool
    t = sqlite_server.SQLiteServerThread(_db_connect)
    t.setDaemon(True)
    t.start()
    sqlite_server.wait_for_server_thread(t)
    bus.db = t.connection
    

    
def _create_db(db_file=None, script_file=None):    
    logger = logging.getLogger(__name__)
    #conn = bus.db
    #logger.debug('conn: %s', conn)
    conn = _db_connect()
    conn.executescript(open(script_file or os.path.join(bus.share_path, DB_SCRIPT)).read())
    conn.commit()
    conn.close()
    
    #conn.commit()
    if linux.os['family'] != 'Windows':
        system2('sync', shell=True)

def _init_logging():
    optparser = bus.optparser
    
    #logging.config.dictConfig(LOGGING_CONFIG)
    logging.config.fileConfig(cStringIO.StringIO(LOGGING_CONFIG))
    globals()['_logging_configured'] = True
    logger = logging.getLogger(__name__)
    
    # During server import user must see all scalarizr general activity in his terminal
    # Change console loggel level from DEBUG to INFO  
    if optparser and optparser.values.import_server:
        for hdlr in logging.getLogger('scalarizr').handlers:
            if isinstance(hdlr, logging.StreamHandler) and hdlr.stream == sys.stderr:
                hdlr.setLevel(logging.INFO)

    logger = logging.getLogger('requests.packages.urllib3.connectionpool')
    logger.addFilter(util.InfoToDebugFilter())


def _init_environ():
    if linux.os.windows:
        return
    pwr = pwd.getpwuid(os.getuid())
    os.environ['USER'] = pwr.pw_name
    os.environ['HOME'] = pwr.pw_dir


def _init_platform():
    logger = logging.getLogger(__name__)
    cnf = bus.cnf; ini = cnf.rawini
    
    platform_name = ini.get('general', 'platform')

    if linux.os['name'] == 'RedHat' and platform_name == 'ec2':
        # Enable RedHat subscription 
        logger.debug('Enable RedHat subscription')
        urllib.urlretrieve('http://169.254.169.254/latest/dynamic/instance-identity/document')

    if httplib2_loaded:
        httplib2.CA_CERTS = os.path.join(os.path.dirname(__file__), 'cacert.pem')

    # Initialize platform
    logger.debug("Initialize platform")
    if platform_name:
        bus.platform = PlatformFactory().new_platform(platform_name)
    else:
        raise ScalarizrError("Platform not defined")


def _apply_user_data(from_scalr=True):
    logger = logging.getLogger(__name__)
    logger.debug('Applying user-data to configuration')    
    if from_scalr:
        queryenv = bus.queryenv_service
        user_data = queryenv.get_server_user_data()
        logger.debug('User-data (QueryEnv):\n%s', pprint.pformat(user_data))
    else:
        user_data = meta.user_data()
        logger.debug('User-data (Instance):\n%s', pprint.pformat(user_data))
 
    def g(key):
        return user_data.get(key, '')
    
    updates = dict(
        general={
            'platform': g('platform') or 'ec2',
            'server_id' : g(UserDataOptions.SERVER_ID),
            'server_index': g('server_index'),
            'role_name' : g(UserDataOptions.ROLE_NAME),
            'queryenv_url' : g(UserDataOptions.QUERYENV_URL),
            'cloud_storage_path': g(UserDataOptions.CLOUD_STORAGE_PATH),
            'farm_role_id' : g(UserDataOptions.FARMROLE_ID),
            'env_id' : g(UserDataOptions.ENV_ID), 
            'farm_id' : g(UserDataOptions.FARM_ID),
            'role_id' : g(UserDataOptions.ROLE_ID),
            'region' : g(UserDataOptions.REGION),
            'owner_email' : g(UserDataOptions.OWNER_EMAIL)
        },
        messaging_p2p={
            'producer_url' : g(UserDataOptions.MESSAGE_SERVER_URL),
            'message_format': g(UserDataOptions.MESSAGE_FORMAT) or 'xml'
        }
    )
    behaviour = g(UserDataOptions.BEHAVIOUR)
    if behaviour:
        if behaviour == 'base':
            behaviour = ''
        updates['general']['behaviour'] = behaviour
        
    cnf = bus.cnf
    if not cnf.rawini.has_option('general', 'scalr_id') and \
            bus.scalr_version >= (3, 5, 7):
        queryenv = bus.queryenv_service
        global_config = queryenv.get_global_config()['params']
        updates['general']['scalr_id'] = global_config['scalr.id']


    cnf.update_ini('config.ini', updates)
    cnf.write_key(cnf.DEFAULT_KEY, g(UserDataOptions.CRYPTO_KEY))
    
    logger.debug('Reloading configuration after user-data applying')
    cnf.bootstrap(force_reload=True)


def _detect_scalr_version():
    pl = bus.platform
    cnf = bus.cnf
    if pl and __node__['state'] != 'importing':
        if pl.get_user_data('cloud_storage_path'):
            if pl.get_user_data('env_id'):
                return (3, 5, 3)
            if pl.get_user_data('behaviors'):
                return (3, 1, 0)
        else:
            return (2, 1)
    return (2, 0)

    
def init_script():
    _init()
    _init_db()
    
    cnf = bus.cnf
    cnf.bootstrap()
    ini = cnf.rawini

    szr_logger = logging.getLogger('scalarizr')
    for hd in list(szr_logger.handlers):
        if 'MessagingHandler' in hd.__class__.__name__:
            szr_logger.handlers.remove(hd)
    
    logger = logging.getLogger(__name__)
    logger.debug("Initialize script messaging")

    # Script producer url is scalarizr consumer url. 
    # Script can't handle any messages by himself. Leave consumer url blank
    adapter = ini.get(config.SECT_MESSAGING, config.OPT_ADAPTER)    
    kwargs = dict(ini.items("messaging_" + adapter))
    kwargs[P2pConfigOptions.SERVER_ID] = ini.get(config.SECT_GENERAL, config.OPT_SERVER_ID)
    kwargs[P2pConfigOptions.CRYPTO_KEY_PATH] = cnf.key_path(cnf.DEFAULT_KEY)
    kwargs[P2pConfigOptions.PRODUCER_URL] = kwargs[P2pConfigOptions.CONSUMER_URL]
    
    factory = MessageServiceFactory()        
    msg_service = factory.new_service(adapter, **kwargs)
    bus.messaging_service = msg_service


def _cleanup_after_rebundle():
    cnf = bus.cnf
    pl = bus.platform
    logger = logging.getLogger(__name__)
    
    if os.path.exists('/etc/chef/client.pem'):
        os.remove('/etc/chef/client.pem')
    if os.path.exists('/etc/chef/client.rb'):
        os.remove('/etc/chef/client.rb')

    # remove storage devices from fstab
    if not linux.os.windows_family:
        def remove_systemd_generated_mount_unit(mpoint):
            unit_name = '{}.mount'.format(mpoint.replace('/', '-')[1:])
            logger.debug('Removing systemd runtime unit %s', unit_name)
            coreutils.remove('/run/systemd/generator/local-fs.target.wants/{}'.format(unit_name))
            coreutils.remove('/run/systemd/generator/{}'.format(unit_name))
        fstab = mount.fstab()
        should_reload_systemd = False
        for entry in fstab:
            if 'comment=scalr' in entry.options:
                logger.debug('Removing %s from fstab', entry.device)
                fstab.remove(entry.device)

                if linux.os['family'] == 'RedHat' and \
                    linux.os['name'] != 'Amazon' and \
                    linux.os['release'] >= (7, 0):

                    remove_systemd_generated_mount_unit(entry.mpoint)
                    should_reload_systemd = True

        if should_reload_systemd:
            linux.system('systemctl daemon-reload', shell=True)

    
    # Reset private configuration
    priv_path = cnf.private_path()
    for file in os.listdir(priv_path):
        if file in ('.user-data', 'update.status', 'keys'):
            # protect user-data and UpdateClient status
            # keys/default maybe already refreshed by UpdateClient
            continue
        path = os.path.join(priv_path, file)
        coreutils.chmod_r(path, 0700)
        try:
            os.remove(path) if (os.path.isfile(path) or os.path.islink(path)) else shutil.rmtree(path)
        except:
            if linux.os.windows and sys.exc_info()[0] == WindowsError:         
                # ScalrUpdClient locks db.sqlite 
                logger.debug(sys.exc_info()[1])
            else:
                raise
    if not linux.os.windows_family:
        system2('sync', shell=True)


def do_validate_cnf():
    errors = list()
    def on_error(o, e, errors=errors):
        errors.append(e)
        print >> sys.stderr, 'error: [%s] %s' % (o.name, e)
        
    cnf = bus.cnf
    cnf.bootstrap()
    cnf.validate(on_error)
    return len(errors)

def do_configure():
    optparser = bus.optparser
    cnf = bus.cnf
    cnf.reconfigure(
        values=CmdLineIni.to_kvals(optparser.values.cnf), 
        silent=optparser.values.import_server, 
        yesall=optparser.values.yesall
    )

def do_keygen():
    from scalarizr.util import cryptotool
    print cryptotool.keygen()    

def _pre_main():
    optparser = bus.optparser = OptionParser()
    optparser.add_option('-v', '--version', dest='version', action='store_true',
            help='Show version information')
    optparser.add_option('-c', '--etc-path', dest='etc_path',
            help='Configuration directory path')
    optparser.add_option('-l', dest='debug', action='store_true', default=False,
            help='Enable debug log')
    optparser.add_option('-n', '--configure', dest='configure', action="store_true", default=False, 
            help="Configure Scalarizr in the interactive mode by default. " 
            + "Use '-y -o' to configure Scalarizr non-interactively")
    optparser.add_option("-k", "--gen-key", dest="gen_key", action="store_true", default=False,
            help='Generate crypto key')
    optparser.add_option('-t', dest='validate_cnf', action='store_true', default=False,
            help='Validate configuration')
    optparser.add_option('-m', '--import', dest="import_server", action="store_true", default=False, 
            help='Import service into Scalr')
    optparser.add_option('-y', dest="yesall", action="store_true", default=False,
            help='Answer "yes" to all questions')
    optparser.add_option('-o', dest='cnf', action='append',
            help='Runtime .ini option key=value')

    if linux.os['family'] != 'Windows':
        optparser.add_option("-z", dest="daemonize", action="store_true", default=False,
                                           help='Daemonize process')
    else:
        optparser.add_option("--install-win-services", dest="install_win_services", action="store_true",
                             default=False, help='Install scalarizr as windows service')
        optparser.add_option("--uninstall-win-services", dest="uninstall_win_services", action="store_true",
                             default=False, help='Uninstall scalarizr windows service')

    if ('cloud-location=' in sys.argv or 'region=' in sys.argv) and 'platform=ec2' in sys.argv:
        region = urllib2.urlopen('http://169.254.169.254/latest/meta-data/placement/availability-zone').read().strip()[:-1]
        try:
            sys.argv[sys.argv.index('region=')] += region
        except ValueError:
            sys.argv += ['-o', 'region=' + region]        
    
    optparser.parse_args()
    
    # Daemonize process
    if linux.os['family'] != 'Windows' and optparser.values.daemonize:
        daemonize()

    if optparser.values.version:
        # Report scalarizr version
        print 'Scalarizr %s' % __version__
        sys.exit()

    elif optparser.values.gen_key:
        # Generate key-pair
        do_keygen()
        sys.exit()

def main():

    try:
        logger = logging.getLogger(__name__)
    except (BaseException, Exception), e:
        print >> sys.stderr, "error: Cannot initiate logging. %s" % (e)
        sys.exit(1)
            
    try:
        _pre_main()
        svs = WindowsService() if linux.os.windows_family else Service()
        svs.start()
            
    except (BaseException, Exception), e:
        if isinstance(e, SystemExit):
            raise
        elif isinstance(e, KeyboardInterrupt):
            pass
        else:
            if _logging_configured:
                logger.exception(e)
            else:
                print >> sys.stderr, 'error: %s' % e
            sys.exit(1)


class Service(object):

    def __init__(self):
        self._logger = logging.getLogger(__name__)
        self._running  = False
        self._msg_thread = None


    def start(self):
        self._logger.debug("Initialize scalarizr...")
        _init()
        _init_environ()

        # Starting scalarizr daemon initialization
        globals()['_pid'] = pid = os.getpid()
        self._logger.info('[pid: %d] Starting scalarizr %s', pid, __version__)
        __node__['start_time'] = time.time()

        if not 'Windows' == linux.os['family']:
            # Check for another running scalarzir
            if os.path.exists(PID_FILE):
                try:
                    another_pid = None
                    with open(PID_FILE, 'r') as fp:
                        another_pid = int(fp.read().strip())
                except ValueError:
                    pass
                else:
                    if pid != another_pid and os.path.exists('/proc/%s/status' % (another_pid,)):
                        self._logger.error('Cannot start scalarizr: Another process (pid: %s) already running', another_pid)
                        sys.exit(1)

            # Write PID
            with open(PID_FILE, 'w') as fp:
                fp.write(str(pid))

        cnf = bus.cnf

        optparser = bus.optparser
        if optparser and optparser.values.configure:
            do_configure()
            sys.exit()

        elif optparser and optparser.values.import_server:
            print "Starting import process..."
            print "Don't terminate Scalarizr until Scalr will create the new role"
            __node__['state'] = 'importing'
            # Load Command-line configuration options and auto-configure Scalarizr

            values = CmdLineIni.to_kvals(optparser.values.cnf)
            if not values.get('server_id'):
                values['server_id'] = str(uuid.uuid4())
            self._logger.info('Configuring Scalarizr. This can take a few minutes...')
            cnf.reconfigure(values=values, silent=True, yesall=True)


        try:
            server_id = __node__['server_id']
        except KeyError:
            server_id = None
        if optparser and not optparser.values.import_server \
                and server_id != meta.user_data()['serverid']:
            # This role was bundled with Cloud API call (i.e. CreateImage)
            # Now we're starting with a new server and should reset it's state
            self._logger.info(('This image was bundled with cloud API call. '
                    'Cleauping ancestor server data'))
            _cleanup_after_rebundle()
            __node__['state'] = 'bootstrapping'

        if __node__['state'] == 'unknown':
            __node__['state'] = 'bootstrapping'

        if __node__['state'] == 'bootstrapping':
            _apply_user_data(from_scalr=False)

        # Load INI files configuration
        cnf.bootstrap(force_reload=True)
        ini = cnf.rawini

        # Initialize platform module
        _init_platform()
        pl = bus.platform

        if linux.os.windows:
            try:
                wintool.wait_boot()
            except wintool.RebootExpected:
                self._logger.info('Waiting for interruption...')
                time.sleep(600)

        # Initialize local database
        _init_db()

        STATE['global.start_after_update'] = int(bool(STATE['global.version'] and STATE['global.version'] != __version__))
        STATE['global.version'] = __version__
            
        if __node__['state'] != 'importing':
            self._talk_to_updclient()

        if __node__['state'] == 'importing':
            try:
                pkgmgr.updatedb()
            except:
                self._logger.warn('Failed to update package manager database: %s', 
                    sys.exc_info()[1], exc_info=sys.exc_info())

        # Check Scalr version
        if not bus.scalr_version:
            version_file = cnf.private_path('.scalr-version')
            if os.path.exists(version_file):
                bus.scalr_version = None
                with open(version_file, 'r') as fp:
                    bus.scalr_version = tuple(fp.read().strip().split('.'))
            else:
                bus.scalr_version = _detect_scalr_version()
                with open(version_file, 'w') as fp:
                    fp.write('.'.join(map(str, bus.scalr_version)))

        # Apply Command-line passed configuration options
        if optparser:
            cnf.update(CmdLineIni.to_ini_sections(optparser.values.cnf))

        # Validate configuration
        num_errors = do_validate_cnf()
        if num_errors or (optparser and optparser.values.validate_cnf):
            sys.exit(int(not num_errors or 1))

        # Initialize scalarizr services
        self._init_services()
        
        if STATE['global.start_after_update'] and __node__['state'] == 'running':
            self._logger.info('Scalarizr was updated to %s', __version__)
            __node__['messaging'].send(
                'HostUpdate',
                body={'scalarizr': {'version': __version__}}
            )

        if __node__['state'] == 'running':
            # ReSync user-data
            _apply_user_data(from_scalr=True)
        try:
            bus.fire('init')
        except:
            self._logger.warn('Caught exception in "init": %s', sys.exc_info()[1],
                        exc_info=sys.exc_info())

        # Install signal handlers
        if not linux.os.windows:
            signal.signal(signal.SIGTERM, self.onSIGTERM)
            signal.signal(signal.SIGHUP, self.onSIGHUP)

        self._start_services()

        # Fire start
        self._running = True
        try:
            bus.fire("start")
        except (BaseException, Exception) as e:
            if isinstance(e, SystemExit):
                raise
            self._logger.warn('Caught exception in "start": %s', e, exc_info=sys.exc_info())

        try:
            while self._running:
                if linux.os.windows_family:
                    rc = win32event.WaitForSingleObject(self.hWaitStop, 30000)
                    if rc == win32event.WAIT_OBJECT_0:
                        # Service stopped, stop main loop
                        break
                else:
                    try:
                        select.select([], [], [], 30)
                    except select.error, e:
                        if e.args[0] == 4:
                            # Interrupted syscall
                            continue
                        raise

        except KeyboardInterrupt:
            self._logger.debug('Mainloop: KeyboardInterrupt')
        finally:
            self._logger.debug('Mainloop: finally')
            if self._running and os.getpid() == _pid:
                self._shutdown()
        self._logger.debug('Mainloop: leave')


    def _port_busy(self, port):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            sock.connect(('0.0.0.0', port))
            sock.close()
            return True
        except socket.error:
            return False


    def _select_control_ports(self):
        defaults = __node__['defaults']['base']

        if __node__['state'] != 'importing':
            try:
                api_port = int(__node__['base']['api_port'])
            except KeyError, ValueError:
                api_port = defaults['api_port']
            try:
                messaging_port = int(__node__['base']['messaging_port'])
            except KeyError, ValueError:
                messaging_port = defaults['messaging_port']

            if messaging_port == defaults['messaging_port'] and self._port_busy(messaging_port):
                messaging_port = 8011
            if api_port == defaults['api_port'] and self._port_busy(api_port):
                api_port = 8009
        else:
            api_port = defaults['api_port']
            messaging_port = defaults['messaging_port']

        __node__['base'].update({
            'api_port': api_port,
            'messaging_port': messaging_port
            })  

        return api_port != defaults['api_port'] or messaging_port != defaults['messaging_port']    


    def _try_resolver(self, url):
        try:
            urllib2.urlopen(url).read()
        except urllib2.URLError, e:
            if len(e.args) and isinstance(e.args[0], socket.gaierror):
                eai = e.args[0]
                if eai.errno == socket.EAI_NONAME:
                    with open('/etc/resolv.conf', 'w+') as fp:
                        fp.write('nameserver 8.8.8.8\n')
                elif eai.errno == socket.EAI_AGAIN:
                    os.chmod('/etc/resolv.conf', 0755)
                else:
                    raise

                # reload resolver 
                res_init()
            else:
                raise 


    def _init_services(self):
        logger = logging.getLogger(__name__)
        cnf = bus.cnf; ini = cnf.rawini
        messaging_adp = ini.get('messaging', 'adapter')

        # Set base URL
        pr = urlparse(__node__['queryenv_url'])
        bus.scalr_url = urlunparse((pr.scheme, pr.netloc, '', '', '', ''))
        logger.debug("Got scalr url: '%s'" % bus.scalr_url)

        if not linux.os.windows and __node__['platform'].name == 'openstack':
            self._try_resolver(bus.scalr_url)

        # Create periodical executor for background tasks (cleanup, rotate, gc, etc...)
        bus.periodical_executor = PeriodicalExecutor()

        logger.debug("Initialize QueryEnv client")
        queryenv = new_queryenv()

        if tuple(map(int, STATE['queryenv.api_version']['version'].split('-'))) >= (2012, 7, 1):
            scalr_version = queryenv.get_global_config()['params'].get('scalr.version')
            if scalr_version:
                bus.scalr_version = tuple(map(int, scalr_version.split('.')))
                version_file = cnf.private_path('.scalr-version')
                with open(version_file, 'w') as fp:
                    fp.write(scalr_version)

        bus.queryenv_service = queryenv
        bus.queryenv_version = tuple(map(int, queryenv.api_version.split('-')))

        if __node__['state'] != 'importing':
            lfrp = bus.queryenv_service.list_farm_role_params(__node__['farm_role_id'])['params']
            __node__['base'].update(lfrp.get('base', {}))
        ports_non_default = self._select_control_ports()

        logger.debug("Initialize messaging")
        factory = MessageServiceFactory()
        try:
            params = dict(ini.items("messaging_" + messaging_adp))
            if ports_non_default:
                consumer_url = list(urlparse(params[P2pConfigOptions.CONSUMER_URL]))
                consumer_url[1] = ':'.join((consumer_url[1].split(':')[0], str(__node__['base']['messaging_port'])))
                params[P2pConfigOptions.CONSUMER_URL] = urlunparse(consumer_url)

            params[P2pConfigOptions.SERVER_ID] = __node__['server_id']
            params[P2pConfigOptions.CRYPTO_KEY_PATH] = cnf.key_path(cnf.DEFAULT_KEY)

            msg_service = factory.new_service(messaging_adp, **params)
            bus.messaging_service = msg_service
        except (BaseException, Exception):
            raise ScalarizrError("Cannot create messaging service adapter '%s'" % (messaging_adp))

        optparser = bus.optparser
        if optparser and not optparser.values.import_server and linux.os['family'] != 'Windows':
            system_packages = pkgmgr.package_mgr().list()
            for behavior in __node__['behavior']:
                if behavior in ['base', 'mongodb'] or behavior not in api.api_routes.keys():
                    continue
                try:
                    api_cls = util.import_class(api.api_routes[behavior])
                    api_cls.check_software(system_packages)
                except exceptions.NotFound as e:
                    logger.error(e)
                except exceptions.UnsupportedBehavior as e:
                    if e.args[0] == 'chef':
                        # We pass it, cause a lot of roles has chef behavior without chef installed on them
                        continue
                    __node__['messaging'].send(
                        'RuntimeError',
                        body={
                            'code': 'UnsupportedBehavior',
                            'message': str(e)
                        }
                    )
                    raise sys.exc_info()[0], sys.exc_info()[1], sys.exc_info()[2]

        logger.debug('Initialize message handlers')
        consumer = msg_service.get_consumer()
        consumer.listeners.append(MessageListener())

        producer = msg_service.get_producer()
        def msg_meta(queue, message):
            """
            Add scalarizr version to meta
            """
            message.meta.update({
                'szr_version': __version__,
                'timestamp': os_time.utcnow().strftime("%a %d %b %Y %H:%M:%S %z")
            })
        producer.on('before_send', msg_meta)

        Storage.maintain_volume_table = True

        if not bus.api_server:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            api_port = 8010
            try:
                sock.connect(('0.0.0.0', api_port))
                api_port = 8009
                sock.close()
            except socket.error:
                pass
            STATE['global.api_port'] = api_port
            api_app = jsonrpc_http.WsgiApplication(rpc.RequestHandler(api.api_routes),
                                                cnf.key_path(cnf.DEFAULT_KEY))
            class ThreadingWSGIServer(SocketServer.ThreadingMixIn, wsgiref.simple_server.WSGIServer):
                pass
            bus.api_server = wsgiref.simple_server.make_server('0.0.0.0',
                                __node__['base']['api_port'], 
                                api_app, 
                                server_class=ThreadingWSGIServer)

        if ports_non_default:
            msg = msg_service.new_message('HostUpdate', None, {
                'base': {
                    'api_port': __node__['base']['api_port'],
                    'messaging_port': __node__['base']['messaging_port']
                }
            })
            msg_service.get_producer().send(Queues.CONTROL, msg)


    def _start_services(self):
        # Create message server thread
        msg_service = bus.messaging_service
        consumer = msg_service.get_consumer()
        msg_thread = threading.Thread(target=consumer.start, name="Message server")

        # Start message server
        msg_thread.start()
        self._msg_thread = msg_thread

        # Start API server
        api_server = bus.api_server
        self._logger.info('Starting API server on http://0.0.0.0:%s', __node__['base']['api_port'])
        api_thread = threading.Thread(target=api_server.serve_forever, name='API server')
        api_thread.start()

        # Start periodical executor
        ex = bus.periodical_executor
        ex.start()

    def _talk_to_updclient(self):
        try:
            upd = jsonrpc_http.HttpServiceProxy('http://127.0.0.1:8008', bus.cnf.key_path(bus.cnf.DEFAULT_KEY))
            upd_svs = ScalrUpdClientScript()
            if not upd_svs.running:
                upd_svs.start()

            upd_status = {}
            def upd_ready():
                try:
                    self._logger.debug('Fetching UpdateClient status...')
                    upd_status.update(upd.status(cached=True))
                    self._logger.debug('UpdateClient status: %s', upd_status)
                    return upd_status['state'] != 'noop'
                except (IOError, socket.error), exc:
                    self._logger.debug('Failed to get UpdateClient status: %s', exc)
                except:
                    exc = sys.exc_info()[1]
                    if 'Server-ID header not presented' in str(exc):
                        self._logger.info(('UpdateClient serves previous API version. '
                            'Looks like we are in a process of migration to new update sytem. '
                            'UpdateClient restart will handle this situation. Restarting'))
                        upd_svs.restart()
                    elif 'No module named' in str(exc):
                        self._logger.info(('Scalarizr was downgraded to previous installer version. '
                            'UpdateClient restart required. Restarting'))
                        upd_svs.restart()
                    else:
                        raise


            wait_until(upd_ready, timeout=60, sleep=1)
            upd_state = upd_status['state']
            self._logger.info('UpdateClient state: %s', upd_state)
            if upd_state == 'in-progress/restart':
                self._logger.info('Scalarizr was restarted by update process')
            elif upd_state.startswith('in-progress'):
                version = (upd_status['candidate'] or upd_status['installed']).split('-')[0]
                if version != __version__:
                    self._logger.info('Update is in-progress, exiting')
                    sys.exit()
            elif upd_state == 'completed/wait-ack':
                self._logger.info('UpdateClient completed update and should be restarted, restarting')
                upd_svs.restart()
        except:
            if sys.exc_info()[0] == SystemExit:
                raise
            self._logger.warn('Failed to talk to UpdateClient: %s', sys.exc_info()[1])


    def _shutdown(self):
        self._running = False
        try:
            bus.fire("shutdown")
        except:
            self._logger.debug('Shutdown hooks exception', exc_info=sys.exc_info())

        try:
            self._logger.info("[pid: %d] Stopping scalarizr %s", os.getpid(), __version__)
            self._shutdown_services()
        except:
            self._logger.debug('Shutdown services exception', exc_info=sys.exc_info())
        finally:
            if os.path.exists(PID_FILE):
                os.remove(PID_FILE)

        self._logger.info('[pid: %d] Scalarizr terminated', os.getpid())


    def _shutdown_services(self, force=False):

        # Shutdown messaging
        self._logger.debug('Shutdowning external messaging')
        msg_service = bus.messaging_service
        msg_service.get_consumer().shutdown(force=True)
        msg_service.get_producer().shutdown()
        bus.messaging_service = None

        # Shutdown API server
        self._logger.debug('Shutdowning API server')
        api_server = bus.api_server
        api_server.shutdown()
        bus.api_server = None


        # Shutdown periodical executor
        self._logger.debug('Shutdowning periodical executor')
        ex = bus.periodical_executor
        ex.shutdown()
        bus.periodical_executor = None


    def onSIGTERM(self, *args):
        pid = os.getpid()
        self._logger.debug('Received SIGTERM (pid: %d)', pid)

        if pid == _pid:
            # Main process
            self._logger.debug('Shutdown main process (pid: %d)', pid)
            self._shutdown()


    def onSIGHUP(self, *args):
        pid = os.getpid()
        self._logger.debug('Received SIGHUP (pid: %d)', pid)
        if pid != _pid:
            return

        self._logger.info('Reloading scalarizr')
        self._running = False
        bus.fire('shutdown')
        self._shutdown_services()

        self._running = True
        cnf = bus.cnf
        cnf.bootstrap(force_reload=True)
        self._init_services()
        self._start_services()
        bus.fire('reload')


if 'Windows' == linux.os['family']:
    import win32serviceutil
    import win32service
    import win32event
    import servicemanager
    import win32api


    class WindowsService(win32serviceutil.ServiceFramework, Service):
        _svc_name_            = "Scalarizr"
        _svc_display_name_    = "Scalarizr"
        _stopping             = None

        def __init__(self, args=None):
            Service.__init__(self)
            if args != None:
                win32serviceutil.ServiceFramework.__init__(self, args)

            self.hWaitStop = win32event.CreateEvent(None, 0, 0, None)

            def handler(*args):
                return True
            win32api.SetConsoleCtrlHandler(handler, True)


        def start(self):
            optparser = bus.optparser
            if optparser and optparser.values.install_win_services:
                # Install win services
                sys.argv = [sys.argv[0], '--startup', 'manual', 'install']
                win32serviceutil.HandleCommandLine(WindowsService)
                # Make scalarizr service bullet-proof
                """
                hscm = win32service.OpenSCManager(None,None,win32service.SC_MANAGER_ALL_ACCESS)
                try:
                    hs = win32serviceutil.SmartOpenService(hscm,
                                        WindowsService._svc_name_, win32service.SERVICE_ALL_ACCESS)
                    try:
                        service_failure_actions = {
                                'ResetPeriod': 100,
                                'RebootMsg': u'',
                                'Command': u'',
                                'Actions': [
                                        (win32service.SC_ACTION_RESTART, 1000),
                                        (win32service.SC_ACTION_RESTART, 1000),
                                        (win32service.SC_ACTION_RESTART, 1000)
                                    ]
                            }
                        win32service.ChangeServiceConfig2(hs,
                                            win32service.SERVICE_CONFIG_FAILURE_ACTIONS,
                                            service_failure_actions)
                    finally:
                        win32service.CloseServiceHandle(hs)
                finally:
                    win32service.CloseServiceHandle(hscm)
                """
                #win32serviceutil.StartService(WindowsService._svc_name_)
                sys.exit()

            elif optparser and optparser.values.uninstall_win_services:
                # Uninstall win services
                sys.argv = [sys.argv[0], 'remove']
                win32serviceutil.HandleCommandLine(WindowsService)
                sys.exit()

            else:
                # Normal start
                _pre_main()
                super(WindowsService, self).start()


        def SvcDoRun(self):
            servicemanager.LogMsg(servicemanager.EVENTLOG_INFORMATION_TYPE,
                                  servicemanager.PYS_SERVICE_STARTED,
                                  (self._svc_name_,''))
            self.start()


        def SvcStop(self):
            self.ReportServiceStatus(win32service.SERVICE_STOP_PENDING)
            win32event.SetEvent(self.hWaitStop)
            '''
            if self._running:
                self._running = False
            else:
                try:
                    self._shutdown()
                finally:
                    self._stopping = True
            '''


        def SvcShutdown(self):
            if __node__['state'] != 'running':
                self._logger.debug('Skipping Win_HostDown firing, server state is: {}'.format(
                    __node__['state']))
                return          
            Flag.set(Flag.REBOOT)
            srv = bus.messaging_service
            message = srv.new_message(Messages.WIN_HOST_DOWN)
            srv.get_producer().send(Queues.CONTROL, message)
