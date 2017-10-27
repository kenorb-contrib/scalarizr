from __future__ import with_statement
'''
Created on Aug 11, 2010
 
@author: marat
'''
 
from scalarizr.bus import bus
from scalarizr.libs.bases import Observable
from scalarizr.util import validators
 
from ConfigParser import ConfigParser, RawConfigParser, NoOptionError, NoSectionError
from getpass import getpass
import os, sys, logging
try:
    import json
except ImportError:
    import simplejson as json
 
 
SECT_GENERAL = "general"
OPT_SERVER_ID = "server_id"
OPT_BEHAVIOUR = "behaviour"
OPT_ROLE_NAME = "role_name"
OPT_FARMROLE_ID = 'farm_role_id'
OPT_STORAGE_PATH = "storage_path"
OPT_CRYPTO_KEY_PATH = "crypto_key_path"
OPT_FARM_CRYPTO_KEY_PATH = "farm_crypto_key_path"
OPT_PLATFORM = "platform"
OPT_QUERYENV_URL = "queryenv_url"
OPT_SCRIPTS_PATH = "scripts_path"
OPT_ROLE_ID = 'role_id'
OPT_FARM_ID = 'farm_id'
 
SECT_MESSAGING = "messaging"
OPT_ADAPTER = "adapter"
 
SECT_SNMP = "snmp"
OPT_PORT = "port"
OPT_SECURITY_NAME = "security_name"
OPT_COMMUNITY_NAME = "community_name"
 
SECT_HANDLERS = "handlers"
 
class ConfigError(BaseException):
    pass
 
class Configurator(object):
    '''
    Scalarizr modules configurator.
    Each configurable module should define `OptionContainer` subclass
    with `Option` subclasses
 
    @see: scalarizr.handlers.apache.ApacheConfig for example
    '''
 
    class Option(object):
        '''
        This is option title.
        Option title and description comes from __doc__ string
        First line is title, all all subsequent forms description
        '''
 
        name = None
        '''
        Path like option name
        '''
 
        _value = None
        '''
        Option value
        @see value.getter value.setter
        '''
 
        default = ''
        '''
        Default value
        '''
 
        type = None
        '''
        Data type (for future use)
        '''
 
        required = False
        '''
        Cannot be blank
        '''
 
        private = False
        '''
        Option is private.
        In terms of Scalarizr configuration it means that it will be stored in $etc/private.d/
        '''
 
        def _get_value(self):
            return self._value
 
        def _set_value(self, v):
            if not v and self.required:
                raise ValueError('empty value')
            self._value = v
 
        value = property(_get_value, _set_value)
 
    class Container(object):
        '''
        This doc string is an option group description
        '''
        cnf_name = None
 
 
    def configure(self, ct, values=None, silent=False, yesall=False, nodefault=False, onerror=None):
        '''
        Configure options in container
        '''
        options = []
        for p in dir(ct):
            try:
                if issubclass(getattr(ct, p), Configurator.Option):
                    options.append(getattr(ct, p)())
            except TypeError:
                pass
 
        if options:
            title, desc = self._extract_doc(ct)
            title = "Configuring %s" % title
            if not silent:
                print title
                print "-"*len(title)
                if desc:
                    print desc
            for o in options:
                try:
                    self.configure_option(o, values.get(o.name, None) if values else None, silent, yesall, nodefault)
                except ValueError, e:
                    if onerror:
                        onerror(o, e)
                    else:
                        raise
        return options
 
    def configure_option(self, option=None, value=None, silent=False, yesall=False, nodefault=False):
        '''
        Assign option value from `value` or command prompt
        @param option: Option to configure
        @param value: Default value
        @param silent: when True doesn't produce any input or output
        @param yesall: when True say yes to all questions (no user input)
        '''
        if nodefault and option.type != 'password':
            auto_value = value
        else:
            if value or (value is not None and not option.required):
                auto_value = value
            # XXX:
            elif option.type == 'password':
                auto_value = value or option.value or option.default
            else:
                auto_value = option.default
 
        if not silent:
            title, desc = self._extract_doc(option)
            if desc:
                print desc
            prompt = "%s (%s): " % (title, auto_value if option.type != 'password' else '******')
            if not yesall:
                # Ask user forever until valid value entered
                while True:
                    user_value = raw_input(prompt) if option.type != 'password' else getpass(prompt)
                    try:
                        option.value = user_value or auto_value
                        break
                    except ValueError, e:
                        print str(e)
            else:
                # Take auto value
                print prompt
                option.value = auto_value
            print "%s = %s\n" % (option.name, option.value if option.type != 'password' else '******')
        else:
            option.value = auto_value
 
    def _extract_doc(self, symbol):
        doc = filter(None, map(str.strip, symbol.__doc__.split("\n")))
        return doc[0], "\n".join(doc[1:])
 
 
 
 
class ScalarizrOptions(Configurator.Container):
    '''
    scalarizr
    '''
 
    class server_id(Configurator.Option):
        '''
        Unique server identificator in Scalr envirounment.
        '''
        name = 'general/server_id'
        default = '00000000-0000-0000-0000-000000000000'
        private = True
        required = True
 
        @validators.validate(validators.uuid4)
        def _set_value(self, v):
            Configurator.Option._set_value(self, v)
        value = property(Configurator.Option._get_value, _set_value)
 
    class role_name(Configurator.Option):
        '''
        Role name
        '''
        name = 'general/role_name'
        private = True
        #required = True
 
    class region(Configurator.Option):
        '''
        Cloud location: us-east-1
        '''
        name = 'general/region'
        private = True
 
 
    class env_id(Configurator.Option):
        '''
        Environment ID
        '''
        name = 'general/env_id'
        private = True
 
    class farm_id(Configurator.Option):
        '''
        Farm ID
        '''
        name = 'general/farm_id'
        private = True
 
    class role_id(Configurator.Option):
        '''
        Role ID
        '''
        name = 'general/role_id'
        private = True
 
    class farm_role_id(Configurator.Option):
        '''
        Farm role ID
        '''
        name = 'general/farm_role_id'
        private = True
 
 
    class queryenv_url(Configurator.Option):
        '''
        QueryEnv URL.
        URL to QueryEnv service. Use https://my.scalr.net/query-env for Scalr.net SaaS
        '''
        name = 'general/queryenv_url'
        default = 'https://my.scalr.net/query-env'
        private = True
        required = True
 
    class message_producer_url(Configurator.Option):
        '''
        Message server URL.
        URL to Scalr message server. Use https://my.scalr.net/messaging for Scalr.net SaaS
        '''
        name = 'messaging_p2p/producer_url'
        default = 'https://my.scalr.net/messaging'
        private = True
        required = True
 
    class messaging_format(Configurator.Option):
        '''
        Message format: xml | json
        '''
        name = 'messaging_p2p/message_format'
        default = 'xml'
        private = True
        required = False
 
    class crypto_key(Configurator.Option):
        '''
        Default crypto key
        '''
        name = 'general/crypto_key'
        type = 'password'
        default = ''
        #required = True
 
        def _get_value(self):
            if self._value is None:
                try:
                    cnf = bus.cnf
                    self._value = cnf.read_key('default')
                except:
                    self._value = ''
 
            return self._value
 
        @validators.validate(validators.base64)
        def _set_value(self, v):
            Configurator.Option._set_value(self, v)
 
        value = property(_get_value, _set_value)
 
        def store(self):
            cnf = bus.cnf
            cnf.write_key('default', self.value, 'Scalarizr crypto key')
 
    class behaviour(Configurator.Option):
        '''
        Server behaviour.
        Server behaviour is a role your server acts as. Built-in behaviours: {behaviours}
        '''
        name = 'general/behaviour'
        private = True
 
        def __init__(self):
            self.__doc__ = self.__doc__.replace('{behaviours}', ','.join(BuiltinBehaviours.values()))
 
        def _set_value(self, v):
            v = split(v.strip())
            #bhvs = BuiltinBehaviours.values()
            #if any(vv not in bhvs for vv in v):
            #       raise ValueError('unknown behaviour')
            self._value = ','.join(v)
 
        value = property(Configurator.Option._get_value, _set_value)
 
    class platform(Configurator.Option):
        '''
        Cloud platform.
        Cloud platform on which scalarizr is deployed. Built-in platforms: {platforms}
        '''
        name = 'general/platform'
        required = True
 
        def __init__(self):
            self.__doc__ = self.__doc__.replace('{platforms}', ','.join(BuiltinPlatforms.values()))
 
        def _set_value(self, v):
            if not v in BuiltinPlatforms.values():
                raise ValueError('unknown platform')
            self._value = v
 
        value = property(Configurator.Option._get_value, _set_value)
 
    class snmp_security_name(Configurator.Option):
        '''
        SNMP security name
        '''
        name = 'snmp/security_name'
        private = True
        required = True
        default = 'notConfigUser'
 
    class snmp_community_name(Configurator.Option):
        '''
        SNMP community name
        '''
        name = 'snmp/community_name'
        private = True
        required = True
        default = 'public'
 
 
class ini_property(property):
    _cfoption = None
    def __init__(self, ini, *args):
        '''
        var1:
        @param ini: ConfigParser object
        @param cfoption: Configurator.Option subclass
 
        var2:
        @param ini: ConfigParser object
        @param section: Section name
        @param option: Option name
        '''
        self._ini = ini
        if len(args) == 1:
            self._cfoption = args[0]
            self._section, self._option = self._cfoption.name.split('/')
        else:
            self._section, self._option = args
        property.__init__(self, self._getter, self._setter)
 
    def _getter(self):
        return self._ini.get(self._section, self._option)
 
    def _setter(self, v):
        if self._cfoption:
            # Apply option validation
            self._cfoption.value = v
            v = self._cfoption.value
        self._ini.set(self._section, self._option, str(v if v is not None else ''))
 
class ini_list_property(ini_property):
    def _setter(self, v):
        if hasattr(v, "__iter__"):
            v = ",".join(v)
        ini_property.setter(self, v)
 
    def _getter(self):
        return split(ini_property._getter(self))
 
 
class ScalarizrIni:
    # FIXME: wrapper doesn't works
    class general:
        _ini = None
        section = 'general'
        server_id = role_name = behaviour = platform = queryenv_url = scripts_path = None
 
        def __init__(self, ini):
            self._ini = ini
            self.server_id = ini_property(self._ini, ScalarizrOptions.server_id)
            self.role_name = ini_property(self._ini, ScalarizrOptions.role_name)
            ScalarizrIni.behaviour = ini_list_property(self._ini, ScalarizrOptions.behaviour)
            self.platform = ini_property(self._ini, ScalarizrOptions.platform)
            self.queryenv_url = ini_property(self._ini, ScalarizrOptions.queryenv_url)
            self.scripts_path = ini_property(self._ini, self.section, 'scripts_path')
 
    class snmp:
        _ini = None
        section = 'snmp'
        port = security_name = community_name = None
 
        def __init__(self, ini):
            self._ini = ini
            self.port = ini_property(self._ini, self.section, 'port')
            self.security_name = ini_property(self._ini, self.section, 'security_name')
            self.community_name = ini_property(self._ini, self.section, 'community_name')
 
    class handlers:
        _ini = None
        section = 'handlers'
        def __init__(self, ini):
            self._ini = ini
 
        def __getitem__(self, k):
            try:
                return self._ini.get(self.section, k)
            except NoOptionError:
                raise KeyError('no such handler %s' % (k,))
 
        def __iter__(self):
            return self._ini.options(self.section).__iter__()
 
    ini = None
 
    def __init__(self, ini):
        self.ini = ini
        self.general = self.general(ini)
        self.snmp = self.snmp(ini)
        self.handlers = self.handlers(ini)
 
 
class ScalarizrCnf(Observable):
    DEFAULT_KEY = 'default'
    FARM_KEY = 'farm'
 
    ini = None
    '''
    @ivar ini: Beautiful ini wrapper
    @type ini: scalarizr.config.ScalarizrIni
    '''
 
    @property
    def rawini(self):
        '''
        Shortcut for underlying ini config parser
        @rtype: ConfigParser.ConfigParser
        '''
        return self.ini.ini
 
    _logger = None
    _root_path = None
    _priv_path = None
    _pub_path = None
    _home_path = None
    _bootstrapped = False
    _explored_keys = None
    _loaded_ini_names = None
 
    _reconfigure = None
    class __reconfigure:
        cnf = None
        tr = None
 
        def __init__(self, cnf):
            self.cnf = cnf
 
        def _ini_to_kvals(self, ini):
            values = {}
            for section in ini.sections():
                for option in ini.options(section):
                    values['%s/%s' % (section, option)] = ini.get(section, option)
            return values
 
        def _kval_to_ini_sections(self, kvals):
            sections = {}
            for k in kvals:
                s, o = k.split('/')
                if not sections.has_key(s):
                    sections[s] = {}
                sections[s][o] = kvals[k]
            return sections
 
 
        def _store_options(self, ini_name, options):
            pub = dict()
            priv = dict()
            ini_name = self.cnf._name(ini_name)
 
            for option in options:
                if hasattr(option, 'store'):
                    option.store()
                else:
                    d = priv if option.private else pub
                    d[option.name] = option.value
 
            if priv:
                self.cnf.update_ini(ini_name, self._kval_to_ini_sections(priv))
            if pub:
                self.cnf.update_ini(ini_name, self._kval_to_ini_sections(pub), private=False)
 
        '''
        def _lookup_option(self, options, cls=None, name=None):
                for option in options:
                        if cls and isinstance(option, cls):
                                return option
                        elif name and name == option.name:
                                return name
                raise LookupError('option not found (class=%s, name=%s)' % (cls, name))
        '''
 
        def _lookup_module_config_container(self, module):
            for p in dir(module):
                try:
                    if issubclass(getattr(module, p), Configurator.Container):
                        return getattr(module, p)
                except TypeError:
                    pass
            raise LookupError('module %s has no config container' % (module,))
 
 
        def _lookup_main_handler(self, ini, section):
            try:
                name = ini.get(section, 'main_handler')
                return name, ini.get('handlers', name)
            except (NoOptionError, NoSectionError):
                raise LookupError('main handler not found')
 
 
        def _configure_handler(self, name, module_name, values=None, silent=False, yesall=False,
                                                nodefault=False, onerror=None, dryrun=False):
            try:
                self.cnf._logger.debug('Importing module %s', module_name)
                __import__(module_name)
                module = sys.modules[module_name]
 
                self.cnf._logger.debug('Lookup config container in module %s', module_name)
                CnfContainer = self._lookup_module_config_container(module)
                ini_name = CnfContainer.cnf_name or name
 
                self.cnf._logger.debug('Configuring handler %s (cnf_name: %s)', module.__name__, ini_name)
                options = self.tr.configure(CnfContainer, values, silent, yesall, nodefault, onerror)
                if not dryrun:
                    self._store_options(ini_name, options)
                return True
            except LookupError:
                return False
 
 
        def __call__(self, values=None, silent=False, yesall=False,
                                nodefault=False, onerror=None, dryrun=False):
            if not self.tr:
                self.tr = Configurator()
            self.cnf.bootstrap()
            ini = self.cnf.rawini
 
            kvals = self._ini_to_kvals(ini)
            kvals.update(values or dict())
 
            # Main config
            options = self.tr.configure(ScalarizrOptions, kvals, silent, yesall, nodefault, onerror)
            if not dryrun:
                self._store_options('config.ini', options)
                # Reload main config
                self.cnf.bootstrap(force_reload=True)
                # ini <- runtime <- configured options
                kvals = self._ini_to_kvals(ini)
                kvals.update(values or dict())
                for option in options:
                    kvals[option.name] = option.value
 
 
            proceed_hdlrs = []
            # Configure platform
            try:
                name, module_name = self._lookup_main_handler(ini, ini.get('general', 'platform'))
                self._configure_handler(name, module_name, kvals, silent, yesall, nodefault, onerror, dryrun)
                proceed_hdlrs.append(name)
            except LookupError:
                pass
 
            # Configure behaviours
            for bh in split(ini.get('general', 'behaviour')):
                try:
                    name, module_name = self._lookup_main_handler(ini, bh)
                    self._configure_handler(name, module_name, kvals, silent, yesall, nodefault, onerror, dryrun)
                    proceed_hdlrs.append(name)
                except LookupError:
                    pass
 
            # Configure remain handlers
            if ini.has_section('handlers'):
                names = tuple(option for option in ini.options('handlers') if option not in proceed_hdlrs)
                for name in names:
                    try:
                        self._configure_handler(name, ini.get('handlers', name), kvals, silent, yesall, nodefault, onerror, dryrun)
                    except LookupError:
                        pass
 
    _update_ini = None
    class __update_ini:
        cnf = None
 
        class Comment:
            type = "comment"
            def __init__(self, text):
                self.text = text
            def __str__(self):
                return self.text
 
        class Option:
            type = "option"
            def __init__(self, key, value):
                self.key = key
                self.value = value
            def __str__(self):
                return "%s = %s%s" % (self.key, self.value, os.linesep)
 
        class Section:
            type = "section"
            def __init__(self, name):
                self.items = []
                self.name = name
            def __str__(self):
                ret = "[%s]%s" % (self.name, os.linesep)
                for item in self.items:
                    ret += str(item)
                return ret
 
        class Config:
            def __init__(self):
                self.items = []
            def __str__(self):
                ret = ""
                for item in self.items:
                    ret += str(item)
                return ret
 
 
        def __init__(self, cnf):
            self.cnf = cnf
 
        def __call__(self, name, ini_sections, private=True):
            config = self.Config()
            name = self.cnf._name(name)
            filename = self.cnf.private_path(name) if private else self.cnf.public_path(name)
            ini = self.cnf.rawini
 
            if os.path.exists(filename):
                cursect = None
                sect_re = RawConfigParser.SECTCRE
                opt_re = RawConfigParser.OPTCRE
                fp = open(filename, "r")
                while True:
                    line = fp.readline()
                    if not line:
                        break
                    mo = sect_re.match(line)
                    if mo:
                        cursect = self.Section(mo.group('header').strip())
                        config.items.append(cursect)
                    else:
                        mo = opt_re.match(line)
                        if mo:
                            cursect.items.append(self.Option(mo.group("option").strip(), mo.group("value").strip()))
                        else:
                            comment = self.Comment(line)
                            if cursect:
                                cursect.items.append(comment)
                            else:
                                config.items.append(comment)
                fp.close()
                fp = None
 
 
            self.cnf._logger.debug("Updating configuration file %s", filename)
 
            # Update configuration
            for sect_name in ini_sections:
                #self.cnf._logger.debug("Find section '%s' in existing sections", sect_name)
                cur_sect = None
                for section in [it for it in config.items if it.type == "section"]:
                    #self.cnf._logger.debug("Compare '%s' with '%s'", sect_name, section.name)
                    if section.name == sect_name:
                        #self.cnf._logger.debug("Found '%s' in existing sections", sect_name)
                        cur_sect = section
                        break
                # Section not found
                if cur_sect is None:
                    # Create new section and append it in the end
                    #self.cnf._logger.debug("Section '%s' wasn't found in existing sections", sect_name)
                    #self.cnf._logger.debug("Create section '%s' and append it in the end", sect_name)
                    cur_sect = self.Section(sect_name)
                    config.items.append(cur_sect)
                if not ini.has_section(cur_sect.name):
                    ini.add_section(cur_sect.name)
 
                for opt_name, value in ini_sections[sect_name].items():
                    #self.cnf._logger.debug("Find option '%s' in section '%s'", opt_name, sect_name)
                    cur_opt = None
                    for option in [it for it in cur_sect.items if it.type == "option"]:
                        #self.cnf._logger.debug("Compare '%s' with '%s'", opt_name, option.key)
                        if option.key == opt_name:
                            #self.cnf._logger.debug("Found option '%s' in existing options in section '%s'",
                            #               opt_name, sect_name)
                            cur_opt = option
                            break
                    # Option not found
                    if cur_opt is None:
                        #self.cnf._logger.debug("Option '%s' wasn't found in existing options of section '%s'",
                        #               opt_name, sect_name)
                        #self.cnf._logger.debug("Create option '%s' and append it in the end of section '%s'",
                        #               opt_name, sect_name)
                        # Create option and append it in the end
                        cur_opt = self.Option(opt_name, value if value != None else "")
                        cur_sect.items.append(cur_opt)
                    else:
                        cur_opt.value = value
                    ini.set(cur_sect.name, cur_opt.key, value)
 
 
            #self.cnf._logger.debug("Write configuration file '%s'", filename)
            fp = None
            try:
                if os.path.exists(filename):
                    os.chmod(filename, 0600)
                fp = open(filename, "w+")
                fp.write(str(config))
                os.chmod(filename, 0400)
            finally:
                if fp:
                    fp.close()
 
 
 
    def __init__(self, root=None):
        Observable.__init__(self)
        if not root:
            root = bus.etc_path
        self._logger = logging.getLogger(__name__)
 
        self._chkdir(root)
        priv_path = os.path.join(root, 'private.d')
        self._chkdir(priv_path)
        pub_path = os.path.join(root, 'public.d')
        self._chkdir(pub_path)
 
        self._root_path = root
        self._priv_path = priv_path
        self._pub_path = pub_path
        if not bus.config:
            bus.config = ConfigParser()
        self.ini = ScalarizrIni(bus.config)
 
        self._explored_keys = dict()
        self.explore_key(self.DEFAULT_KEY, 'Scalarizr crypto key', True)
        self.explore_key(self.FARM_KEY, 'Farm crypto key', True)
 
        self._loaded_ini_names = set()
 
        self.define_events(
                # Fires when modules must apply user-data to configuration
                # @param cnf: This configuration object
                'apply_user_data'
        )
 
 
    def _chkdir(self, dir):
        if not os.path.exists(dir) and os.path.isdir(dir):
            raise OSError("dir %s doesn't exists", dir)
 
    def _name(self, name):
        if not name.endswith('.ini'):
            name += '.ini'
        return name
 
    def load_ini(self, name, configparser=None):
        name = self._name(name)
        if not name in self._loaded_ini_names:
            files = (os.path.join(self._pub_path, name), os.path.join(self._priv_path, name))
            ini = configparser or self.rawini
            for file in files:
                if os.path.exists(file):
                    self._logger.debug('Reading configuration file %s', file)
                    ini.read(file)
                    self._loaded_ini_names.add(name)
 
    def update_ini(self, name, ini_sections, private=True):
        if not self._update_ini:
            self._update_ini = self.__update_ini(self)
        return self._update_ini(name, ini_sections, private)
 
 
    def update(self, sections):
        '''
        Override runtime configuration with passed values
        '''
        for section in sections:
            if self.rawini.has_section(section):
                for option in sections[section]:
                    self.rawini.set(section, option, sections[section][option])
 
 
    def bootstrap(self, force_reload=False):
        '''
        Bootstrap configuration from INI files.
        '''
        self._logger.debug('Bootstrap INI configuration (reload=%s)', force_reload)
        if self._bootstrapped:
            if force_reload:
                for section in self.rawini.sections():
                    self.rawini.remove_section(section)
                self._loaded_ini_names = set()
            else:
                return
 
        self._logger.debug('Loading main configuration')
        self.load_ini('config.ini')
 
        self._logger.debug('Loading platform configuration')
        pl = self.rawini.get('general', 'platform')
        if pl:
            self.load_ini(pl)
 
        self._logger.debug('Loading behaviours configuration')
        bhs = split(self.rawini.get('general', 'behaviour'))
        for bh in bhs:
            self.load_ini(bh)
 
        self._logger.debug('Loading handlers configuration')
        for hd in self.rawini.options('handlers'):
            self.load_ini(hd)
 
        '''
        if runtime_ini_sections:
                self._logger.debug('Apply run-time configuration values')
                for section in runtime_ini_sections:
                        if self.rawini.has_section(section):
                                for option in runtime_ini_sections:
                                        self.rawini.set(section, option, runtime_ini_sections[section][option])
        '''
 
        self._bootstrapped = True
 
    def reconfigure(self, values=None, silent=False, yesall=False, nodefault=False, onerror=None, dryrun=False):
        if not self._reconfigure:
            self._reconfigure = self.__reconfigure(self)
        return self._reconfigure(values, silent, yesall, nodefault, onerror, dryrun)
 
    def validate(self, onerror=None):
        self.reconfigure(silent=True, yesall=True, nodefault=True, onerror=onerror, dryrun=True)
 
    def read_key(self, name, title=None, private=True):
        '''
        Read keys from $etc/.private.d/keys, $etc/public.d/keys
        '''
        if os.path.isabs(name):
            filename = name
        else:
            filename = self.key_path(name, private)
            title = self._explored_keys.get((name, private), title)
 
        file = None
        try:
            file = open(filename, "r")
            return file.read().strip()
        except IOError, e:
            raise ConfigError("Cannot read %s file '%s'. %s" % (title or "key", filename, str(e)))
        finally:
            if file:
                file.close()
 
 
    def write_key(self, name, key, title=None, private=True):
        '''
        Write keys into $etc/.private.d/keys, $etc/public.d/keys
        '''
        if os.path.isabs(name):
            filename = name
        else:
            filename = self.key_path(name, private)
            title = self._explored_keys.get((name, private), title)
 
        file = None
        try:
            keys_dir = os.path.dirname(filename)
            if not os.path.exists(keys_dir):
                os.makedirs(keys_dir)
            if os.path.exists(filename):
                os.chmod(filename, 0600)
            file = open(filename, "w+")
            file.write(key)
        except (IOError, OSError), e:
            raise ConfigError("Cannot write %s in file '%s'. %s" % (title or "key", filename, str(e)))
        finally:
            if file:
                file.close()
            if os.path.exists(filename):
                os.chmod(filename, 0400)
        return filename
 
    def _get_state(self):
        filename = self.private_path('.state')
        if not os.path.exists(filename):
            return ScalarizrState.UNKNOWN
        with open(filename, "r") as fp:
            return str.strip(fp.read())
 
    def _set_state(self, v):
        with open(self.private_path('.state'), 'w') as fp:
            fp.write(v)
        self._logger.info('State: %s', v)
 
    state = property(_get_state, _set_state)
 
    def private_path(self, name=None):
        return name and os.path.join(self._priv_path, name) or self._priv_path
 
    def public_path(self, name=None):
        return name and os.path.join(self._pub_path, name) or self._pub_path
 
    def private_exists(self, name=None):
        return os.path.exists(self.private_path(name))
 
    def public_exists(self, name=None):
        return os.path.exists(self.public_path(name))
 
    @property
    def storage_path(self):
        return self.private_path('storage')
 
    @property
    def home_path(self):
        #expanduser ocasionaly got us an error related to $HOME and daemon process
        #if not self._home_path:
            #self._home_path = os.path.expanduser('~/.scalr')
        #return self._home_path
        return '/root/.scalr'
 
    def key_path(self, name, private=True):
        return os.path.join(self._priv_path if private else self._pub_path, 'keys', name)
 
    def key_exists(self, name, private=True):
        return os.path.exists(self.key_path(name, private))
 
    def explore_key(self, name, title, private=True):
        self._explored_keys[(name, private)] = title
 
 
class State(dict):
 
    def _conn(self):
        return bus.db
 
 
    def __getitem__(self, name):
        conn = self._conn()
        cur = conn.cursor()
        try:
            cur.execute("SELECT value FROM state WHERE name = ?", [name])
            ret = cur.fetchone()
            try:
                return json.loads(ret['value'])
            except (TypeError, KeyError, ValueError):
                return ret
        finally:
            cur.close()
 
    def __setitem__(self, name, value):
        conn = self._conn()
        cur = conn.cursor()
        try:
            cur.execute("INSERT INTO state VALUES (?, ?)", [name, json.dumps(value) ])
        finally:
            cur.close()
        conn.commit()
 
    def get_all(self, name):
        conn = self._conn()
        cur = conn.cursor()
        try:
            cur.execute("SELECT * FROM state WHERE name LIKE '%s%%'" % name)
            ret = {}
            for row in cur.fetchall():
                ret[row['name']] = row['value']
            return ret
        finally:
            cur.close()
 
 
STATE = State()
 
 
class BuiltinBehaviours:
    APP = 'app'
    WWW = 'www'
    MYSQL = 'mysql'
    MYSQL2 = 'mysql2'
    PERCONA = 'percona'
    MARIADB = 'mariadb'
    CASSANDRA = 'cassandra'
    MEMCACHED = 'memcached'
    POSTGRESQL = 'postgresql'
    RABBITMQ = 'rabbitmq'
    REDIS = 'redis'
    HAPROXY = 'haproxy'
    MONGODB = 'mongodb'
    CHEF = 'chef'
    TOMCAT = 'tomcat'
 
    CF_ROUTER = 'cf_router'
    CF_CLOUD_CONTROLLER = 'cf_cloud_controller'
    CF_HEALTH_MANAGER = 'cf_health_manager'
    CF_DEA = 'cf_dea'
    CF_SERVICE = 'cf_service'
 
 
    CUSTOM = 'custom'
 
    @staticmethod
    def values():
        return tuple(getattr(BuiltinBehaviours, k)
                        for k in dir(BuiltinBehaviours) if not k.startswith('_') and k != 'values')
 
 
class BuiltinPlatforms:
    VPS             = 'vps'
    EC2             = 'ec2'
    EUCA            = 'eucalyptus'
    RACKSPACE       = 'rackspace'
    NIMBULA         = 'nimbula'
    CLOUDSTACK      = 'cloudstack'
    IDCF            = 'idcf'
    UCLOUD          = 'ucloud'
    GCE             = 'gce'
    OPENSTACK       = 'openstack'
    LXC             = 'lxc'
 
    @staticmethod
    def values():
        return tuple(getattr(BuiltinPlatforms, k)
                        for k in dir(BuiltinPlatforms) if not k.startswith('_') and k != 'values')
 
 
class CmdLineIni:
    '''
    Scalarizr .ini can be overriden in runtime by passing them into command:
    `scalarizr -o opt1=value1 -o sect2.opt2=value2`
    This class implements various convert functions for such options
    '''
 
    @staticmethod
    def _translate_key(key):
        '''
        .ini options passed
        The general rule that .ini options are passed  `-o section.option=value`
        '''
        sp = key.replace('-', '_').split('.', 1)
        return tuple(sp) if len(sp) == 2 else ('general', sp[0])
 
    @staticmethod
    def to_kvals(options):
        '''
        Convert OptionParser .ini options parse result to key/value form
        understandable by Configurator.configure
        '''
        options = options or {}
        values = {}
        for o in options:
            key, val = o.split('=', 1)
            values['%s/%s' % CmdLineIni._translate_key(key)] = val
        return values
 
    @staticmethod
    def to_ini_sections(options):
        '''
        Convert OptionParser .ini options parse result to ConfigParser sections form
        '''
        options = options or {}
        sections = {}
        for o in options:
            key, val = o.split('=', 1)
            sect, opt = CmdLineIni._translate_key(key)
            if not sect in sections:
                sections[sect] = {}
            sections[sect][opt] = val
        return sections
 
 
def split(value, separator=",", allow_empty=False, ct=list):
    return ct(v.strip() for v in value.split(separator) if allow_empty or (not allow_empty and v)) if value else ct()
 
 
class ScalarizrState:
    BOOTSTRAPPING = "bootstrapping"
    IMPORTING = "importing"
    INITIALIZING = "initializing"
    RUNNING = "running"
    UNKNOWN = "unknown"
    REBUNDLING = "rebundling"
 
