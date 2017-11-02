from __future__ import with_statement
'''
Created on Sep 7, 2010

@author: marat
@author: Dmytro Korsakov
'''
from scalarizr.bus import bus
from scalarizr.libs.metaconf import Configuration, NoPathError


import os, time, logging
import shutil, urllib2

from cStringIO import StringIO


class CnfPreset:
    name = None
    settings = None
    behaviour = None

    def __init__(self, name=None, settings=None, behaviour = None):
        self.name = name
        self.settings = settings or {}
        self.behaviour = behaviour

    def __repr__(self):
        return 'name=' + str(self.name) \
                        + "behaviour=" + str(self.behaviour) \
                        + "; settings=" + str(self.settings)

class PresetType:
    DEFAULT = 'default'
    LAST_SUCCESSFUL = 'last_successful'
    CURRENT = 'current'

class CnfPresetStore:
    service_name = None

    def __init__(self, service_name):
        self._logger = logging.getLogger(__name__)
        self.service_name = service_name
        cnf = bus.cnf
        self.presets_path = os.path.join(cnf.home_path, 'presets')
        if not os.path.exists(self.presets_path):
            try:
                os.makedirs(self.presets_path)
            except OSError:
                pass

    def _filename(self, preset_type):
        return os.path.join(self.presets_path, self.service_name + '.' + preset_type)

    def load(self, preset_type):
        '''
        @rtype: Preset
        @raise OSError: When cannot read preset file
        @raise MetaconfError: When experience problems with preset file parsing
        '''
        self._logger.debug('Loading %s %s preset' % (preset_type, self.service_name))
        ini = Configuration('ini')
        ini.read(self._filename(preset_type))

        return CnfPreset(ini.get('general/name'), dict(ini.items('settings/')))

    def save(self, preset, preset_type):
        '''
        @type preset: CnfPreset
        @type preset_type: CnfPresetStore.PresetType
        @raise ValueError: When `preset` is not an instance of CnfPreset
        @raise OSError: When cannot save preset file
        '''
        if not isinstance(preset, CnfPreset):
            raise ValueError('argument `preset` should be a CnfPreset instance, %s is given', type(preset))

        self._logger.debug('Saving preset as %s' % preset_type)
        ini = Configuration('ini')
        ini.add('general')
        ini.add('general/name', preset.name if (hasattr(preset, 'name') and preset.name) else 'Noname')
        ini.add('settings')

        for k, v in preset.settings.items():
            ini.add('settings/%s' % k, v)
        ini.write(self._filename(preset_type))

    def copy(self, src_preset_type, dst_preset_type, override = True):
        src = self._filename(src_preset_type)
        dst = self._filename(dst_preset_type)

        if not override and os.path.exists(dst):
            self._logger.debug('%s file already exists.' % dst_preset_type)
            return
        elif not os.path.exists(src):
            self._logger.error('Source file %s does not exist. Nothing to copy.' % src)
        else:
            shutil.copy(src, dst)


class CnfController(object):
    _logger = None
    behaviour = None

    _config_path = None
    _config_format = None

    def __init__(self, behaviour, config_path, config_format, definitions=None):
        #you may redefine config values in defenitions (like {'1':'on','0':'off'})
        self._logger = logging.getLogger(__name__)
        self.behaviour = behaviour
        self._config_path = config_path
        self._config_format = config_format
        self.definitions = definitions

    def preset_equals(self, this, that):
        #usage: preset_equals(queryenv_preset, local_preset)
        if not this.settings or not that.settings:
            return False

        if this.settings == that.settings:
            return True

        for variable in self._manifest:
            if variable.inaccurate:
                continue

            if not variable.name in this.settings and not variable.name in that.settings:
                continue

            elif not variable.name in this.settings:
                if variable.default_value and that.settings[variable.name] == variable.default_value:
                    continue
                elif variable.default_value:
                    self._logger.debug('Variable %s has default value %s and variable in %s = %s' % (variable.name, variable.default_value,that.name,that.settings[variable.name]))
                    return False
                else:
                    continue


            elif not variable.name in that.settings:
                if variable.default_value and this.settings[variable.name] == variable.default_value:
                    continue
                elif variable.default_value:
                    self._logger.debug('Variable %s has default value %s and variable in %s = %s' % (variable.name, variable.default_value,this.name,this.settings[variable.name]))
                    return False
                else:
                    continue

            else:
                that_value = that.settings[variable.name]
                this_value = this.settings[variable.name]

                #mind aliases
                if self.definitions:
                    if that_value in self.definitions:
                        that_value = self.definitions[that_value]
                    if this_value in self.definitions:
                        this_value = self.definitions[this_value]

                if that_value != this_value:
                    self._logger.debug('Variable %s changed: %s != %s' % (variable.name, this_value, that_value))
                    return False


        return True

    def get_system_variables(self):
        #TESTING REQUIRED!
        conf = Configuration(self._config_format)
        conf.read(self._config_path)
        vars = {}
        for section in conf.sections('./'):
            vars[section] = conf.get(section)
        return vars

    def current_preset(self):
        self._logger.debug('Getting %s current configuration preset', self.behaviour)
        preset = CnfPreset(name='System', behaviour = self.behaviour)

        preset.settings = {}
        sys_vars = self.get_system_variables()

        for opt in self._manifest:
            try:
                val = sys_vars[opt.name]
                if val:
                    preset.settings[opt.name] = val
            except KeyError:
                if opt.default_value:
                    preset.settings[opt.name] = opt.default_value
        return preset

    def apply_preset(self, preset):
        conf = Configuration(self._config_format)
        conf.read(self._config_path)

        self._before_apply_preset()

        ver = self._software_version
        for opt in self._manifest:
            path = opt.name if not opt.section else '%s/%s' % (opt.section, opt.name)

            try:
                value = conf.get(path)
            except NoPathError:
                value = ''

            if opt.name in preset.settings:
                new_value = preset.settings[opt.name]

                # Skip unsupported
                if ver and opt.supported_from and opt.supported_from > ver:
                    self._logger.debug("Skipping option '%s' supported from %s; installed %s" %
                                    (opt.name, opt.supported_from, ver))
                    continue

                if not opt.default_value:
                    self._logger.debug("Option '%s' has no default value" % opt.name)
                    pass
                elif new_value == opt.default_value:
                    if value:
                        self._logger.debug("Option '%s' equal to default. Removing." % opt.name)
                        conf.remove(path)
                    self._after_remove_option(opt)
                    continue

                if self.definitions and new_value in self.definitions:
                    manifest = Configuration('ini')
                    if os.path.exists(self._manifest_path):
                        manifest.read(self._manifest_path)
                    try:
                        if manifest.get('%s/type' % opt.name) == 'boolean':
                            new_value = self.definitions[new_value]
                    except NoPathError, e:
                        pass

                self._logger.debug("Check that '%s' value changed:'%s'='%s'"%(opt.name, value, new_value))

                if new_value == value:
                    self._logger.debug("Skip option '%s'. Not changed" % opt.name)
                    pass
                else:
                    self._logger.debug("Set option '%s' = '%s'" % (opt.name, new_value))
                    self._logger.debug('Set path %s = %s', path, new_value)
                    conf.set(path, new_value, force=True)
                    self._after_set_option(opt, new_value)
            else:
                if value:
                    self._logger.debug("Removing option '%s'. Not found in preset" % opt.name)
                    conf.remove(path)
                self._after_remove_option(opt)

        self._after_apply_preset()
        conf.write(self._config_path)

    def _after_set_option(self, option_spec, value):
        pass

    def _after_remove_option(self, option_spec):
        pass

    def _before_apply_preset(self):
        pass

    def _after_apply_preset(self):
        pass

    @property
    def _manifest_path(self):
        cnf = bus.cnf
        presets_path = os.path.join(cnf.home_path, 'presets')
        manifests_dir = presets_path + "/manifests"

        if not os.path.exists(manifests_dir):
            os.makedirs(manifests_dir)

        path = os.path.join(manifests_dir, self.behaviour + '.ini')
        return path

    @property
    def _manifest(self):

        class HeadRequest(urllib2.Request):
            def get_method(self):
                return "HEAD"

        manifest_url = bus.scalr_url + '/storage/service-configuration-manifests/%s.ini' % self.behaviour
        path = self._manifest_path


        url_handle = urllib2.urlopen(HeadRequest(manifest_url))
        headers = url_handle.info()
        url_last_modified = headers.getdate("Last-Modified")

        file_modified = tuple(time.localtime(os.path.getmtime(path))) if os.path.exists(path) else None

        if not file_modified or url_last_modified > file_modified:
            self._logger.debug('Fetching %s', manifest_url)
            response = urllib2.urlopen(manifest_url)
            data = response.read()
            if data:
                old_manifest = Configuration('ini')
                if os.path.exists(path):
                    old_manifest.read(path)

                new_manifest = Configuration('ini')
                o = StringIO()
                o.write(data)
                o.seek(0)
                new_manifest.readfp(o)

                new_sections = new_manifest.sections('./')
                old_sections = old_manifest.sections('./')

                diff_path = os.path.join(os.path.dirname(path), self.behaviour + '.incdiff')
                diff = Configuration('ini')

                if old_sections and old_sections != new_sections:
                    #skipping diff if no previous manifest found or it is equal to the new one
                    if os.path.exists(diff_path):
                        diff.read(diff_path)

                    sys_vars = self.get_system_variables()

                    for section in new_sections:
                        if section not in old_sections and sys_vars.has_key(section):
                            sys_var = sys_vars[section]
                            if self.definitions:
                                if self.definitions.has_key(sys_var):
                                    sys_var = self.definitions[sys_var]
                            diff.add('./%s/default-value' % section, sys_var, force=True)
                            diff.write(diff_path)

                if os.path.exists(diff_path):
                    diff.read(diff_path)

                for variable in diff.sections('./'):
                    sys_value = diff.get('./%s/default-value' % variable)
                    if sys_value and variable in new_manifest.sections('./'):
                        new_manifest.set('./%s/default-value' % variable, sys_value, force=True)
                new_manifest.write(path)

        return _CnfManifest(path)

    @property
    def _software_version(self):
        '''
        Override is subclass
        '''
        pass

    '''
    Move into <Service>CnfController
    def _get_config_type(self, service_name):
            services = {'mysql':'mysql',
                            'app':'apache',
                            'www':'nginx',
                            'cassandra':'xml'}
            return services[service_name] if services.has_key(service_name) else service_name
    '''


class Options:

    _options = None
    def __init__(self, *args):

        self._options = args

        for optspec in args:
            setattr(self, optspec.name, optspec)

    def __iter__(self):
        return self._options.__iter__()


class _OptionSpec():
    name = None
    section = None
    default_value = None
    supported_from = None
    need_restart = None
    inaccurate = None
    extension = None

    def __init__(self, name=None, section=None, default_value=None, supported_from=None,
                            need_restart=True, inaccurate=False, **extension):
        self.name = name
        self.section = section
        self.default_value = default_value
        self.supported_from = supported_from
        self.need_restart = need_restart
        self.inaccurate = inaccurate
        self.extension = extension or dict()

    @staticmethod
    def from_ini(ini, section, defaults=None):
        ret = _OptionSpec(section)
        spec = dict(ini.items(section))
        defaults = defaults or dict()

        key = 'config-section'
        ret.section = spec.get(key, defaults.get(key, None))

        key = 'default-value'
        ret.default_value = spec.get(key, defaults.get(key, None))

        key = 'supported-from'
        tmp = spec.get(key, defaults.get(key, None))
        ret.supported_from = tmp and tuple(map(int, tmp.split('.'))) or None

        key = 'need-restart'
        ret.need_restart = bool(int(spec.get(key, defaults.get(key))))

        key = 'inaccurate'
        ret.inaccurate = bool(spec.get(key, defaults.get(key, False)))

        for key, value in spec.items():
            if not key in ('config-section', 'default-value',
                            'supported-from', 'need-restart', 'inaccurate'):
                ret.extension[key] = value

        return ret

    def __repr__(self):
        return '%s (section: %s, default_value: %s)' % (self.name, self.section, self.default_value)


class _CnfManifest:
    _options = None
    _defaults = None

    def __init__(self, manifest_path):
        self._options = []
        ini = Configuration('ini')
        ini.read(manifest_path)
        try:
            self._defaults = dict(ini.items('__defaults__'))
        except NoPathError:
            self._defaults = dict()

        for name in ini.sections("./"):
            if name == '__defaults__':
                continue
            self._options.append(_OptionSpec.from_ini(ini, name, self._defaults))

    def __iter__(self):
        return self._options.__iter__()
