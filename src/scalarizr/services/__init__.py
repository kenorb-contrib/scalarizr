from __future__ import with_statement
'''
Created on Jul 7, 2011

@author: shaitanich
'''

import os
import logging
import urllib2

try:
    import json
except ImportError:
    import simplejson as json

from scalarizr.bus import bus
from scalarizr.libs.metaconf import Configuration, NoPathError
from scalarizr.util import initdv2, PopenError
import shutil


UNSET_CONST = '*unset*'
LOG = logging.getLogger(__name__)


def lazy(init):
    def wrapper(cls, *args, **kwargs):
        obj = init(cls, *args, **kwargs)
        return LazyInitScript(obj)
    return wrapper


class LazyInitScript(object):

    _script = None
    reload_queue = None
    restart_queue = None

    def __getattr__(self, name):
        return getattr(self._script, name)

    def __init__(self, script):
        self._script = script
        self.reload_queue = []
        self.restart_queue = []

    def start(self):
        try:
            if not self._script.running:
                self._script.start()
            elif self.restart_queue:
                reasons = ' '.join([req+',' for req in self.restart_queue])[:-1]
                self._script.restart(reasons)
            elif self.reload_queue:
                reasons = ' '.join([req+',' for req in self.reload_queue])[:-1]
                self._script.reload(reasons)
        finally:
            self.restart_queue = []
            self.reload_queue = []

    def stop(self, reason=None):
        if self._script.running:
            try:
                LOG.info('Stopping service: %s' % reason)
                self._script.stop(reason)
            finally:
                self.restart_queue = []
                self.reload_queue = []

    def restart(self, reason=None, force=False):
        if force:
            self._script.restart(reason)
        elif  self._script.running:
            self.restart_queue.append(reason)

    def reload(self, reason=None, force=False):
        if force:
            self._script.reload(reason)
        elif self._script.running:
            self.reload_queue.append(reason)

    def configtest(self, path=None):
        if hasattr(self._script, 'configtest'):
            self._script.configtest(path)


    @property
    def running(self):
        return self._script.running

    def status(self):
        return self._script.status()


class BaseService(object):

    _objects = None

    def _set(self, key, obj):
        self._objects[key] = obj

    def _get(self, key, callback, *args, **kwargs):
        if not self._objects.has_key(key):
            self._set(key, callback(*args, **kwargs))
        return self._objects[key]


class BaseConfig(object):

    '''
    Parent class for object representations of postgresql.conf and recovery.conf which fortunately both have similar syntax
    '''

    autosave = None
    path = None
    data = None
    config_name = None
    config_type = None
    comment_empty = False


    def __init__(self, path, autosave=True):
        self._logger = logging.getLogger(__name__)
        self.autosave = autosave
        self.path = path


    @classmethod
    def find(cls, config_dir):
        return cls(os.path.join(config_dir.path, cls.config_name))


    def set(self, option, value):
        self.apply_dict({option:value})


    def set_path_type_option(self, option, path):
        if not os.path.exists(path):
            raise ValueError('%s %s does not exist' % (option, path))
        self.set(option, path)


    def set_numeric_option(self, option, number):
        try:
            assert number is None or type(number) is int
        except AssertionError:
            raise ValueError('%s must be a number (got %s instead)' % (option, number))

        is_numeric = type(number) is int
        self.set(option, str(number) if is_numeric else None)


    def get(self, option):
        self._init_configuration()
        try:
            value = self.data.get(option)
        except NoPathError:
            try:
                value = getattr(self, option+'_default')
            except AttributeError:
                value = None
        self._cleanup()
        return value


    def get_numeric_option(self, option):
        value = self.get(option)
        return value if value is None else int(value)


    def to_dict(self):
        self._init_configuration()

        result = {}

        for section in self.data.sections('./'):
            try:
                kv = dict(self.data.items(section))
            except NoPathError:
                kv = {}
            for variable, value in kv.items():
                path = '%s/%s' % (section,variable)
                result[path] = value
        '''
        variables in root section
        '''
        for variable,value in self.data.items('.'):
            if value and value.strip():
                result[variable] = value

        self._cleanup()
        return result


    def apply_dict(self, kv):
        self._init_configuration()
        for path, value in kv.items():
            if not value and self.comment_empty:
                self.data.comment(path)
            else:
                self.data.set(path,str(value), force=True)
        self._cleanup(True)


    def delete_options(self, options):
        self._init_configuration()
        for path  in options:
            self.data.remove(path)
        self._cleanup(True)


    def _init_configuration(self):
        if not self.data:
            self.data = Configuration(self.config_type)
            if os.path.exists(self.path):
                self.data.read(self.path)


    def _cleanup(self, save_data=False):
        if self.autosave:
            if save_data and self.data:
                self.data.write(self.path)
            self.data = None


class ServiceError(BaseException):
    pass


class PresetError(BaseException):
    pass


class PresetProvider(object):

    service = None
    config_mapping = None
    backup_prefix = '.scalr.backup'
    preset_version = '2012-09-03'


    '''

    settings = {
            'httpd.conf': {
                    'key': 'value'
            },
            'ssl.conf': {
                    'key': 'value'
            }
    }

    pvd = PresetPvd(svs, {
            'httpd.conf': httpd_conf,
            'ssl.conf': ssl_conf
    })

    '''

    def __init__(self, service, config_mapping):
        self.service = service
        self.config_mapping = config_mapping


    def get_preset(self, manifest):
        preset = {}
        for obj in self.config_mapping.values():
            preset[obj.config_name] = obj.to_dict()
        return self._filter(preset, manifest)


    def set_preset(self, settings, manifest):
        self.backup()

        settings = self._filter(settings, manifest)

        for config_name in settings:
            if config_name in self.config_mapping and settings[config_name]:

                data = {}
                odds = []
                for k,v in settings[config_name].items():
                    if v == UNSET_CONST:
                        odds.append(k)
                    else:
                        data[k] = v

                obj = self.config_mapping[config_name]
                LOG.debug("Applying data: %s ; Deleting odds: %s" % (data, odds))
                obj.apply_dict(data)
                obj.delete_options(odds)

        try:
            self.configtest()
        except (initdv2.InitdError, PopenError), e:
            self.rollback()
            raise PresetError('Service %s was unable to pass configtest: %s' % (self.service.name, e))

        try:
            self.restart('Applying configuration preset to %s service' % self.service.name, force=True)
        except BaseException, err:
            if not self.service.running:
                self.rollback()
                self.service.start()
                raise PresetError('Service %s was unable to start with the new preset: %s' % (self.service.name, err))
            else:
                raise PresetError(err)

        finally:
            self.cleanup()


    def _filter(self, settings, manifest):
        result = {}
        for fname, kv in settings.items():
            filtered = {}

            includes = manifest[fname]['include']
            excludes = manifest[fname]['exclude']

            if not includes:
                filtered.update(kv)

            else:
                for mask in includes:
                    if mask in kv:
                        filtered[mask] = kv[mask]
                    elif mask == '/*':
                        for var in kv:
                            if '/' not in var:
                                filtered[var] = kv[var]
                    elif mask.endswith('/*'):
                        section = mask[:-1]
                        for var in kv:
                            if var.startswith(section):
                                filtered[var] = kv[var]

            if includes:
                for mask in excludes:
                    if mask in filtered:
                        del filtered[mask]
                    elif mask == '/*':
                        for var in filtered:
                            if '/' not in var:
                                del filtered[var]
                    elif mask.endswith('/*'):
                        section = mask[:-1]
                        for var in kv:
                            if var.startswith(section):
                                del filtered[mask]

            result[fname] = filtered
        return result


    def backup(self):
        for obj in self.config_mapping.values():
            src = obj.path
            if os.path.exists(src):
                dst = src + self.backup_prefix
                shutil.copy2(src, dst)


    def cleanup(self):
        for obj in self.config_mapping.values():
            src = obj.path + self.backup_prefix
            if os.path.exists(src):
                os.remove(src)


    def rollback(self):
        self._before_rollback()
        for obj in self.config_mapping.values():
            src = obj.path + self.backup_prefix
            if os.path.exists(src):
                dst = obj.path
                shutil.copy2(src, dst)
        self._after_rollback()


    def _before_rollback(self):
        LOG.info("Starting configuration rollback")


    def _after_rollback(self):
        LOG.info("Configuration has been successfully reverted to previous version.")


    def configtest(self):
        if hasattr(self.service, 'configtest'):
            self.service.configtest()


    def restart(self, reason=None, force=False):
        self.service.restart(reason, force=force)


    def get_manifest_url(self, behaviour):
        return bus.scalr_url + '/storage/service-configuration-manifests/%s/%s.json' % (self.preset_version, behaviour)


    def get_manifest(self, behaviour):
        result = {}
        raw = None
        #download manifest
        manifest_url = self.get_manifest_url(behaviour)
        try:
            response = urllib2.urlopen(manifest_url)
            raw = response.read()
        except (Exception, BaseException), e:
            LOG.warning('Unable to fetch manifest: %s' % e)
        else:
            if raw:
                #parse manifest
                json_obj = json.loads(raw)
                #return black and white lists of variables for each config
                for conf_name, data in json_obj.items():
                    result[conf_name] = dict(include=data['include'], exclude=data['exclude'])
        return result

