import inspect
import re
import os
import itertools
import sys
from urllib2 import HTTPError
from xml.dom import minidom
try:
    import json as json_module
except ImportError:
    import simplejson as json_module
import yaml

from scalarizr.util import system2
from scalarizr.adm.command import Command
from scalarizr.adm.command import printable_usage
from scalarizr.adm.command import TAB_SIZE
from scalarizr.adm.command import CommandError
from scalarizr.adm.util import make_table
from scalarizr.node import __node__
from scalarizr.queryenv import xml2dict
from scalarizr.queryenv import QueryEnvError
from scalarizr.queryenv import new_queryenv

if sys.version_info[0:2] >= (2, 7):
    from xml.etree import ElementTree as ET
else:
    from scalarizr.externals.etree import ElementTree as ET


def transpose(l):
    return map(list, zip(*l))


class Queryenv(Command):
    """
    Launches queryenv methods.

    Usage:
      queryenv [--format=(xml|json|yaml)] <method> [<args>...]
    
    Options:
      -f <format>, --format=<format>  Output format: xml (default), json or yaml.
    """

    aliases = ['q']
    method_aliases = {'list-virtual-hosts': ['list-virtualhosts']}

    def __init__(self):
        super(Queryenv, self).__init__()

    # def help(self):
    #     doc = super(Queryenv, self).help()
    #     methods = [(' '*TAB_SIZE) + m for m in self.get_supported_methods()]
    #     return doc + '\nSupported methods:\n' + '\n'.join(methods)

    @classmethod
    def get_method_name(cls, alias):
        """
        Returns method name if alias to supported method exists,
        None otherwise.
        """
        if alias in cls.get_supported_oldstyle_methods():
            return alias
        for method, aliases in cls.method_aliases.items():
            if alias in aliases:
                return method
        return None

    @classmethod
    def supports_oldstyle_method(cls, method_or_alias):
        """
        Returns True if method is supported or is alias of supported method
        with table output.
        """
        return cls.get_method_name(method_or_alias) is not None


    @classmethod
    def get_supported_oldstyle_methods(cls):
        """ 
        Returns list of methods that are supported for old-style calling with
        szradm <queryenv_method> [args...] with table output
        """
        # usage_section = printable_usage(cls.__doc__)
        # usages = re.findall(r'queryenv .+?\s', usage_section)
        # methods = [s.split()[1] for s in usages if '<method>' not in s]
        # return methods
        return ['get-https-certificate',
            'get-latest-version',
            'list-ebs-mountpoints',
            'list-roles',
            'list-virtual-hosts']

    @classmethod
    def queryenv(cls):
        if not hasattr(cls, '_queryenv'):
            cls._queryenv = new_queryenv()
        return cls._queryenv

    def _display_get_https_certificate(self, out):
        headers = ['cert', 'pkey', 'cacert']
        print make_table(out, headers)

    def _display_get_latest_version(self, out):
        print make_table([[out]], ['version'])

    def _display_list_ebs_mountpoints(self, out):
        headers = ['name', 'dir', 'createfs', 'isarray', 'volume-id', 'device']
        table_data = []
        for d in out:
            volume_params = [(v.volume_id, v.device) for v in d.volumes]
            volumes, devices = transpose(volume_params)
            table_data.append([d.name, d.dir, d.create_fs, d.is_array, volumes, devices])
        print make_table(table_data, headers)

    def _display_list_roles(self, out):
        headers = ['behaviour',
           'name',
           'farm-role-id',
           'index',
           'internal-ip',
           'external-ip',
           'replication-main']
        table_data = []
        for d in out:
            behaviour = ', '.join(d.behaviour)
            for host in d.hosts:
                table_data.append([behaviour, 
                                   d.name,
                                   d.farm_role_id,
                                   str(host.index),
                                   host.internal_ip,
                                   host.external_ip,
                                   str(host.replication_main)])
        print make_table(table_data, headers)

    def _display_list_virtual_hosts(self, out):
        headers = ['hostname', 'https', 'type', 'raw']
        table_data = [[d.hostname, d.https, d.type, d.raw] for d in out]
        print make_table(table_data, headers)

    def _display_list_global_variables(self, out):
        headers = ['key', 'value']
        table_data = out['public'].items()
        print make_table(table_data, headers)

    def _display_fetch(self, out, format='xml'):
        if format == 'xml':
            print minidom.parseString(out).toprettyxml(encoding='utf-8')
        elif format == 'json':
            out_dict = xml2dict(ET.XML(out))
            print json_module.dumps(out_dict, indent=4, sort_keys=True, ensure_ascii=False)
        elif format == 'yaml':
            out_dict = xml2dict(ET.XML(out))
            print yaml.dump(out_dict, default_flow_style=False, allow_unicode=True)
        else:
            raise CommandError('Unknown output format.\nAvailable formats: xml, json, yaml')

    def _display_out(self, method, out, format='xml'):
        """
        General display method. Searches for certain display method and calls it
        with `out` or prints out in table form or raw. Custom display methods
        should be named by next pattern '_display_<method_name>' where
        <method_name> is queryenv method name with hyphens replaced with
        underscores.
        """
        all_display_methods = [m for m in dir(self) if m.startswith('_display')]
        display_method = None
        if method:
            for m in all_display_methods:
                if method.replace('-', '_') in m:
                    display_method = getattr(self, m)
                    break

        if display_method:
            display_kwds = {}
            argspec = inspect.getargspec(display_method)
            argnames = argspec.args
            if 'format' in argnames:
                display_kwds['format'] = format
            display_method(out, **display_kwds)
        elif isinstance(out, list) and isinstance(out[0], list) and method != 'fetch':
            print make_table(out)
        else:
            print out

    def _run_queryenv_method(self, method, kwds, kwds_mapping=None):
        """
        Executes queryenv method with given `kwds`. `kwds` can contain excessive
        key-value pairs, this method filters it and passes only acceptable
        kwds by target method. `kwds_mapping` defines how `kwds` keys will be
        renamed when passed to queryenv method.
        """
        if not method:
            return
        if not kwds_mapping:
            kwds_mapping = {}

        method = method.replace('-', '_')
        try:
            m = getattr(self.queryenv(), method)
        except AttributeError:
            raise CommandError('Unknown QueryEnv method\n')
        argspec = inspect.getargspec(m)
        argnames = argspec.args
        filtered_kwds = {}
        for k, v in kwds.items():
            arg_name = kwds_mapping.get(k, k)
            if argspec.keywords or arg_name in argnames:
                filtered_kwds[arg_name] = v
                if method == 'fetch':
                    del kwds[k]

        if method == 'fetch':
            filtered_kwds['params'] = kwds
        try:
            return m(**filtered_kwds)
        except (QueryEnvError, HTTPError), e:
            if isinstance(e, HTTPError) and method == 'fetch':
                message = '%s method is not supported' % filtered_kwds['command']
            else:
                message = str(e)
            raise CommandError(message)

    def __call__(self, method=None, format=None, args=None, shortcut=False, **kwds):
        """
        All work is made by fetch method, other methods display methods and
        support remains only for backward compatibility.
        """
        if not args:
            args = []

        if not format:
            format = "xml"

        # we need to find method in kwds because parser places it there
        # param method is presented only when default parsing rule is applied
        # (queryenv <method> [<args>...])
        if not method:
            for kwd in kwds.keys():
                hyphen_kwd = kwd.replace('_', '-')
                _method = self.get_method_name(hyphen_kwd)
                if _method:
                    method = _method
                    kwds.pop(kwd)
                    break
        else:
            # if it's alias - taking real method name
            # it can be not known method so get_method_name() can return None
            # for valid method (such as 'fetch')
            method = self.get_method_name(method) or method

        for pair in args:
            if not pair.startswith('-') and '=' in pair:
                k, v = pair.split('=')
                kwds[k] = v

        kwds_mapping = {}
        if method == 'list-roles':
            kwds_mapping = {'with_initializing': 'with_init'}

        if not shortcut and method != 'fetch':
            kwds['command'] = method
            method = 'fetch'

        out = self._run_queryenv_method(method, kwds, kwds_mapping)

        self._display_out(method, out, format)


commands = [Queryenv]
