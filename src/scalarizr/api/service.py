'''
Created on Aug 16, 2012

@author: Dmytro Korsakov
'''

from __future__ import with_statement

import inspect

from scalarizr import rpc
from scalarizr.util import Singleton
from scalarizr.api import operation
from scalarizr.bus import bus
from scalarizr.node import __node__
from scalarizr.api.nginx import NginxAPI
from scalarizr.api.apache import ApacheAPI
from scalarizr.services import mysql, postgresql, redis

services = {
    "mysql2": mysql.MySQLPresetProvider,
    "percona": mysql.MySQLPresetProvider,
    "mariadb": mysql.MySQLPresetProvider,
    "postgresql": postgresql.PgSQLPresetProvider,
    "redis": redis.RedisPresetProvider
}


behavior_apis = {
    'www': NginxAPI,
    'app': ApacheAPI,
}


class ServiceAPI(object):
    """
    Basic API for managing service configuration presets.

    Namespace::

        service
    """
    __metaclass__ = Singleton

    def __init__(self):
        self._op_api = operation.OperationAPI()
        self.queryenv = bus.queryenv_service

    @rpc.query_method
    def get_preset(self, behavior):
        """
        Returns current service configuration preset

        :param behavior: service name.
        :type behavior: str.
        """
        if behavior not in services:
            raise AssertionError('Behaviour %s is not registred in ServiceAPI (%s)' % (behavior, str(services.keys())))

        provider_cls = services[behavior]
        provider = provider_cls()
        manifest = provider.get_manifest(behavior)
        if manifest:
            return provider.get_preset(manifest)
        else:
            raise BaseException('Cannot retrieve preset: Manifest not found.')

    @rpc.command_method
    def set_preset(self, behavior, values):
        """
        Sets configuration preset.
        """
        if behavior not in services:
            raise AssertionError('Behaviour %s is not registred in ServiceAPI (%s)' % (behavior, str(services.keys())))

        provider_cls = services[behavior]
        provider = provider_cls()
        manifest = provider.get_manifest(behavior)
        if manifest:
            provider.set_preset(values, manifest)
        else:
            raise AssertionError('Cannot apply preset: Manifest not found.')

    def do_reconfigure(self, op, behavior_params=None):
        """
        behavior_params is dict where behavior names are keys and they
        reconfigure params are values (they are itself dicts)
        """
        queryenv_answer = self.queryenv.list_farm_role_params(__node__['farm_role_id'])
        queryenv_behavior_params = queryenv_answer['params']
        if not behavior_params:
            behavior_params = queryenv_behavior_params

        behaviors = behavior_params.keys()
        for behavior in behaviors:
            if behavior not in behavior_apis:
                continue
            api = behavior_apis[behavior]()
            #TODO:
            reconfigure_argspecs = inspect.getargspec(api.reconfigure).args
            reconfigure_argspecs.remove('self')

            queryenv_reconf_params = queryenv_behavior_params.get(behavior, {})

            reconfigure_params = behavior_params.get(behavior, {})
            if reconfigure_params == None:
                reconfigure_params = queryenv_reconf_params
            reconfigure_params = dict((k, v)
                                      for k, v in reconfigure_params.items()
                                      if k in reconfigure_argspecs)

            if hasattr(api, 'init_service'):
                api.init_service()
            api.do_reconfigure(op, **reconfigure_params)

    @rpc.query_method
    def reconfigure(self, behavior_params=None, async=True):
        """
        If behavior_params is None - reconfiguring all behaviors.
        If behavior_params has behavior key and its value is None - taking its
        params from queryenv.
        """
        self._op_api.run('api.service.reconfigure',
                         func=self.do_reconfigure,
                         func_kwds={'behavior_params': behavior_params},
                         async=async,
                         exclusive=True)
