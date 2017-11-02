import sys
from xml.dom import minidom
try:
    import json as json_module
except ImportError:
    import simplejson as json_module
import yaml

from scalarizr.bus import bus
from scalarizr.adm.command import Command
from scalarizr.adm.command import CommandError
from scalarizr.adm.command import TAB_SIZE
from scalarizr.node import __node__
from scalarizr.util import initdv2
from scalarizr.api.apache import ApacheAPI
from scalarizr.api.chef import ChefAPI
from scalarizr.api.haproxy import HAProxyAPI
from scalarizr.api.memcached import MemcachedAPI
from scalarizr.api.mysql import MySQLAPI
from scalarizr.api.nginx import NginxAPI
from scalarizr.api.postgresql import PostgreSQLAPI
from scalarizr.api.rabbitmq import RabbitMQAPI
from scalarizr.api.redis import RedisAPI
from scalarizr.api.tomcat import TomcatAPI


service_apis = {
    'apache': ApacheAPI,
    'chef': ChefAPI,
    'haproxy': HAProxyAPI,
    'mariadb': MySQLAPI,
    'memcached': MemcachedAPI,
    # 'mongodb',
    'mysql': MySQLAPI,
    'nginx': NginxAPI,
    'percona': MySQLAPI,
    'postgresql': PostgreSQLAPI,
    'rabbitmq': RabbitMQAPI,
    'redis': RedisAPI,
    'tomcat': TomcatAPI,
}


service_to_behavior = {
    'nginx': 'www',
    'mysql': 'mysql2',
    'apache': 'app'
}


class ReturnCode:
    RUNNING = 0
    STOPPED = 3
    UNKNOWN = 4
    MIXED = 150


class Service(Command):
    """
    Scalarizr service control.

    Usage:
      service redis (start | stop) [(<index> | --port=<port>)]
      service redis status [--format=(xml|json|yaml)] [(<index> | --port=<port>)]
      service <service> (start | stop)
      service <service> status [--format=(xml|json|yaml)]

    Options:
      -p <port>, --port=<port>
      -f <format>, --format=<format>
    """
    # TODO: add usage for mongo:
    # service mongodb (start | stop | status) [(mongos | mongod |
    #        configsrv | configsrv-2 | configsrv-3 | arbiter)]

    aliases = ['s']

    def help(self):
        doc = super(Service, self).help()
        services = [(' '*TAB_SIZE) + s for s in service_apis.keys()]
        return doc + '\nSupported services:\n' + '\n'.join(services)

    def _start_service(self, service, **kwds):
        api = service_apis[service]()
        for key, value in kwds.items():
            if value == None:
                kwds.pop(key)
        try:
            print 'Starting %s' % service
            api.start_service(**kwds)
        except (BaseException, Exception), e:
            print 'Service start failed.\n%s' % e
            return int(CommandError())

    def _stop_service(self, service, **kwds):
        api = service_apis[service]()
        for key, value in kwds.items():
            if value == None:
                kwds.pop(key)
        try:
            print 'Stopping %s' % service
            api.stop_service(**kwds)
        except (BaseException, Exception), e:
            print 'Service stop failed.\n%s' % e
            return int(CommandError())

    def _dict_to_xml(self, d, name):
        doc = minidom.Document()
        container = doc.createElement(name)
        doc.appendChild(container)
        for k, v in d.items():
            el = doc.createElement(str(k))
            text = doc.createTextNode(str(v))
            el.appendChild(text)
            container.appendChild(el)
        return doc

    def _dict_list_to_xml(self, list_of_dicts, list_name, element_name):
        doc = minidom.Document()
        container = doc.createElement(list_name)
        doc.appendChild(container)
        for element in list_of_dicts:
            child_doc = self._dict_to_xml(element, element_name)
            container.appendChild(child_doc.childNodes[0])
        return doc

    def _format_status(self, status_struct, format='xml'):
        if format == 'xml':
            if isinstance(status_struct, dict):
                doc = self._dict_to_xml(status_struct, 'status')
            else:
                doc = self._dict_list_to_xml(status_struct, 'statuses', 'status')
            return doc.toprettyxml(encoding='utf-8')
        elif format == 'json':
            return json_module.dumps(status_struct, indent=4, sort_keys=True, ensure_ascii=False)
        elif format == 'yaml':
            return yaml.dump(status_struct, default_flow_style=False, allow_unicode=True)
        else:
            raise CommandError('Unknown output format.\nAvailable formats: xml, json, yaml')

    def _display_service_status(self, service, print_format='xml', **kwds):
        api = service_apis[service]()
        for key, value in kwds.items():
            if value == None:
                kwds.pop(key)
        status = api.get_service_status(**kwds)

        if service == 'redis':
            return self._print_redis_status(status, print_format)
        # TODO: make xml, json or yaml and dump it to out
        status_message = '%s service is stopped' % service
        code = ReturnCode.STOPPED
        if status == initdv2.Status.RUNNING:
            status_message = '%s service is running' % service
            code = ReturnCode.RUNNING
        elif status == initdv2.Status.UNKNOWN:
            status_message = '%s service has unknown status' % service
            code = ReturnCode.UNKNOWN
        # print service + status_message

        status_dict = {'code': code, 'message': status_message}
        print self._format_status(status_dict, print_format)

        return code

    def _print_redis_status(self, statuses, print_format='xml'):
        if not statuses:
            print 'No redis configuration found.'
            return ReturnCode.STOPPED

        statuses_list = []
        for i, (port, status) in enumerate(statuses.items()):
            status_message = 'Redis process on port %s is stopped' % port
            if status == initdv2.Status.RUNNING:
                status_message = 'Redis process on port %s is running' % port
            status_dict = {'port': port, 'code': status, 'message': status_message}
            statuses_list.append(status_dict)
            # print '- port: %s\n  status: %s' % (port, status_message)
        print self._format_status(statuses_list, print_format)

        overall_status = set(statuses.values())
        if len(overall_status) > 1:
            return ReturnCode.MIXED
        if overall_status.pop() == initdv2.Status.RUNNING:
            return ReturnCode.RUNNING
        return ReturnCode.STOPPED

    def __call__(self,
        service=None,
        start=False,
        stop=False,
        status=False,
        format='xml',

        redis=False,
        index=None,
        port=None,

        mongodb=False,
        mongos=False,
        mongod=False,
        configsrv=False,
        configsrv_2=False,
        configsrv_3=False,
        arbiter=False,
        ):
        if redis:
            service = 'redis'
            if index:
                if '-' in index:
                    _start, _end = map(int, index.split('-'))
                    index = range(_start-1, _end)
                else:
                    index = int(index) - 1
            elif port and '-' in port:
                _start, _end = map(int, port.split('-'))
                port = range(_start-1, _end)
        elif mongodb:
            mongo_component = ((mongos and 'mongos') or
                               (mongod and 'mongod') or
                               (configsrv and 'configsrv') or
                               (configsrv_2 and 'configsrv-2') or
                               (configsrv_3 and 'configsrv-3') or
                               (arbiter and 'arbiter'))
            # TODO: finish

        if service not in service_apis:
            print 'Unknown service/behavior.'
            return ReturnCode.UNKNOWN

        if service not in __node__['behavior'] and \
            service_to_behavior.get(service) not in __node__['behavior']:
            print 'Not installed service/behavior.'
            return ReturnCode.UNKNOWN

        if start:
            return self._start_service(service, indexes=index, ports=port)
        elif stop:
            return self._stop_service(service, indexes=index, ports=port)
        elif status:
            return self._display_service_status(service,
                indexes=index,
                ports=port,
                print_format=format)


commands = [Service]
