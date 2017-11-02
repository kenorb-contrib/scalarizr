import os
import logging
import shutil
import json


from scalarizr import handlers, linux
from scalarizr.bus import bus
from scalarizr.node import __node__
from scalarizr.linux import pkgmgr, iptables
from scalarizr.bus import bus


LOG = logging.getLogger(__name__)


def get_handlers():
    return [RouterHandler()]


class RouterHandler(handlers.Handler):

    def __init__(self):
        self._data = None
        bus.on(init=self.on_init, start=self.on_start)
        super(RouterHandler, self).__init__()

    def on_init(self):
        bus.on(
            host_init_response=self.on_host_init_response,
            before_host_up=self.on_before_host_up
        )

    def on_host_init_response(self, hir):
        """
        Accept HostInitResponse configuration

        .. code-block:: xml

            <router>
                <cidr>10.0.0.0/16</cidr>
                <whitelist>
                    <item></item>
                </whitelist>
                <scalr_addr>https://my.scalr.net<scalr_addr>
            <router>
        """
        if not hir.body.get('router'):
            msg = "HostInitResponse message for Router behavior " \
                        "must have 'router' property"
            raise handlers.HandlerError(msg)

        self._data = hir.router

    def on_start(self):
        if __node__['state'] == 'running':
            lfrp = bus.queryenv_service.list_farm_role_params(farm_role_id=__node__['farm_role_id'])['params']
            self._data = lfrp.get('router', {})
            if self._data:
                self._configure()
            else:
                linux.system('/etc/init.d/nginx start', shell=True, raise_exc=False)

    def on_before_host_up(self, hostup):
        self._configure()

    def _configure(self):
        pkgmgr.installed('augeas-tools' if linux.os['family'] == 'Debian' else 'augeas', updatedb=True)
        augscript = '\n'.join([
            'set /files/etc/sysctl.conf/net.ipv4.ip_forward 1',
            'rm /files/etc/sysctl.conf/net.bridge.bridge-nf-call-ip6tables',
            'rm /files/etc/sysctl.conf/net.bridge.bridge-nf-call-iptables',
            'rm /files/etc/sysctl.conf/net.bridge.bridge-nf-call-arptables',
            'save'
        ])
        linux.system(('augtool',), stdin=augscript) 
        linux.system(('sysctl', '-p'))

        if self._data.get('cidr'):
            iptables.ensure({'POSTROUTING': [{
                'table': 'nat', 
                'source': self._data['cidr'], 
                'not_destination': self._data['cidr'],
                'jump': 'MASQUERADE'
                }]})

        solo_home = '/tmp/chef'
        solo_rb = '%s/solo.rb' % solo_home
        solo_attr = '%s/attr.json' % solo_home
        pkgmgr.installed('git')
        if os.path.exists(solo_home):
            shutil.rmtree(solo_home)
        linux.system('git clone https://github.com/Scalr/cookbooks.git %s' % solo_home, shell=True)
        with open(solo_attr, 'w+') as fp:
            json.dump({
                'run_list': ['recipe[scalarizr_proxy]'],
                'scalarizr_proxy': {
                    'scalr_addr': self._data['scalr_addr'],
                    'whitelist': self._data['whitelist']
                }
            }, fp)
        with open(solo_rb, 'w+') as fp:
            fp.write(
                'file_cache_path "%s"\n'
                'cookbook_path "%s/cookbooks"' % (solo_home, solo_home)
            )
        linux.system(('chef-solo', '-c', solo_rb, '-j', solo_attr), 
                close_fds=True, preexec_fn=os.setsid, log_level=logging.INFO)

