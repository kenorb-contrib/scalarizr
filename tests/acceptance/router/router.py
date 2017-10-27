from scalarizr.handlers import router
from scalarizr import messaging, linux
from scalarizr.linux import iptables
 
import mock
from lettuce import step, world, before, after
 
 
HIR_MSG = '''<?xml version="1.0"?>
<message id="d2d2251c-8f65-4175-a131-fc5b99b609e6" name="HostInitResponse">
  <meta>
    <scalr_version>4.1.0</scalr_version>
  </meta>
<body>
  <router>
    <cidr>10.0.0.0/16</cidr>
    <subnets>
      <item>10.0.0.0/24</item>
    </subnets>
    <scalr_addr>https://my.scalr.net</scalr_addr>
    <whitelist>
      <item>184.173.242.34</item>
      <item>174.37.32.18</item>
    </whitelist>
  </router>
</body>
</message>'''
 
 
@before.each_feature
def setup(feature):
    if feature.name == 'Router role for VPC support':
        p = mock.patch.object(router, '__node__', new={
            'state': 'initializing'
        })
        p.start()
        world.node_patcher = p
        world.router_hldr = router.RouterHandler()
 
 
@after.each_feature
def teardown(feature):
    if feature.name == 'Router role for VPC support':
        world.node_patcher.stop()
        world.router_hldr = None
 
 
@step(u'When i receive HIR message')
def when_i_receive_hir_message(step):
    hir_msg = messaging.Message()
    hir_msg.fromxml(HIR_MSG)
    hup_msg = messaging.Message()
    world.router_hldr.on_host_init_response(hir_msg)
    world.router_hldr.on_before_host_up(hup_msg)
 
 
@step(u'Then i see recipe scalarizr_proxy applied')
def then_i_see_recipe_scalarizr_proxy_applied(step):
    assert 'is running' in linux.system('/sbin/service nginx status', shell=True)[0], 'Nginx is running'
    for port in (8008, 8010, 8013):
        assert 'nginx' in linux.system('/sbin/fuser -n tcp %d -v' % port, shell=True)[1], 'Nginx is listening port %d' % port
 
 
@step(u'And iptables masquerading rules applied')
def and_iptables_masquerading_rules_applied(step):
    rules = iptables.POSTROUTING.list('nat')
    assert rules[0]
 
