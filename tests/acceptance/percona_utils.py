from scalarizr.linux import pkgmgr
import scalarizr.node
 
import mock
from lettuce import step, before, after, world
 
FEATURE = 'Percona utilities'
 
@before.each_feature
def setup(feature):
    if feature.name == FEATURE:
        p = mock.patch.object(scalarizr.node, '__node__', new={
                        'behavior': ['percona'],
                        'percona': {}
                })
        p.start()
        world.patcher = p
 
        from scalarizr.services import mysql2
        world.mysql2 = mysql2
 
@after.each_feature
def teardown(feature):
    if feature.name == FEATURE:
        if hasattr(world, 'patcher'):
            world.patcher.stop()
 
@step(u'Given i have no percona repository on machine')
def given_i_have_no_percona_repository_on_machine(step):
    mgr = pkgmgr.package_mgr()
    assert 'percona' not in mgr.repos()
 
@step(u'When i execute innobackup --version')
def when_i_execute_innobackup_help(step):
    world.innobackupex_ret = world.mysql2.innobackupex(version=True)[2]
 
@step(u'Then it finishes with 0 code')
def then_it_finishes_with_0_code(step):
    assert world.innobackupex_ret == 0, 'innobackupex exit code: %s' % world.innobackupex_ret
 
@step(r'(And|Given) i have percona repostory on machine')
def and_i_have_percona_repostory_on_machine(step, w1):
    mgr = pkgmgr.package_mgr()
    assert 'percona' in mgr.repos()
 
