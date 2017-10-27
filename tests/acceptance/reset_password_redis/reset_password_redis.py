# -*- coding: utf-8 -*-
 
from lettuce import step
from lettuce import world
import redis
 
from scalarizr.api.binding.jsonrpc_http import HttpServiceProxy
 
 
@step(u'Given I am connected to Redis server')
def given_i_have_mysql_server(step):
    world.conn = HttpServiceProxy('http://localhost:8010',
                                  '/etc/scalr/private.d/keys/default')
 
 
@step(u'When I call reset password')
def when_i_call_reset_password(step):
    world.conn.redis.reset_password(new_password='test_pwd')
 
 
@step(u'Then password should be changed')
def then_password_should_be_changed(step):
    conn = redis.StrictRedis(host='localhost', port=6379, password='test_pwd')
    assert conn is not None
 
