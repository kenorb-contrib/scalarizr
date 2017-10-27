"""
In this example we'll try to explain basic habibi principles by creating block-device
behavior mock.
"""
 
import os
import json
import uuid
import time
 
import habibi
from habibi import events
from lettuce import step, world
 
import xml.etree.ElementTree as etree
 
world.servers = list()
world.per_server_data = dict()
world.storages_created = dict()
 
storage_conf = None
 
"""
 
"""
 
 
class SpyExample(object):
 
    """
    SpyExample object reacts on HostInitResponse message hook, and updates it with storage configuration.
    It also reacts on HostUp event, where it takes volume information from server and stores it locally.
    Subscribe spy's methods using `listener` decorator.
 
    Since block device uses queryenv to recieve storage configuration in runtime, list_farm_role_params
    request should be handled and fullfilled with storage configurations. We store storage configurations in
    spy objects, so it will be responsible for queryenv (see SpyExample __init__ method)
 
    Habibi implements base scalr <-> scalarizr communication, which includes:
        HostInit distribution
        HostInitResponse on HostInit
        BeforeHostUp distribution
        HostUp distribution
 
    You still can alter messages habibi sends to scalarizrs if your spy is subscribed for
    specific events (will be described futher in this test)
 
    Standard queryenv queries implemented in habibi as well. You could expand methods and existent
    responses if you subscribe to farm's queryenv.
 
    """
    def __init__(self, role):
        self.role = role
        self.storages_for_servers = dict()
 
    @events.listener(event='queryenv')
    def queryenv(self, event):
        """
        Queryenv event has 3 attributes:
 
        method_name - queryenv method name, e.g. list_roles,
        response - response object, you should update this,
        server - server object of server who requested queryenv
        """
        method_name = event.method_name
        if method_name == 'list_farm_role_params':
            # Let's fill queryenv response with storages for specific server
            volumes_el = etree.Element('volumes')
            for volume in self.storages_for_servers.get(event.server.index, []):
                volume_el = habibi.util.dict2xml(volume, root_name='volume')
                volumes_el.append(volume_el)
            event.response.append(volumes_el)
 
        elif method_name == 'list_ebs_mountpoints':
            # Return empty list
            event.response.append(etree.Element('mountpoints'))
 
 
    @events.listener({'event': 'outgoing_message', 'message.name': 'HostInitResponse'})
    def hir(self, event):
        """
        outgoing_message event has 2 necessarily attributes:
 
        message - outgoing message itself,
        target_server - recipient server
 
        if message sending was triggered by some other message (e.g. HostInit message triggers
        broadcast HostInit to all servers in farm, and also triggers HostInitResponse), outgoing_event
        has two additional attributes:
 
        trigger_server - server who sent trigger message
        trigger_message - message, that triggered this event
        """
        server = event.target_server # destination server
        hir_msg = event.message # HostInitResponse message which we can alter
        hi_msg = event.trigger_message # Message that triggered target_msg creation (HostInit in this case)
 
        # Create new storage if not exist for current server index
        if not server.index in self.storages_for_servers:
            # Server with such an index has never created storage, send configuration for creation
            hir_msg.body['volumes'] = [globals()['storage_conf']]
        else:
            # Send volume for specific server index
            hir_msg.body['volumes'] = self.storages_for_servers[server.index]
 
    @events.listener({'event': 'incoming_message', 'message.name': 'HostUp'})
    def hostup(self, event):
        """
        incoming_message event has only 2 attributes:
 
        server - server who sent the message
        message - incoming message itself
        """
 
        # Save volume configurations from hostup
        msg = event.message
        server = event.server
        self.storages_for_servers[server.index] = msg.body['volumes']
 
 
 
@step('have configured role with additional storage')
def configured_role(step):
    globals()['storage_conf'] = storage_conf = json.loads(step.multiline)
    mpoint = storage_conf['mpoint']
    world.data_path = os.path.join(mpoint, 'unique.data')
 
 
@step('start farm')
def start_farm(step):
    """
    Test should create Habibi object (scalr farm), add essential roles (.add_role method):
    """
 
    world.farm = habibi.Habibi()
    world.role = world.farm.add_role('my-role', [])
 
    """
    Next step is to add user-defined object as a spy to our farm. This object will
    react to farm's events and create events itself, thus emulating scalr's side actions
    for different behaviors.
    """
 
    # Passing role to spy is convinient but not necessary
    world.spy = SpyExample(world.role)
    world.farm.spy(world.spy)
 
    """
    Then start farm (messaging, queryenv and storage services)
    """
    world.farm.start()
 
 
@step('I scale my role to (\d) servers?')
def scale_up(step, n):
    # Note that there's NO autoscaling in habibi - all servers should be launched manually in test.
    n = int(n)
    world.min_instances = n
    for _ in range(n):
        world.servers.append(world.role.run_server())
 
 
@step('I see (\d) running servers?')
def i_see_n_running_servers(step, n):
    n = int(n)
 
    assert len(world.servers) == n, world.servers
 
    for _ in range(48):
        if all(map(lambda s: s.status == 'running', world.servers)):
            break
        time.sleep(5)
    else:
        raise Exception("I don't see %d running servers in farm" % n)
 
 
@step('I see additional storages were created and attached')
def check_storages(step):
    for server in world.servers:
        assert server.index in world.spy.storages_for_servers
        storage_conf = world.spy.storages_for_servers[server.index][0]
        assert 'id' in storage_conf
 
        world.storages_created[server.index] = storage_conf['id']
 
 
@step('I create some unique data on these storages')
def create_data(step):
    for server in world.servers:
        data = str(uuid.uuid4())
        world.per_server_data[server.index] = data
 
        server.execute('echo %s | sudo tee %s' % (data, world.data_path))
 
 
@step('I restart my farm')
def restart_farm(step):
    for server in world.servers:
        server.terminate()
 
    world.servers = list()
 
    for _ in range(world.min_instances):
        world.servers.append(world.role.run_server())
 
 
@step('I see that old storages were attached')
def old_storages_attached(step):
    for server in world.servers:
        assert world.spy.storages_for_servers[server.index][0]['id'] == \
                world.storages_created[server.index]
 
 
@step('I see my unique data on those storages')
def check_integrity(step):
    for server in world.servers:
        data_on_server = server.execute("cat %s" % world.data_path)[0]
        assert world.per_server_data[server.index] in data_on_server
 
