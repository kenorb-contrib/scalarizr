import itertools
import mock
from nose.tools import *
 
from scalarizr.api.operation import OperationAPI
from scalarizr.storage2.volumes.base import Volume
 
 
@mock.patch('scalarizr.linux')
class TestBlockDevice(object):
 
	op_api = OperationAPI()
 
	@mock.patch('scalarizr.bus.bus', init_op=op_api.create('system.init', mock.Mock()))
	@mock.patch.object(Volume, 'ensure')
	def test_on_host_init_response_new_style_volumes(self, *args):
		'''
		plug new-style volumes
		'''
 
		from scalarizr.bus import bus
		from scalarizr.handlers.block_device import BlockDeviceHandler
		from scalarizr.messaging import Message
 
		hir = Message('HostInitResponse', None, {
			'volumes': [{
				'type': 'ebs',
				'size': '1',
				'mpoint': '/mnt/ebs'
			}, {
				'type': 'ebs',
				'size': '10',
				'snap': {
					'type': 'ebs',
					'id': 'snap-12345678'
				},
				'mpoint': '/mnt/volume'
			}, {
				'id': 'vol-12345678',
				'type': 'ebs',
				'fstype': 'xfs',
				'mpoint': '/mnt/kazu'
			}, {
				'type': 'raid',
				'level': '5',
				'disks': list(itertools.repeat({'type': 'ebs', 'size': 50}, 3)),
				'fstype': 'xfs',
				'mpoint': '/mnt/raid5'
			}, {
				'type': 'raid',
				'level': '5',
				'disks': list(itertools.repeat({'type': 'ebs', 'size': 50}, 3)),
				'snap': {
					'type': 'raid',
					'disks': [{
						'type': 'ebs',
						'id': 'snap-a1b2c3d4'
					}, {
						'type': 'ebs',
						'id': 'snap-12345678'
					}, {
						'type': 'ebs',
						'id': 'snap-1a2b3c4d'
					}]
				}
			}, {
				'id': 'raid-vol-12345678',
				'type': 'raid',
				'level': '1',
				'disks': [{
					'id': 'vol-12345678',
					'type': 'ebs'
				}, {
					'id': 'vol-87654321',
					'type': 'ebs'
				}],
				'mpoint': '/mnt/keystone'
			}]
		})
		hdlr = BlockDeviceHandler('ebs')
		hdlr.on_host_init_response(hir)
 
		messages = list(rec.message for rec in bus.init_op.logs)
		assert_equals(messages[1], 'Ensure ebs: create volume, make ext3 filesystem, mount to /mnt/ebs')
		assert_equals(messages[2], 'Ensure ebs: create volume from snap-12345678, mount to /mnt/volume')
		assert_equals(messages[3], 'Ensure ebs: take vol-12345678, mount to /mnt/kazu')
		assert_equals(messages[4], 'Ensure raid5: create 3 ebs volumes, make xfs filesystem, mount to /mnt/raid5')
		assert_equals(messages[5], 'Ensure raid5: create 3 ebs volumes from snapshots (snap-a1b2c3d4, snap-12345678, snap-1a2b3c4d)')
		assert_equals(messages[6], 'Ensure raid1: take raid-vol-12345678 (2 ebs disks: vol-12345678, vol-87654321), mount to /mnt/keystone')
 
 
 
