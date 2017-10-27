__author__ = 'Nick Demyanchuk'
 
import mock
import unittest
 
from scalarizr.storage2.volumes import gce_ephemeral
from scalarizr import storage2
 
 
device_name = 'google-ephemeral-0'
 
 
@mock.patch('os.path.exists')
class TestGceEphemeralStorage(unittest.TestCase):
 
    def test_device_exists(self, exists):
        exists.return_value = False
        vol = gce_ephemeral.GceEphemeralVolume(name=device_name)
        self.assertRaises(storage2.StorageError, vol.ensure)
        assert vol.device is None
 
    def test_device_exists(self, exists):
        exists.return_value = True
        vol = gce_ephemeral.GceEphemeralVolume(name=device_name)
        vol.ensure()
        assert vol.device == '/dev/disk/by-id/%s' % device_name
 
 
    def test_name_is_none(self, exists):
        vol = gce_ephemeral.GceEphemeralVolume()
        self.assertRaises(AssertionError, vol.ensure)
 
