 
import mock
 
from scalarizr.services import backup
 
 
@mock.patch('scalarizr.storage2.volume')
class TestSnapBackup(object):
    def test_run(self, vol_factory):
        volume = {'type': 'ebs'}
        def ln(vol, state):
            state['custom'] = 1
        listener = mock.Mock(side_effect=ln)
        bak = backup.backup(
                        type='snap',
                        volume=volume)
        bak.on(freeze=listener)
 
        rst = bak.run()
 
        listener.assert_called_with(vol_factory.return_value, mock.ANY)
        assert rst.type == 'snap'
        assert rst.snapshot == vol_factory.return_value.snapshot.return_value
        assert rst.custom == 1
 
 
@mock.patch('scalarizr.storage2.volume')
@mock.patch('scalarizr.storage2.snapshot')
class TestSnapRestore(object):
    def test_run_with_volume(self, snap_factory, vol_factory):
        rst = backup.restore(
                        type='snap',
                        volume={'type': 'ebs', 'iops': 10, 'avail_zone': 'us-east-1c'},
                        snapshot={'type': 'ebs', 'id': 'snap-12345678'})
 
        result = rst.run()
 
        vol = vol_factory.return_value
        snap = snap_factory.return_value
 
        assert vol.snap == snap
        vol.ensure.assert_called_with()
        assert result == vol
        assert rst.result() == vol
 
 
    def test_run_without_volume(self, snap_factory, vol_factory):
        rst = backup.restore(
                        type='snap',
                        snapshot={'type': 'ebs', 'id': 'snap-12345678'})
 
        result = rst.run()
 
        assert result == snap_factory.return_value.restore.return_value
 
