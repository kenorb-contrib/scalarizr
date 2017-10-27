 
from scalarizr import handlers
 
import mock
 
 
def test_transfer_result_to_backup_result():
    mnf = mock.Mock(
                    files=[{'chunks': [
                            ['f101.part', None, 1048576],
                            ['f102.part', None, 5942]
                    ]}, {'chunks': [
                            ['f201.part', None, 65403]
                    ]}],
                    cloudfs_path='s3://path/to/backup/manifest.json')
 
    result = handlers.transfer_result_to_backup_result(mnf)
 
    assert len(result) == 3
    assert result[0] == dict(path='s3://path/to/backup/f101.part', size=1048576)
    assert result[1] == dict(path='s3://path/to/backup/f102.part', size=5942)
    assert result[2] == dict(path='s3://path/to/backup/f201.part', size=65403)
 
