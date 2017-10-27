 
import mock
 
from scalarizr.storage2.cloudfs import glacier
 
 
@mock.patch.object(glacier.GlacierFilesystem, '_connect_glacier')
class TestGlacierFilesystem(object):
 
    def test_multipart_init(self, *args):
        driver = glacier.GlacierFilesystem()
        driver.multipart_init('glacier://Vault_1/', 1024)
 
        driver._conn.initiate_multipart_upload.assert_called_with('Vault_1', 1024, None)
 
    def test_multipart_put(self, *args):
        driver = glacier.GlacierFilesystem()
        driver.multipart_init('glacier://Vault_1/', 1024)
 
        with mock.patch('__builtin__.open', mock.mock_open(read_data = 'xxx')):
            driver.multipart_put('LSKGJ236AD36EQ36_e42YT5', 0, 'foo')
 
        driver._conn.upload_part.assert_called_with(
                'Vault_1',
                'LSKGJ236AD36EQ36_e42YT5',
                'cd2eb0837c9b4c962c22d2ff8b5441b7b45805887f051d39bf133b583baf6860',
                'cd2eb0837c9b4c962c22d2ff8b5441b7b45805887f051d39bf133b583baf6860',
                (0, -1),
                'xxx'
        )
 
    def test_multipart_complete(self, *args):
        driver = glacier.GlacierFilesystem()
        driver.multipart_init('glacier://Vault_1/', 1024)
 
        with mock.patch('__builtin__.open', mock.mock_open(read_data = 'xxx')):
            driver.multipart_put('LSKGJ236AD36EQ36_e42YT5', 0, 'foo')
 
        driver.multipart_complete('LSKGJ236AD36EQ36_e42YT5')
 
        driver._conn.complete_multipart_upload.assert_called_with(
                'Vault_1',
                'LSKGJ236AD36EQ36_e42YT5',
                'cd2eb0837c9b4c962c22d2ff8b5441b7b45805887f051d39bf133b583baf6860',
                0
        )
 
    def test_multipart_abort(self, *args):
        driver = glacier.GlacierFilesystem()
        driver.multipart_init('glacier://Vault_1/', 1024)
        driver.multipart_abort('LSKGJ236AD36EQ36_e42YT5')
 
        driver._conn.abort_multipart_upload.assert_called_with('Vault_1', 'LSKGJ236AD36EQ36_e42YT5')
 
