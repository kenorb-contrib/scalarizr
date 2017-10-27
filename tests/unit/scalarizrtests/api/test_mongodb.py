 
import mock
import unittest
 
from scalarizr.api import mongodb as mongodb_api
 
 
class MMSAgent_Test(unittest.TestCase):
 
    def setUp(self):
        pass
 
 
    def test_configure(self):
        with mock.patch('scalarizr.api.mongodb.__node__', new={'mongodb':{'password':'PASSWORD'}}):
            agent = mongodb_api._MMSAgent()
            data = '\nmms_key = "aPi_KeY"\nsecret_key = "sEcReT_kEy"\nglobalAuthUsername = """Name"""\nglobalAuthPassword = "Password"\n'
            open_name = '__builtin__.open'
            with mock.patch(open_name, create=True) as mock_open:
                mock_open.return_value = mock.MagicMock(spec=file)
                mock_open.return_value.__enter__.return_value.read.return_value = data
                agent.configure('API_KEY', 'SECRET_KEY')
 
                file_handler = mock_open.return_value.__enter__.return_value
                file_handler.write.assert_called_with(
                        '\nmms_key = "API_KEY"\nsecret_key = "SECRET_KEY"\nglobalAuthUsername = """scalr"""\nglobalAuthPassword = """PASSWORD"""\n')
 
