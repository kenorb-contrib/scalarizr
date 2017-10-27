 
import mock
import unittest
 
from scalarizr.handlers import mongodb
 
 
class MongoDBHandler_Test(unittest.TestCase):
 
    def setUp(self):
        pass
 
 
    @mock.patch('scalarizr.handlers.mongodb.MongoDBHandler.__init__',\
                mock.Mock(return_value=None))
    def test_add_host_to_mms(self):
        handler = mongodb.MongoDBHandler()
        with mock.patch.dict('scalarizr.handlers.mongodb.__mongodb__',\
                {'mms':{'api_key':'this_is_api_key'}}):
            with mock.patch('urllib2.urlopen') as urlopen:
                handler._add_host_to_mms('mongo-0-0', 20017)
                urlopen.assert_called_once_with(
                        'https://mms.10gen.com/host/v1/addHost/this_is_api_key?hostname=mongo-0-0&port=20017')
 
 
    @mock.patch('scalarizr.handlers.mongodb.MongoDBHandler.__init__',\
                mock.Mock(return_value=None))
    def test_delete_host_from_mms(self):
        handler = mongodb.MongoDBHandler()
        with mock.patch.dict('scalarizr.handlers.mongodb.__mongodb__',\
                {'mms':{'api_key':'this_is_api_key'}}):
            with mock.patch('urllib2.urlopen') as urlopen:
                handler._delete_host_from_mms('mongo-0-0', 20017)
                urlopen.assert_called_once_with(
                        'https://mms.10gen.com/host/v1/deleteHost/this_is_api_key?hostname=mongo-0-0&port=20017')
 
 
    @mock.patch('scalarizr.handlers.mongodb.MongoDBHandler.__init__',\
                mock.Mock(return_value=None))
    def test_on_new_host_mms_configure(self):
        handler = mongodb.MongoDBHandler()
        handler._shard_index = 0
        handler._rs_index = 0
 
        handler._add_host_to_mms = mock.Mock()
        handler._delete_host_from_mms = mock.Mock()
        handler._get_shard_hosts = mock.Mock(return_value=['mongo-0-0'])
        handler._on_new_host_mms_configure(0, 0)
        expected_calls = [
                mock.call(mongodb.HOSTNAME_TPL % (0, 0), mongodb.mongo_svc.REPLICA_DEFAULT_PORT),
                mock.call(mongodb.HOSTNAME_TPL % (0, 0), mongodb.mongo_svc.ROUTER_DEFAULT_PORT),
                mock.call(mongodb.HOSTNAME_TPL % (0, 0), mongodb.mongo_svc.CONFIG_SERVER_DEFAULT_PORT)]
        assert handler._add_host_to_mms.mock_calls == expected_calls
        expected_calls = [
                mock.call(mongodb.HOSTNAME_TPL % (0, 0), mongodb.mongo_svc.ARBITER_DEFAULT_PORT)]
        assert handler._delete_host_from_mms.mock_calls == expected_calls
 
        handler._add_host_to_mms = mock.Mock()
        handler._delete_host_from_mms = mock.Mock()
        handler._get_shard_hosts = mock.Mock(return_value=['mongo-0-0', 'mongo-0-1'])
        handler._on_new_host_mms_configure(0, 1)
        expected_calls = [
                mock.call(mongodb.HOSTNAME_TPL % (0, 1), mongodb.mongo_svc.REPLICA_DEFAULT_PORT),
                mock.call(mongodb.HOSTNAME_TPL % (0, 1), mongodb.mongo_svc.ROUTER_DEFAULT_PORT),
                mock.call(mongodb.HOSTNAME_TPL % (0, 0), mongodb.mongo_svc.ARBITER_DEFAULT_PORT)]
        assert handler._add_host_to_mms.mock_calls == expected_calls
 
        handler._add_host_to_mms = mock.Mock()
        handler._delete_host_from_mms = mock.Mock()
        handler._get_shard_hosts = mock.Mock(return_value=['mongo-0-0', 'mongo-0-1', 'mongo-0-2'])
        handler._on_new_host_mms_configure(0, 2)
        expected_calls = [
                mock.call(mongodb.HOSTNAME_TPL % (0, 2), mongodb.mongo_svc.REPLICA_DEFAULT_PORT)]
        assert handler._add_host_to_mms.mock_calls == expected_calls
        expected_calls = [
                mock.call(mongodb.HOSTNAME_TPL % (0, 0), mongodb.mongo_svc.ARBITER_DEFAULT_PORT)]
        assert handler._delete_host_from_mms.mock_calls == expected_calls
 
        handler._add_host_to_mms = mock.Mock()
        handler._delete_host_from_mms = mock.Mock()
        handler._get_shard_hosts = mock.Mock(return_value=['mongo-1-0'])
        handler._on_new_host_mms_configure(1, 0)
        expected_calls = [
                mock.call(mongodb.HOSTNAME_TPL % (1, 0), mongodb.mongo_svc.REPLICA_DEFAULT_PORT),
                mock.call(mongodb.HOSTNAME_TPL % (1, 0), mongodb.mongo_svc.ROUTER_DEFAULT_PORT)]
        assert handler._add_host_to_mms.mock_calls == expected_calls
        expected_calls = [
                mock.call(mongodb.HOSTNAME_TPL % (1, 0), mongodb.mongo_svc.ARBITER_DEFAULT_PORT)]
        assert handler._delete_host_from_mms.mock_calls == expected_calls
 
        handler = mongodb.MongoDBHandler()
        handler._shard_index = 0
        handler._rs_index = 1
 
        handler._add_host_to_mms = mock.Mock()
        handler._delete_host_from_mms = mock.Mock()
        handler._get_shard_hosts = mock.Mock(return_value=['mongo-0-0'])
        handler._on_new_host_mms_configure(0, 0)
        expected_calls = []
        assert handler._add_host_to_mms.mock_calls == expected_calls
        expected_calls = []
        assert handler._delete_host_from_mms.mock_calls == expected_calls
 
 
    @mock.patch('scalarizr.handlers.mongodb.MongoDBHandler.__init__',\
                mock.Mock(return_value=None))
    def test_on_host_terminate_mms_configure(self):
        handler = mongodb.MongoDBHandler()
        handler._shard_index = 0
        handler._rs_index = 0
 
        with mock.patch('scalarizr.handlers.mongodb.STATE', new={'mongodb.cluster_state':'running'}):
            handler._add_host_to_mms = mock.Mock()
            handler._delete_host_from_mms = mock.Mock()
            handler._get_shard_hosts = mock.Mock(return_value=['mongo-0-0', 'mongo-0-1'])
            handler._on_host_terminate_mms_configure(0, 2)
            expected_calls = [
                    mock.call(mongodb.HOSTNAME_TPL % (0, 2), mongodb.mongo_svc.REPLICA_DEFAULT_PORT)]
            assert handler._delete_host_from_mms.mock_calls == expected_calls
            expected_calls = [
                    mock.call(mongodb.HOSTNAME_TPL % (0, 0), mongodb.mongo_svc.ARBITER_DEFAULT_PORT)]
            assert handler._add_host_to_mms.mock_calls == expected_calls
 
            handler._add_host_to_mms = mock.Mock()
            handler._delete_host_from_mms = mock.Mock()
            handler._get_shard_hosts = mock.Mock(return_value=['mongo-0-0'])
            handler._on_host_terminate_mms_configure(0, 1)
            expected_calls = [
                    mock.call(mongodb.HOSTNAME_TPL % (0, 1), mongodb.mongo_svc.REPLICA_DEFAULT_PORT),
                    mock.call(mongodb.HOSTNAME_TPL % (0, 1), mongodb.mongo_svc.ROUTER_DEFAULT_PORT),
                    mock.call(mongodb.HOSTNAME_TPL % (0, 0), mongodb.mongo_svc.ARBITER_DEFAULT_PORT)]
            assert handler._delete_host_from_mms.mock_calls == expected_calls
            expected_calls = []
            assert handler._add_host_to_mms.mock_calls == expected_calls
 
        with mock.patch('scalarizr.handlers.mongodb.STATE', new={'mongodb.cluster_state':'terminating'}):
            handler._add_host_to_mms = mock.Mock()
            handler._delete_host_from_mms = mock.Mock()
            handler._get_shard_hosts = mock.Mock(return_value=['mongo-0-0', 'mongo-0-1'])
            handler._on_host_terminate_mms_configure(0, 2)
            expected_calls = [
                    mock.call(mongodb.HOSTNAME_TPL % (0, 2), mongodb.mongo_svc.REPLICA_DEFAULT_PORT),
                    mock.call(mongodb.HOSTNAME_TPL % (0, 0), mongodb.mongo_svc.ARBITER_DEFAULT_PORT)]
            assert handler._delete_host_from_mms.mock_calls == expected_calls
            expected_calls = []
            assert handler._add_host_to_mms.mock_calls == expected_calls
 
            handler._add_host_to_mms = mock.Mock()
            handler._delete_host_from_mms = mock.Mock()
            handler._get_shard_hosts = mock.Mock(return_value=['mongo-0-0'])
            handler._on_host_terminate_mms_configure(0, 1)
            expected_calls = [
                    mock.call(mongodb.HOSTNAME_TPL % (0, 1), mongodb.mongo_svc.REPLICA_DEFAULT_PORT),
                    mock.call(mongodb.HOSTNAME_TPL % (0, 1), mongodb.mongo_svc.ROUTER_DEFAULT_PORT),
                    mock.call(mongodb.HOSTNAME_TPL % (0, 0), mongodb.mongo_svc.ARBITER_DEFAULT_PORT)]
            assert handler._delete_host_from_mms.mock_calls == expected_calls
            expected_calls = []
            assert handler._add_host_to_mms.mock_calls == expected_calls
 
