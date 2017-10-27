 
import mock
import wsgi_intercept
from nose.tools import raises
from wsgi_intercept import urllib2_intercept
 
 
from scalarizr import storage2
from scalarizr.storage2.volumes import ebs
from scalarizr.storage2.volumes import ec2_ephemeral
 
 
Ec2Eph = ec2_ephemeral.Ec2EphemeralVolume
class TestEc2EphemeralVolume(object):
 
    def setup(self, *args, **kwargs):
        urllib2_intercept.install_opener()
        def response(environ, start_response):
            response_headers = [('Content-type','text/plain')]
            if environ['PATH_INFO'].endswith('ephemeral3'):
                status = '404 Not Found'
                start_response(status, response_headers)
                return []
            else:
                status = '200 OK'
            start_response(status, response_headers)
            return ['/dev/sdb']
        wsgi_intercept.add_wsgi_intercept('169.254.169.254', 80, lambda : response)
 
 
    def test_ensure(self, *args, **kwargs):
        eph = Ec2Eph(name='ephemeral0')
        eph.ensure()
        assert eph.device == '/dev/sdb'
 
    @mock.patch.object(ebs, 'storage2')
    def test_ensure_rhel(self, s, *args, **kwargs):
        #wsgi_intercept.add_wsgi_intercept('169.254.169.254', 80, lambda : self._response)
        eph = Ec2Eph(name='ephemeral0')
        s.RHEL_DEVICE_ORDERING_BUG = True
        eph.ensure()
        assert eph.device == '/dev/xvdf'
 
    @raises(storage2.StorageError)
    def test_ensure_metadata_server_error(self):
        #wsgi_intercept.add_wsgi_intercept('169.254.169.254', 80, lambda : self._response)
        eph = Ec2Eph(name='ephemeral3')
        eph.ensure()
 
