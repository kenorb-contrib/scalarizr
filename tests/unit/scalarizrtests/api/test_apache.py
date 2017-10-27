__author__ = 'shaitanich'
 
import os
import unittest
 
try:
    from cStringIO import StringIO
except ImportError:
    from StringIO import StringIO
 
from scalarizr import bus
from scalarizr.api import apache
from scalarizr.libs.metaconf import NoPathError
 
 
class QEMock(object):
 
    @classmethod
    def get_ssl_certificate(*args, **kwargs):
        if args:
            return dima_cert, dima_key
        return marat_cert, marat_key
 
    @classmethod
    def list_farm_role_params(*args, **kwargs):
        app = {
            'app': {
                'virtual_hosts':
                [{
                    'hostname': 'www.dima.com',
                    'port': '80',
                    'ssl': '0',
                    'ssl_certificate_id': None,
                    'template': simple_template},
                {
                    'hostname': 'www.dima.com',
                    'port': '443',
                    'ssl': '1',
                    'ssl_certificate_id': '410',
                    'template': ssl_template
                }]},
            'volumes': None}
        return dict(params=app)
 
 
class PlatrormMock(object):
    name = 'ec2'
 
 
apache.bus.queryenv_service = QEMock
bus.etc_path = '/etc/scalr'
 
 
class ApacheAPITest(unittest.TestCase):
 
    def setUp(self):
        apache.__node__ = dict(
            platform=PlatrormMock(),
            farm_role_id=1
        )
        self.api = apache.ApacheAPI()
 
    def test__fetch_virtual_hosts(self):
        fetched = self.api._fetch_virtual_hosts()
        expected = QEMock.list_farm_role_params()['params']['app']['virtual_hosts']
        self.assertEquals(len(fetched), 2)
        self.assertEqual(expected[0]['hostname'], fetched[0]['hostname'])
        self.assertEqual(expected[0]['port'], fetched[0]['port'])
        self.assertEqual(expected[0]['template'], fetched[0]['template'])
        self.assertFalse(fetched[0]['ssl'])
        self.assertFalse(fetched[0]['ssl_certificate_id'])
        self.assertTrue(fetched[1]['ssl'])
 
 
    def test_update_setup(self):
        data = [
            'domain.vhost.conf',
            'domain-ssl.vhost.conf',
            'new-style.80.vhost.conf',
            'new-style.443.vhost.conf',
            'not_a_domain'
        ]
        expected = {
            'domain-ssl.vhost.conf': 'domain-443.vhost.conf',
            'domain.vhost.conf': 'domain-80.vhost.conf'
        }
        pairs = apache.get_updated_file_names(data)
        self.assertEqual(pairs, expected)
 
    def _test_statistics(self):
        keys = [
            'Uptime',
            'IdleWorkers',
            'Total Accesses',
            'Scoreboard',
            'BytesPerReq',
            'Total kBytes',
            'ReqPerSec',
            'BusyWorkers',
            'BytesPerSec'
        ]
        self.assertEquals(keys, self.api.get_webserver_statistics().keys())
 
    def _test_mod_rpaf(self):
        raise NotImplementedError
 
    def _test_mod_ssl(self):
        raise NotImplementedError
 
    def test_parse_simple_template(self):
        v1 = apache.VirtualHost(simple_template)
 
        self.assertTrue(v1.body)
 
        self.assertEqual(80, v1.port)
        v1.port = 8080
 
        self.assertEqual(8080, v1.port)
 
        self.assertEqual('dima.com', v1.server_name)
        v1.server_name = 'new.dima.com'
        self.assertEqual(v1.server_name, 'new.dima.com')
 
        self.assertEqual('/var/log/http-dima.com-access.log', v1.custom_log_path)
        self.assertEqual(['/var/www'], v1.document_root_paths)
 
        with self.assertRaises(NoPathError):
            print v1.error_log_path
            print v1.ssl_cert_path
            print v1.ssl_key_path
            print v1.ssl_chain_path
 
    def test_parse_ssl_template(self):
        sv1 = apache.VirtualHost(ssl_template)
        self.assertTrue(sv1.body)
        self.assertTrue('/etc/aws/' in sv1.body)
        self.assertEqual(443, sv1.port)
        self.assertEqual('secure.dima.com', sv1.server_name)
        self.assertEqual('/var/log/http-dima.com-access.log', sv1.custom_log_path)
        self.assertEqual('/var/log/http-dima.com-ssl.log', sv1.error_log_path)
        self.assertEqual(['/var/www'], sv1.document_root_paths)
        self.assertEqual('/etc/aws/keys/ssl/https.crt', sv1.ssl_cert_path)
        self.assertEqual('/etc/aws/keys/ssl/https.key', sv1.ssl_key_path)
        with self.assertRaises(NoPathError):
            print sv1.ssl_chain_path
 
    def test_use_certificate(self):
        sv1 = apache.VirtualHost(ssl_template)
 
        self.assertEqual('/etc/aws/keys/ssl/https.crt', sv1.ssl_cert_path)
        self.assertEqual('/etc/aws/keys/ssl/https.key', sv1.ssl_key_path)
        with self.assertRaises(NoPathError):
            print sv1.ssl_chain_path
 
        custom_certificate = apache.SSLCertificate(1)
        cert_path = custom_certificate.cert_path
        key_path = custom_certificate.key_path
        chain_path = custom_certificate.chain_path
        sv1.use_certificate(cert_path, key_path, chain_path)
        self.assertEqual('/etc/scalr/private.d/keys/https_1.crt', sv1.ssl_cert_path)
        self.assertEqual('/etc/scalr/private.d/keys/https_1.key', sv1.ssl_key_path)
        self.assertEqual('/etc/scalr/private.d/keys/https_1-ca.crt', sv1.ssl_chain_path)
 
        default_certificate = apache.SSLCertificate()
        sv1.use_certificate(default_certificate.cert_path, default_certificate.key_path)
        self.assertEqual('/etc/scalr/private.d/keys/https.crt', sv1.ssl_cert_path)
        self.assertEqual('/etc/scalr/private.d/keys/https.key', sv1.ssl_key_path)
        with self.assertRaises(NoPathError):
            print sv1.ssl_chain_path
 
        v1 = apache.VirtualHost(simple_template)
        with self.assertRaises(apache.ApacheError):
            v1.use_certificate(default_certificate.cert_path, default_certificate.key_path)
 
    def _test_virtual_host_lifecycle(self):
        #TODO: Enable when all system objects are mocked
        #creating objects
        path = self.api.create_vhost('dima.com', 80, simple_template, ssl=False)
        cv_path = self.api.create_vhost('custom.dima.com', 8080, custom_template, ssl=False)
        ov_path = self.api.create_vhost('old.dima.com', 443, old_ssl_template, ssl=True)
        sv_path = self.api.create_vhost(
            hostname='secure.dima.com',
            port=443,
            template=ssl_template,
            ssl=True,
            ssl_certificate_id=1
        )
 
        #files created
        self.assertTrue(os.path.exists(path))
        self.assertTrue(os.path.exists(cv_path))
        self.assertTrue(os.path.exists(ov_path))
        self.assertTrue(os.path.exists(sv_path))
 
        #reading created files
        template = open(path, 'r').read().strip()
        cv_template = open(cv_path, 'r').read().strip()
        ov_template = open(ov_path, 'r').read().strip()
        sv_template = open(sv_path, 'r').read().strip()
 
        #re-creating objects
        v_host = apache.VirtualHost(template)
        cv_host = apache.VirtualHost(cv_template)
        old_v_host = apache.VirtualHost(ov_template)
        ssl_v_host = apache.VirtualHost(sv_template)
 
        #asserting ports
        self.assertEqual(80, v_host.port)
        self.assertEqual(8080, cv_host.port)
        self.assertEqual(443, old_v_host.port)
        self.assertEqual(443, ssl_v_host.port)
 
        #asserting bodies
        self.assertEqual(simple_template, v_host.body)
        self.assertEqual(custom_template, cv_host.body)
        #ssl cert paths changed
        self.assertNotEqual(old_ssl_template, v_host.body)
        self.assertNotEqual(ssl_template, cv_host.body)
 
        #asserting created paths
        self.assertTrue(os.path.exists(v_host.custom_log_path))
        self.assertTrue(os.path.exists(cv_host.custom_log_path))  # TODO: USE DIFFERENT PATHS
        self.assertTrue(os.path.exists(old_v_host.custom_log_path))
        self.assertTrue(os.path.exists(ssl_v_host.custom_log_path))
 
        self.assertTrue(os.path.exists(v_host.document_root_paths[0]))
        self.assertTrue(os.path.exists(cv_host.document_root_paths[0]))
        self.assertTrue(os.path.exists(old_v_host.document_root_paths[0]))
        self.assertTrue(os.path.exists(ssl_v_host.document_root_paths[0]))
 
        self.assertTrue(os.path.exists(old_v_host.error_log_path))
        self.assertTrue(os.path.exists(ssl_v_host.error_log_path))
 
        #each virtual host is served by apache
        self.assertTrue(os.path.basename(path) in self.api.list_served_virtual_hosts())
        self.assertTrue(os.path.basename(cv_path) in self.api.list_served_virtual_hosts())
        self.assertTrue(os.path.basename(ov_path) in self.api.list_served_virtual_hosts())
        self.assertTrue(os.path.basename(sv_path) in self.api.list_served_virtual_hosts())
        #TODO: check apache.conf for port
        #TODO: check if service restart occured
 
        svh_data = ['dima.com', 80, simple_template, False]
        ovh_data = ['old.dima.com', 443, old_ssl_template, True]
 
        data = ovh_data, svh_data
        self.api.reconfigure(data)
 
        #Files of removed virtual hosts no longer exist.
        self.assertFalse(os.path.exists(cv_path))
        self.assertFalse(os.path.exists(sv_path))
 
        #Removed virtual hosts are no longer served by apache
        self.assertFalse(os.path.basename(cv_path) in self.api.list_served_virtual_hosts())
        self.assertFalse(os.path.basename(sv_path) in self.api.list_served_virtual_hosts())
 
        #TODO: expand check on reconfigure
 
        #But reconfigured virtual hosts stil healthy.
        self.assertTrue(os.path.exists(path))
        self.assertTrue(os.path.exists(ov_path))
 
        #TODO: Update two virtual hosts left
 
        #removing all created virtual hosts
        signature1 = (v_host.server_name, v_host.port)
        signature2 = (old_v_host.server_name, old_v_host.port)
        hosts_to_remove = (signature1, signature2)
        self.api.delete_vhosts(hosts_to_remove, reload=True)
 
        #Files of removed virtual hosts no longer exist.
        self.assertFalse(os.path.exists(path))
        self.assertFalse(os.path.exists(ov_path))
 
        #Removed virtual hosts are no longer served by apache
        self.assertFalse(os.path.basename(path) in self.api.list_served_virtual_hosts())
        self.assertFalse(os.path.basename(ov_path) in self.api.list_served_virtual_hosts())
 
 
simple_template = '''<VirtualHost *:80>
    ServerAlias www.dima.com
    ServerAdmin dmitry@scalr.com
    DocumentRoot /var/www/
    ServerName dima.com
    CustomLog /var/log/http-dima.com-access.log combined
    ScriptAlias /cgi-bin/ /var/www/cgi-bin/
    </VirtualHost>'''
 
 
custom_template = '''<VirtualHost *:8080>
    ServerAlias www.dima.com
    ServerAdmin dmitry@scalr.com
    DocumentRoot /var/www/
    ServerName custom.dima.com
    CustomLog /var/log/http-dima.com-access.log combined
    ScriptAlias /cgi-bin/ /var/www/cgi-bin/
    </VirtualHost>'''
 
 
old_ssl_template = '''<IfModule mod_ssl.c>
       <VirtualHost *:443>
               ServerName old.dima.com
               ServerAlias www.dima.com
               ServerAdmin dmitry@scalr.com
               DocumentRoot /var/www
               CustomLog /var/log/http-dima.com-access.log combined
 
               SSLEngine on
               SSLCertificateFile /etc/aws/keys/ssl/https.crt
               SSLCertificateKeyFile /etc/aws/keys/ssl/https.key
               ErrorLog /var/log/http-dima.com-ssl.log
 
               ScriptAlias /cgi-bin/ /var/www/cgi-bin/
               SetEnvIf User-Agent ".*MSIE.*" nokeepalive ssl-unclean-shutdown
       </VirtualHost>
</IfModule>'''
 
 
ssl_template = '''<IfModule mod_ssl.c>
       <VirtualHost *:443>
               ServerName secure.dima.com
               ServerAlias www.dima.com
               ServerAdmin dmitry@scalr.com
               DocumentRoot /var/www
               CustomLog /var/log/http-dima.com-access.log combined
 
               SSLEngine on
               SSLCertificateFile /etc/aws/keys/ssl/https.crt
               SSLCertificateKeyFile /etc/aws/keys/ssl/https.key
               ErrorLog /var/log/http-dima.com-ssl.log
 
               ScriptAlias /cgi-bin/ /var/www/cgi-bin/
               SetEnvIf User-Agent ".*MSIE.*" nokeepalive ssl-unclean-shutdown
       </VirtualHost>
</IfModule>'''
 
 
dima_cert = '''-----BEGIN CERTIFICATE-----
MIIE/jCCAuYCAQEwDQYJKoZIhvcNAQEFBQAwRTELMAkGA1UEBhMCQVUxEzARBgNV
BAgTClNvbWUtU3RhdGUxITAfBgNVBAoTGEludGVybmV0IFdpZGdpdHMgUHR5IEx0
ZDAeFw0xMDEwMjExMTUzMTZaFw0xMTEwMjExMTUzMTZaMEUxCzAJBgNVBAYTAkFV
MRMwEQYDVQQIEwpTb21lLVN0YXRlMSEwHwYDVQQKExhJbnRlcm5ldCBXaWRnaXRz
IFB0eSBMdGQwggIiMA0GCSqGSIb3DQEBAQUAA4ICDwAwggIKAoICAQDO9nPeP9/P
eiAHNrC5b9of6bjmiUiksQtY046GcnhjFzZ+xTrRZFCqdMGtgrtstHzUWyAR98rD
FEOJZko3x494YexXV5lpih69J+LYPwsDIeqBJuTxMIvdfVO5xsf79pDQEQvjH1oy
Z8u9eH/fxOefxdyJeqf77lUNEu3I508P+tAoeD+AkqJOtJJNZdsuUEuQAh06Sgqh
m3Deze8vi0xUIr7QD0PKY/jBiuWk94c7Fsp6MpWOOjVqbrgS0RWX+BYUKDWG0v0G
WD7vSz2nnqHS2wk49GdanFkWRge7rNep71b4pWRUGtPcUZlRFs1xbP8XA1BgC0lM
wnHFqhp/0lanLrWLbSId/jUED0Bgl5rBpQ+i3QjWulSVBdeeCwX6Kxz4c3y8RNbe
taOhAH4g+y+k5MTm2hO+jqoNK12NpQBcgmoP2+z1BB0DstE4jSgezmIpMHmt65Zq
j/ZqJMZ/eeXipSuvGyWn+BMiXtvkQz8pLckqmJfUF/Pk0PqZ6SWLuUP+4IZcO9nW
hulyuUtBQCb5eC0TdkB8nboSo7I1Hkvb/6Gt4cGiJiqHYSiZ4Xk/oO8AmH6rdA6j
Z9l+mCKdO+HV2m9PW1Z8orP0PvfIwyld/YSdsj/+nfJW224kCdHMu3dcHbeydBg9
Los5SqnQr+7yeR92kCdvaXW/wzGSelVczQIDAQABMA0GCSqGSIb3DQEBBQUAA4IC
AQALh5YDT2jNvzJ4Ehwzt7Nq5Xo30cYb3TmVcW+oO82G9HPN4+de1kdNU6HWHq91
HXSrPmoGFLms7q40A1k2vxBdfBDIO0Ikb8OUUQCTTMW4g8M3/ku0K4Nm0XyTh8Ge
U8Qw5ef6E7i4oM/XNCjempPngcUiYhr/exrYxi+/PbnTQkemfEAPLXb5q68soQqg
0lZ/KMUsOMT0QsgxbEP3+qDb96Ogi8XIYqiHnqSR1KqYVJVfA4/BneY3khlPahWQ
LuU1RAUdAwjNujdBH3Rm2/5g1/hYMUEuhhyEsrMxTlyNptQIpsjjBFs9G16MYjfu
2Gc0wvwa1nVI2JwZutIOdzQ8p6F+Qf2v4sIj3v5RtpVgVC8zGENIbL+a7ElMUEqW
NKBSfK8mLZqpa3IYHf7c+re/rZhigyxmppiG9U3bjjWt2E3wzPVEQUMAYq8/eYJt
yzwBnESMaeoK7HKwZbgIRNgqRdXzx+qxE4OVAjBOGDB8KON2zngzAwW8NmfVOu7V
bQ9ZKz2SfsVNM7hjmfvMO6T8oWyD1wDWau5T2iT16PmYliGnovHiGQICtEzDNDHq
pEO/ac8jQi5r7M+owLJkHFHrpvlNGtb2O7xBCrd1UkSgzG7NIo/VM03m/k2O0p/4
Ul6NRT4WIiyv6A6zKmgfekeLKJtJG84WIHL7R5mtZNLRhQ==
-----END CERTIFICATE-----'''
 
 
dima_key = '''-----BEGIN RSA PRIVATE KEY-----
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
-----END RSA PRIVATE KEY-----'''
 
 
marat_cert = '''-----BEGIN CERTIFICATE-----
MIICATCCAWoCCQCa70wlx0hEuTANBgkqhkiG9w0BAQUFADBFMQswCQYDVQQGEwJB
VTETMBEGA1UECBMKU29tZS1TdGF0ZTEhMB8GA1UEChMYSW50ZXJuZXQgV2lkZ2l0
cyBQdHkgTHRkMB4XDTEzMDExODE2NDU0OFoXDTE0MDExODE2NDU0OFowRTELMAkG
A1UEBhMCQVUxEzARBgNVBAgTClNvbWUtU3RhdGUxITAfBgNVBAoTGEludGVybmV0
IFdpZGdpdHMgUHR5IEx0ZDCBnzANBgkqhkiG9w0BAQEFAAOBjQAwgYkCgYEA1WQL
TsY/M413+wDZs7H+SV79adcbms9RuynHXVMV87AKOSSR2ddHPUkOzn9j+RdURLo5
ctMER7U2fRh2VSoNGCe3Hmpk1lkgdWahWR3Do2EPbPDQ/dOafqEq4pr8rvGTsimU
yAMc75nTidIHrOGhhq9VsqkeDiZ80YvCE6qG5h8CAwEAATANBgkqhkiG9w0BAQUF
AAOBgQDIkgdUZYh+cCbMLPIVFojCWAjIsfon4GA9QUv/Iqqpy4c0IV1QaNuG3DAo
29nCZRH6/cJ1FmIMfqUOjiDgEJKDo7voCmOppE92n26p78KnYPzFJKEHuNXlO5No
ACVSAyN/IeWPVySIpd4wcy2OfZks6vXgerKa4o4Vqg/CdRdbkA==
-----END CERTIFICATE-----'''
 
 
marat_key = '''-----BEGIN RSA PRIVATE KEY-----
MIICXAIBAAKBgQDVZAtOxj8zjXf7ANmzsf5JXv1p1xuaz1G7KcddUxXzsAo5JJHZ
10c9SQ7Of2P5F1REujly0wRHtTZ9GHZVKg0YJ7ceamTWWSB1ZqFZHcOjYQ9s8ND9
05p+oSrimvyu8ZOyKZTIAxzvmdOJ0ges4aGGr1WyqR4OJnzRi8ITqobmHwIDAQAB
AoGAVPqxch6K/snA1SnCiMhCfw+gFa0/ZghYARQjO+whmtkpSRZiKwGLckGM7vlW
Y9VBjtkmla6pTrFA0NKUFVhYu+J/rFJO2PnB1vq88Km35Y/tlqbv+9v2xw2PwO2Z
0MahP/923b/tg+kibpDolXrg0vKay/mYLv32larrqfbfY8ECQQD1AU6AelAcbyNy
OLHsOHEjPoJ9/9KOvpNTgdNyjZmCecMV4gZpKDyYfHyzGB7y6QlpzdlfT9zNi+Mn
CZ8peYlhAkEA3veK8Ds393lGdACleveKo+AODyI9pDHEAYa+QuGHd39UxwYwFvxw
oeqoNt4mWNRPDuDlsU5v6BNLiu6waTkffwJAHt1LRmQiM3LMxFbgEyIJHqeBSN4x
aEoZxStVt9ievhEYwmj25chr2cnU67reKzuwM+P6vkcRSdOVihVsN41YAQJAPCIQ
86dU+cZYbPK7roVSe83ynLxEWaMeVLcNWyZODblmmOKfV6OvkMujoGCbgPuJct7O
s9oOrk194zNqmoZQawJBANIYJ0pYLcjznkozQUrBLlX5+IcTUYGNdg8yPrlWi/IL
3/c/yh3vzT/Ka61DQiGg+2WnOAfMb8EIi/dVnWSFpww=
-----END RSA PRIVATE KEY-----'''
 
 
if __name__ == '__main__':
    unittest.main()
 
