'''
Created on Jul 1, 2010
 
@author: marat
'''
import unittest
from scalarizr.libs.metaconf import *
from cStringIO import StringIO
import os, sys
 
if sys.version_info[0:2] >= (2, 7):
    from xml.etree import ElementTree as ET
else:
    from scalarizr.externals.etree import ElementTree as ET
 
RESOURCE_PATH = os.path.abspath(os.path.dirname(__file__) + "../../../resources/libs/metaconf")
 
 
class PhpIniTest(unittest.TestCase):
    conf = None
    def setUp(self):
        self.conf = Configuration("ini")
        self.conf.read(RESOURCE_PATH + "/php.ini")
 
    def tearDown(self):
        del self.conf
 
    def test_get(self):
        self.assertEqual(self.conf.get("PHP/error_reporting"), "E_ALL & ~E_NOTICE")
        self.assertEqual(self.conf.get("PHP/error_log"), "/var/log/php_error.log")
        self.assertEqual(self.conf.get("PHP/include_path"), '".:/php/includes"')
        self.assertEqual(self.conf.get("PHP/auto_append_file"), "")
 
    def test_get_boolean1(self):
        self.assertEqual(self.conf.get("Tidy/tidy.clean_output"), "Off")
        self.assertFalse(self.conf.get_boolean("Tidy/tidy.clean_output"))
 
    def test_get_boolean2(self):
        self.assertEqual(self.conf.get("MSSQL/mssql.allow_persistent"), "On")
        self.assertTrue(self.conf.get_boolean("MSSQL/mssql.allow_persistent"))
 
    def __test_get_int(self):
        self.assertEqual(self.conf.get_int("PHP/post_max_size"), 8)
 
    extensions = ("curl.so", "dom.so", "http.so", "imap.so", "json.so", "mcrypt.so", "mongo.so", "mysqli.so")
 
    def test_get_list(self):
        self.assertEqual(set(self.extensions), set(self.conf.get_list("PHP/extension")))
 
    def test_get_list_order(self):
        self.assertEqual(self.extensions, tuple(self.conf.get_list("PHP/extension")))
 
    def test_comments(self):
        self.assertEqual(self.conf.get("PHP/memory_limit"), "128M")
        pass
 
    def test_set(self):
        self.conf.set("MSSQL/mssql.min_message_severity", "10")
        self.assertEqual(self.conf.get_int("MSSQL/mssql.min_message_severity"), 10)
 
    def test_set_list(self):
        ext = list(self.extensions)
        self.assertEqual(ext, self.conf.get_list("PHP/extension"))
        self.conf.add("PHP/extension", "mysql.so")
        ext.append("mysql.so")
        self.assertEqual(ext, self.conf.get_list("PHP/extension"))
 
    def test_del(self):
        self.assertEqual(self.conf.get('PHP/zend.ze1_compatibility_mode'), 'Off')
        self.conf.remove('PHP/zend.ze1_compatibility_mode')
        self.assertRaises(NoPathError, self.conf.get, 'PHP/zend.ze1_compatibility_mode')
 
    def test_errors(self):
        self.assertRaises(MetaconfError, Configuration, 'ini', '', 'not_tree')
        conf = Configuration('ini')
        c = StringIO()
        self.assertRaises(MetaconfError, conf.write, c)
        del(c)
 
class XmlTest(unittest.TestCase):
    conf = None
 
    def setUp(self):
        self.conf = Configuration("xml")
        self.conf.read(RESOURCE_PATH + "/cassandra.xml")
 
    def tearDown(self):
        del self.conf
 
    def test_get(self):
        self.assertEqual(self.conf.get("Storage/Authenticator"), "org.apache.cassandra.auth.AllowAllAuthenticator")
 
    def test_get2(self):
        self.assertEqual(self.conf.get(".//Seeds/Seed[2]"), "192.168.1.200")
 
    def test_get_boolean(self):
        self.assertFalse(self.conf.get_boolean("Storage/AutoBootstrap"))
 
    def test_get_boolean2(self):
        self.assertTrue(self.conf.get_boolean("Storage/HintedHandoffEnabled"))
 
    def test_get_list(self):
        seeds = ["127.0.0.1", "192.168.1.200"]
        self.assertEqual(seeds, self.conf.get_list(".//Seeds/Seed"))
 
    def test_get_dict(self):
        col = self.conf.get_dict(".//Storage/Keyspaces/Keyspace/ColumnFamily[@Name='Standard2']")
        self.assertEqual(col["CompareWith"], "UTF8Type")
        self.assertEqual(col["KeysCached"], "100%")
        self.assertEqual(len(col), 3)
 
    def test_remove(self):
        self.assertEqual(self.conf.get('.//Seeds/Seed[2]'), '192.168.1.200')
        self.conf.remove('.//Seeds/Seed', '192.168.1.200')
        self.assertRaises(NoPathError, self.conf.get, './/Seeds/Seed[2]')
 
    def test_add(self):
        self.assertRaises(NoPathError, self.conf.get, './/Seeds/Test')
        self.conf.add('.//Seeds/Test', '123', before_path='Seed[1]')
        self.assertEqual(self.conf.get('.//Seeds/Test'), '123')
        self.assertEqual(self.conf.get_list('.//Seeds/*')[1], '123')
        self.conf.add('.//Seeds/Test', '456')
        self.assertEqual(self.conf.get_list('.//Seeds/*')[2], '456')
 
class NginxTest(unittest.TestCase):
    conf = None
 
    def setUp(self):
        self.conf = Configuration("nginx")
        self.conf.read(RESOURCE_PATH + "/nginx.conf")
 
    def tearDown(self):
        del self.conf
 
    def test_get(self):
        self.assertEqual(self.conf.get("events/worker_connections"), "1024")
 
    def test_get2(self):
        self.assertEqual(self.conf.get("http/server/location[2]/root"), "/usr/share/nginx/html")
 
    def test_get_boolean(self):
        self.assertTrue(self.conf.get_boolean("http/sendfile"))
 
    def test_get_list(self):
        roots = ["/usr/share/nginx/html", "/usr/share/nginx/html", "/usr/share/nginx/html"]
        self.assertEqual(roots, self.conf.get_list(".//server/*/root"))
 
    def test_get_int(self):
        self.assertEqual(self.conf.get_int('http/server/listen'), 80)
 
    def test_del(self):
        self.assertEqual(self.conf.get('http/server/listen'), '80')
        self.conf.remove('http/server/listen')
        self.assertRaises(NoPathError, self.conf.get, 'http/server/listen')
 
    def test_set(self):
        self.assertEqual(self.conf.get('http/server/listen'), '80')
        self.conf.set('http/server/listen', '8080')
        self.assertEqual(self.conf.get('http/server/listen'), '8080')
 
    def test_comment(self):
        self.assertEqual(self.conf.get('http/server/listen'), '80')
        self.conf.comment('http/server/listen')
        self.assertRaises(NoPathError, self.conf.get, 'http/server/listen')
        self.conf.uncomment('http/server/listen')
        self.assertEqual(self.conf.get('http/server/listen'), '80')
 
    def test_get_float(self):
        self.assertEqual(self.conf.get_float('http/server/listen'), 80.0)
 
 
class IniFpTest(unittest.TestCase):
    provider = None
 
    def setUp(self):
        c = StringIO('')
        self.provider = IniFormatProvider()
        self.provider.read(c)
        self.root = self.provider._cursect
 
    def tearDown(self):
        del self.provider
 
    def test_read_comment(self):
        self.assertTrue(self.provider.read_comment('# Test ', self.root))
        self.assertTrue(self.provider.read_comment('#Test ', self.root))
        self.assertTrue(self.provider.read_comment('; Test ', self.root))
        self.assertTrue(self.provider.read_comment(';Test ', self.root))
        self.assertFalse(self.provider.read_comment('Test # Another one', self.root))
 
    def test_read_section(self):
        self.assertTrue(self.provider.read_section('[Test]', self.root))
        self.assertFalse(self.provider.read_section('{Test}', self.root))
 
    def test_read_blank(self):
        self.assertTrue(self.provider.read_blank('                         ', self.root))
        self.assertTrue(self.provider.read_blank('', self.root))
        self.assertFalse(self.provider.read_blank('!', self.root))
 
    def test_read_option(self):
        self.assertTrue(self.provider.read_option('some_option = some_value', self.root))
        self.assertFalse(self.provider.read_option('some_option', self.root))
 
    def test_write_comment(self):
        el = ET.Comment('Test comment')
        c = StringIO()
        self.assertTrue(self.provider.write_comment(c, el))
        self.assertEqual(c.getvalue(), '#Test comment\n')
        del(c, el)
 
    def test_write_section(self):
        el = ET.Element('testsection')
        subel = ET.Comment('test sub comment')
        el.append(subel)
        c = StringIO()
        self.assertTrue(self.provider.write_section(c, el))
        self.assertEqual(c.getvalue(), '[testsection]\n#test sub comment\n')
        del(c, el)
 
    def test_write_option(self):
        el = ET.Element('option')
        el.text = 'value'
        c = StringIO()
        self.assertTrue(self.provider.write_option(c, el))
        self.assertEqual(c.getvalue(), 'option\t= value\n')
        del(c, el)
 
    def test_write_amp(self):
        el = ET.Element('option')
        el.text = '&'
        c = StringIO()
        self.assertTrue(self.provider.write_option(c, el))
        self.assertEqual(c.getvalue(), 'option\t= &\n')
 
 
    def test_write_blank(self):
        el = ET.Element('')
        c = StringIO()
        self.assertTrue(self.provider.write_blank(c, el))
        self.assertEqual(c.getvalue(), '\n')
        del(c, el)
 
    def test_read(self):
        fp = open(RESOURCE_PATH + "/php.ini")
        self.assertTrue(type(self.provider.read(fp)), list)
        fp.close()
 
    def test_write(self):
        self.assertRaises(MetaconfError, self.provider.write, 'dummy', 'String is not element tree')
        root = ET.Element('root')
        el = ET.SubElement(root, 'section')
        com = ET.Comment('comment')
        el.append(com)
        option = ET.SubElement(el, 'option')
        option.text = 'value'
        c = StringIO()
        self.provider.write(c, root)
        self.assertEqual(c.getvalue(), '[section]\n#comment\noption\t= value\n')
        del(c, el, com, root)
 
 
 
 
class NginxFpTest(unittest.TestCase):
    provider = None
 
    def setUp(self):
        c = StringIO('')
        self.provider = NginxFormatProvider()
        self.provider.read(c)
        self.root = self.provider._cursect
 
    def tearDown(self):
        del self.provider
 
    def test_read_option(self):
        c = StringIO('listen 80;')
        self.provider._fp = c
        self.assertTrue(self.provider.read_option(c.readline(), self.root))
 
        c = StringIO('listen 80')
        self.provider._fp = c
        self.assertFalse(self.provider.read_option(c.readline(), self.root))
 
        c = StringIO('listen 80; # comment')
        self.provider._fp = c
        self.assertTrue(self.provider.read_option(c.readline(), self.root))
        self.assertEqual(self.provider._cursect.getiterator()[2].text.strip(), 'comment')
 
        c = StringIO("log_format  main  '$remote_addr - $remote_user [$time_local] \"$request\" '\n"+
                                 "'$status $body_bytes_sent \"$http_referer\" '\n"+
                                 "'\"$http_user_agent\" \"$http_x_forwarded_for\"';")
        self.provider._fp = c
        self.assertTrue(self.provider.read_option(c.readline(), self.root))
 
        c = StringIO("log_format  main  '$remote_addr - $remote_user [$time_local] \"$request\" '\n"+
                                 "# suddenly")
        self.provider._fp = c
        self.assertFalse(self.provider.read_option(c.readline(), self.root))
 
 
        c = StringIO("log_format  main  '$remote_addr - $remote_user [$time_local] \"$request\" '\n"+
                                 "'something else'; # Another comment")
        self.provider._fp = c
        self.assertTrue(self.provider.read_option(c.readline(), self.root))
        self.assertEqual(self.root.getiterator()[5].text.strip(), 'Another comment')
        self.assertTrue(callable(self.root.getiterator()[5].tag))
 
 
        del(c)
 
    def test_read_section(self):
        c = StringIO('http http_value { # http section\n\tsome write_sectionvalue;\n another value;\n}')
        self.provider._fp = c
        self.assertTrue(self.provider.read_section(c.readline(), self.root))
        self.assertEqual(self.provider._cursect.find('http').text.strip(), 'http_value')
        self.assertEqual(self.provider._cursect.find('http').getiterator()[1].text.strip(), 'http section')
        del(c)
 
    def test_write_section(self):
        el = ET.Element('testsection')
        subel = ET.Comment('test sub comment')
        el.append(subel)
        c = StringIO()
        self.assertTrue(self.provider.write_section(c, el))
        self.assertEqual(c.getvalue(), 'testsection  {\n\t#test sub comment\n}\n')
        del(c, el)
 
    def test_write_option(self):
        el = ET.Element('option')
        el.text = 'value'
        c = StringIO()
        self.assertTrue(self.provider.write_option(c, el))
        self.assertEqual(c.getvalue(), 'option\tvalue;\n')
 
        el = ET.Element('option')
        el.text = "'value'\n'another'"
        c = StringIO()
        self.assertTrue(self.provider.write_option(c, el))
        self.assertEqual(c.getvalue(), "option\t'value'\n      'another';\n")
 
        del(c, el)
 
 
class MysqlFpTest(unittest.TestCase):
    provider = None
 
    def setUp(self):
        c = StringIO('')
        self.provider = MysqlFormatProvider()
        self.provider.read(c)
        self.root = self.provider._cursect
 
    def tearDown(self):
        del self.provider
 
    def test_read_statement(self):
        self.assertTrue(self.provider.read_statement('quote-names', self.root))
 
    def test_read_include(self):
        self.assertTrue(self.provider.read_include('!include /etc/somefile.conf', self.root))
        self.assertTrue(self.provider.read_include('!includedir /etc/somedir', self.root))
        self.assertTrue(self.provider.read_include('!includedir "/etc/somedir"', self.root))
        self.assertTrue(self.provider.read_include('!includedir "C:\\Program Files\\MySQL\\conf\\twink.ini"', self.root))
        self.assertFalse(self.provider.read_include('!includedir', self.root))
        self.assertTrue(self.provider.read_include('!include "/etc/somefile.conf"', self.root))
 
    def test_write_statement(self):
        el = ET.Element('quote-names')
        c = StringIO()
        self.assertTrue(self.provider.write_statement(c, el))
        self.assertEqual(c.getvalue(), 'quote-names\n')
 
        el = ET.Element('quote-names')
        el.text = 'Some value'
        c = StringIO()
        self.assertFalse(self.provider.write_statement(c, el))
 
 
    def test_write_include(self):
        el = ET.Element('!include')
        el.text = '/etc/mysql/somefile.cnf'
        c = StringIO()
        self.assertTrue(self.provider.write_include(c, el))
        self.assertEqual(c.getvalue(), '!include /etc/mysql/somefile.cnf\n')
 
        el = ET.Element('!includedir')
        el.text = '/etc/mysql/somedir'
        c = StringIO()
        self.assertTrue(self.provider.write_include(c, el))
        self.assertEqual(c.getvalue(), '!includedir /etc/mysql/somedir\n')
 
        el = ET.Element('!includedir')
        el.text = '"C:\\Program files\\twink"'
        c = StringIO()
        self.assertTrue(self.provider.write_include(c, el))
        self.assertEqual(c.getvalue(), '!includedir "C:\\Program files\\twink"\n')
 
 
        del(c, el)
 
'''
 
class Test(unittest.TestCase):
 
 
        _ini = """
[general]
; Unique server identificator in Scalr envirounment
server_id = 40c2a5f1-57e3-4b80-9642-138ea8514fb1
 
 
; Server behaviour (app|www|mysql)
behaviour = app
behaviour = www
behaviour = mysql
 
; Role name ex = mysqllvm64
role_name = scalarizr-first
        """
 
        _xml = """
<Storage>
  <Keyspaces>
    <Keyspace Name="Keyspace1">
        <ColumnFamily Name="Standard2"
                    CompareWith="UTF8Type"
                    KeysCached="100%"/>
        <ColumnFamily Name="StandardByUUID1" CompareWith="TimeUUIDType" />
    </Keyspace>
    <Keyspace Name="Keyspace2">
        <ColumnFamily Name="Standard2"
                    CompareWith="UTF8Type"
                    KeysCached="100%"/>
        <ColumnFamily Name="StandardByUUID1" CompareWith="TimeUUIDType" />
    </Keyspace>
  </Keyspaces>
</Storage>
        """
 
 
        def _test_php_ini(self):
                conf = Configuration("ini")
                conf.read(RESOURCE_PATH + "/php.ini")
                #self.assertEqual(conf.get_int("PHP/memory_limit"), 128)
                self.assertEqual(conf.get("PHP/error_reporting"), "E_ALL & ~E_NOTICE")
 
 
 
                """
                conf.readfp(StringIO(self._ini))
                self.assertEqual(conf.get("general/server_id"), "40c2a5f1-57e3-4b80-9642-138ea8514fb1")
                self.assertEqual(len(conf.get_list("general/*")), 12)
                conf.add('general/behaviour', 'cassandra')
                self.assertEqual(conf.get_list("general/behaviour")[-1].strip(), 'cassandra')
                conf.add('general/some_param', 'some_value', before_path='server_id')
                self.assertEqual(conf.get_list("general/*")[3].strip(), 'some_value')
                conf.set('general/behaviour[1]', '1', typecast=float)
                self.assertEqual(conf.get("general/behaviour[1]"), '1.0')
                conf.remove('general/behaviour', 'www')
                self.assertEqual(len(conf.get_list("general/behaviour")), 3)
                conf.remove('general/behaviour')
                self.assertEqual(len(conf.get_list("general/behaviour")), 0)
                """
 
        def _test_xml(self):
                conf = Configuration("xml")
                conf.readfp(StringIO(self._xml))
                self.assertEqual(conf.get("Storage/Keyspaces/Keyspace[1]").strip(), "")
                self.assertEqual(len(conf.get_list("Storage/Keyspaces/Keyspace")), 2)
 
                col = conf.get_dict("Storage/Keyspaces/Keyspace[@Name='Keyspace1']/ColumnFamily[1]")
                self.assertTrue(len(col.keys()), 3)
                self.assertTrue("CompareWith" in col)
                self.assertEqual(col["CompareWith"], "UTF8Type")
#               self.assertEqual(len(conf.get_list("Storage/Keyspaces/*")), 7)
 
        def _test_subset(self):
                conf = Configuration("xml")
                conf.readfp(StringIO(self._xml))
 
                ks_conf = conf.subset("Storage/Keyspaces/Keyspace[@Name='Keyspace2']")
                self.assertEqual(ks_conf.get_dict("ColumnFamily[1]")["Name"], "Standard2")
 
        def _test_parent(self):
                conf = Configuration("xml")
                conf.readfp(StringIO(self._xml))
                ks_conf = conf.subset("Storage/Keyspaces/Keyspace[@Name='Keyspace2']")
                self.assertEqual(ks_conf._find("..").tag, 'Keyspaces')
 
        def _test_set(self):
                conf = Configuration("xml")
                conf.readfp(StringIO(self._xml))
                conf.set("Storage/Keyspaces/Keyspace[@Name='Keyspace2']", dict(Keyword="TestKeyword", CompareWith="TestCompareWith"))
                attribs = conf.get_dict("Storage/Keyspaces/Keyspace[@Name='Keyspace2']")
                self.assertEqual(attribs['Keyword'], "TestKeyword")
                self.assertEqual(attribs['CompareWith'], "TestCompareWith")
 
        def _test_phpini_bench(self):
                start = time.time()
                conf = Configuration("ini")
                if os.path.exists('/etc/php5/apache2/php.ini'):
                        conf.readfp(open('/etc/php5/apache2/php.ini'))
                else:
                        conf.readfp(open('/etc/php.ini'))
                conf.set('PHP/memory_limit', '256')
                conf.set('MySQLi/mysqli.default_host', 'sql.trololo.net')
                conf.write(open('/home/spike/php.ini', 'w'))
                print "Php.ini time: ", time.time() - start
 
        def _test_extend(self):
                conf = Configuration("xml")
                conf.readfp(StringIO(self._xml))
                self.assertEqual(len(conf.get_list("Storage/Keyspaces/Keyspace")), 2)
                self.assertEqual(len(conf.get_list("Storage/Keyspaces")), 1)
                conf.readfp(StringIO(re.sub('UTF8Type', 'UTF16Type', self._xml, 1)))
                self.assertEqual(len(conf.get_list("Storage/Keyspaces/Keyspace")), 3)
                self.assertEqual(len(conf.get_list("Storage/Keyspaces")), 1)
                conf.readfp(StringIO(re.sub('"Keyspace2">', '"Keyspace2">some_text', self._xml, 1)))
                self.assertEqual(len(conf.get_list("Storage/Keyspaces/Keyspace")), 4)
                self.assertEqual(len(conf.get_list("Storage/Keyspaces")), 1)
                conf.readfp(StringIO(re.sub('<Keyspaces>', '<Keyspaces Name="SomeName">', self._xml, 1)))
                self.assertEqual(len(conf.get_list("Storage/Keyspaces")), 2)
 
        def _test_nginx(self):
                conf = Configuration("nginx")
                conf.read('/etc/nginx/nginx.conf')
                self.assertEqual(conf.get_int('worker_processes'), 1)
                conf.set('user', 'www www')
                self.assertEqual(conf.get('user'),'www www')
'''
 
if __name__ == "__main__":
    #init_tests()
    unittest.main()
 
