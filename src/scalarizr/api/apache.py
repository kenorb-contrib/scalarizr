"""
Created on Jun 10, 2013
 
@author: Dmytro Korsakov
"""
 
from __future__ import with_statement
 
import os
import re
import pwd
import time
import uuid
import shutil
import logging
import urllib2
 
try:
    from cStringIO import StringIO
except ImportError:
    from StringIO import StringIO
 
from scalarizr import rpc
from scalarizr import linux
from telnetlib import Telnet
from scalarizr.bus import bus
from scalarizr.node import __node__
from scalarizr.util.initdv2 import InitdError
from scalarizr.util import system2, initdv2
from scalarizr.util import wait_until, dynimp, PopenError
from scalarizr.linux import coreutils, iptables, pkgmgr
from scalarizr.libs.metaconf import Configuration, NoPathError, ParseError
 
 
LOG = logging.getLogger(__name__)
 
etc_path = bus.etc_path or "/etc/scalr"
 
apache = {
    "vhosts_dir":           os.path.join(etc_path, "private.d/vhosts"),
    "keys_dir":             os.path.join(etc_path, "private.d/keys"),
    "vhost_extension":      ".vhost.conf",
    "logrotate_conf_path":  "/etc/logrotate.d/scalarizr_app"}
 
if linux.os.debian_family:
    apache.update({
        "httpd.conf":       "/etc/apache2/apache2.conf",
        "ssl_conf_path":    "/etc/apache2/sites-available/default-ssl",
        "default_vhost":    "/etc/apache2/sites-enabled/000-default",
        "ports_conf_deb":   "/etc/apache2/ports.conf",
        "ssl_load_deb":     "/etc/apache2/mods-enabled/ssl.load",
        "mod_rpaf_path":    "/etc/apache2/mods-available/rpaf.conf",
        "default-ssl_path": "/etc/apache2/sites-enabled/default-ssl",
        "key_path_default": "/etc/ssl/private/ssl-cert-snakeoil.key",
        "crt_path_default": "/etc/ssl/certs/ssl-cert-snakeoil.pem",
        "apachectl":        "/usr/sbin/apache2ctl",
        "bin_path":         "/usr/sbin/apache2",
        "a2enmod_path":     "/usr/sbin/a2enmod",
        "a2ensite_path":    "/usr/sbin/a2ensite",
        "initd_script":     "/etc/init.d/apache2",
        "group":            "www-data",
 
        "logrotate_conf":   """/var/log/http-*.log {
         weekly
         missingok
         rotate 52
         compress
         delaycompress
         notifempty
         create 640 root adm
         sharedscripts
         postrotate
                 if [ -f "`. /etc/apache2/envvars ; echo ${APACHE_PID_FILE:-/var/run/apache2.pid}`" ]; then
                         /etc/init.d/apache2 reload > /dev/null
                 fi
         endscript
        }
        """})
 
else:
    apache.update({
        "httpd.conf":       "/etc/httpd/conf/httpd.conf",
        "ssl_conf_path":    "/etc/httpd/conf.d/ssl.conf",
        "default_vhost":    "/etc/httpd/sites-enabled/000-default",  # Not used
        "mod_ssl_file":     "/etc/httpd/modules/mod_ssl.so",
        "mod_rpaf_path":    "/etc/httpd/conf.d/mod_rpaf.conf",
        "key_path_default": "/etc/pki/tls/private/localhost.key",
        "crt_path_default": "/etc/pki/tls/certs/localhost.crt",
        "apachectl":        "/usr/sbin/apachectl",
        "bin_path": "/usr/sbin/httpd",
        "initd_script":     "/etc/init.d/httpd",
        "group":            "apache",
 
        "logrotate_conf":   """/var/log/http-*.log {
         missingok
         notifempty
         sharedscripts
         delaycompress
         postrotate
             /sbin/service httpd reload > /dev/null 2>/dev/null || true
         endscript
        }
        """})
 
__apache__ = __node__["apache"]
__apache__.update(apache)
 
 
class ApacheError(BaseException):
    pass
 
 
class ApacheAPI(object):
 
    service = None
    mod_ssl = None
    current_open_ports = None
 
    def __init__(self):
        self.service = initdv2.lookup("apache")
        self.mod_ssl = DebianBasedModSSL() if linux.os.debian_family else RedHatBasedModSSL()
        self.current_open_ports = []
        self._query_env = bus.queryenv_service
 
    @rpc.command_method
    def create_vhost(self, hostname, port, template, ssl, ssl_certificate_id=None, reload=True, allow_port=False):
        """
        Creates Name-Based Apache VirtualHost
 
        @param hostname: Server Name
        @param port: port to listen to
        @param template: VirtualHost body with no certificate paths
        @param ssl: True if VirtualHost uses SSL certificate
        @param ssl_certificate_id: ID of SSL certificate
        @param reload: True if immediate apache reload is required.
        @return: path to VirtualHost file
        """
        #TODO: add Listen and NameVirtualHost directives to httpd.conf or ports.conf if needed
 
        name = "%s:%s" % (hostname, port)
        LOG.info("Creating Apache VirtualHost %s" % name)
 
        v_host = VirtualHost(template)
 
        if ssl:
            ssl_certificate = SSLCertificate(ssl_certificate_id)
            if not ssl_certificate.exists():
                ssl_certificate.ensure()
 
            v_host.use_certificate(
                ssl_certificate.cert_path,
                ssl_certificate.key_path,
                ssl_certificate.chain_path if os.path.exists(ssl_certificate.chain_path) else None
            )
 
            LOG.info("Certificate %s is set to VirtualHost %s" % (ssl_certificate_id, name))
 
            #Compatibility with old apache handler
            if not self.mod_ssl.has_valid_certificate() or self.mod_ssl.is_system_certificate_used():
                self.mod_ssl.set_default_certificate(ssl_certificate)
 
        for directory in v_host.document_root_paths:
            docroot_parent_path = os.path.dirname(directory)
 
            if not os.path.exists(docroot_parent_path):
                os.makedirs(docroot_parent_path, 0755)
                LOG.info("Created parent directory of document root %s for %s" % (directory, name))
 
            if not os.path.exists(directory):
                shutil.copytree(os.path.join(bus.share_path, "apache/html"), directory)
                files = ", ".join(os.listdir(directory))
                LOG.debug("Copied document root files: %s" % files)
 
                try:
                    pwd.getpwnam("apache")
                    uname = "apache"
                except KeyError:
                    uname = "www-data"
 
                coreutils.chown_r(directory, uname)
                LOG.debug("Changed owner to %s: %s" % (
                    uname, ", ".join(os.listdir(directory))))
            else:
                LOG.debug("Document root %s already exists." % directory)
 
        try:
            clog_path = os.path.dirname(v_host.custom_log_path)
            if not os.path.exists(clog_path):
                os.makedirs(clog_path, 0755)
                LOG.info("Created CustomLog directory for VirtualHost %s:%s: %s" % (
                    hostname,
                    port,
                    clog_path,
                ))
        except NoPathError:
            LOG.debug("Directive 'CustomLog' not found in %s" % name)
 
        try:
            errlog_path = os.path.dirname(v_host.error_log_path)
            if not os.path.exists(errlog_path):
                os.makedirs(errlog_path, 0755)
                LOG.info("Created ErrorLog directory for VirtualHost %s:%s: %s" % (
                    hostname,
                    port,
                    errlog_path,
                ))
        except NoPathError:
            LOG.debug("Directive 'ErrorLog' not found in %s" % name)
 
        v_host_changed = True
        v_host_path = get_virtual_host_path(hostname, port)
        if os.path.exists(v_host_path):
            with open(v_host_path, "r") as old_v_host:
                if old_v_host.read() == v_host.body:
                    v_host_changed = False
 
        if v_host_changed:
            with open(v_host_path, "w") as fp:
                fp.write(v_host.body)
            LOG.info("VirtualHost %s configuration saved to %s" % (name, v_host_path))
        else:
            LOG.info("VirtualHost %s configuration (%s) has no changes." % (name, v_host_path))
 
        if allow_port:
            self._open_ports([port])
 
        if reload:
            try:
                self.configtest()
            except initdv2.InitdError, e:
                LOG.error("ConfigTest failed with error: '%s'." % str(e))
                raise
            else:
                self.reload_service("Applying Apache VirtualHost %s" % name)
        else:
            LOG.info("Apache VirtualHost %s has been applied without service reload." % name)
 
        return v_host_path
 
    @rpc.command_method
    def update_vhost(self,
                     signature,
                     hostname=None,
                     port=80,
                     template=None,
                     ssl=False,
                     ssl_certificate_id=None,
                     reload=True):
        """
        Changes settings of VirtualHost defined by @signature
 
        @param signature: tuple, (hostname,port)
        @param hostname: String, new hostname
        @param port: int, new port
        @param: ssl: bool, indicates if the updated VirtualHost is going to be ssl-based.
        @param: ssl_certificate_id: int, ID of the new certificate to fetch from Scalr
        @param: reload: bool, indicates if immediate reload is required.
        @param template: String, new template. If new template is passed,
            all other changes (e.g. hostname, port, cert) will be applied to it.
            Otherwice changes will be applied to old VirtualHost`s body.
        """
 
        old_hostname, old_port = signature
        old_path = get_virtual_host_path(old_hostname, old_port)
        old_body = open(old_path, "r").read() if os.path.exists(old_path) else None
 
        v_host = VirtualHost(template or old_body)
        if hostname:
            v_host.server_name = hostname
        if port:
            v_host.port = port
        if ssl and ssl_certificate_id:
            ssl_certificate = SSLCertificate(ssl_certificate_id)
            if not ssl_certificate.exists():
                ssl_certificate.ensure()
            v_host.use_certificate(ssl_certificate)
 
        path = get_virtual_host_path(hostname or old_hostname, port or old_port)
 
        if old_path != path:
            os.remove(old_path)
            v_host_changed = True
        elif old_body != v_host.body:
            v_host_changed = True
        if v_host_changed:
            with open(path, "w") as fp:
                fp.write(v_host.body)
 
        if reload:
            try:
                self.configtest()
            except initdv2.InitdError, e:
                LOG.error("ConfigTest failed with error: '%s'." % str(e))
                raise
            else:
                self.reload_service()
 
    @rpc.command_method
    def delete_vhosts(self, vhosts, reload=True):
        """
        Deletes VirtualHost
        @param vhosts: list, [(hostname:password),]
        @param reload: indicates if immediate service reload is needed
        @return: None
        """
        LOG.info("Removing Apache VirtualHosts: %s" % str(vhosts))
 
        for signature in vhosts:
            v_host_path = get_virtual_host_path(*signature)
 
            if os.path.exists(v_host_path):
                os.remove(v_host_path)
                LOG.info("VirtualHost %s:%s removed." % signature)
            else:
                LOG.warning("Cannot remove %s: %s does not exist." % (
                    str(signature), v_host_path))
 
        if reload:
            try:
                self.configtest()
            except initdv2.InitdError, e:
                LOG.error("ConfigTest failed with error: '%s'." % str(e))
                raise
            else:
                self.reload_service('%s VirtualHosts removed.' % len(vhosts))
 
 
    @rpc.command_method
    def reconfigure(self, vhosts, reload=True, rollback_on_error=True):
        """
        Deploys multiple VirtualHosts and removes odds.
        @param vhosts: list(dict(vhost_data),)
        @return: list, paths to reconfigured VirtualHosts
        """
        ports = []
        applied_vhosts = []
 
        old_files = []
        LOG.info("Started reconfiguring Apache VirtualHosts.")
 
        bm = BackupManager()
        if rollback_on_error:
            for fname in os.listdir(__apache__["vhosts_dir"]):
                if fname.endswith(__apache__["vhost_extension"]):
                    old_files.append(os.path.join(__apache__["vhosts_dir"], fname))
            bm.add(old_files)
 
        try:
            for virtual_host_data in vhosts:
 
                hostname = virtual_host_data["hostname"]
                port = virtual_host_data["port"]
                template = virtual_host_data["template"]
                ssl = virtual_host_data["ssl"]
                cert_id = virtual_host_data["ssl_certificate_id"]
                path = self.create_vhost(hostname, port, template, ssl, cert_id, allow_port=False, reload=False)
                applied_vhosts.append(path)
                ports.append(port)
 
            #cleanup
            for fname in os.listdir(__apache__["vhosts_dir"]):
                old_vhost_path = os.path.join(__apache__["vhosts_dir"], fname)
                if old_vhost_path not in applied_vhosts:
                    os.remove(old_vhost_path)
                    LOG.info("Removed old VirtualHost file %s" % old_vhost_path)
        except:
            if rollback_on_error:
                bm.restore()
            raise
 
        self._open_ports(set(ports))  # consolidated ports for single request
 
        if reload:
            try:
                self.configtest()
            except (BaseException, Exception), e:
                LOG.error("ConfigTest failed with error: '%s'." % str(e))
                if rollback_on_error:
                    bm.restore()
                raise
            else:
                self.reload_service("Applying new apache configuration.")
        else:
            LOG.info("Apache configuration has been changed without service reload.")
 
        return applied_vhosts
 
    @rpc.query_method
    def get_webserver_statistics(self):
        """
        @return: dict, parsed mod_status data
 
        i.e.
        Current Time
        Restart Time
        Parent Server Generation
        Server uptime
        Total accesses
        CPU Usage
 
        The machine readable file can be accessed by using the following link:
        http://your.server.name/server-status?auto
 
        Available only when mod_stat is enabled
        """
        d = dict()
        try:
            f = urllib2.urlopen("http://127.0.0.1/server-status?auto")
            data = f.read()
        except urllib2.HTTPError, e:
            if "404" in str(e):
                return {"errmsg": "mod_status is not enabled"}
            return {"errmsg": str(e)}
        except urllib2.URLError, e:
            if "111" in str(e):
                return {"errmsg": "Connection refused"}
            return {"errmsg": str(e)}
        if data:
            for line in data.split("\n"):
                pairs = line.split(":")
                if len(pairs) > 1:
                    key, value = pairs
                    d[key.strip()] = value.strip()
        return d
 
    @rpc.query_method
    def list_served_virtual_hosts(self):
        """
        Returns all VirtualHosts deployed by Scalr
        and available on web server
 
        @return: list, paths to available VirtualHosts
        """
        text = system2((__apache__["apachectl"], "-S"))[0]
        directory = __apache__["vhosts_dir"]
        ext = __apache__["vhost_extension"]
        result = []
 
        for file_name in os.listdir(directory):
            path = os.path.join(directory, file_name)
            if path.endswith(ext) and path in text:
                result.append(file_name)
 
        return result
 
    @rpc.command_method
    def set_default_ssl_certificate(self, id):
        """
        If the certificate with given ID already exists on disk
        this method adds it to the default SSL virtual host.
        Otherwice default system certificate will be used.
        """
        cert = SSLCertificate(id)
        self.mod_ssl.set_default_certificate(cert)
 
    @rpc.command_method
    def start_service(self):
        self.service.start()
 
    @rpc.command_method
    def stop_service(self, reason=None):
        self.service.stop(reason)
 
    @rpc.command_method
    def restart_service(self, reason=None):
        self.service.restart(reason)
 
    @rpc.command_method
    def reload_service(self, reason=None):
        try:
            self.service.reload(reason)
        except initdv2.InitdError, e:
            if "not running" in e.message:
                LOG.info("Apache service is not running. Doing start instead of reload.")
                LOG.info(reason)
                self.service.start()
            else:
                raise
 
    @rpc.command_method
    def configtest(self):
        self.service.configtest()
 
    def init_service(self):
        """
        Configures apache service
        """
 
        self._open_ports([80, 443])
 
        self.enable_virtual_hosts_directory()
 
        self.fix_default_virtual_host()
 
        self.update_log_rotate_config()
 
        self.mod_ssl.ensure()
 
        if linux.os.debian_family:
            mod_rpaf_path = __apache__["mod_rpaf_path"]
            if os.path.exists(mod_rpaf_path):
 
                with open(mod_rpaf_path, "r") as fp:
                    body = fp.read()
 
                mod_rpaf = ModRPAF(body)
                mod_rpaf.fix_module()
 
                with open(mod_rpaf_path, "w") as fp:
                    fp.write(mod_rpaf.body)
 
        ModRPAF.ensure_permissions()
 
    def enable_virtual_hosts_directory(self):
        if not os.path.exists(__apache__["vhosts_dir"]):
            os.makedirs(__apache__["vhosts_dir"])
            LOG.info("Created new directory for VirtualHosts: %s" % __apache__["vhosts_dir"])
 
        with ApacheConfigManager(__apache__["httpd.conf"]) as apache_config:
            inc_mask = __apache__["vhosts_dir"] + "/*" + __apache__["vhost_extension"]
 
            if not inc_mask in apache_config.get_list("Include"):
                apache_config.add("Include", inc_mask)
                LOG.info("VirtualHosts directory included in %s" % __apache__["httpd.conf"])
 
    def fix_default_virtual_host(self):
        if linux.os.debian_family:
            if os.path.exists(__apache__["default_vhost"]):
 
                with ApacheConfigManager(__apache__["default_vhost"]) as default_vhost:
                    default_vhost.set("NameVirtualHost", "*:80", force=True)
 
                with open(__apache__["default_vhost"], "r") as fp:
                    dv = fp.read()
 
                vhost_regexp = re.compile("<VirtualHost\s+\*>")
                dv = vhost_regexp.sub("<VirtualHost *:80>", dv)
 
                with open(__apache__["default_vhost"], "w") as fp:
                    fp.write(dv)
 
                LOG.info("Replaced NameVirtualhost and Virtualhost values in %s." % __apache__["default_vhost"])
            else:
                LOG.warning("Cannot find default vhost config file %s." % __apache__["default_vhost"])
 
        else:
            with ApacheConfigManager(__apache__["httpd.conf"]) as apache_config:
                if not apache_config.get_list("NameVirtualHost"):
                    apache_config.set("NameVirtualHost", "*:80", force=True)
 
    def update_log_rotate_config(self):
        if not os.path.exists(__apache__["logrotate_conf_path"]):
            if not os.path.exists("/etc/logrotate.d/httpd"):
                with open(__apache__["logrotate_conf_path"], "w") as fp:
                    fp.write(__apache__["logrotate_conf"])
                LOG.info("LogRorate config updated.")
 
    def reload_virtual_hosts(self):
        """
        Reloads all VirtualHosts assigned to the server
        @return: list(virtual_host_path,)
        """
        vh_data = self._fetch_virtual_hosts()
        return self.reconfigure(vh_data, reload=True, rollback_on_error=True)
 
    def rename_old_virtual_hosts(self):
        vhosts_dir = __apache__["vhosts_dir"]
        for fname, new_fname in get_updated_file_names(os.listdir(vhosts_dir)):
            os.rename(os.path.join(vhosts_dir, fname), os.path.join(vhosts_dir, new_fname))
 
    def _fetch_virtual_hosts(self):
        """
        Combines list of virtual hosts in unified format
        regardless of Scalr version.
        @return: list(dict(vhost_data))
        """
        LOG.info("Fetching Apache VirtualHost configuration data from Scalr.")
        result = []
        scalr_version = bus.scalr_version or (4, 4, 0)
 
        if scalr_version < (4, 4):
            raw_data = self._query_env.list_virtual_hosts()
 
            for virtual_host_data in raw_data:
                data = dict()
                data["hostname"] = virtual_host_data.hostname
                data["template"] = virtual_host_data.raw
                ssl = bool(int(virtual_host_data.https))
                data["port"] = 443 if ssl else 80
                data["ssl"] = ssl
                if ssl:
                    data["ssl_certificate_id"] = virtual_host_data["ssl_certificate_id"]
                else:
                    data["ssl_certificate_id"] = None
                result.append(data)
 
        else:
            raw_data = self._query_env.list_farm_role_params(__node__["farm_role_id"])
            params = raw_data.get("params", {})
            LOG.debug("QueryEnv returned list of farmrole params: %s" % params)
            app_data = params.get("app", {})
            virtual_hosts_section = app_data.get("virtual_hosts", [])
            if virtual_hosts_section:
                for virtual_host_data in virtual_hosts_section:
                    virtual_host_data["ssl"] = bool(int(virtual_host_data["ssl"]))
                    if not virtual_host_data["ssl"]:
                        virtual_host_data["ssl_certificate_id"] = None  # Handling "0"
                    result.append(virtual_host_data)
 
        return result
 
    def _open_ports(self, ports):
        if iptables.enabled():
            rules = []
            for port in ports:
                if port not in self.current_open_ports:
                    self.current_open_ports.append(port)
                    rules.append({"jump": "ACCEPT", "protocol": "tcp", "match": "tcp", "dport": str(port)})
            if rules:
                LOG.info("Ensuring ports %s are allowed in IPtables" % str(ports))
                iptables.FIREWALL.ensure(rules)
        else:
            LOG.warning("Cannot open ports %s: IPtables disabled" % str(ports))
 
 
class BasicApacheConfiguration(object):
 
    _cnf = None
 
    def __init__(self, body):
        config = Configuration("apache")
        try:
            config.reads(str(body))
        except ParseError, e:
            LOG.error("MetaConf failed to parse Apache VirtualHost body: \n%s" % body)
            e._err = body + "\n" + e._err
            raise
        self._cnf = config
 
    @property
    def body(self):
        return self._cnf.dumps()
 
 
class VirtualHost(BasicApacheConfiguration):
 
    def __repr__(self):
        return "%s:%s" % (self.server_name, self.port)
 
    def __cmp__(self, other):
        return self.body == other.body
 
    @property
    def error_log_path(self):
        raw_value = self._cnf.get(".//ErrorLog")
        return raw_value.split(" ")[0]
 
    @property
    def custom_log_path(self):
        raw_value = self._cnf.get(".//CustomLog")
        return raw_value.split(" ")[0]
 
    @property
    def ssl_cert_path(self):
        return self._cnf.get(".//SSLCertificateFile")
 
    @property
    def ssl_key_path(self):
        return self._cnf.get(".//SSLCertificateKeyFile")
 
    @property
    def ssl_chain_path(self):
        return self._cnf.get(".//SSLCertificateChainFile")
 
    @property
    def document_root_paths(self):
        doc_roots = []
        for item in self._cnf.items(".//VirtualHost"):
            if "DocumentRoot" == item[0]:
                doc_root = item[1][:-1] if item[1][-1] == "/" else item[1]
                doc_roots.append(doc_root)
        return doc_roots
 
    def use_certificate(self, cert_path, key_path, chain_path=None):
 
        try:
            self._cnf.get(".//SSLCertificateFile")
            self._cnf.get(".//SSLCertificateKeyFile")
        except NoPathError, e:
            LOG.error("Cannot apply SSL certificate %s. Error: %s. Check VirtualHost configuration: %s" % (
                (cert_path, key_path, chain_path), e.message, self.body
            ))
            raise ApacheError(e)
 
        self._cnf.set(".//SSLCertificateFile", cert_path)
        self._cnf.set(".//SSLCertificateKeyFile", key_path)
 
        if chain_path:
            try:
                self._cnf.set(".//SSLCertificateChainFile", chain_path, force=False)
            except NoPathError:
                parent = self._cnf.etree.find(".//SSLCertificateFile/..")
                before_el = self._cnf.etree.find(".//SSLCertificateFile")
                ch = self._cnf._provider.create_element(
                    self._cnf.etree,
                    ".//SSLCertificateChainFile",
                    chain_path)
                ch.text = chain_path
                parent.insert(list(parent).index(before_el), ch)
        else:
            self._cnf.comment(".//SSLCertificateChainFile")
 
    def _get_port(self):
        raw_host = self._cnf.get(".//VirtualHost").split(":")
        if len(raw_host) > 1 and raw_host[1].isdigit():
            return int(raw_host[1])
        elif self.ssl_cert_path:
            return 443
        else:
            return 80
 
    def _set_port(self, port):
        old_value = self._cnf.get(".//VirtualHost")
        host = old_value.split(":")[0]
        new_value = "%s:%s" % (host, port)
        self._cnf.set(".//VirtualHost", dict(value=new_value))
 
    def _get_server_name(self):
        try:
            server_name = self._cnf.get(".//ServerName")
        except NoPathError:
            server_name = ''
        return server_name
 
    def _set_server_name(self, new_name):
        self._cnf.set(".//ServerName", new_name)
 
    server_name = property(_get_server_name, _set_server_name)
 
    port = property(_get_port, _set_port)
 
 
class ModRPAF(BasicApacheConfiguration):
 
    def list_proxy_ips(self):
        raw_value = self._cnf.get(".//RPAFproxy_ips")
        ips = set(re.split(r"\s+", raw_value))
        return ips
 
    def add(self, ips):
        proxy_ips = self.list_proxy_ips()
        proxy_ips |= set(ips)
        self._cnf.set(".//RPAFproxy_ips", " ".join(proxy_ips))
 
    def remove(self, ips):
        proxy_ips = self.list_proxy_ips()
        proxy_ips -= set(ips)
        self._cnf.set(".//RPAFproxy_ips", " ".join(proxy_ips))
 
    def update(self, ips):
        proxy_ips = set(ips)
        self._cnf.set(".//RPAFproxy_ips", " ".join(proxy_ips))
 
    def fix_module(self):
        """
        fixing bug in rpaf 0.6-2
        """
        pm = dynimp.package_mgr()
        if "0.6-2" == pm.installed("libapache2-mod-rpaf"):
            try:
                self._cnf.set('./IfModule[@value="mod_rpaf.c"]', {"value": "mod_rpaf-2.0.c"})
            except NoPathError:
                pass
            else:
                LOG.info("Patched IfModule value in rpaf.conf")
 
    @staticmethod
    def ensure_permissions():
        httpd_conf_path = __apache__["httpd.conf"]
        mod_rpaf_path = __apache__["mod_rpaf_path"]
 
        if os.path.exists(httpd_conf_path) and os.path.exists(mod_rpaf_path):
            st = os.stat(httpd_conf_path)
            os.chown(mod_rpaf_path, st.st_uid, st.st_gid)
 
 
class ApacheConfigManager(object):
 
    _cnf = None
    path = None
 
    def __init__(self, path):
        self._cnf = Configuration("apache")
        self.path = path
 
    def __enter__(self):
        self._cnf.read(self.path)
        return self._cnf
 
    def __exit__(self, type, value, traceback):
        self._cnf.write(self.path)
 
 
class BackupManager(object):
 
    id = None
    data = None
 
    def __init__(self):
        self.id = uuid.uuid4()
        self.data = {}
 
    def add(self, list_files):
        for path in set(list_files):
            with open(path, "r") as fp:
                self.data[path] = fp.read()
        LOG.debug("BackupManager %s created snapshot of %s" % (self.id, list_files))
 
    def restore(self):
        for path, body in self.data.items():
            with open(path, "r") as fp:
                new_body = fp.read()
            if body == new_body:
                LOG.debug("BackupManager %s did not detect changes in %s. Restore skipped." % (self.id, path))
            else:
                with open(path, "w") as fp:
                    fp.write(body)
                    LOG.info("BackupManager %s restored %s from backup." % (self.id, path))
 
 
class SSLCertificate(object):
 
    id = None
 
    def __init__(self, ssl_certificate_id=None):
        self.id = ssl_certificate_id
 
    def update(self, cert, key, authority=None):
        """
        Dumps certificate on disk.
        @param cert: String, certificate pem
        @param key: String, certificate key
        @param authority: String, CA Cert
        """
        st = os.stat(__apache__["httpd.conf"])
 
        with open(self.cert_path, "w") as fp:
            fp.write(cert)
        os.chown(self.cert_path, st.st_uid, st.st_gid)
 
        with open(self.key_path, "w") as fp:
            fp.write(key)
        os.chown(self.key_path, st.st_uid, st.st_gid)
 
        if authority:
            with open(self.chain_path, "w") as fp:
                fp.write(authority)
            os.chown(self.chain_path, st.st_uid, st.st_gid)
 
    def ensure(self):
        """
        Fetches SSL Certificate from Scalr and dumps data on disk.
        """
        LOG.info("Retrieving SSL certificate %s from Scalr." % self.id or "'default'")
        query_env = bus.queryenv_service
        cert_data = query_env.get_ssl_certificate(self.id)
        authority = cert_data[2] if len(cert_data) > 2 else None
        self.update(cert_data[0], cert_data[1], authority)
 
    def delete(self):
        """
        Removes SSL Certificate files from disk.
        @return:
        """
        for path in (self.cert_path, self.key_path, self.chain_path):
            if os.path.exists(path):
                os.remove(path)
 
    @property
    def cert_path(self):
        id = "_" + str(self.id) if self.id else ''
        return os.path.join(__apache__["keys_dir"], "https%s.crt" % id)
 
    @property
    def key_path(self):
        id = "_" + str(self.id) if self.id else ''
        return os.path.join(__apache__["keys_dir"], "https%s.key" % id)
 
    @property
    def chain_path(self):
        id = "_" + str(self.id) if self.id else ''
        return os.path.join(__apache__["keys_dir"], "https%s-ca.crt" % id)
 
    def exists(self):
        return os.path.exists(self.cert_path) and os.path.exists(self.key_path)
 
 
class ModSSL(object):
 
    def set_default_certificate(self, cert):
        """
        If certificate files exist on disk
        this method adds this certificate to the default SSL virtual host.
        Otherwice default system certificate will be used.
        """
        ssl_conf_path = __apache__["ssl_conf_path"]
 
        if os.path.exists(cert.cert_path):
            cert_path = cert.cert_path
            cert_str = cert.id
        else:
            cert_path = __apache__["crt_path_default"]
            cert_str = "System default"
 
        if os.path.exists(cert.key_path):
            key_path = cert.key_path
        else:
            key_path = __apache__["key_path_default"]
 
        if os.path.exists(cert.chain_path):
            ca_crt_path = cert.chain_path
        else:
            ca_crt_path = None
 
        with open(ssl_conf_path, "r") as fp:
            body = fp.read()
 
        v_host = VirtualHost(body)
        v_host.use_certificate(cert_path, key_path, ca_crt_path)
 
        with open(ssl_conf_path, "w") as fp:
            fp.write(v_host.body)
 
        LOG.info("Certificate %s is set to %s" % (cert_str, ssl_conf_path))
 
    def is_system_certificate_used(self):
        with open(__apache__["ssl_conf_path"], "r") as fp:
            body = fp.read()
 
        v_host = VirtualHost(body)
 
        has_certificate = v_host.ssl_cert_path and v_host.ssl_key_path
        system_crt = v_host.ssl_cert_path == __apache__["crt_path_default"]
        system_pkey = v_host.ssl_key_path == __apache__["key_path_default"]
 
        return has_certificate and system_crt and system_pkey
 
    def has_valid_certificate(self):
        with open(__apache__["ssl_conf_path"], "r") as fp:
            body = fp.read()
        v_host = VirtualHost(body)
        return os.path.exists(v_host.ssl_cert_path) and os.path.exists(v_host.ssl_key_path)
 
    def ensure(self):
        raise NotImplementedError
 
 
class DebianBasedModSSL(ModSSL):
 
    def ensure(self, ssl_port=443):
        """
        Enables mod_ssl and default SSL-based virtual host.
        Sets NameVirtualHost and Listen values in this virtual host.
        @param ssl_port: int, port number
        the default SSL-based virtual host will listen to.
        """
        self._enable_mod_ssl()
        self._enable_default_ssl_virtual_host()
        self._set_name_virtual_host(ssl_port)
        # Cleaning ssl.conf after rebundle
        # Replacing unexisting certificate with snakeoil.
        self.set_default_certificate(SSLCertificate())
 
 
    def _enable_mod_ssl(self):
        if not os.path.exists(__apache__["ssl_load_deb"]):
            system2((__apache__["a2enmod_path"], "ssl"))
            LOG.info("mod_ssl enabled.")
 
    def _enable_default_ssl_virtual_host(self):
        if os.path.exists(__apache__["ssl_load_deb"]):
            system2((__apache__["a2ensite_path"], "default-ssl"))
            LOG.info("Default SSL virtualhost enabled.")
 
    def _set_name_virtual_host(self, ssl_port):
        if os.path.exists(__apache__["ports_conf_deb"]):
            with ApacheConfigManager(__apache__["ports_conf_deb"]) as conf:
                i = 0
                for section in conf.get_dict("IfModule"):
                    i += 1
                    if section["value"] in ("mod_ssl.c", "mod_gnutls.c"):
                        conf.set("IfModule[%d]/Listen" % i, str(ssl_port), True)
                        conf.set("IfModule[%d]/NameVirtualHost" % i, "*:%s" % ssl_port, True)
            LOG.info("NameVirtualHost *:%s added to %s" % (ssl_port, __apache__["ports_conf_deb"]))
 
 
class RedHatBasedModSSL(ModSSL):
 
    def ensure(self, ssl_port=443):
        """
        Installs and enables mod_ssl. Then enables default SSL-based virtual host
        by adding module path to the main apache2 config.
        Sets NameVirtualHost and Listen values in this virtual host.
        @param ssl_port: int, port number
        the default SSL-based virtual host will listen to.
        """
        self._install_mod_ssl()
        self._ensure_ssl_conf()
        self._enable_mod_ssl()
        self._set_name_virtual_host(ssl_port)
 
    def _install_mod_ssl(self):
        if not os.path.exists(__apache__["mod_ssl_file"]):
            LOG.info("%s does not exist. Trying to install mod_ssl." % __apache__["mod_ssl_file"])
            pkgmgr.installed("mod_ssl")
 
 
    def _ensure_ssl_conf(self):
        ssl_conf_path = __apache__["ssl_conf_path"]
        if not os.path.exists(ssl_conf_path):
            LOG.warning("SSL config %s doesn`t exist", ssl_conf_path)
            open(ssl_conf_path, "w").close()
            st = os.stat(__apache__["httpd.conf"])
            os.chown(ssl_conf_path, st.st_uid, st.st_gid)
 
    def _enable_mod_ssl(self):
        with ApacheConfigManager(__apache__["httpd.conf"]) as main_config:
            loaded_in_main = [m for m in main_config.get_list("LoadModule") if "mod_ssl.so" in m]
            if not loaded_in_main:
                loaded_in_ssl = False
                if os.path.exists(__apache__["ssl_conf_path"]):
                    with ApacheConfigManager(__apache__["ssl_conf_path"]) as ssl_config:
                        loaded_in_ssl = [m for m in ssl_config.get_list("LoadModule") if "mod_ssl.so" in m]
                if not loaded_in_ssl:
                    main_config.add("LoadModule", "ssl_module modules/mod_ssl.so")
                    LOG.info("Default SSL virtualhost enabled.")
 
    def _set_name_virtual_host(self, ssl_port=443):
        ssl_conf_path = __apache__["ssl_conf_path"]
        with ApacheConfigManager(ssl_conf_path) as ssl_conf:
            if ssl_conf.empty:
                LOG.error("SSL config file %s is empty. Filling in with minimal configuration.", ssl_conf_path)
                ssl_conf.add("Listen", str(ssl_port))
                ssl_conf.add("NameVirtualHost", "*:%s" % ssl_port)
 
            else:
                if not ssl_conf.get_list("NameVirtualHost"):
                    LOG.debug("NameVirtualHost directive not found in %s", ssl_conf_path)
                    if not ssl_conf.get_list("Listen"):
                        LOG.info("Listen directive not found in %s. ", ssl_conf_path)
                        LOG.info("Patching %s with Listen & NameVirtualHost directives.",     ssl_conf_path)
                        ssl_conf.add("Listen", str(ssl_port))
                        ssl_conf.add("NameVirtualHost", "*:%s" % ssl_port)
                    else:
                        ssl_conf.add("NameVirtualHost", "*:%s" % ssl_port, before_path="Listen")
                        LOG.info("NameVirtualHost directive inserted after Listen directive.")
 
 
class ApacheInitScript(initdv2.ParametrizedInitScript):
 
    _apachectl = None
 
    def __init__(self):
 
        pid_file = self._get_pid_file_path()
        initdv2.ParametrizedInitScript.__init__(
            self,
            "apache",
            __apache__["initd_script"],
            pid_file=pid_file,
        )
 
    def _get_pid_file_path(self):
        #TODO: fix assertion when platform becomes an object (commit 58921b6303a96c8975e417fd37d70ddc7be9b0b5)
 
        if "gce" == __node__["platform"]:
            gce_pid_dir = "/var/run/httpd"
            if not os.path.exists(gce_pid_dir):
                os.makedirs(gce_pid_dir)
 
        pid_file = None
        if linux.os.redhat_family:
            pid_file = "/var/run/httpd/httpd.pid" if linux.os["release"].version[0] == 6 else "/var/run/httpd.pid"
        elif linux.os.debian_family:
            if os.path.exists("/etc/apache2/envvars"):
                pid_file = system2("/bin/sh", stdin=". /etc/apache2/envvars; echo -n $APACHE_PID_FILE")[0]
            if not pid_file:
                pid_file = "/var/run/apache2.pid"
        return pid_file
 
    def status(self):
        status = initdv2.ParametrizedInitScript.status(self)
        # If "running" and socks were passed
        if not status and self.socks:
            ip, port = self.socks[0].conn_address
            try:
                expected = "server: apache"
                telnet = Telnet(ip, port)
                telnet.write("HEAD / HTTP/1.0\n\n")
                if expected in telnet.read_until(expected, 5).lower():
                    return initdv2.Status.RUNNING
            except EOFError:
                pass
            return initdv2.Status.NOT_RUNNING
        return status
 
    def configtest(self, path=None):
        args = __apache__["apachectl"] + " configtest"
        if path:
            args += "-f %s" % path
        try:
            out = system2(args, shell=True)[1]
            if "error" in out.lower():
                raise initdv2.InitdError("Invalid Apache configuration: %s" % out)
        except PopenError, e:
            raise InitdError(e)
 
    def start(self):
        if not self._main_process_started() and not self.running:
            LOG.info("Starting apache")
            initdv2.ParametrizedInitScript.start(self)
            if self.pid_file:
                try:
                    wait_until(lambda: os.path.exists(self.pid_file) or self._main_process_started(), sleep=0.2, timeout=30)
                except (Exception, BaseException), e:
                    raise initdv2.InitdError("Cannot start Apache (%s)" % str(e))
            time.sleep(0.5)
 
    def stop(self, reason=None):
        if self._main_process_started() and self.running:
            LOG.info("Stopping apache: %s" % str(reason) if reason else '')
            initdv2.ParametrizedInitScript.stop(self)
 
    def restart(self, reason=None):
        if not self._main_process_started():
            self.start()
        else:
            LOG.info("Restarting apache: %s" % str(reason) if reason else '')
            initdv2.ParametrizedInitScript.restart(self)
        if self.pid_file:
            try:
                wait_until(
                    lambda: os.path.exists(self.pid_file),
                    sleep=0.2,
                    timeout=5,
                    error_text="Apache pid file %s doesn`t exists" % self.pid_file)
            except:
                raise initdv2.InitdError("Cannot start Apache: pid file %s hasn`t been created" % self.pid_file)
        time.sleep(0.5)
 
    def reload(self, reason=None):
        if self.running:
            LOG.info("Reloading apache: %s" % str(reason) if reason else '')
            try:
                out, err, retcode = system2(__apache__["apachectl"] + " graceful", shell=True)
                if retcode > 0:
                    raise initdv2.InitdError("Cannot reload apache: %s" % err)
            except PopenError, e:
                raise InitdError(e)
        else:
            raise InitdError("Service '%s' is not running" % self.name, InitdError.NOT_RUNNING)
 
    @staticmethod
    def _main_process_started():
        res = False
        try:
            out = system2(("ps", "-G", __apache__["group"], "-o", "command", "--no-headers"), raise_exc=False)[0]
            res = __apache__["bin_path"] in out
        except (Exception, BaseException):
            pass
        return res
 
 
initdv2.explore("apache", ApacheInitScript)
 
 
def get_virtual_host_path(hostname, port):
    ext = __apache__["vhost_extension"]
    end = "%s-%s%s" % (hostname, port, ext)
    return os.path.join(__apache__["vhosts_dir"], end)
 
 
def get_updated_file_names(virtual_host_file_names):
    ext = __apache__["vhost_extension"]
 
    plaintext_pattern = re.compile("(.+)\.vhost.conf")
    ssl_pattern = re.compile("(.+)-ssl%s" % ext)
    newstyle_pattern = re.compile("(\d+)%s" % ext)
 
    pairs = {}
    for fname in virtual_host_file_names:
        new_fname = None
 
        if fname.endswith("-ssl%s" % ext):
            res = ssl_pattern.search(fname)
            if res:
                new_fname = res.group(1) + "-443" + ext
 
        elif fname.endswith(ext):
            res = newstyle_pattern.search(fname)
            if res:
                continue
            else:
                res = plaintext_pattern.search(fname)
                new_fname = res.group(1) + "-80" + ext
        else:
            continue
        pairs[fname] = new_fname
    return pairs
 
