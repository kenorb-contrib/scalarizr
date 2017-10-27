import os
from stat import ST_MODE
from setuptools import setup, findall, find_packages
from distutils import sysconfig, log
from distutils.util import change_root
from distutils.command.install_data import install_data
import platform
 
 
 
class my_install_data(install_data):
    def run(self):
        install_data.run(self)
 
        # Install scripts
        shbang = "#!" + os.path.join(
                sysconfig.get_config_var("BINDIR"),
                "python%s%s" % (sysconfig.get_config_var("VERSION"), sysconfig.get_config_var("EXE"))
        )
 
        d = platform.dist();
        rhel = (int(d[0].lower() in ['centos', 'rhel', 'redhat', 'fedora']) and int(os.environ.get('RHEL_VER', 0) or int(d[1].split('.')[0])))
 
        entries = list(t for t in self.data_files if t[0].startswith("/usr/local"))
        for ent in entries:
            dir = change_root(self.root, ent[0])
            for file in ent[1]:
                path = os.path.join(dir, os.path.basename(file))
 
                # Change #! to current python binary for RHEL4,5
                if rhel >= 4 and rhel <= 5:
                    f = None
                    try:
                        f = open(path, "r")
                        script = f.readline()
                        script = script.replace("#!/usr/bin/python", shbang)
                        script += f.read()
                    finally:
                        f.close()
 
                    try:
                        f = open(path, "w")
                        f.write(script)
                    finally:
                        f.close()
 
                if os.name == "posix":
                    oldmode = os.stat(path)[ST_MODE] & 07777
                    newmode = (oldmode | 0555) & 07777
                    log.info("changing mode of %s from %o to %o", path, oldmode, newmode)
                    os.chmod(path, newmode)
 
 
def make_data_files(dst, src):
    ret = []
    for dir, dirname, files in os.walk(src):
        if dir.find(".svn") == -1:
            ret.append([
                    dir.replace(src, dst),
                    list(os.path.join(dir, f) for f in files)
            ])
    return ret
 
description = "Scalarizr converts any server to Scalr-manageable node"
 
data_files = make_data_files('/etc/scalr', 'etc')
data_files.extend(make_data_files('/usr/share/scalr', 'share'))
data_files.extend(make_data_files('/usr/local/scalarizr/scripts', 'scripts'))
data_files.append(["/usr/local/bin", ["bin/scalarizr", 'bin/szradm']])
 
 
cfg = dict(
        name = "scalarizr",
        version = open('src/scalarizr/version').read().strip(),
        description = description,
        long_description = description,
        author = "Scalr Inc.",
        author_email = "info@scalr.net",
        url = "https://scalr.net",
        license = "GPL",
        platforms = "any",
        package_dir = {"" : "src"},
        packages = find_packages("src"),
        include_package_data = True,
        requires = ["m2crypto (>=0.20)", "boto"],
        data_files = data_files,
        cmdclass={"install_data" : my_install_data}
)
setup(**cfg)
 
