import os
from fabric.api import *
 
env.user = 'root'
 
def _target(hostname=None, keyname=None):
	if hostname and keyname:
		env.host_string = hostname
		for place in ('~/keys', '~/Workspace/login', "~/Downloads"):
			keyfile = os.path.expanduser(place + '/' + keyname)
			if os.path.exists(keyfile):
				env.key_filename = keyfile
				break
 
 
def upload(hostname=None, keyname=None):
	# fab upload:hostname=23.20.29.154,keyname=vova-percona-centos6-devel.pem
	_target(hostname, keyname)
	local('rm -f /tmp/scalarizr-0.9.tar.gz')
	local('tar -czf /tmp/scalarizr-0.9.tar.gz .')
	run('rm -rf /root/scalarizr')
	run('mkdir -p /root/scalarizr')
	put('/tmp/scalarizr-0.9.tar.gz', '/root/scalarizr')
	run('tar -xzf /root/scalarizr/scalarizr-0.9.tar.gz -C /root/scalarizr')
	setup_tests_deps()
 
 
def setup_tests_deps(hostname=None, keyname=None):
	_target(hostname, keyname)
	with settings(warn_only=True):
		if run('cat /etc/*-release | head -1 | grep -q Ubuntu').succeeded:
			run('which easy_install || apt-get install python-setuptools')
			run("which pip || apt-get install python-pip")
			run("which git || apt-get install -y git")
			run("which lettuce || pip install git+https://github.com/Scalr/lettuce.git")
		else:
			run('which easy_install || yum install python-setuptools')
			run("which pip-python || yum install python-pip")
			run("which git || yum install git")
			run("which lettuce || pip-python install git+https://github.com/Scalr/lettuce.git")
	run('easy_install mock nose')
 
