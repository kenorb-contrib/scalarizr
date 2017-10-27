from __future__ import with_statement
'''
Created on Oct 12, 2010
 
@author: marat
'''
 
from scalarizr.bus import bus
from scalarizr.handlers import rebundle as rebundle_hdlr
from scalarizr.handlers.ec2 import rebundle as ec2_rebundle_hdlr
from scalarizr.handlers import HandlerError
from scalarizr import linux
from scalarizr.linux import coreutils
 
import os
import glob
import subprocess
 
 
LOG = ec2_rebundle_hdlr.LOG
 
def get_handlers ():
    return [EucaRebundleHandler()]
 
 
class EucaRebundleStrategy(ec2_rebundle_hdlr.RebundleInstanceStoreStrategy):
    def run(self):
        if not linux.which('euca-bundle-vol'):
            raise HandlerError('euca-bundle-vol command not found, please install "euca2ools" package')
 
        cert_path = pk_path = cloud_cert_path = fstab_path = None
        try:
            cert, pk = self._platform.get_cert_pk()
            cert_path = bus.cnf.write_key('euca-cert.pem', cert)
            pk_path = bus.cnf.write_key('euca-pk.pem', pk)
            cloud_cert_path = bus.cnf.write_key('euca-cloud-cert.pem', self._platform.get_ec2_cert())
            access_key, secret_key = self._platform.get_access_keys()
 
            environ = os.environ.copy()
            environ.update({
                'EUCALYPTUS_CERT': cloud_cert_path,
                'EC2_CERT': cert_path,
                'EC2_PRIVATE_KEY': pk_path,
                'EC2_USER_ID': self._platform.get_account_id(),
                'EC2_ACCESS_KEY': access_key,
                'AWS_ACCESS_KEY': access_key,
                'EC2_SECRET_KEY': secret_key,
                'AWS_SECRET_KEY': secret_key,
                'EC2_URL': self._platform.get_access_data('ec2_url'),
                'S3_URL': self._platform.get_access_data('s3_url')
            })
            # LOG.info('environ: %s', environ)
            # LOG.info('============')
            # LOG.info('EC2_PRIVATE_KEY: %s', open(pk_path).read())
            # LOG.info('============')
            # LOG.info('EC2_CERT: %s', open(cert_path).read())
            # LOG.info('============')
            # LOG.info('EUCALYPTUS_CERT: %s', open(cloud_cert_path).read())
            # LOG.info('============')
 
            # with open('/etc/fstab') as fp:
            #     fstab_path = bus.cnf.write_key('euca-fstab', fp.read())
            # # disable_root_fsck=False - cause current fstab wrapper adds updated entry 
            # # to the end of the file, and this breaks CentOS boot 
            # # because 'mount -a' process fstab sequentically
            # self._fix_fstab(
            #     filename=fstab_path, 
            #     disable_root_fsck=False)
 
            coreutils.touch('/.autorelabel')
            coreutils.touch('/.autofsck')
 
            # Create image object for gathering directories exclude list
            # image = rebundle_hdlr.LinuxImage('/', 
            #             os.path.join(self._destination, self._image_name), 
            #             self._excludes)
 
            excludes = [
                self._destination,
                '/selinux/*',
                '/var/lib/dhclient',
                '/var/lib/dhcp',
                '/var/lib/dhcp3'
            ]
 
            LOG.info('Bundling image')
            cmd = (
                linux.which('euca-bundle-vol'), 
                '--arch', linux.os['arch'],
                '--size', str(self._image_size),
                '--destination', self._destination,
                '--exclude', ','.join(excludes),
                #'--fstab', fstab_path,
                '--prefix', self._image_name,
                '--volume', '/',
                '--debug'
            )
            LOG.info(' '.join(cmd))
            LOG.info(linux.system(cmd, env=environ, stdout=subprocess.PIPE, 
                    stderr=subprocess.STDOUT)[0])
 
            LOG.info('Uploading image (with euca-upload-bundle)')
            #files_prefix = os.path.join(self._destination, self._image_name)
            #files = glob.glob(files_prefix + '*')
            #s3_manifest_path = self._upload_image_files(files, files_prefix + '.manifest.xml')
            manifest = os.path.join(self._destination, self._image_name) + '.manifest.xml'
            bucket = os.path.basename(self._platform.scalrfs.root())
            cmd = (
                linux.which('euca-upload-bundle'),
                '--bucket', bucket,
                '--manifest', manifest            
            )
            LOG.info(' '.join(cmd))
            LOG.info(linux.system(cmd, env=environ)[0])
 
            # LOG.info('Registering image (with euca-register)')
            # cmd = (
            #     linux.which('euca-register'),
            #     '--name', self._image_name,
            #     '{0}/{1}'.format(bucket, os.path.basename(manifest))
            # )
            # LOG.info(' '.join(cmd))
            # LOG.info(linux.system(cmd, env=environ.copy())[0])
 
            LOG.info('Registering image')
            s3_manifest_path = '{0}/{1}'.format(bucket, os.path.basename(manifest))
            return self._register_image(s3_manifest_path)
 
        finally:
            linux.system('chmod 755 {0}/keys/euca-*'.format(bus.cnf.private_path()), shell=True)
            linux.system('rm -f {0}/keys/euca-*'.format(bus.cnf.private_path()), shell=True)
            linux.system('rm -f {0}/{1}.*'.format(self._destination, self._image_name), shell=True)
 
 
class EucaRebundleHandler(ec2_rebundle_hdlr.Ec2RebundleHandler):
    def __init__(self):
        ec2_rebundle_hdlr.Ec2RebundleHandler.__init__(self, instance_store_strategy_cls=EucaRebundleStrategy)
 
    @property
    def _s3_bucket_name(self):
        pl = bus.platform
        return 'scalr2-images-%s' % pl.get_account_id()
 
