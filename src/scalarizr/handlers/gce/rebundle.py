from __future__ import with_statement

__author__ = 'Nick Demyanchuk'

import os
import sys
import glob
import time
import random
import logging
import shutil
import tempfile

from scalarizr import util
from scalarizr.util import software
from scalarizr.linux import pkgmgr, os as os_dist
from scalarizr.bus import bus
from scalarizr.storage2 import largetransfer
from scalarizr.handlers import HandlerError, rebundle as rebundle_hndlr



def get_handlers():
    return [GceRebundleHandler()]


LOG = logging.getLogger(__name__)

ROLEBUILDER_USER = 'scalr-rolesbuilder'


class GceRebundleHandler(rebundle_hndlr.RebundleHandler):
    exclude_dirs = set(['/tmp', '/proc', '/dev',
                        '/mnt', '/var/lib/google/per-instance',
                        '/sys', '/cdrom', '/media', '/run', '/selinux'])
    exclude_files = ('/etc/ssh/.host_key_regenerated',
                     '/lib/udev/rules.d/75-persistent-net-generator.rules')

    if os_dist.debian_family:
        gcimagebundle_pkg_name = 'gce-imagebundle' if os_dist.ubuntu else 'python-gcimagebundle'
    else:
        gcimagebundle_pkg_name = 'gcimagebundle'

    def rebundle(self):
        rebundle_dir = tempfile.mkdtemp()

        try:
            pl = bus.platform
            proj_id = pl.get_numeric_project_id()
            proj_name = pl.get_project_id()
            cloudstorage = pl.get_storage_conn()

            if os.path.exists('/dev/root'):
                root_part_path = os.path.realpath('/dev/root')
            else:
                rootfs_stat = os.stat('/')
                root_device_minor = os.minor(rootfs_stat.st_dev)
                root_device_major = os.major(rootfs_stat.st_dev)
                root_part_path = os.path.realpath('/dev/block/{0}:{1}'.format(root_device_major, root_device_minor))

            root_part_sysblock_path = glob.glob('/sys/block/*/%s' % os.path.basename(root_part_path))[0]
            root_device = '/dev/%s' % os.path.basename(os.path.dirname(root_part_sysblock_path))


            arch_name = '%s.tar.gz' % self._role_name.lower()
            arch_path = os.path.join(rebundle_dir, arch_name)

            # update gcimagebundle
            try:
                pkgmgr.latest(self.gcimagebundle_pkg_name)
            except:
                e = sys.exc_info()[1]
                LOG.warn('Gcimagebundle update failed: %s' % e)

            if os_dist.redhat_family:
                semanage = software.which('semanage')
                if not semanage:
                    pkgmgr.installed('policycoreutils-python')
                    semanage = software.which('semanage')

                util.system2((semanage, 'permissive', '-a', 'rsync_t'))

            gc_img_bundle_bin = software.which('gcimagebundle')

            o, e, p = util.system2((gc_img_bundle_bin,
                                    '-d', root_device,
                                    '-e', ','.join(self.exclude_dirs),
                                    '-o', rebundle_dir,
                                    '--output_file_name', arch_name), raise_exc=False)
            if p:
                raise HandlerError('Gcimagebundle util returned non-zero code %s. Stderr: %s' % (p, e))


            try:
                LOG.info('Uploading compressed image to cloud storage')
                tmp_bucket_name = 'scalr-images-%s-%s' % (random.randint(1, 1000000), int(time.time()))
                remote_dir = 'gcs://%s' % tmp_bucket_name

                def progress_cb(progress):
                    LOG.debug('Uploading {perc}%'.format(perc=progress / os.path.getsize(arch_path)))

                uploader = largetransfer.Upload(arch_path, remote_dir, simple=True,
                                                progress_cb=progress_cb)
                uploader.apply_async()
                try:
                    try:
                        uploader.join()
                    except:
                        if uploader.error:
                            error = uploader.error[1]
                        else:
                            error = sys.exc_info()[1]
                        msg = 'Image upload failed. Error:\n{error}'
                        msg = msg.format(error=error)
                        raise HandlerError(msg)
                except:
                    with util.capture_exception(LOG):
                        objs = cloudstorage.objects()
                        objs.delete(bucket=tmp_bucket_name, object=arch_name).execute()
                    cloudstorage.buckets().delete(bucket=tmp_bucket_name).execute()
            finally:
                os.unlink(arch_path)

        finally:
            shutil.rmtree(rebundle_dir)

        goog_image_name = self._role_name.lower().replace('_', '-') + '-' + str(int(time.time()))
        try:
            LOG.info('Registering new image %s' % goog_image_name)
            compute = pl.get_compute_conn()

            image_url = 'http://storage.googleapis.com/%s/%s' % (tmp_bucket_name, arch_name)

            req_body = dict(
                    name=goog_image_name,
                    sourceType='RAW',
                    rawDisk=dict(
                            source=image_url
                    )
            )

            req = compute.images().insert(project=proj_id, body=req_body)
            operation = req.execute()['name']

            LOG.info('Waiting for image to register')

            def image_is_ready():
                req = compute.globalOperations().get(project=proj_id, operation=operation)
                res = req.execute()
                if res['status'] == 'DONE':
                    if res.get('error'):
                        errors = []
                        for e in res['error']['errors']:
                            err_text = '%s: %s' % (e['code'], e['message'])
                            errors.append(err_text)
                        raise Exception('\n'.join(errors))
                    return True
                return False
            util.wait_until(image_is_ready, logger=LOG, timeout=600)

        finally:
            try:
                objs = cloudstorage.objects()
                objs.delete(bucket=tmp_bucket_name, object=arch_name).execute()
                cloudstorage.buckets().delete(bucket=tmp_bucket_name).execute()
            except:
                e = sys.exc_info()[1]
                LOG.error('Failed to remove image compressed source: %s' % e)

        return '%s/images/%s' % (proj_name, goog_image_name)
