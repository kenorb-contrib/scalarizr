
import os
import sys
import time
import shutil


from scalarizr.node import __node__
from scalarizr import linux, handlers
from scalarizr.linux import coreutils
from scalarizr.handlers import rebundle as rebundle_hdlr
from scalarizr.util import software, system2, wait_until
from scalarizr.messaging import Messages


LOG = rebundle_hdlr.LOG


def get_handlers():
    if linux.os.windows_family:
        return [OpenstackRebundleWindowsHandler()]
    else:
        return [OpenstackRebundleLinuxHandler()]


class OpenstackRebundleWindowsHandler(handlers.Handler):
    logger = None

    def accept(self, message, queue, **kwds):
        return message.name == Messages.WIN_PREPARE_BUNDLE

    def on_Win_PrepareBundle(self, message):
        try:
            # XXX: server is terminated during sysprep.
            # we should better understand how it works
            #shutil.copy(r'C:\Windows\System32\sysprep\RunSysprep_2.cmd', r'C:\windows\system32\sysprep\RunSysprep.cmd')
            #shutil.copy(r'C:\Windows\System32\sysprep\SetupComplete_2.cmd', r'C:\windows\setup\scripts\SetupComplete.cmd')
            #linux.system((r'C:\windows\system32\sysprep\RunSysprep.cmd', ))

            result = dict(
                status = "ok",
                bundle_task_id = message.bundle_task_id              
            )
            result.update(software.system_info())
            self.send_message(Messages.WIN_PREPARE_BUNDLE_RESULT, result)

        except:
            e = sys.exc_info()[1]
            LOG.exception(e)
            last_error = hasattr(e, "error_message") and e.error_message or str(e)
            self.send_message(Messages.WIN_PREPARE_BUNDLE_RESULT, dict(
                status = "error",
                last_error = last_error,
                bundle_task_id = message.bundle_task_id
            ))


class OpenstackRebundleLinuxHandler(rebundle_hdlr.RebundleHandler):

    def rebundle(self):
        image_name = self._role_name + "-" + time.strftime("%Y%m%d%H%M%S")
        nova = __node__['openstack'].connect_nova()

        server_id = __node__['openstack']['server_id']
        system2("sync", shell=True)
        LOG.info('Creating server image (server_id: %s)', server_id)
        image_id = nova.servers.create_image(server_id, image_name)
        LOG.info('Server image %s created', image_id)

        result = [None]
        def image_finished():
            try:
                result[0] = nova.images.get(image_id)
                status = result[0].status
                LOG.debug('Image status: %s', status)
                return status and status.lower() not in ('queued', 'saving')
            except:
                e = sys.exc_info()[1]
                if 'Unhandled exception occurred during processing' in str(e):
                    return
                raise
        def delete_image():
            if not result[0]:
                return
            image_id = result[0].id
            try:
                LOG.info('Cleanup: deleting image %s', image_id)
                nova.images.delete(image_id)
            except:
                LOG.warn('Cleanup failed: %s', exc_info=sys.exc_info())

        try:
            # 2h timeout
            wait_until(image_finished, start_text='Polling image status', sleep=30, timeout=7200)
            image = result[0]
            if image.status != 'ACTIVE':
                raise handlers.HandlerError('Image %s becomes %s', image.id, image.status)
            LOG.info('Image %s completed and available for use!', image.id)
            return image.id
        except:
            exc = sys.exc_info()
            delete_image()
            raise exc[0], exc[1], exc[2]


    def before_rebundle(self):
        rulename = '70-persistent-net.rules'
        coreutils.remove('/tmp/' + rulename)
        if os.path.exists('/etc/udev/rules.d/' + rulename):
            shutil.move('/etc/udev/rules.d/' + rulename, '/tmp')


    def after_rebundle(self):
        rulename = '70-persistent-net.rules'
        if os.path.exists('/tmp/' + rulename):
            shutil.move('/tmp/' + rulename, '/etc/udev/rules.d')
