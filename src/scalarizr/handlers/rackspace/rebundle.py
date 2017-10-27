from __future__ import with_statement
'''
Created on Nov 22, 2010
 
@author: spike
'''
 
from scalarizr.bus import bus
from scalarizr.handlers import HandlerError
from scalarizr.handlers import rebundle as rebundle_hdlr
from scalarizr.util import wait_until, system2
from scalarizr.config import ScalarizrState
 
import time
import sys
 
from cloudservers.exceptions import CloudServersException, NotFound
 
LOG = rebundle_hdlr.LOG
 
def get_handlers ():
    return [RackspaceRebundleHandler()]
 
 
class RackspaceRebundleHandler(rebundle_hdlr.RebundleHandler):
 
    def rebundle(self):
        pl = bus.platform
        conn = pl.new_cloudservers_conn()
 
 
        LOG.debug('Lookup server %s on CloudServers', pl.get_private_ip())
        try:
            server = conn.servers.find(private_ip=pl.get_private_ip())
            LOG.debug('Found server %s. server id: %s', pl.get_private_ip(), server.id)
        except NotFound:
            raise HandlerError('Server %s not found in servers list' % pl.get_private_ip())
 
 
        duplicate_errmsg = 'Another image is currently creating from this server. ' \
                                        'Image: id=%s name=%s status=%s\n' \
                                        'Rackspace allows to create only ONE image per server at a time. ' \
                                        'Try again later.'
 
        images = [image for image in conn.images.findall(serverId=server.id)
                                if image.status not in ('ACTIVE', 'FAILED')]
        if images:
            image = images[0]
            raise HandlerError(duplicate_errmsg % (image.id, image.name, image.status))
 
        image = None
        image_name = self._role_name + "-" + time.strftime("%Y%m%d%H%M%S")
        retry_seconds = 30
        try:
            system2("sync", shell=True)
            LOG.info("Creating server image: serverId=%s name=%s", server.id, image_name)
            for _ in range(0, 2):
                try:
                    image = conn.images.create(image_name, server.id)
                    LOG.debug('Image created: id=%s', image.id)
                    break
                except CloudServersException, e:
                    if 'Unhandled exception occurred during processing' in str(e):
                        continue
                    if 'Cannot create a new backup request while saving a prior backup or migrating' in str(e):
                        LOG.warning('Rackspace answered with "%s"', str(e))
                        for _ in range(0, 10):
                            LOG.info('Lookup image %s', image_name)
                            try:
                                image = conn.images.find(serverId=server.id)
                                break
                            except NotFound:
                                LOG.info('Image not found. Sleeping %s seconds and retry', retry_seconds)
                                time.sleep(retry_seconds)
                        else:
                            raise HandlerError('Image not found. Retry limits exceed')
                        if image.name != image_name:
                            raise HandlerError(duplicate_errmsg % (image.id, image.name, image.status))
                    else:
                        raise
 
 
            LOG.info('Checking that image %s is completed', image.id)
 
            start_time = time.time()
            def completed(image_id):
                try:
                    image = conn.images.get(image_id)
                    if time.time() - start_time > 191:
                        globals()['start_time'] = time.time()
                        LOG.info('Progress: %s', image.progress)
                    return image.status in ('ACTIVE', 'FAILED')
                except:
                    LOG.debug('Caught exception', exc_info=sys.exc_info())
 
            wait_until(completed, args=(image.id, ), sleep=30, logger=LOG, timeout=3600,
                            error_text="Image %s wasn't completed in a reasonable time" % image.id)
            image = conn.images.get(image.id)
            if image.status == 'FAILED':
                raise HandlerError('Image %s becomes failed' % image.id)
            LOG.info('Image %s completed and available for use!', image.id)
        except:
            exc_type, exc_value, exc_trace = sys.exc_info()
            if image:
                try:
                    conn.images.delete(image)
                except:
                    LOG.debug('Image delete exception', exc_info=sys.exc_info())
            raise exc_type, exc_value, exc_trace
 
        return image.id
 
