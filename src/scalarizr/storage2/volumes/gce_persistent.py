__author__ = 'Nick Demyanchuk'

import os
import re
import time
import sys
import uuid
import logging
import datetime

from apiclient.errors import HttpError

from scalarizr import storage2
from scalarizr.bus import bus
from scalarizr.node import __node__
from scalarizr.storage2.volumes import base
from scalarizr.storage2.util import gce as gce_util

LOG = logging.getLogger(__name__)
compute_api_version = bus.platform.compute_api_version

STORAGE_TYPE = 'gce_persistent'

def to_current_api_version(link):
    if link:
        return re.sub('compute/[^/]+/projects', 'compute/%s/projects' % compute_api_version, link, 1)


class GcePersistentVolume(base.Volume):
    '''
    def _get_device_name(self):
            return 'google-%s' % (self.alias or self.name)
    '''


    def __init__(self, name=None, link=None, size=None, zone=None,
                 last_attached_to=None, disk_type="pd-standard", **kwargs):
        name = name or 'scalr-disk-%s' % uuid.uuid4().hex[:8]
        super(GcePersistentVolume, self).__init__(name=name, link=link,
                                                  size=size, zone=zone,
                                                  last_attached_to=last_attached_to,
                                                  disk_type=disk_type,
                                                  **kwargs)
        self.features.update({'grow': True})


    def _clone(self, config):
        config.pop('name')
        config.pop('link')


    def _ensure(self):

        garbage_can = []
        zone = os.path.basename(__node__['gce']['zone'])
        project_id = __node__['gce']['project_id']
        server_name = __node__['server_id']

        try:
            connection = __node__['gce'].connect_compute()
        except:
            e = sys.exc_info()[1]
            LOG.debug('Can not get GCE connection: %s' % e)
            """ No connection, implicit check """
            try:
                self._check_attr('name')
            except:
                raise storage2.StorageError('Disk is not created yet, and GCE connection is unavailable')
            device = gce_util.devicename_to_device(self.name)
            if not device:
                raise storage2.StorageError("Disk is not attached and GCE connection is unavailable")

            self.device = device
        else:
            LOG.debug('Successfully created connection to cloud engine')
            try:
                create = False
                if not self.link:
                    # Disk does not exist, create it first
                    create_request_body = dict(name=self.name)
                    if self.snap:
                        snap_dict = dict(self.snap)
                        snap_dict['type'] = STORAGE_TYPE
                        self.snap = storage2.snapshot(snap_dict)
                        LOG.debug('Ensuring that snapshot is ready, before creating disk from it')
                        gce_util.wait_snapshot_ready(self.snap)
                        create_request_body['sourceSnapshot'] = to_current_api_version(self.snap.link)
                    else:
                        create_request_body['sizeGb'] = self.size

                    create = True
                else:
                    self._check_attr('zone')
                    LOG.debug('Checking that disk already exists')
                    try:
                        disk_dict = connection.disks().get(disk=self.name, project=project_id,
                                                                            zone=zone).execute()
                        self.link = disk_dict['selfLink']
                    except HttpError, e:
                        code = int(e.resp['status'])
                        if code == 404:
                            raise storage2.VolumeNotExistsError(self.name)
                        else:
                            raise

                    if self.zone != zone:
                        # Volume is in different zone, snapshot it,
                        # create new volume from this snapshot, then attach
                        temp_snap = self.snapshot('volume')
                        garbage_can.append(temp_snap)
                        new_name = self.name + zone
                        create_request_body = dict(
                            name=new_name, sourceSnapshot=to_current_api_version(temp_snap.link))
                        create = True

                attach = False
                if create:
                    disk_name = create_request_body['name']
                    if "pd-standard" != self.disk_type:
                        disk_type = gce_util.get_disktype(conn=connection,
                            project_id=project_id, zone=zone, disktype=self.disk_type)
                        create_request_body.update({'type': disk_type['selfLink']})

                    LOG.debug('Creating new GCE disk %s' % disk_name)
                    op = connection.disks().insert(project=project_id,
                                                   zone=zone,
                                                   body=create_request_body).execute()
                    gce_util.wait_for_operation(connection, project_id, op['name'], zone)
                    disk_dict = connection.disks().get(disk=disk_name,
                                                       project=project_id,
                                                       zone=zone).execute()
                    self.id = disk_dict['id']
                    self.link = disk_dict['selfLink']
                    self.zone = zone
                    self.name = disk_name
                    attach = True

                else:
                    if self.last_attached_to and self.last_attached_to != server_name:
                        LOG.debug("Making sure that disk %s detached from previous attachment place." % self.name)
                        try:
                            gce_util.ensure_disk_detached(connection,
                                                          project_id,
                                                          zone,
                                                          self.last_attached_to,
                                                          self.link)
                        except:
                            e = sys.exc_info()[1]
                            if 'resource was not found' in str(e):
                                raise storage2.VolumeNotExistsError(self.link)
                            raise
                        
                    attachment_inf = self._attachment_info(connection)
                    if attachment_inf:
                        disk_devicename = attachment_inf['deviceName']
                    else:
                        attach = True

                if attach:
                    LOG.debug('Attaching disk %s to current instance' % self.name)
                    try:
                        op = connection.instances().attachDisk(instance=server_name, project=project_id,
                                            zone=zone, body=dict(deviceName=self.name,
                                                                    source=self.link,
                                                                    mode="READ_WRITE",
                                                                    type="PERSISTENT")).execute()
                    except:
                        e = sys.exc_info()[1]
                        if 'resource was not found' in str(e):
                            raise storage2.VolumeNotExistsError(self.link)
                        raise

                    gce_util.wait_for_operation(connection, project_id, op['name'], zone=zone)
                    disk_devicename = self.name

                for i in range(10):
                    device = gce_util.devicename_to_device(disk_devicename)
                    if device:
                        break
                    LOG.debug('Device not found in system. Retrying in 1s.')
                    time.sleep(1)
                else:
                    raise storage2.StorageError("Disk should be attached, but corresponding device not found in system")

                self.device = device
                self.last_attached_to = server_name
                self.snap = None

            finally:
                # Perform cleanup
                for garbage in garbage_can:
                    try:
                        garbage.destroy(force=True)
                    except:
                        e = sys.exc_info()[1]
                        LOG.debug('Failed to destroy temporary storage object %s: %s', garbage, e)


    def _attachment_info(self, con):
        self.link = to_current_api_version(self.link)
        zone = os.path.basename(__node__['gce']['zone'])
        project_id = __node__['gce']['project_id']
        server_name = __node__['server_id']

        return gce_util.attachment_info(con, project_id, zone, server_name, self.link)


    def _detach(self, force, **kwds):
        connection = __node__['gce'].connect_compute()
        attachment_inf = self._attachment_info(connection)
        if attachment_inf:
            zone = os.path.basename(__node__['gce']['zone'])
            project_id = __node__['gce']['project_id']
            server_name = __node__['server_id']

            def try_detach():
                op = connection.instances().detachDisk(instance=server_name, project=project_id, zone=zone,
                                                            deviceName=attachment_inf['deviceName']).execute()

                gce_util.wait_for_operation(connection, project_id, op['name'], zone=zone)

            for _time in range(3):
                try:
                    try_detach()
                    return
                except:
                    e = sys.exc_info()[1]
                    LOG.debug('Detach disk attempt failed: %s' % e)
                    if _time == 2:
                        raise storage2.StorageError('Can not detach disk: %s' % e)
                    time.sleep(1)
                    LOG.debug('Trying to detach disk again.')


    def _grow(self, new_vol, **growth_cfg):
        size = int(growth_cfg.get('size'))
        snap = self.snapshot('Temporary snapshot for volume growth', {'temp': 1}, nowait=False)
        try:
            new_vol.snap = snap
            new_vol.size = size
            new_vol.ensure()
        finally:
            try:
                snap.destroy()
            except:
                e = sys.exc_info()[1]
                LOG.error('Temporary snapshot desctruction failed: %s' % e)



    def _destroy(self, force, **kwds):
        self._check_attr('link')
        self._check_attr('name')

        connection = __node__['gce'].connect_compute()
        project_id = __node__['gce']['project_id']
        zone = os.path.basename(__node__['gce']['zone'])
        try:
            op = connection.disks().delete(project=project_id, zone=zone, disk=self.name).execute()
            gce_util.wait_for_operation(connection, project_id, op['name'], zone=zone)
        except:
            e = sys.exc_info()[1]
            raise storage2.StorageError("Disk destruction failed: %s" % e)

    @property
    def resource(self):
        connection = __node__['gce'].connect_compute()
        project_id = __node__['gce']['project_id']

        try:
            return connection.disks().get(disk=self.name, project=project_id, zone=self.zone).execute()
        except HttpError, e:
            code = int(e.resp['status'])
            if code == 404:
                raise storage2.VolumeNotExistsError('Volume %s not found in %s zone' % (self.name, self.zone))
            else:
                raise


    def _snapshot(self, description, tags, **kwds):
        """
        :param nowait: if True - do not wait for snapshot to complete, just create and return
        """
        self._check_attr('name')
        connection = __node__['gce'].connect_compute()
        project_id = __node__['gce']['project_id']
        nowait = kwds.get('nowait', True)

        now_raw = datetime.datetime.utcnow()
        now_str = now_raw.strftime('%d-%b-%Y-%H-%M-%S-%f')
        snap_name = ('%s-snap-%s' % (self.name, now_str)).lower()[:62]

        # We could put it to separate method, like _get_self_resource

        operation = connection.disks().createSnapshot(disk=self.name, project=project_id, zone=self.zone,
                                                    body=dict(name=snap_name, description=description)).execute()
        #operation = connection.snapshots().insert(project=project_id, body=dict(name=snap_name,
        #                           kind="compute#snapshot", description=description, sourceDisk=self.link)).execute()
        try:
            # Wait until operation at least started
            gce_util.wait_for_operation(connection, project_id, operation['name'], self.zone,
                                                            status_to_wait=("DONE", "RUNNING"))
            # If nowait=false, wait until operation is totally complete
            snapshot_info = connection.snapshots().get(project=project_id, snapshot=snap_name,
                                                    fields='id,name,diskSizeGb,selfLink').execute()
            snapshot = GcePersistentSnapshot(id=snapshot_info['id'], name=snapshot_info['name'],
                                             size=snapshot_info['diskSizeGb'], link=snapshot_info['selfLink'],
                                             type=STORAGE_TYPE)
            if not nowait:
                gce_util.wait_snapshot_ready(snapshot)
            return snapshot
        except:
            e = sys.exc_info()[1]
            raise storage2.StorageError('Google disk snapshot creation '
                            'failed. Error: %s' % e)




class GcePersistentSnapshot(base.Snapshot):

    type = STORAGE_TYPE

    def __init__(self, name, **kwds):
        super(GcePersistentSnapshot, self).__init__(name=name, **kwds)
        self._status_map = dict(CREATING=self.IN_PROGRESS, UPLOADING=self.IN_PROGRESS, READY=self.COMPLETED,
                                ERROR=self.FAILED, FAILED=self.FAILED)


    def _destroy(self):
        try:
            connection = __node__['gce'].connect_compute()
            project_id = __node__['gce']['project_id']
            op = connection.snapshots().delete(project=project_id, snapshot=self.name).execute()
            gce_util.wait_for_operation(connection, project_id, op['name'])
        except:
            e = sys.exc_info()[1]
            raise storage2.StorageError('Failed to delete google disk snapshot. Error: %s' % e)

    def _status(self):
        self._check_attr("name")
        connection = __node__['gce'].connect_compute()
        project_id = __node__['gce']['project_id']
        snapshot = connection.snapshots().get(project=project_id, snapshot=self.name, fields='status').execute()
        status = snapshot['status']

        return self._status_map.get(status, self.UNKNOWN)


storage2.volume_types[STORAGE_TYPE] = GcePersistentVolume
storage2.snapshot_types[STORAGE_TYPE] = GcePersistentSnapshot
