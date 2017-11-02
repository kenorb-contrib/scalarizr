import os
import sys
import time

from scalarizr import util

def devicename_to_device(device_name):
    path = '/dev/disk/by-id/google-{0}'.format(device_name)
    if os.path.exists(path):
        rel_path = os.readlink(path)
        abs_path = os.path.abspath(os.path.join(
                os.path.dirname(path), rel_path
        ))
        return abs_path


def get_op_status(conn, proj_id, op_name, zone=None, fields=None):
    fields = fields if isinstance(fields, str) else ', '.join(fields) if fields else None
    kwargs = dict(project=proj_id, operation=op_name, fields=fields)
    if zone:
        kwargs['zone'] = zone
        return conn.zoneOperations().get(**kwargs).execute()
    else:
        return conn.globalOperations().get(**kwargs).execute()


def get_disktype(conn, project_id, zone, disktype):
    return conn.diskTypes().get(project=project_id, 
        zone=zone, diskType=disktype).execute()


def wait_for_operation(connection, project_id, operation_name,
                            zone=None, status_to_wait=("DONE",), timeout=3600):

    def op_reached_status():
        status = get_op_status(connection,
                               project_id,
                               operation_name,
                               zone,
                               ('status', 'error'))
        if status['status'] in status_to_wait:
            error = status.get('error')
            if error:
                err_msg = '\n'.join([err['message'] for err in error['errors']])
                raise Exception(err_msg)
            return True
        return False

    util.wait_until(op_reached_status, timeout=timeout)


def attachment_info(connection, project_id, zone, instance_name, disk_link):
    instance = connection.instances().get(zone=zone,
                                          project=project_id,
                                          instance=instance_name).execute()
    attached = filter(lambda x: x.get('source') == disk_link, instance['disks'])
    if attached:
        return attached[0]


def ensure_disk_detached(connection, project_id, zone, instance_name, disk_link):
    """
    Make sure that disk is detached from specified instance (since there is no way to detach disk
    from any instance)

    Handles: - Disk already detached
             - Instance doesn't exist

    """
    def try_detach():
        try:
            attached = attachment_info(connection, project_id, zone, instance_name, disk_link)
            if not attached:
                return
            device_name = attached['deviceName']
            op = connection.instances().detachDisk(project=project_id,
                                                                                       zone=zone,
                                                                                       instance=instance_name,
                                                                                       deviceName=device_name).execute()
            wait_for_operation(connection, project_id, op['name'], zone)
        except:
            e = str(sys.exc_info()[1])
            if "Invalid value for field 'disk'" in e:
                # Disk already detached
                return
            if "HttpError 404" in e:
                # Instance is gone
                return
            raise

    for _time in range(3):
        try:
            try_detach()
        except:
            if _time == 2:
                raise
            time.sleep(5)


def wait_snapshot_ready(snapshot):
        while True:
            status = snapshot.status()
            if status == snapshot.COMPLETED:
                break
            elif status == snapshot.FAILED:
                raise Exception('Snapshot status is "Failed"')
            time.sleep(5)
