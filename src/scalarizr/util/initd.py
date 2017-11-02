'''
Created on Jun 17, 2010

@author: marat
'''
from scalarizr.util import UtilError, system2
from scalarizr import linux

import time
import os
import socket
import platform


class InitdError(UtilError):
    output = None
    def __init__(self, *args, **kwargs):
        UtilError.__init__(self, *args)
        self.output = kwargs.get("output", "")

    def __str__(self):
        s = "(output: %s)" % self.output
        return UtilError.__str__(self) +  s


_services = dict()

def explore(name, initd_script, pid_file=None, lock_file=None, host=None, tcp_port=None, udp_port=None, so_timeout=5):
    _services[name] = dict(initd_script=initd_script, pid_file=pid_file, lock_file=lock_file,\
                                                    host=host, tcp_port=tcp_port, udp_port=udp_port, so_timeout=so_timeout)

def start(name):
    return _start_stop_reload(name, "start")

def stop(name):
    return _start_stop_reload(name, "stop")

def restart(name):
    return _start_stop_reload(name, "restart")

def reload(name, force=False):
    return _start_stop_reload(name, "force-reload" if force else "reload")

def _start_stop_reload(name, action):
    if not _services.has_key(name):
        raise InitdError("Unknown service '%s'" % (name,))
    try:
        out, err, returncode = system2([_services[name]["initd_script"], action], close_fds=True)
    except OSError, e:
        raise InitdError("Popen failed with error %s" % (e.strerror,))

    if returncode:
        raise InitdError("Cannot %s %s" % (action, name), output=out + " " + err)

    pid_file = _services[name]["pid_file"]

    # 1. on start init.d scripts often returns control right after daemon is forked
    # but pid-file is not touched
    # 2. when doing apache reload

    if action != "stop":
        so_timeout = _services[name]["so_timeout"]
        if _services[name]["tcp_port"]:
            port = _services[name]["tcp_port"]
            ping_service(_services[name]["host"], port, so_timeout)
        elif _services[name]["udp_port"]:
            port = _services[name]["udp_port"]
            ping_service(_services[name]["host"], port, so_timeout, 'udp')


    if pid_file:
        if (action == "start" or action == "restart") and not os.path.exists(pid_file):
            raise InitdError("Cannot start %s. pid file %s doesn't exists" % (name, pid_file))
        if action == "stop" and os.path.exists(pid_file):
            raise InitdError("Cannot stop %s. pid file %s still exists" % (name, pid_file))

    return True

def is_running(name):
    if not _services.has_key(name):
        raise InitdError("Unknown service '%s'" % (name,))
    cmd = [_services[name]["initd_script"], "status"]
    out, err = system2(cmd)[0:2]
    out += err
    if name == "mysql" and linux.os.ubuntu and platform.dist()[1] == '8.04':
        return out.lower().find("Uptime:") != -1
    else:
        return out.lower().find("running") != -1 or out.lower().find("[ ok ]") != -1 or out.lower().find("done.") != -1

def ping_service(host=None, port=None, timeout=None, proto='tcp'):
    if None == timeout:
        timeout = 5
    if None == host:
        host = '127.0.0.1'
    if 'udp' == proto:
        socket_proto = socket.SOCK_DGRAM
    else:
        socket_proto = socket.SOCK_STREAM
    s = socket.socket(socket.AF_INET, socket_proto)
    time_start = time.time()
    while time.time() - time_start < timeout:
        try:
            s.connect((host, port))
            s.shutdown(2)
            return
        except:
            time.sleep(0.1)
            pass
    raise InitdError("Service unavailable after %d seconds of waiting" % timeout)
