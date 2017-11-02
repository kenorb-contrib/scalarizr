from __future__ import with_statement
'''
Created on Mar 3, 2010

@author: marat
@author: Dmytro Korsakov
'''

from __future__ import with_statement

from scalarizr import config
from scalarizr.bus import bus
from scalarizr.handlers import Handler
import logging
import os
import subprocess
import re

def get_handlers ():
    return [HooksHandler()]

class HooksHandler(Handler):
    name = "hooks"
    _logger = None
    _hooks_path = None

    def __init__(self):
        self._logger = logging.getLogger(__name__)
        bus.on(init=self.on_init, reload=self.on_reload)
        self.on_reload()

    def on_init(self):
        for event in bus.list_events():
            bus.on(event, self.create_hook(event))

    def on_reload(self):
        cnf = bus.cnf
        self._hooks_path = cnf.rawini.get(self.name, 'hooks_path')

    def create_hook(self, event):
        def hook(*args, **kwargs):
            self._logger.debug("Hook on '"+event+"'" + str(args) + " " + str(kwargs))

            cnf = bus.cnf; ini = cnf.rawini
            environ = kwargs
            environ["server_id"] = ini.get(config.SECT_GENERAL, config.OPT_SERVER_ID)
            environ["behaviour"] = ini.get(config.SECT_GENERAL, config.OPT_BEHAVIOUR)

            if os.path.isdir(self._hooks_path):
                reg = re.compile(r"^\d+\-"+event+"$")
                matches_list = list(fname for fname in os.listdir(self._hooks_path) if reg.search(fname))
                if matches_list:
                    matches_list.sort()
                    for fname in matches_list:
                        hook_file = os.path.join(self._hooks_path, fname)
                        if os.access(hook_file, os.X_OK):
                            start_command = [hook_file]
                            start_command += args
                            try:
                                p = subprocess.Popen(
                                         start_command,
                                         stdin=subprocess.PIPE,
                                         stdout=subprocess.PIPE,
                                         stderr=subprocess.PIPE,
                                         env=environ)
                                stdout, stderr = p.communicate()

                                is_start_failed = p.poll()

                                if is_start_failed:
                                    self._logger.error("stderr: %s", stderr)

                                self._logger.debug("stdout: %s", stdout)
                            except OSError, e:
                                self._logger.error("Error in script '%s'. %s", fname, str(e.strerror))
        return hook
