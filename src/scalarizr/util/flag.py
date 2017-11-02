import os
import logging

from scalarizr.bus import bus

class Flag(object):
    _cnf = None
    _logger = logging.getLogger(__name__)

    REBOOT = "reboot"
    HALT = "halt"
    HIR = "hir"

    @classmethod
    def set(cls, name):
        flag_path = cls._get_flag_filename(name)
        try:
            cls._logger.debug("Touch file '%s'", flag_path)
            open(flag_path, "w+").close()
        except IOError, e:
            cls._logger.error("Cannot touch file '%s'. IOError: %s", flag_path, str(e))

    @classmethod
    def clear(cls, name):
        if cls.exists(name):
            os.remove(cls._get_flag_filename(name))

    @classmethod
    def exists(cls, name):
        return os.path.exists(cls._get_flag_filename(name))

    @classmethod
    def _get_flag_filename(cls, name):
        return bus.cnf.private_path('.%s' % name)