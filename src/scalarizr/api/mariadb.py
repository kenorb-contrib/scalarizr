from __future__ import with_statement

import sys
import logging
from scalarizr import linux
from scalarizr.api import mysql
from scalarizr.linux import pkgmgr
from scalarizr.util import Singleton
from scalarizr import exceptions
from scalarizr.api import SoftwareDependencyError


LOG = logging.getLogger(__name__)


class MariaDBAPI(mysql.MySQLAPI):

    __metaclass__ = Singleton

    behavior = 'mariadb'

    def __init__(self):
        super(MariaDBAPI, self).__init__()

    @classmethod
    def do_check_software(cls, system_packages=None):
        requirements = None
        if linux.os.debian_family:
            requirements = [
                ['mariadb-server>=5.5,<5.6', 'mariadb-client>=5.5,<5.6'],
                ['mariadb-server>=5.5,<5.6', 'mariadb-client-5.5'],
            ]
        elif linux.os.redhat_family or linux.os.oracle_family:
            requirements = [
                ['mariadb-server>=5.5,<5.6'],
                ['MariaDB-server>=5.5,<5.6', 'MariaDB-client>=5.5,<5.6'],
            ]
        if requirements is None:
            raise exceptions.UnsupportedBehavior(
                    cls.behavior,
                    "Not supported on {0} os family".format(linux.os['family']))
        errors = list()
        for requirement in requirements:
            try:
                installed = pkgmgr.check_software(requirement[0], system_packages)[0]
                try:
                    pkgmgr.check_software(requirement[1:], system_packages)
                    return installed
                except pkgmgr.NotInstalledError:
                    e = sys.exc_info()[1]
                    raise SoftwareDependencyError(e.args[0])
            except:
                e = sys.exc_info()[1]
                errors.append(e)
        for cls in [pkgmgr.VersionMismatchError, SoftwareDependencyError, pkgmgr.NotInstalledError]:
            for error in errors:
                if isinstance(error, cls):
                    raise error

