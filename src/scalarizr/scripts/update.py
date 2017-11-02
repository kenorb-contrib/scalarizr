'''
Created on Jul 20, 2010

@author: marat
'''

import logging
import platform

from scalarizr.app import init_script
from scalarizr.util import system2
from scalarizr import linux


def main():
    init_script()
    logger = logging.getLogger("scalarizr.scripts.update")
    logger.info("Starting update script...")

    if linux.os.debian_family:
        logger.info("Updating scalarizr with Apt")
        system2("apt-get -y install scalarizr", shell=True)
    elif linux.os.redhat_family:
        logger.info("Updating scalarizr with Yum")
        system2("yum -y update scalarizr", shell=True)
    else:
        logger.error("Don't know how to update scalarizr on %s", " ".join(platform.dist()))
