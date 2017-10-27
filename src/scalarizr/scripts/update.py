from __future__ import with_statement
'''
Created on Jul 20, 2010
 
@author: marat
'''
 
from scalarizr.app import init_script
from scalarizr.util import disttool, system2
import logging
 
def main():
    init_script()
    logger = logging.getLogger("scalarizr.scripts.update")
    logger.info("Starting update script...")
 
    if disttool.is_debian_based():
        logger.info("Updating scalarizr with Apt")
        system2("apt-get -y install scalarizr", shell=True)
    elif disttool.is_redhat_based():
        logger.info("Updating scalarizr with Yum")
        system2("yum -y update scalarizr", shell=True)
    else:
        logger.error("Don't know how to update scalarizr on %s", " ".join(disttool.linux_dist()))
 
