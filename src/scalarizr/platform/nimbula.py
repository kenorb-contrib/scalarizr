from __future__ import with_statement
'''
Created on Feb 14, 2011
 
@author: spike
'''
from scalarizr.libs import nimbula
from scalarizr.util import wait_until
from . import Platform, Ec2LikePlatform, PlatformError
import os
 
def get_platform():
    return NimbulaPlatform()
OPT_USERDATA_TIMEOUT = "wait_user_data_timeout"
 
class NimbulaPlatform(Ec2LikePlatform):
 
    name = "nimbula"
 
    features = []
 
    _userdata_key = 'latest/user-data'
    _metadata_key = 'latest/meta-data'
 
    get_user_data = Platform.get_user_data
 
    '''
    def get_user_data(self, key=None):
            """
            Since userdata is not implemented on Nimbula cloud yet, .user-data file is used.
            """
            if self._userdata is None:
                    path = self._cnf.private_path('.user-data')
                    try:
                            timeout = int(self._cnf.rawini.get(self.name, OPT_USERDATA_TIMEOUT))
                    except:
                            timeout = 600
                    self._logger.info('Waiting for user data.')
                    wait_until(os.path.exists, (path, ),
                                    logger=self._logger, timeout=timeout, sleep=1,
                                    error_text="File %s not exists" % path)
                    rawmeta = None
                    with open(path, 'r') as fp:
                            rawmeta = fp.read()
 
                    if not rawmeta:
                            raise PlatformError("Empty user-data")
                    self._userdata = self._parse_user_data(rawmeta)
            if key:
                    return self._userdata[key] if key in self._userdata else None
            else:
                    return self._userdata
    '''
 
    def set_access_data(self, access_data):
        Ec2LikePlatform.set_access_data(self, access_data)
        os.environ[nimbula.ENV_NIMBULA_PASSWORD]        = self.get_access_data('password')
        os.environ[nimbula.ENV_NIMBULA_URL]             = self.get_access_data('api_url')
        os.environ[nimbula.ENV_NIMBULA_USERNAME]        = self.get_access_data('username')
 
    def clear_access_data(self):
        Ec2LikePlatform.clear_access_data(self)
        try:
            del os.environ[nimbula.ENV_NIMBULA_PASSWORD]
            del os.environ[nimbula.ENV_NIMBULA_URL]
            del os.environ[nimbula.ENV_NIMBULA_USERNAME]
        except KeyError:
            pass
 
    def new_nimbula_connection(self):
        return nimbula.NimbulaConnection()
 
