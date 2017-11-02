import logging
import os
import shutil

from scalarizr import linux
from scalarizr import rpc
from scalarizr.api.operation import OperationAPI
from scalarizr.linux import coreutils
from scalarizr.node import __node__
from scalarizr.bus import bus
from scalarizr.config import ScalarizrState
from scalarizr.config import BuiltinPlatforms
from scalarizr.util import Singleton
from scalarizr.util import software
from scalarizr.util import system2

from scalarizr.api.image import ImageAPIDelegate
from scalarizr.api.image import ImageAPIError


LOG = logging.getLogger(__name__)


WALL_MESSAGE = 'Server is going to make image'


class ImageAPI(object):

    __metaclass__ = Singleton

    platform_to_delegate = {
        BuiltinPlatforms.OPENSTACK: ('scalarizr.api.image.openstack', 'OpenStackImageAPIDelegate'),
        BuiltinPlatforms.EC2: ('scalarizr.api.image.ec2', 'EC2ImageAPIDelegate'),
        BuiltinPlatforms.CLOUDSTACK: ('scalarizr.api.image.cloudstack', 'CloudStackImageAPIDelegate'),
        BuiltinPlatforms.IDCF: ('scalarizr.api.image.cloudstack', 'CloudStackImageAPIDelegate'),
        BuiltinPlatforms.GCE: ('scalarizr.api.image.gce', 'GCEImageAPIDelegate'),
    }

    def __init__(self):
        self._op_api = OperationAPI()
        self.delegate = None

    def delegate_for_platform(self, platform_name):
        delegate = self.platform_to_delegate.get(platform_name)
        if not delegate:
            LOG.debug('platform object: %s, class: %s' % (__node__['platform'], type(__node__['platform'])))
            raise ImageAPIError('unknown platform: %s' % __node__['platform'].name)
        module = __import__(delegate[0], globals(), locals(), [delegate[1]], -1)
        return module.__getattribute__(delegate[1])()

    def init_delegate(self):
        if self.delegate:
            return
        self.delegate = self.delegate_for_platform(__node__['platform'].name)

    @rpc.command_method
    def prepare(self, name=None, async=False):
        self.init_delegate()
        if not system2(('which', 'wall'), raise_exc=False)[2]:
            system2(('wall'), stdin=WALL_MESSAGE, raise_exc=False)
        prepare_result = self._op_api.run('api.image.prepare',
            func=self.delegate.prepare,
            async=async,
            exclusive=True,
            func_kwds={'name': name})
        result = {}
        if prepare_result:
            result['prepare_result'] = prepare_result

        result.update(software.system_info())
        return result

    @rpc.command_method
    def snapshot(self, name=None, async=False):
        self.init_delegate()
        cnf = bus.cnf
        saved_state = cnf.state
        try:
            cnf.state = ScalarizrState.REBUNDLING
            return self._op_api.run('api.image.snapshot',
                func=self.delegate.snapshot,
                async=async,
                exclusive=True,
                func_kwds={'name': name})
        finally:
            cnf.state = saved_state

    @rpc.command_method
    def finalize(self, name=None, async=False):
        self.init_delegate()
        self._op_api.run('api.image.finalize',
            func=self.delegate.finalize,
            async=async,
            exclusive=True,
            func_kwds={'name': name})
        LOG.info('Image created. If you imported this server to Scalr, '
                     'you can terminate Scalarizr now.')

    @rpc.command_method
    def create(self, name=None, async=False):
        """ Creates image """
        self.init_delegate()
        return self._op_api.run('api.image.create',
            func=self._create,
            async=async,
            func_kwds={'name': name},
            exclusive=True)

    def _create(self, op, name):
        prepare_result = self.prepare(name)
        image_id = self.snapshot(name)
        finalize_result = self.finalize(name)

        result = {'image_id': image_id}
        if prepare_result:
            result.update(prepare_result)
        if finalize_result:
            result.update(finalize_result)
        
        return result

    def _clean(self, image_rootdir):
        # TODO: revise method, rewrite if needed
        LOG.info('Performing image cleanup')
        LOG.debug('Truncating log files')
        logs_path = os.path.join(image_rootdir, 'var/log')
        if os.path.exists(logs_path):
            for basename in os.listdir(logs_path):
                filename = os.path.join(logs_path, basename)
                if os.path.isfile(filename):
                    try:
                        coreutils.truncate(filename)
                    except OSError, e:
                        LOG.error("Cannot truncate file '%s'. %s", filename, e)
            shutil.rmtree(os.path.join(logs_path, 'scalarizr/scripting'))

        # Cleanup users homes
        LOG.debug('Removing users activity')
        for homedir in ('root', 'home/ubuntu', 'home/scalr'):
            homedir = os.path.join(image_rootdir, homedir)
            self._clean_user_activity(homedir)
            self._clean_ssh_keys(homedir)

        # Cleanup scalarizr private data
        LOG.debug('Removing scalarizr private data')
        etc_path = os.path.join(image_rootdir, bus.etc_path[1:])
        privated = os.path.join(etc_path, "private.d")
        if os.path.exists(privated):
            shutil.rmtree(privated)
            os.mkdir(privated)

        # Sync filesystem buffers
        system2('sync')

        LOG.debug('Cleanup completed')

    def _clean_user_activity(self, homedir):
        for name in (
            ".bash_history",
            ".lesshst",
            ".viminfo",
            ".mysql_history",
            ".history",
            ".sqlite_history"):
            filename = os.path.join(homedir, name)
            if os.path.exists(filename):
                os.remove(filename)


    def _clean_ssh_keys(self, homedir):
        filename = os.path.join(homedir, '.ssh/authorized_keys')
        if os.path.exists(filename):
            LOG.debug('Removing Scalr SSH keys from %s', filename)
            with open(filename + '.tmp', 'w+') as dest:
                with open(filename) as source:
                    lines = [l for l in source if 'SCALR-ROLESBUILDER' not in l]
                    dest.writelines(lines)
            os.rename(filename + '.tmp', filename)
    
