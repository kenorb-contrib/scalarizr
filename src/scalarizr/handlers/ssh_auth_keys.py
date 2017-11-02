
import re
import os
import sys
import logging

from scalarizr.bus import bus
from scalarizr.handlers import Handler
from scalarizr.messaging import Messages
from scalarizr.util import firstmatched
from scalarizr.util.initdv2 import ParametrizedInitScript
from scalarizr import linux

class UpdateSshAuthorizedKeysError(BaseException):
    pass

def get_handlers ():
    if linux.os['family'] == 'Windows':
        return []
    else:
        return [SSHKeys()]

class SSHKeys(Handler):
    sshd_config_path = '/etc/ssh/sshd_config'
    authorized_keys_file = '/root/.ssh/authorized_keys'

    _logger = None
    _sshd_init = None

    def __init__(self):
        self._logger = logging.getLogger(__name__)

        if linux.os.redhat_family:
            init_script = ('/sbin/service', 'sshd')
        elif linux.os.ubuntu and linux.os['version'] >= (10, 4):
            init_script = ('/usr/sbin/service', 'ssh')
        else:
            init_script = firstmatched(os.path.exists, ('/etc/init.d/ssh', '/etc/init.d/sshd'))
        self._sshd_init = ParametrizedInitScript('sshd', init_script)

        bus.on(init=self.on_init)

    def on_init(self):
        self._setup_sshd_config()

    def _setup_sshd_config(self):
        # Enable public key authentification
        sshd_config = open(self.sshd_config_path)
        lines = sshd_config.readlines()
        sshd_config.close()

        updates = {
            'RSAAuthentication' : 'yes',
            'PubkeyAuthentication' : 'yes',
            'AuthorizedKeysFile' :  '%h/.ssh/authorized_keys'
        }
        if linux.os.amazon:
            updates.update({'PermitRootLogin': 'without-password'})

        updated_keys = set()
        new_lines = []
        for line in lines:
            for key, new_value in updates.items():
                if line.startswith(key):
                    try:
                        old_value = line.split(' ', 1)[1].strip()
                    except IndexError:
                        old_value = None
                    if old_value != new_value:
                        # update
                        self._logger.debug('Updating %s, old/new: %s/%s', key, old_value, new_value)
                        line = '{0} {1}\n'.format(key, new_value)
                    updated_keys.add(key)
            new_lines.append(line)
        # Ensure NL at the end of the file
        if new_lines[-1][-1] != '\n':
            new_lines[-1] = new_lines[-1] + '\n'
        for key, new_value in updates.items():
            if key not in updated_keys:
                # add
                self._logger.debug('Adding %s: %s', key, new_value)
                line = '{0} {1}\n'.format(key, new_value)
                new_lines.append(line)

        if new_lines != lines:
            self._logger.debug('Writing new %s', self.sshd_config_path)
            fp = open(self.sshd_config_path, 'w')
            fp.write(''.join(new_lines))
            fp.close()
            try:
                self._sshd_init.restart()
            except:
                self._logger.debug('Failed to restart sshd: %s', sys.exc_info()[1])


        # Setup .ssh directory structure
        ssh_dir = os.path.dirname(self.authorized_keys_file)
        if not os.path.exists(ssh_dir):
            self._logger.debug('Creating %s', ssh_dir)
            os.makedirs(ssh_dir)
        os.chmod(ssh_dir, 0700)

        if not os.path.exists(self.authorized_keys_file):
            self._logger.debug('Creating empty authrized keys file %s', self.authorized_keys_file)
            open(self.authorized_keys_file, 'w').close()
        os.chmod(self.authorized_keys_file, 0600)

    def on_UpdateSshAuthorizedKeys(self, message):
        if not message.add and not message.remove:
            self._logger.debug('Empty key lists in message. Nothing to do.')
            return

        self._setup_sshd_config()

        ak = self._read_ssh_keys_file()
        if message.add:
            for key in message.add:
                ak = self._add_key(ak, key)
        if message.remove:
            for key in message.remove:
                ak = self._remove_key(ak, key)

        if ak:
            ak = self._check(ak)
            self._write_ssh_keys_file(ak)

    def _read_ssh_keys_file(self):
        self._logger.debug('Reading autorized keys from %s' % self.authorized_keys_file)
        if os.path.exists(self.authorized_keys_file):
            os.chmod(self.authorized_keys_file, 0600)
            with open(self.authorized_keys_file) as fp:
                return fp.read()

    def _write_ssh_keys_file(self, content):
        self._logger.debug('Writing authorized keys')
        try:
            with open(self.authorized_keys_file, 'w') as fp:
                fp.write(content)
        except IOError:
            raise UpdateSshAuthorizedKeysError('Unable to write ssh keys to %s' % self.authorized_keys_file)
        os.chmod(self.authorized_keys_file, 0600)

    def _add_key(self, content, key):
        if not key in content:
            return content + '\n%s\n' % key
        else:
            self._logger.debug('Key already exists in %s' % self.authorized_keys_file)
            return content

    def _remove_key(self, content, key):
        if content:
            return content.replace(key, '')
        else:
            self._logger.debug('No keys found. Keys file %s is probably empty' % self.authorized_keys_file)
            return content

    def _check(self, content):
        while '\n\n' in content:
            content = content.replace('\n\n', '\n')
        if not content.endswith('\n'):
            content += '\n'
        return content

    def accept(self, message, queue, behaviour=None, platform=None, os=None, dist=None):
        return (message.name == Messages.UPDATE_SSH_AUTHORIZED_KEYS)
