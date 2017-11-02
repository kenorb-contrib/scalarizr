import logging
import time
import _winreg as winreg


LOG = logging.getLogger(__name__)


class RebootExpected(Exception): pass


def wait_sysprep_completion():
    def read_hklm_key(sub_key):
        try:
            return winreg.OpenKey(winreg.HKEY_LOCAL_MACHINE, sub_key, 0, winreg.KEY_READ)
        except WindowsError as e:
            if e.winerror == 2:
                LOG.debug('{} not found in registry'.format(sub_key))
            else:
                raise

    def log_unattend_passes():
        LOG.debug('UnattendPasses:')
        key = read_hklm_key(r'System\Setup\Status\UnattendPasses')
        if not key:
            return
        for i in xrange(winreg.QueryInfoKey(key)[1]):
            LOG.debug('  %s', winreg.EnumValue(key, i))

    def pending_reboot():
        auto_update_reboot = r'Software\Microsoft\Windows\CurrentVersion\WindowsUpdate\Auto Update\RebootRequired'
        security_update_reboot = r'Software\Microsoft\Windows\CurrentVersion\Component Based Servicing\RebootPending'
        return read_hklm_key(auto_update_reboot) or read_hklm_key(security_update_reboot)

    def poll_registry_key(key, value_name, value):
        values = set()
        with key:
            LOG.debug('Waiting %s: %s ...', value_name, value)
            while True:
                curvalue = winreg.QueryValueEx(key, value_name)[0]
                values.add(curvalue)
                if curvalue == value:
                    LOG.debug('Reached %s: %s', value_name, value)
                    return values
                time.sleep(1)

    def wait_generalization_state():
        key = read_hklm_key(r'System\Setup\Status\SysprepStatus')
        return poll_registry_key(key, 'GeneralizationState', 7)

    def wait_unattend_specialize_pass():
        key = read_hklm_key(r'System\Setup\Status\UnattendPasses')
        return poll_registry_key(key, 'specialize', 9)


    LOG.info('Checking sysprep completion')
    gen_values = wait_generalization_state()
    spec_values = wait_unattend_specialize_pass()
    log_unattend_passes()
    # when we observe values transition, pending reboot is expected 
    if len(gen_values) > 1 or len(spec_values) > 1:
        LOG.info('Pending reboot is expected')
        LOG.debug('  GeneralizationState: %s', tuple(gen_values))
        LOG.debug('  specialize: %s', tuple(spec_values))
        raise RebootExpected()


def wait_boot():
    wait_sysprep_completion()
    LOG.info('Windows is ready!')
