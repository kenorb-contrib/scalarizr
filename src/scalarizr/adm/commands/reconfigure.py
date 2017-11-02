import sys
import inspect

from scalarizr.node import __node__
from scalarizr.bus import bus
from scalarizr.adm.command import Command
from scalarizr.adm.command import CommandError
from scalarizr.queryenv import new_queryenv
from scalarizr.api.service import ServiceAPI
from scalarizr.api.service import behavior_apis
from scalarizr.api import operation


class Reconfigure(Command):
    """
    Usage:
        reconfigure [<behavior>]
    """

    def __call__(self, behavior=None):

        # if behavior not in behavior_apis:
        #     raise CommandError('Unknown behavior.')

        bus.queryenv_service = new_queryenv()
        api = ServiceAPI()
        # api.init_service()
        if behavior and behavior not in behavior_apis:
            raise CommandError('Unknown behavior.')
        if behavior:
            print "Reconfiguring behavior %s..." % behavior
        else:
            print "Reconfiguring..."

        behavior_params = {behavior: None} if behavior else None

        try:
            api.reconfigure(behavior_params=behavior_params, async=False)
        except (BaseException, Exception), e:
            raise CommandError('Reconfigure failed.\n%s' % e)
        print "Done"

        return 0


commands = [Reconfigure]
