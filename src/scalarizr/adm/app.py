#!/usr/bin/python


"""
Purpose of this script is to administrate scalarizr. It launches predefined commands
which do some tasks. Commands could have inner commands themselves. Inner
commands in relation to parent are called subcommands. Commands must have
docstring which describes it and defines its usage. Docstring is used by docopt
system to determine parsing rules. Commands are classes that inherit from 
Command class or methods or functions. Method or function-defined commands
should be used only for simple tasks, for more complicated (>4 if's or for's,
or uses some context variables) please use classes. Enter point is Szradm
command that is launched directly from main() function. Other commands are
launched with Szradm or other commands with run_subcommand() method. Szradm
searches for commands in modules of given directory. Module that wants to provide
commands should define commands variable - list of Command class inheritors
and/or functions. Result of execution is printed output or return code or both.
If you want to write new command, you can check existing commands for example
those that are defined in scalarizr.adm.commands.messages. For additional info
see http://docopt.org.
As already mentioned before, remember that docstrings for commands are essential
to write. Not only parser uses them to know what to do with argument list, but
you are writing command-line tool that people will be using, write them as
detailed as possible.
Options must be described under 'Options:' section, otherwise they won't be 
parsed.
"""


import sys
import os
import imp
import time
import inspect
from textwrap import dedent

from scalarizr import linux
from scalarizr.adm import command as command_module
from scalarizr.adm.commands.queryenv import Queryenv as QueryenvCmd
from scalarizr.app import _init_platform
from scalarizr.app import init_script


__dir__ = os.path.dirname(__file__)


windows_supported_modules = ('fire_event', 'messages', 'msgsend', 'queryenv')


def find_modules(directory):
    """
    Method returns iterator over modules in given directory.
    Module is imported at the time it is accessed.
    """
    directory = os.path.abspath(directory)
    result = []

    dir_content = os.listdir(directory)
    module_names = [m.replace('.py', '') for m in dir_content]
    module_names = list(set(module_names))
    this_module = os.path.basename(__file__).replace('.py', '')

    for name in module_names:
        if name in ('__init__', this_module) or \
            (linux.os.windows_family and name not in windows_supported_modules):
            continue
        try:
            module_info = imp.find_module(name, [directory])
        except ImportError:
            continue
        module = imp.load_module(name, *module_info)
        if module:
            yield module


class Szradm(command_module.Command):
    """
    Szradm is scalarizr administration tool.

    Usage:
      szradm [options] [<command>] [<args>...]

    Options:
      -v, --version                Display version.
      -h, --help                   Display this message.
      -m, --msgsnd                 Message sender CLI.
      -n, --name=<name>            Sets message name.
      -e, --endpoint=<endpoint>    Sets endpoint for message send.
      -f, --msgfile=<msgfile>      Sets message file.
      -o, --queue=<queue>          Sets queue which will be used for message delivery.
      -q, --queryenv               QueryEnv CLI with a raw XML output. 
      --api-version=<api-version>  Set QueryEnv API version which will be used in call.
                                   QueryEnv parameters should be passed in <key>=<value> form.
      --fire-event=<event_name>    Fires custom event on Scalr.
                                   Parameters should be passed in a <key>=<value> form.
      -b, --behaviour=<bhvr>       Role behaviour.
      -r, --role-name=<rolename>   Role name.
      -i, --with-initializing      Show initializing servers
      -s, --https                  Show virtual hosts by https
    """

    version = (0, 3)

    def help(self):
        """
        Redefining this method because we don't need to print subcommands list
        for szradm here.
        """
        return dedent(self.__doc__)

    def __init__(self, commands_dir=None):
        super(Szradm, self).__init__()
        self.subcommands = self.find_commands(commands_dir)

    def __call__(self, 
        command=None,
        version=False,
        help=False,
        queryenv=False,
        msgsnd=False,
        qa_report=False,
        repair=False,
        name=None,
        msgfile=None,
        queue=None,
        api_version=None,
        fire_event=None,
        endpoint=None,
        behaviour=None,
        role_name=None,
        with_initializing=None,
        https=None,
        args=[]):

        if version:
            print 'Szradm version: %s.%s' % self.version
            return

        if help:
            self.subcommands = list(self.subcommands)
            print self.help() + self._command_help()
            return

        try:
            # old-style command execution for backward compatibility
            if queryenv:
                if not command:
                  raise command_module.CommandError('No queryenv method given.')
                args = ['fetch', 'command='+command] + args
                return self.run_subcommand('queryenv', args)

            if msgsnd:
                kwds = {'name': name,
                    'msgfile': msgfile,
                    'endpoint': endpoint,
                    'queue': queue}
                return self.run_subcommand('msgsnd', [command] + args, kwds)

            if fire_event:
                return self.run_subcommand('fire-event', [fire_event, command] + args)

            if not command:
                return self(help=True)

            # queryenv shortcuts for table-form output
            if QueryenvCmd.supports_oldstyle_method(command):
                kwds = {'shortcut': True,
                    'name': name,
                    'behaviour': behaviour,
                    'role_name': role_name,
                    'with_initializing': with_initializing,
                    'https': https}
                kwds.update(command_module.parse_command_line(args, self.help()))
                kwds['args'] = args
                try:
                    return self.run_subcommand('queryenv', [command], kwds)
                except command_module.InvalidCall, e:
                    raise command_module.InvalidCall('Invalid call', usage=self.help())

            # Standard command execution style
            return self.run_subcommand(command, args)

        except (command_module.UnknownCommand, command_module.InvalidCall), e:
            message = '\n'.join((str(e), e.usage))
            raise e.__class__(message)

        except command_module.CommandError, e:
            # except-section for user-defined exceptions, semantic errors, etc.
            raise command_module.CommandError(str(e))

    def find_commands(self, directory=None):
        """
        Method returns iterator over Command subclasses that found in modules of given directory
        """
        result = []
        if not directory:
            directory = __dir__
        modules = find_modules(directory)
        for module in modules:
            # 'commands' is name of attr that defines list of provided top-level szradm commands.
            # Subcommands should not be included
            if hasattr(module, 'commands'):
                for cmd in module.commands:
                    yield cmd


def _exit_code_excepthook(exctype, value, trace):
    """
    Hook for exceptions to customize exit codes.
    """
    old_excepthook(exctype, value, trace)
    if isinstance(value, SystemExit) and '__int__' in dir(value):
        sys.exit(int(value))


sys.excepthook, old_excepthook = _exit_code_excepthook, sys.excepthook


def main(argv=None):
    if not argv:
        argv = sys.argv
    init_script()
    _init_platform()
    szradm = Szradm(os.path.join(__dir__, 'commands'))
    # If szradm called with no arguments - print help() and all/most used possible commands
    szradm_kwds = command_module.parse_command_line(argv[1:], szradm.help(), options_first=True)
    sys.exit(szradm(**szradm_kwds))


if __name__ == '__main__':
    main(sys.argv)
