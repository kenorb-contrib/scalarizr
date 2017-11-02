import re
import inspect
from textwrap import dedent

from docopt import docopt
from docopt import DocoptExit
from docopt import DocoptLanguageError
from docopt import printable_usage


TAB_SIZE = 2


def camel_to_underscore(name):
    s1 = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', name)
    return re.sub('([a-z0-9])([A-Z])', r'\1_\2', s1).lower()


def extended_doc(doc, usage_case):
    """Returns doc extended with new usage case"""
    match = re.search(r'usage:\s+', doc, re.IGNORECASE | re.DOTALL)
    help_usage_index = match.end()
    tab_str = match.group().split('\n')[1]
    return doc[:help_usage_index] + usage_case + tab_str + doc[help_usage_index:]


class UnknownCommand(SystemExit):
    """
    This exception is thrown when command couldn't find subcommand with given
    alias or name
    """

    def __int__(self):
        """Exit code"""
        return 1
    
    def __init__(self, message, command_name=None, usage=None):
        super(UnknownCommand, self).__init__(message)
        self.command_name = command_name
        self.usage = usage


class InvalidCall(SystemExit):
    """
    This exception is thrown when command with bad pararmeters - bad usage.
    """

    def __int__(self):
        """Exit code"""
        return 2
    
    def __init__(self, message, command_name=None, usage=None):
        super(InvalidCall, self).__init__(message)
        self.command_name = command_name
        self.usage = usage


class CommandError(SystemExit):
    """
    This exception is thrown when some command-specific runtime error is occured.
    Such as unknown message id in message-details or similiar
    """
    
    def __int__(self):
        """Exit code"""
        return 3


class Command(object):
    """
    Class that represents scalarizr command.

    Command execution runs __call__() method and command's task should be defined there.
    Command can have subcommands - other Command subclasses or just functions.
    Command name is its class name translated from camel-case to hyphen-case:
        ListMessages -> list-messages
    Command can have aliases - list of strings.
    run_subcommand() searches among names and aliases to find given subcommand.
    Commands return value is exit code return by Szradm if no exception is thrown.
    If one of exception standard exceptions is thrown, return code is taken 
        from their int() value.

    """

    # list or generator of Command subclasses, that will be used as subcommands
    subcommands = []
    aliases = []

    def __init__(self):
        super(Command, self).__init__()

    def __call__(self):
        raise NotImplementedError('You need to define __call__ method')

    def _command_help(self):
        """
        Returns help section that contains list of subcommands and their aliases.
        """
        usages = []
        for c in self.subcommands:
            usage = (' '*TAB_SIZE) + get_command_name(c)
            if c.aliases:
                usage += ' (%s)' % ', '.join(c.aliases)
            usages.append(usage)
        if usages:
            return '\nCommands:\n' + '\n'.join(usages) + '\n'
        else:
            return ''

    def help(self):
        """
        Returns __doc__ with some additional info or parser rules.
        By default it's adding --help usage to every command that doesnt have it,
        so every command could be run with --help key.
        """
        doc = self.__doc__
        if not '--help' in doc:
            help_usage_string = get_command_name(self) + ' -h | --help\n'
            doc = extended_doc(self.__doc__, help_usage_string)

        doc = dedent(doc + self._command_help())

        return doc

    def _find_subcommand(self, subcommand):
        """
        Searches subcommand in self.subcommands.
        If no subcommand found - raises exception.
        """
        for sub_cmd in self.subcommands:
            name = get_command_name(sub_cmd)
            is_alias = hasattr(sub_cmd, 'aliases') and subcommand in sub_cmd.aliases
            if subcommand == name or is_alias:
                return sub_cmd

        usage = printable_usage(self.help())
        raise UnknownCommand('%s: unknown subcommand %s' % (get_command_name(self), subcommand),
                             subcommand,
                             usage)

    def run_subcommand(self, subcommand, args, kwds=None, options_first=False):
        """
        Launches subcommands with given args.
        kwds - is dict of keywords that are passed directly to command call and
        are not parsed with docopt.
        """
        if not kwds:
            kwds = {}
        sub_cmd_definition = self._find_subcommand(subcommand)
        is_class = inspect.isclass(sub_cmd_definition)
        if is_class:
            sub_cmd = sub_cmd_definition()
            sub_cmd_doc = sub_cmd.help()
            accepts_help = 'help' in inspect.getargspec(sub_cmd.__call__).args
        else:
            sub_cmd = sub_cmd_definition
            sub_cmd_doc = sub_cmd.__doc__
            spec_args = inspect.getargspec(sub_cmd_definition).args
            accepts_help = 'help' in spec_args
            if 'self' in spec_args:
                kwds['self'] = self
        try:
            kwds.update(parse_command_line(args, sub_cmd_doc, options_first=options_first))
        except (DocoptExit, DocoptLanguageError), e:
            # TODO: think about showing whole help not just usage
            usage = printable_usage(sub_cmd_doc)
            msg = '%s: invalid call.' % subcommand
            raise InvalidCall(msg, subcommand, usage)

        # Alias for --help is processed in this way because it is hard to inject it into
        # the command doc
        if 'h' in kwds:
            kwds['help'] = kwds.pop('h')

        if 'help' in kwds and not accepts_help:
            print sub_cmd_doc
        else:
            return sub_cmd(**kwds)


def parse_command_line(argv, doc, options_first=False):
    """
    Parses list of command-line argv using doc and translates
    them to keyword dictionary which can be used to call Command instance.
    If options_first is True - options (keys) must go before subcommands.
    If it is False - it implies that command have no subcommands or subcommands
    have no options, otherwise those options will be parsed while command is parsed
    which may cause errors.
    Example:
        call `szradm queryenv --help` with options_first=True --help will be
        passed to queryenv subcomand, and with options_first=False - to szradm.
    """
    arguments = docopt(doc, argv=argv, help=False, options_first=options_first)
    kwds = _docopt_out_to_kwds(arguments)
    return kwds


def _docopt_out_to_kwds(arguments):
    """
    Renaming arguments from command-line-like to python-like.
    """
    result = {}
    for k, v in arguments.items():
        if v:
            new_k = k.lstrip('-').replace('-', '_')
            new_k = new_k.replace('<', '').replace('>', '')
            result[new_k] = v
    return result


def get_command_name(obj):
    """
    Returns command name from given class or class name. 
    (cls or func or str) -> str
    """
    name = obj
    if inspect.isclass(obj) or inspect.isfunction(obj):
        name = obj.__name__
    elif isinstance(obj, object):
        name = obj.__class__.__name__
    return camel_to_underscore(name).replace('_', '-')
