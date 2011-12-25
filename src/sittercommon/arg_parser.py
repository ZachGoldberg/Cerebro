"""
A wrapper around optparse to provide argparse compatibility
"""
PARSER = "argparse"

import optparse

try:
    import argparse
except ImportError:
    PARSER = "optparse"


class OptParserWrapper(optparse.OptionParser):
    def __init__(self, description=None, *args, **kwargs):
        optparse.OptionParser.__init__(self, usage=description, *args,
                                     **kwargs)

    def add_argument(self, name, dest=None, helpstr=None, action=None,
                     const=None, default=None, required=None):
        """
        Same args / output as argparse.add_argument
        """
        self.add_option(name, dest=dest, help=helpstr, action=action,
                      default=default)

    def parse_args(self, args):
        """
        Same args / output as argparse.parse_args
        """
        (options, args) = optparse.OptionParser.parse_args(self, args)
        return options


class CustomArgParser(argparse.ArgumentParser):
    def __init__(self, *args, **kwargs):
        argparse.ArgumentParser.__init__(
            self,
            formatter_class=argparse.ArgumentDefaultsHelpFormatter,
            *args,
            **kwargs)


if PARSER == "argparse":
    ArgumentParser = CustomArgParser
else:
    ArgumentParser = OptParserWrapper
