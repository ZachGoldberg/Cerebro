PARSER = "argparse"

try:
    import argparse
except ImportError:
    PARSER = "optparse"
    import optparse


class OptParserWrapper(optparse.OptionParser):
    def __init__(self, description=None, *args, **kwargs):
        optparse.OptionParser.__init__(self, usage=description, *args, 
                                     **kwargs)

    def add_argument(self, name, dest=None, help=None, action=None, 
                     const=None, default=None, required=None):
        self.add_option(name, dest=dest, help=help, action=action,
                      default=default)
        
    def parse_args(self, args):
        (options, args) = optparse.OptionParser.parse_args(self, args)
        return options

if PARSER == "argparse":
  ArgumentParser = argparse.ArgumentParser
else:
  ArgumentParser = OptParserWrapper
