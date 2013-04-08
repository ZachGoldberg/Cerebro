import json
import re
import os
import sys

CFG_FILE = os.path.expanduser("~/.cerebro.cfg")


class Output(object):
    def __init__(self):
        self.quiet = False

    def echo(self, msg):
        if not self.quiet:
            print msg

    def stderr(self, msg):
        if not self.quiet:
            sys.stderr.write(msg)

output = Output()


def strip_html(text):
    return re.sub('<[^<]+?>', '', text)


def load_defaults():
    default_options = {}

    try:
        default_data = open(CFG_FILE).read()
        default_options = json.loads(default_data)
    except:
        pass

    return default_options


def write_defaults(default_options):
    default_file = open(CFG_FILE, 'w')
    default_file.write(json.dumps(default_options))
    default_file.close()
