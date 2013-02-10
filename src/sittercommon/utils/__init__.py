import json
import re
import os


CFG_FILE = os.path.expanduser("~/.cerebro.cfg")


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
