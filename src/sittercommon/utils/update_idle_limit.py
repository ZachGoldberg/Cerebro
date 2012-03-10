import json
import requests

from sittercommon import arg_parser


def get_help_string():
    return "Update clustersitter idle machine limit"


def get_command():
    return "update_idle_limit"


def get_parser(parser):
    parser.add_argument("--idle-limit", dest="idle_limit",
                        required=True,
                        help='Maximum number of idle machines per zone')

    return parser


def run_command(clustersitter_url, idle_limit):
    data = {'data': json.dumps({'idle_count_per_zone': idle_limit})}
    print "%s/update_idle_limit" % clustersitter_url
    resp = requests.post("%s/update_idle_limit" % clustersitter_url,
                         data=data)
    print resp.content
