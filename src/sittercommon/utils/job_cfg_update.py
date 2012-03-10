import logging
import json
import os
import requests
import sys

from sittercommon import arg_parser


def get_help_string():
    return "Update clustersitter job config"


def get_command():
    return "update_job_cfg"


def get_parser(parser):
    parser.add_argument("--job-config", dest="job_config",
                        required=True,
                        help='JSON file of Job configuration')

    return parser


def run_command(clustersitter_url=None,
             job_config=None):
    try:
        jobs = json.load(open(job_config))
    except:
        print "Error opening config file %s" % job_config
        import traceback
        traceback.print_exc()
        return

    print "%s/add_job" % clustersitter_url
    for job in jobs:
        data = {'data': json.dumps(job)}
        resp = requests.post("%s/add_job" % clustersitter_url, data=data)
        print resp.content
