import json
import requests

from sittercommon import arg_parser


def get_help_string():
    return "Update a job to a new version"


def get_command():
    return "update_job"


def get_parser(parser):
    parser.add_argument("--job-name", dest="job_name",
                        required=True,
                        help='The name of the file to upload in'
                        ' the appropriate releases folder.  Leave blank for '
                        'latest.')

    parser.add_argument("--version", dest="version",
                        help='The name of the file (without .tgz) to upload in'
                        ' the appropriate releases folder.  Leave blank for '
                        'latest.')

    return parser


def run_command(clustersitter_url=None,
                job_name=None,
                version=None):

    print "%s/update_job" % clustersitter_url
    data = {'data': json.dumps({'job_name': job_name,
                                'version': version})}
    resp = requests.post("%s/update_job" % clustersitter_url, data=data)
    print resp.content
