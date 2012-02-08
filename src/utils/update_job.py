import logging
import json
import os
import requests
import sys

from sittercommon import arg_parser


def parse_args(args):
    parser = arg_parser.ArgumentParser(
        description="Update clustersitter job config")

    parser.add_argument("--job_name", dest="job_name",
                        help='The name of the file to upload in'
                        ' the appropriate releases folder.  Leave blank for '
                        'latest.')

    parser.add_argument("--version", dest="version",
                        help='The name of the file (without .tgz) to upload in'
                        ' the appropriate releases folder.  Leave blank for '
                        'latest.')

    parser.add_argument("--clustersitter-url", dest="clustersitter_url",
                        help="URL to the root of the clustersitter")

    return parser.parse_args(args=args)


def update_version(url, job_name, version):
    print "%s/update_job" % url
    data = {'data': json.dumps({'job_name': job_name,
                                'version': version})}
    resp = requests.post("%s/update_job" % url, data=data)
    print resp.content


def main(sys_args=None):
    if not sys_args:
        sys_args = sys.argv[1:]

    args = parse_args(sys_args)

    update_version(args.clustersitter_url,
                   args.job_name,
                   args.version)

if __name__ == '__main__':
    main()
