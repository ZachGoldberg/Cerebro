import logging
import json
import os
import requests
import sys

from sittercommon import arg_parser


def parse_args(args):
    parser = arg_parser.ArgumentParser(
        description="Update clustersitter job config")

    parser.add_argument("--job-config", dest="job_config",
                        help='JSON file of Job configuration')

    parser.add_argument("--clustersitter-url", dest="clustersitter_url",
                        help="URL to the root of the clustersitter")

    return parser.parse_args(args=args)


def post_job(url, job):
    print "%s/add_job" % url
    print job
    data = {'data': json.dumps(job)}
    resp = requests.post("%s/add_job" % url, data=data)
    print resp.content


def main(sys_args=None):
    if not sys_args:
        sys_args = sys.argv[1:]

    args = parse_args(sys_args)

    try:
        jobs = json.load(open(args.job_config))
    except:
        print "Error opening config file %s" % args.job_config
        import traceback
        traceback.print_exc()
        return
    for job in jobs:
        post_job(args.clustersitter_url, job)


if __name__ == '__main__':
    main()
