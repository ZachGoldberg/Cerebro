import logging
import json
import os
import requests
import sys

from sittercommon import arg_parser


def parse_args(args):
    parser = arg_parser.ArgumentParser(
        description="Update clustersitter job config")

    parser.add_argument("--idle-limit", dest="idle_limit",
                        help='Maximum number of idle machines per zone')

    parser.add_argument("--clustersitter-url", dest="clustersitter_url",
                        help="URL to the root of the clustersitter")

    return parser.parse_args(args=args)


def update_limit(url, limit):
    data = {'data': json.dumps({'idle_count_per_zone': limit})}
    print "%s/update_idle_limit" % url
    resp = requests.post("%s/update_idle_limit" % url, data=data)
    print resp.content


def main(sys_args=None):
    if not sys_args:
        sys_args = sys.argv[1:]

    args = parse_args(sys_args)

    update_limit(args.clustersitter_url,
                 args.idle_limit)


if __name__ == '__main__':
    main()
