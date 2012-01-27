import logging
import json
import os
import requests
import sys

from sittercommon import arg_parser


def parse_args(args):
    parser = arg_parser.ArgumentParser(
        description="Update clustersitter job config")

    parser.add_argument("--level", dest="level",
                        help='New Debug Level '
                        '(10=debug,20=info,30=warning,40=error)',
                        type=int)

    parser.add_argument("--clustersitter-url", dest="clustersitter_url",
                        help="URL to the root of the clustersitter")

    return parser.parse_args(args=args)


def update_limit(url, level):
    data = {'data': json.dumps({'level': level})}
    print "%s/update_idle_limit" % url
    resp = requests.post("%s/update_logging_level" % url, data=data)
    print resp.content


def main(sys_args=None):
    if not sys_args:
        sys_args = sys.argv[1:]

    args = parse_args(sys_args)

    update_limit(args.clustersitter_url,
                 args.level)


if __name__ == '__main__':
    main()
