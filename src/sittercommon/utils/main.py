#!/usr/bin/python -u
"""
Cerebro client tool
"""
import json
import os
import sys

from sittercommon import arg_parser
from sittercommon.utils import (
    update_job, job_cfg_update, change_debug_level,
    update_idle_limit)

COMMANDS = [update_job,
            job_cfg_update,
            change_debug_level,
            update_idle_limit]


def get_parser(default_options):
    parser = arg_parser.ArgumentParser(
        description="Cerebro Command Line Interaction Tool")

    parser.add_argument(
        "--clustersitter-url", dest="clustersitter_url",
        help="URL to the root of the clustersitter",
        default=default_options.get('clustersitter_url'))

    return parser


def main(sys_args=None):
    if not sys_args:
        sys_args = sys.argv[1:]

    default_options = {}
    CFG_FILE = os.path.expanduser("~/.cerebro.cfg")

    try:
        default_data = open(CFG_FILE).read()
        default_options = json.loads(default_data)
    except:
        pass

    command_parsers = {}
    print default_options
    parser = get_parser(default_options)
    subparsers = parser.add_subparsers(help='Cerebro Command')
    for command in COMMANDS:
        command_parsers[command.get_command()] = subparsers.add_parser(
            command.get_command(),
            help=command.get_help_string())
        command.get_parser(command_parsers[command.get_command()])
        command_parsers[command.get_command()].set_defaults(
            command=command.get_command())

    args = parser.parse_args(sys_args)

    if not args.command:
        print "No Command Given"
        print parser.print_help()
        sys.exit(0)

    if not args.clustersitter_url:
        print "Please pass a clustersitter URL the first time you run cerebro"
        sys.exit(0)

    default_options['clustersitter_url'] = args.clustersitter_url
    default_file = open(CFG_FILE, 'w')
    default_file.write(json.dumps(default_options))
    default_file.close()

    for command in COMMANDS:
        if command.get_command() == args.command:
            newargs = dict(args._get_kwargs())
            del newargs['command']
            command.run_command(**newargs)
