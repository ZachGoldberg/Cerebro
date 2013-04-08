#!/usr/bin/python -u
"""
Cerebro client tool
"""
import sys

from sittercommon import arg_parser
from sittercommon.utils import (
    update_job, update_job_cfg, change_debug_level,
    update_idle_limit, list_jobs, list_machines,
    login)
from sittercommon.utils import load_defaults, write_defaults, output

COMMANDS = [
    change_debug_level,
    list_jobs,
    list_machines,
    login,
    update_idle_limit,
    update_job,
    update_job_cfg,
]


def get_parser(default_options):
    parser = arg_parser.ArgumentParser(
        description="Cerebro Command Line Interaction Tool")

    parser.add_argument(
        "--clustersitterurl", dest="clustersitter_url",
        help="URL to the root of the clustersitter",
        default=default_options.get('clustersitter_url'))

    parser.add_argument(
        "--quiet", dest="quiet",
        action="store_true")

    return parser


def main(sys_args=None):
    if not sys_args:
        sys_args = sys.argv[1:]

    default_options = load_defaults()

    command_parsers = {}
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

    if args.quiet:
        output.quiet = True

    default_options['clustersitter_url'] = args.clustersitter_url
    write_defaults(default_options)

    for command in COMMANDS:
        if command.get_command() == args.command:
            newargs = dict(args._get_kwargs())
            del newargs['command']
            del newargs['quiet']
            command.run_command(**newargs)

if __name__ == '__main__':
    main()
