#!/usr/bin/python -u
"""
This is the main file of the clustersitter application.

It's job is to manage an entire fleet of machines, each running
machine sitters and task sitters.  It reaches out across
the cluster and monitors everything, and exposes several semantic
interfaces (all via HTTP).  There are NAGIOS type monitoring
endpoints as well as an API for interactions via a command line tool.
"""
import logging
import os
import sys
import time

import sittercommon.arg_parser as argparse

import clustersitter


def parse_args(args):
    parser = argparse.ArgumentParser(
        description="Start the cluster sitter daemon")

    parser.add_argument("--daemon", dest="daemon",
                        default=False,
                        action="store_true",
                        help='Daemonize and split from launching shell')

    return parser.parse_args(args=args)


def daemonize():
    pid = os.fork()
    if pid > 0:
        sys.exit(0)

    os.setsid()
    os.umask(0)
    os.chdir("/")

    pid = os.fork()
    if pid > 0:
        sys.exit(0)

    sys.stdout.flush()
    sys.stderr.flush()
    sys.stdin.close()

    print "Sitter PID: %s" % os.getpid()


def main(sys_args=None):

    if not sys_args:
        sys_args = sys.argv[1:]

    args = parse_args(sys_args)

    if args.daemon:
        daemonize()

    logging.getLogger().setLevel(logging.INFO)

    sitter = clustersitter.ClusterSitter(daemon=args.daemon,
                                         log_location="/mnt/data")
    sitter.start()

    sitter.add_machines(["localhost"])

    # wait forever
    while True:
        try:
            time.sleep(1)
        except KeyboardInterrupt:
            print "Caught Control-C, exiting"
            os._exit(0)
