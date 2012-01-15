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

from clustersitter import ClusterSitter, ProductionJob
from clustersitter import MachineConfig
from sittercommon import arg_parser


def parse_args(args):
    parser = arg_parser.ArgumentParser(
        description="Start the cluster sitter daemon")

    parser.add_argument("--daemon", dest="daemon",
                        default=False,
                        action="store_true",
                        help='Daemonize and split from launching shell')

    parser.add_argument("--aws-access-key", dest="aws_access_key")

    parser.add_argument("--aws-secret-key", dest="aws_secret_key")

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

    if not os.getenv('AWS_ACCESS_KEY_ID'):
        os.putenv('AWS_ACCESS_KEY_ID', args.aws_access_key)
        os.putenv('AWS_SECRET_ACCESS_KEY', args.aws_secret_key)

    if args.daemon:
        daemonize()

    logging.getLogger().setLevel(logging.INFO)

    sitter = ClusterSitter(daemon=args.daemon,
                           log_location="/mnt/data")
    sitter.start()

    localhost = MachineConfig("localhost",
                              "zg-workstation",
                              6, 16000)

    sitter.add_machines([localhost])

    time.sleep(2)

    job = ProductionJob(
        task_configuration={
            "allow_exit": False,
            "name": "Simple Web Server",
            "command": "/usr/bin/python -m SimpleHttpServer",
            "auto_start": True,
            "ensure_alive": True,
            "max_restarts": -1,
            "restart": True,
            "uid": 0
        },
        deployment_layout={'aws-us-east-1a': {'cpu': 4, 'mem': 500}},
        deployment_recipe='NOOPRecipe',
        )

    #sitter.add_job(job)

    # wait forever
    while True:
        try:
            time.sleep(1)
        except KeyboardInterrupt:
            print "Caught Control-C, exiting"
            os._exit(0)
