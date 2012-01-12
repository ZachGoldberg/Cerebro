#!/usr/bin/python -u
"""
This is the main file of the machinesitter application.

It's job is to manage multiple tasks on a machine -- starting
and stopping them at the whim of an administrator.
"""

import sittercommon.arg_parser as argparse
import machinemanager


import os
import simplejson
import sys
import time


def parse_args(args):
    """ Parse command line args."""

    parser = argparse.ArgumentParser(
        description="Start the machine sitter daemon")

    parser.add_argument('--task_definitions', dest='taskfile',
                        required=True,
                        help='The location of the task definition file')

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

    config = simplejson.load(open(args.taskfile))

    try:
        os.makedirs(config['log_location'])
    except:
        pass

    if args.daemon:
        daemonize()

    manager = machinemanager.MachineManager(args.taskfile,
                                            config['log_location'],
                                            starting_port=40000,
                                            daemon=args.daemon)

    task_definitions = config['task_definitions']

    for task in task_definitions:
        manager.add_new_task(task)

    manager.start()

    # wait forever
    while True:
        time.sleep(1)