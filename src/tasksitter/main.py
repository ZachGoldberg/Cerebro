#!/usr/bin/python
"""
This is the main file of the tasksitter application.

The task sitter's job is to spawn a job and the monitor
it's resource usage.  If it exceeds the specified parameters it reboots
the job.

If a job is 'flapping' or being rebooted too often it knows how to trigger
an alert.
"""
import os
import simplejson
import sys


import sittercommon.arg_parser as argparse
import sittercommon.http_monitor as http_monitor
import sittercommon.logmanager as logmanager
import constraints
import process_harness
import stats_collector


def run_command_with_harness(command, args, constraints_list):
    """Execute the child command.

    Args:
      command: a string which includes the file and args
      args: An object which containts configuration options
      constraints: an array of Constraint objects

    Return:
      A Harness object encapsulating the child process
      and all the current constraints
    """

    logs = logmanager.LogManager(args.stdout_location,
                                 args.stderr_location)

    return process_harness.ProcessHarness(command, constraints_list,
                                          restart=args.restart,
                                          max_restarts=args.max_restarts,
                                          poll_interval=args.poll_interval,
                                          collect_stats=args.collect_stats,
                                          logmanager=logs,
                                          uid=args.uid)


def parse_args(args):
    """ Parse command line args."""

    parser = argparse.ArgumentParser(description="Run a task with a"
                                     "cpu/memory harness")
    parser.add_argument('--cpu', dest='cpu', type=float,
                        help='The amount of cores (in float) '
                        'this task can use')

    parser.add_argument('--mem', dest='mem', type=float,
                        help='The amount of memory in MB that this '
                        'task can use')

    parser.add_argument('--time-limit', dest='time_limit', type=float,
                        help='Maximum time the child can run for in seconds')

    parser.add_argument('--disable_stat_collection', dest='collect_stats',
                        action='store_false',
                        default=True,
                        help='Disable collecting child process stats unless'
                        'required by a constraint.  Saves some CPU overhead.')

    parser.add_argument('--restart', dest='restart',
                        action='store_true',
                        default=False,
                        help='Restart the task if it violates any of its '
                        'constraints')

    parser.add_argument('--max-restarts', dest='max_restarts',
                        default=-1, type=int,
                        help='Number of times to reboot the task when it '
                        'violates constraints before bailing out.')

    parser.add_argument('--ensure-alive', dest='ensure_alive',
                        default=False,
                        action='store_true',
                        help='Restart the task if it exists normally.  A '
                        'normal exit does incremement the restart counter')

    parser.add_argument('--http-monitoring', dest='http_monitoring',
                        default=False,
                        action='store_true',
                        help='Expose an interface via HTTP for collecting '
                        'task statistics and metadata')

    parser.add_argument('--http-monitoring-port', dest='http_monitoring_port',
                        default=8080, type=int,
                        help='Port to do HTTP Monitoring (Default: 80)')

    parser.add_argument('--keep-http-running', dest='keep_http_running',
                        default=False,
                        action='store_true',
                        help='Keep http server up after children die')

    parser.add_argument('--command', dest='command',
                        required=True,
                        help='The command to run')

    parser.add_argument('--poll-interval', dest='poll_interval',
                        default=0.1, type=float,
                        help='How frequently (seconds) to poll the child '
                        'process for constraint violations '
                        '(default=0.1 seconds)')

    parser.add_argument('--stdout-location', dest='stdout_location',
                        default='-', type=str,
                        help='Directory where stdout logs should be placed '
                        'default is to print to caller\'s STDOUT')

    parser.add_argument('--stderr-location', dest='stderr_location',
                        default='-', type=str,
                        help='Directory where stdout logs should be placed '
                        'default is to print to caller\'s STDERR')

    parser.add_argument('--uid', dest='uid',
                        help='Change to UID before executing child process'
                        'requires root priviledges.  Can be an ID or name.')

    # explicitly offer args the param incase we're parsing not from
    # sys.argv
    return parser.parse_args(args=args)


def build_constraints(args):
    """
    Build an array of Constraint objects based on invokation ars.
    """
    proc_constraints = []

    if args.ensure_alive:
        proc_constraints.append(constraints.LivingConstraint())

    if args.cpu:
        proc_constraints.append(constraints.CPUConstraint(args.cpu))

    if args.mem:
        proc_constraints.append(constraints.MemoryConstraint(args.mem))

    if args.time_limit:
        proc_constraints.append(constraints.TimeConstraint(args.time_limit))

    return proc_constraints


def main(sys_args=None, wait_for_child=True, allow_spam=False):
    """Run the task sitter."""

    if not sys_args:
        sys_args = sys.argv[1:]

    print sys_args

    args = parse_args(sys_args)
    constraints_list = build_constraints(args)

    # Set outselves to our own pgrp to separate from machine sitter
    os.setpgrp()

    harness = run_command_with_harness(args.command, args, constraints_list)
    harness.allow_spam = allow_spam
    harness.begin_monitoring()

    stats = stats_collector.StatsCollector(harness)
    httpd = None

    if args.http_monitoring:
        httpd = http_monitor.HTTPMonitor(stats, harness,
                                         args.http_monitoring_port)
        httpd.start()

    if wait_for_child:
        exit_code = harness.wait_for_child_to_finish()

        print simplejson.dumps(harness.logmanager.get_logfile_names())

        if httpd and not args.keep_http_running:
            httpd.stop()

        if not args.keep_http_running:
            sys.exit(exit_code)

    else:
        return stats, httpd, harness

if __name__ == '__main__':
    main()
