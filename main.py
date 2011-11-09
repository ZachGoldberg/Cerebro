#!/usr/bin/python
"""
This is the main file of the tasksitter application.

The task sitter's job is to spawn a job and the monitor
it's resource usage.  If it exceeds the specified parameters it reboots
the job.

If a job is 'flapping' or being rebooted too often it knows how to trigger
an alert.
"""

import arg_parser as argparse
import sys

import constraints
import http_monitor
import process_harness
import stats_collector


def RunCommandWithHarness(command, args, constraints):
    """Execute the child command.

    Args:
      command: a string which includes the file and args
      args: An object which containts configuration options
      constraints: an array of Constraint objects

    Return:
      A Harness object encapsulating the child process
      and all the current constraints
    """
    return process_harness.ProcessHarness(command, constraints,
                                          restart=args.restart,
                                          max_restarts=args.max_restarts)


def ParseArgs(args):
    """ Parse command line args."""

    parser = argparse.ArgumentParser(description="Run a task with a"
                                     "cpu/memory harness")
    parser.add_argument('--cpu', dest='cpu',
                        help='The amount of cores (in float) '
                        'this task can use')

    parser.add_argument('--mem', dest='mem',
                        help='The amount of memory in MB that this '
                        'task can use')

    parser.add_argument('--restart', dest='restart',
                        action='store_true',
                        default=False,
                        help='Restart the task if it violates any of its '
                        'constraints')

    parser.add_argument('--max_restarts', dest='max_restarts',
                        default=-1,
                        help='Number of times to reboot the task when it '
                        'violates constraints before bailing out.')

    parser.add_argument('--ensure_alive', dest='ensure_alive',
                        default=False,
                        action='store_true',
                        help='Restart the task if it exists normally.  A '
                        'normal exit does incremement the restart counter')

    parser.add_argument('--http_monitoring', dest='http_monitoring',
                        default=False,
                        action='store_true',
                        help='Expose an interface via HTTP for collecting '
                        'task statistics and metadata')

    parser.add_argument('--http_monitoring_port', dest='http_monitoring_port',
                        default=8080,
                        help='Port to do HTTP Monitoring (Default: 80)')

    parser.add_argument('--command', dest='command',
                        required=True,
                        help='The command to run')

    # explicitly offer args the param incase we're parsing not from
    # sys.argv
    return parser.parse_args(args=args)


def BuildConstraints(args):
    proc_constraints = []

    if args.ensure_alive:
        proc_constraints.append(constraints.LivingConstraint())

    if args.cpu:
        proc_constraints.append(constraints.CPUConstraint(args.cpu))

    if args.mem:
        proc_constraints.append(constraints.MemoryConstraint(args.mem))

    return proc_constraints


def main(sys_args=None, wait_for_child=True):
    """Run the task sitter."""

    if not sys_args:
        sys_args = sys.argv[1:]

    print sys_args

    args = ParseArgs(sys_args)
    constraints = BuildConstraints(args)

    harness = RunCommandWithHarness(args.command, args, constraints)
    harness.BeginMonitoring()

    stats = stats_collector.StatsCollector(harness)
    httpd = None

    if args.http_monitoring:
        httpd = http_monitor.HTTPMonitor(stats, args.http_monitoring_port)
        httpd.start()

    if wait_for_child:
        exit_code = harness.WaitForChildToFinish()
        if httpd:
            httpd.stop()

        sys.exit(exit_code)

    else:
        return stats, httpd, harness

if __name__ == '__main__':
    main()
