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
import os
import sys

import constraints
import process
import process_harness


def RunCommandWithHarness(command, constraints):
    """Execute the child command.

    Args:
      command: a string which includes the file and args
      constraints: an array of Constraint objects

    Return:
      A Harness object encapsulating the child process
      and all the current constraints
    """
    pid = os.fork()
    if pid == 0:
        # We're the child, we'll exec
        # parse the command
        cmd = '/bin/bash'
        args = [cmd, "-c", command]
        os.execvp(cmd, args)

    child_proc = process.Process(pid)
    return process_harness.ProcessHarness(child_proc, constraints)


def ParseArgs(args):
    """ Parse command line args."""

    parser = argparse.ArgumentParser(description="Run a task with a"
                                     "cpu/memory harness")
    parser.add_argument('--cpu', dest='cpu',
                        help='The amount of cores (in float)'
                        'this task can use')

    parser.add_argument('--mem', dest='mem',
                        help='The amount of memory in MB that this'
                        'task can use')

    parser.add_argument('--command', dest='command',
                        required=True,
                        help='The command to run')

    # explicitly offer args the param incase we're parsing not from
    # sys.argv
    return parser.parse_args(args=args)


def BuildConstraints(args):
    proc_constraints = []

    if args.cpu:
        proc_constraints.append(constraints.CPUConstraint(args.cpu))

    if args.mem:
        proc_constraints.append(constraints.MemoryConstraint(args.mem))

    return proc_constraints


def main(sys_args=None):
    """Run the task sitter."""

    if not sys_args:
        sys_args = sys.argv[1:]

    print sys_args

    args = ParseArgs(sys_args)
    constraints = BuildConstraints(args)

    #StartHTTPMonitor()
    harness = RunCommandWithHarness(args.command, constraints)
    harness.BeginMonitoring()

    _, exit_code = harness.WaitForChildToFinish()

    sys.exit(exit_code)

if __name__ == '__main__':
    main()
