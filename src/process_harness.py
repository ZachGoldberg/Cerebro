"""
A class which encapsulates a process and a set of constraints
and ensures that they are constantly fulfilled.
"""
import datetime
import os
import threading
import time

import process


class ProcessHarness(object):
    """
    An object which manages the lifecycle of a single child process, killing it
    when it violates constraitns and rebooting it as necessary
    """
    def __init__(self, command, constraints, restart=False,
                 max_restarts=-1, poll_interval=.1):
        self.command = command
        self.child_proc = None
        self.constraints = constraints
        self.restart = restart
        self.max_restarts = max_restarts
        self.start_count = 0
        self.child_running = True
        self.poll_interval = poll_interval

        #Statistics
        self.task_start = datetime.datetime.now()
        self.process_start = None

        # Start the child process
        self.start_process()

    def start_process(self):
        """
        Start a new instance of the child task
        """
        self.process_start = datetime.datetime.now()

        pid = os.fork()
        if pid == 0:
            # We're the child, we'll exec
            # Put ourselves into our own pgrp, for sanity
            os.setpgrp()

            # parse the command
            cmd = '/bin/bash'
            args = [cmd, "-c", self.command]
            os.execvp(cmd, args)

        self.child_proc = process.Process(pid)
        self.start_count += 1

    def do_monitoring(self):
        """
        Begin monitoring the child process
        """
        while True:
            restarted = False

            for constraint in self.constraints:
                if constraint.check_violation(self.child_proc):
                    if self.child_violation_occured(constraint):
                        print "Restarting child command %s" % self.command
                        self.start_process()
                        restarted = True

            if not restarted and not self.child_proc.is_alive():
                # The child proc could have died inbetween checking
                # constraints and now.  If there is a LivingConstraint
                # then fire it
                for constraint in self.constraints:
                    if str(constraint) == "LivingConstraint":
                        if self.child_violation_occured(constraint):
                            print "Restarting child command %s" % self.command
                            self.start_process()
                            restarted = True

                # There is no living constraint and child is dead,
                # so set running to false
                if not restarted:
                    self.child_running = False
                    return

            time.sleep(self.poll_interval)

    def child_violation_occured(self, violated_constraint):
        """
        Take appropriate action when we're in violation
        Returns:
          True means the process should be restarted
          False means the caller should take no action
        """
        print "Violated Constraint %s" % str(violated_constraint)
        if violated_constraint.kill_on_violation:
            self.child_proc.force_exit()

        if self.restart:
            if (self.max_restarts == -1 or
                self.start_count <= self.max_restarts):
                return True

        self.child_running = False
        return False

    def begin_monitoring(self):
        """Split off a thread to monitor the child process"""
        monitoring_thread = threading.Thread(target=self.do_monitoring,
                                             name='Child Monitoring')

        monitoring_thread.start()

    def terminate_child(self):
        """
        Kill the child process
        """
        self.child_proc.force_exit()
        self.wait_for_child_to_finish()

    def wait_for_child_to_finish(self):
        """
        Wait for the child process to complete naturally.
        """
        code = 0
        while self.child_running:
            _, newcode = self.child_proc.wait_for_completion()
            if newcode:
                code = newcode

            print "Child %s exited %s" % (self.child_proc.pid, code)
            time.sleep(.1)

        return code
