"""
A class which encapsulates a process and a set of constraints
and ensures that they are constantly fulfilled.
"""
import datetime
import os
import pwd
import signal
import simplejson
import sys
import threading
import time

import process


class ProcessHarness(object):
    """
    An object which manages the lifecycle of a single child process, killing it
    when it violates constraitns and rebooting it as necessary
    """
    def __init__(self, command, constraints, restart=False,
                 max_restarts=-1, poll_interval=.1,
                 logmanager=None, uid=None, allow_spam=False,
                 collect_stats=True):
        self.launch_location = os.getcwd()
        self.child_proc = None
        self.child_running = True
        self.collect_stats = collect_stats
        self.command = command
        self.constraints = constraints
        self.max_restarts = max_restarts
        self.poll_interval = poll_interval
        self.restart = restart
        self.start_count = 0
        self.uid = uid
        self.parent_pid = os.getpid()
        self.logmanager = logmanager
        self.logmanager.set_harness(self)
        self.stop_running = False
        self.last_start = datetime.datetime.min
        self.allow_spam = allow_spam
        # Statistics
        self.task_start = datetime.datetime.now()
        self.violations = {}
        for constraint in self.constraints:
            self.violations[str(constraint)] = 0

        signal.signal(signal.SIGTERM, self.exit_now)
        signal.signal(signal.SIGINT, self.exit_now)

        if self.uid != None:
            actual_uid = None
            try:
                actual_uid = int(self.uid)
            except:
                try:
                    import pwd
                    actual_uid = pwd.getpwnam(self.uid)[2]
                except:
                    sys.stderr.write("Invalid UID: %s" % uid)
                    os._exit(1)

            self.uid = actual_uid

        # Start the child process
        self.start_process()

    def exit_now(self, *_):
        self.stop_running = True
        self.child_running = False
        self.terminate_child()
        print simplejson.dumps(self.logmanager.get_logfile_names())
        os._exit(0)

    def start_process(self):
        """
        Start a new instance of the child task
        """
        # Avoid spam-restarts, only allow restarting
        # once per second
        now = datetime.datetime.now()
        if not self.allow_spam and \
                now - self.last_start < datetime.timedelta(seconds=1):
            time.sleep(1)

        self.last_start = datetime.datetime.now()

        pid = os.fork()
        if pid == 0:
            # We're the child, we'll exec
            # Put ourselves into our own pgrp, for sanity
            os.setpgrp()

            # Configure STDOUT and STDERR
            self.logmanager.setup_stdout()
            self.logmanager.setup_stderr()

            if self.uid != None:
                try:
                    os.setuid(self.uid)
                except OSError:  # no permission
                    sys.stderr.write("Must be root to set UID!")
                    os._exit(1)

                # Now that we're the child and the child's PID setup
                # the environment for child scripts.
                userpwd = pwd.getpwuid(self.uid)
                os.environ['HOME'] = userpwd.pw_dir
                os.environ['USER'] = userpwd.pw_name

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
            if self.stop_running:
                return

            if self.collect_stats:
                self.child_proc.update_usage(deep=True)

            restarted = False

            for constraint in self.constraints:
                if constraint.check_violation(self.child_proc):
                    if self.child_violation_occured(constraint):
                        print "Restarting child command %s" % self.command
                        self.start_process()
                        restarted = True

            if not self.child_proc.is_alive():
                # The child proc could have died inbetween checking
                # constraints and now.  If there is a LivingConstraint
                # then fire it
                for constraint in self.constraints:
                    if str(constraint) == "LivingConstraint":
                        if self.child_violation_occured(constraint):
                            print "Restarting child command %s" % self.command
                            self.start_process()
                            restarted = True

                # If we restarted the child proc we don't want to set
                # child_running to False because... its True =P
                if not restarted:
                    # There is no living constraint and child is dead,
                    # so set running to false
                    print "Child exited on its own, not asked to " + \
                        "restart it, exiting"
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

        self.violations[str(violated_constraint)] += 1

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
