"""
A class which encapsulates a process and a set of constraints
and ensures that they are constantly fulfilled.
"""
import os
import threading
import time

import process


class ProcessHarness(object):

    def __init__(self, command, constraints, restart=False,
                 max_restarts=-1):
        self.command = command
        self.child_proc = None
        self.constraints = constraints
        self.restart = restart
        self.max_restarts = int(max_restarts)
        self.start_count = 0
        self.child_running = True
        self.StartProcess()

    def StartProcess(self):
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

    def DoMonitoring(self):
        while True:
            for constraint in self.constraints:
                if constraint.CheckViolation(self.child_proc):
                    self.ChildViolationOccured(constraint)
                    return

            if not self.child_proc.IsAlive():
                # The child proc could have died inbetween checking
                # constraints and now.  If there is a LivingConstraint
                # then fire it
                for c in self.constraints:
                    if str(c) == "LivingConstraint":
                        self.ChildViolationOccured(c)
                        return

                self.child_running = False
                return

            time.sleep(.1)

    def ChildViolationOccured(self, violated_constraint):
        print "Violated Constraint %s" % str(violated_constraint)
        if violated_constraint.kill_on_violation:
            self.child_proc.ForceExit()

        if self.restart:
            if self.start_count <= self.max_restarts:
                print "Restarting child command %s" % self.command
                self.StartProcess()
                self.DoMonitoring()
            else:
                self.child_running = False
        else:
            self.child_running = False

    def BeginMonitoring(self):
        """Split off a thread to monitor the child process"""
        monitoring_thread = threading.Thread(target=self.DoMonitoring,
                                             name='Child Monitoring')

        monitoring_thread.start()

    def WaitForChildToFinish(self):
        code = 0
        while self.child_running:
            _, code = self.child_proc.WaitForCompletion()

        return code
