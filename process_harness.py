"""
A class which encapsulates a process and a set of constraints
and ensures that they are constantly fulfilled.
"""
import threading
import time


class ProcessHarness(object):
    def __init__(self, child_proc, constraints):
        self.child_proc = child_proc
        self.constraints = constraints

    def DoMonitoring(self):
        while True:
            # Check that child PID still exists
            if not self.child_proc.IsAlive():
                return

            for constraint in self.constraints:
                if constraint.CheckViolation(self.child_proc):
                    self.child_proc.ForceExit()

            time.sleep(.1)

    def BeginMonitoring(self):
        """Split off a thread to monitor the child process"""
        monitoring_thread = threading.Thread(target=self.DoMonitoring,
                                             name='Child Monitoring')

        monitoring_thread.start()

    def WaitForChildToFinish(self):
        return self.child_proc.WaitForCompletion()
