"""
Define a class which represents a constraint on the sub task
"""


class Constraint(object):
    """A Constraint on a task."""
    def __init__(self, name, value):
        self.name = name
        self.value = value
        self.kill_on_violation = True

    def CheckViolation(self, child_proc):
        print "check generic violation %s" % child_proc.pid
        return 0


class LivingConstraint(Constraint):
    def __init__(self):
        super(LivingConstraint, self).__init__("Child is alive? Constraints",
                                               True)
        self.kill_on_violation = False

    def CheckViolation(self, child_proc):
        return child_proc.IsAlive() != self.value

    def __str__(self):
        return "LivingConstraint"


class CPUConstraint(Constraint):

    def __init__(self, cpu_limit):
        super(CPUConstraint, self).__init__("CPU Based Constraint",
                                             cpu_limit)

    def CheckViolation(self, child_proc):
        """
        Calculate child CPU usage and see if it violates the constraint.
        """
        if child_proc.UpdateUsage(deep=True):
            print child_proc.cpu_usage, self.value
            if child_proc.cpu_usage > float(self.value):
                print "CPU Limit Exceeded"
                return True

        return False

    def __str__(self):
        return "CPU Constraint (%s)" % self.value


class MemoryConstraint(Constraint):

    def __init__(self, mem_limit):
        # convert MB to bytes
        mem_limit = int(mem_limit) * 1024 * 1024
        super(MemoryConstraint, self).__init__("Memory Based Constraint",
                                             mem_limit)

    def CheckViolation(self, child_proc):
        if child_proc.UpdateUsage(deep=True):
            print child_proc.mem_usage, self.value
            if child_proc.mem_usage[1] > self.value:
                print "Memory Limit Exceeded"
                return True
            return False

    def __str__(self):
        return "Memory Constraint (%s MB)" % (self.value / 1024 / 1024)
