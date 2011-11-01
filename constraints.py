"""
Define a class which represents a constraint on the sub task
"""


class Constraint(object):
    """A Constraint on a task."""
    def __init__(self, name, value):
        self.name = name
        self.value = value

    def CheckViolation(self, child_proc):
        print "check generic violation %s" % child_proc.pid
        return 0


class CPUConstraint(Constraint):

    def __init__(self, cpu_limit):
        super(CPUConstraint, self).__init__("CPU Based Constraint",
                                             cpu_limit)

    def CheckViolation(self, child_proc):
        """
        Calculate child CPU usage and see if it violates the constraint.
        """
        if child_proc.UpdateUsage():
            if child_proc.cpu_usage > float(self.value):
                print "CPU Limit Exceeded"
                return True

        return False


class MemoryConstraint(Constraint):

    def __init__(self, mem_limit):
        # convert MB to bytes
        mem_limit = int(mem_limit) * 1024 * 1024
        super(MemoryConstraint, self).__init__("Memory Based Constraint",
                                             mem_limit)

    def CheckViolation(self, child_proc):
        if child_proc.UpdateUsage():
            print child_proc.mem_usage
            print self.value
            if child_proc.mem_usage[1] > self.value:
                print "Memory Limit Exceeded"
                return True
            return False
