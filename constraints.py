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
        print "CPU Usage:"


class MemoryConstraint(Constraint):

    def __init__(self, mem_limit):
        super(MemoryConstraint, self).__init__("Memory Based Constraint",
                                             mem_limit)

    def CheckViolation(self, child_proc):
        print "Memory Usage:"
