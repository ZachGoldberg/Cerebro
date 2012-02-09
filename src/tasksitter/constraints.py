"""
Define a class which represents a constraint on the sub task
"""
import datetime


class Constraint(object):
    """A Constraint on a task."""
    def __init__(self, name, value):
        self.name = name
        self.value = value
        self.kill_on_violation = True

    def check_violation(self, child_proc):
        """
        Look for a violation of a constraint

        Args: child_proc - a Process object

        Returns: True if in violation
        """
        print "check generic violation %s" % child_proc.pid
        return 0


class LivingConstraint(Constraint):
    """
    Ensure the child process is alive.
    """
    def __init__(self):
        super(LivingConstraint, self).__init__("Child is alive? Constraints",
                                               True)
        self.kill_on_violation = False

    def check_violation(self, child_proc):
        """
        Ensure the child process is alive.
        """
        return child_proc.is_alive() != self.value

    def __str__(self):
        return "LivingConstraint"


class TimeConstraint(Constraint):
    """
    Only allow a child process to live so long
    """
    def __init__(self, time_limit):
        super(TimeConstraint, self).__init__("Time Limit Constraint",
                                               datetime.timedelta(
                seconds=time_limit))

    def check_violation(self, child_proc):
        """
        Check how long a child has been alive.
        """
        now = datetime.datetime.now()
        return now - child_proc.start_time > self.value

    def __str__(self):
        return "TimeConstraint (%ss)" % self.value.seconds


class CPUConstraint(Constraint):
    """
    Ensure we satisfy a CPU based constraint.
    """
    def __init__(self, cpu_limit):
        super(CPUConstraint, self).__init__("CPU Based Constraint",
                                             cpu_limit)

    def check_violation(self, child_proc):
        """
        Calculate child CPU usage and see if it violates the constraint.
        """
        child_proc.update_usage(deep=True)

        if child_proc.cpu_usage > float(self.value):
            print "CPU Limit Exceeded"
            return True

        return False

    def __str__(self):
        return "CPU Constraint (%s)" % self.value


class MemoryConstraint(Constraint):
    """
    Ensure we satisfy a memory based constraint
    """
    def __init__(self, mem_limit):
        # convert MB to bytes
        mem_limit = int(mem_limit) * 1024 * 1024
        super(MemoryConstraint, self).__init__("Memory Based Constraint",
                                             mem_limit)

    def check_violation(self, child_proc):
        """ Check for using too much Memory"""
        child_proc.update_usage(deep=True)

        if child_proc.mem_usage[1] > self.value:
            print "Memory Limit Exceeded"
            return True
        return False

    def __str__(self):
        return "Memory Constraint (%s MB)" % (self.value / 1024 / 1024)
