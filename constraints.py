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
        if child_proc.UpdateUsage() and child_proc.last_usage:
            proc_user_time_diff = (child_proc.usage[0] -
                                   child_proc.last_usage[0])

            kernel_user_time_diff = ((child_proc.system_usage[0] +
                                      child_proc.system_usage[1]) -
                                     (child_proc.last_system_usage[0] +
                                      child_proc.last_system_usage[1])) or 1

            proc_sys_time_diff = (child_proc.usage[2] -
                                  child_proc.last_usage[2])

            kernel_sys_time_diff = (child_proc.system_usage[2] -
                                    child_proc.last_system_usage[2]) or 1

            #print "kernel:", kernel_sys_time_diff, kernel_user_time_diff
            #print "proc:", proc_sys_time_diff, proc_user_time_diff

            user_time_perc = float(proc_user_time_diff) / kernel_user_time_diff
            sys_time_perc = float(proc_sys_time_diff) / kernel_sys_time_diff

            total_usage = user_time_perc + sys_time_perc

            print total_usage, self.value

            if total_usage > float(self.value):
                print "CPU Limit Exceeded"
                return True

        return False


class MemoryConstraint(Constraint):

    def __init__(self, mem_limit):
        super(MemoryConstraint, self).__init__("Memory Based Constraint",
                                             mem_limit)

    def CheckViolation(self, child_proc):
        child_proc.UpdateUsage()
