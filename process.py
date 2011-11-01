"""
A class to encapsulate data about a process
"""
import os
import signal
import time


def GetSystemCPUUsage():
    """
    Read out an array of info from /proc/stat
    utime, nicetime, stime, idle, iowate, irq, softiq
    """
    cpu_stats = file("/proc/stat", "r").readline()
    columns = cpu_stats.replace("cpu", "").split(" ")
    return map(int, filter(None, columns))


def GetProcCPUUsage(pid):
    """
    Read out an array of info from /proc/pid/stat
    utime, nicetime, stime, idle, iowate, irq, softiq
    """
    cpu_stats = file("/proc/%d/stat" % pid, "r").readline()
    columns = cpu_stats.split(" ")
    return map(int, [columns[13], 0, columns[14], 0, 0, 0, 0])


class Process(object):
    def __init__(self, pid):
        self.pid = pid

        self.previous_update_time = 0
        self.last_usage_update = 0
        self.usage = None
        self.last_usage = None

        self.system_usage = None
        self.last_system_usage = None

    def IsAlive(self):
        try:
            os.waitpid(self.pid, os.WNOHANG)
            return True
        except OSError:
            return False

    def ForceExit(self):
        os.kill(self.pid, signal.SIGKILL)

    def WaitForCompletion(self):
        return os.waitpid(self.pid, 0)

    def UpdateUsage(self):
        """Update process usage.

        Only updates at most once per 0.1 seconds

        Return: True if we updated, otherwise False
        """

        now = time.time()
        if now - self.last_usage_update > 0.1:
            self.previous_update_time = self.last_usage_update
            self.last_usage_update = now
            self.last_usage = self.usage
            self.usage = GetProcCPUUsage(self.pid)
            self.last_system_usage = self.system_usage
            self.system_usage = GetSystemCPUUsage()

            return True

        return False
