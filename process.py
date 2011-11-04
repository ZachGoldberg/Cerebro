"""
A class to encapsulate data about a process
"""
import os
import resource
import signal
import time


class Process(object):

    def __init__(self, pid):
        self.pid = pid

        self.previous_update_time = 0
        self.last_usage_update = 0
        self.usage = None
        self.last_usage = None
        self.system_usage = None
        self.last_system_usage = None
        self.cpu_usage = 0
        self.mem_usage = None

    def IsAlive(self):
        try:
            os.waitpid(self.pid, os.WNOHANG)
            return True
        except OSError:
            return False

    def ForceExit(self):
        os.kill(self.pid, signal.SIGKILL)
        os.killpg(self.pid, signal.SIGKILL)

    def WaitForCompletion(self):
        try:
            return os.waitpid(self.pid, 0)
        except OSError:
            # child already exited
            return 0, 0

    @classmethod
    def GetSystemCPUUsage(klass):
        """
        Read out an array of info from /proc/stat
        utime, nicetime, stime, idle, iowate, irq, softiq
        """

        cpu_stats = file("/proc/stat", "r").readline()
        columns = cpu_stats.replace("cpu", "").split(" ")
        return map(int, filter(None, columns))

    def GetProcCPUUsage(self, deep=False):
        """
        Read out an array of info from /proc/pid/stat
        utime, nicetime, stime, idle, iowate, irq, softiq
        """

        def get_proc_cpu(pid, pgrp=None):
            cpu_stats = file("/proc/%d/stat" % pid, "r").readline()
            columns = cpu_stats.split(" ")
            if not pgrp or pgrp == int(columns[4]):
                return map(int, [columns[13], 0, columns[14], 0, 0, 0, 0])
            else:
                return [0] * 7

        if not deep:
            return get_proc_cpu(self.pid)

        # Deep means get the cpu usage for all processes in our pgrp
        # Best way I can figure to do this is to check ALL procs in
        # the kernel process table, sadface.
        cpu_usage = [0] * 7
        for proc in os.listdir('/proc'):
            try:
                usage = get_proc_cpu(int(proc), self.pid)
                for index, i in enumerate(usage):
                    cpu_usage[index] += i
            except ValueError:
                # proc isn't a pid
                pass

        return cpu_usage

    def GetProcMemUsage(self, deep=False):

        def get_proc_mem(pid, pgrp=None):
            mem_stats = file("/proc/%d/stat" % pid, "r").readline()
            columns = mem_stats.split(" ")
            if not pgrp or pgrp == int(columns[4]):
                return [int(columns[22]), (int(columns[23])
                                           * resource.getpagesize())]
            else:
                return [0, 0]

        if not deep:
            return get_proc_mem(self.pid)

        # Deep means get the memory for all processes in our pgrp
        # Best way I can figure to do this is to check ALL procs in
        # the kernel process table, sadface.
        mem_usage = [0, 0]
        for proc in os.listdir('/proc'):
            try:
                usage = get_proc_mem(int(proc), self.pid)
                mem_usage[0] += usage[0]
                mem_usage[1] += usage[1]
            except ValueError:
                # proc isn't a pid
                pass


        return mem_usage

    def CalculateCPUUsage(self):
        """
        Calculate CPU Usage for this process.

        Reference: http://stackoverflow.com/questions/1420426/
                   calculating-cpu-usage-of-a-process-in-linux

        Formula:
        utime_jiffys_proc_used / utime_jiffys_system_used +
        stime_jiffys_proc_used / stime_jiffys_system_used

        Return: True if child is in violation, false otherwise
        """
        if not self.usage or not self.last_usage:
            return

        proc_user_time_diff = (self.usage[0] -
                               self.last_usage[0])

        system_user_time_diff = ((self.system_usage[0] +
                                  self.system_usage[1]) -
                                 (self.last_system_usage[0] +
                                  self.last_system_usage[1])) or 1

        proc_sys_time_diff = (self.usage[2] -
                              self.last_usage[2])

        system_sys_time_diff = (self.system_usage[2] -
                                self.last_system_usage[2]) or 1

        #print "system:", system_sys_time_diff, system_user_time_diff
        #print "proc:", proc_sys_time_diff, proc_user_time_diff

        user_time_perc = float(proc_user_time_diff) / system_user_time_diff
        sys_time_perc = float(proc_sys_time_diff) / system_sys_time_diff

        self.cpu_usage = user_time_perc + sys_time_perc

        #print self.cpu_usage
        return self.cpu_usage

    def UpdateUsage(self, deep=False):
        """Update process usage.

        Only updates at most once per 0.1 seconds

        Return: True if we updated, otherwise False
        """

        now = time.time()
        if now - self.last_usage_update > 0.1:
            self.previous_update_time = self.last_usage_update
            self.last_usage_update = now
            self.last_usage = self.usage
            try:
                self.usage = self.GetProcCPUUsage(deep)
                self.last_system_usage = self.system_usage
                self.system_usage = Process.GetSystemCPUUsage()
                self.mem_usage = self.GetProcMemUsage(deep)
            except IOError:
                # Process died and /proc/PID no longer exists
                return

            self.CalculateCPUUsage()

            return True

        return False
