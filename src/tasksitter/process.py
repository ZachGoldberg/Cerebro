"""
A class to encapsulate data about a process
"""
import datetime
import os
import resource
import signal
import sys
import time


class Process(object):
    """
    An object representing the child process or running task
    """
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
        self.proc_stats = []
        self.start_time = datetime.datetime.now()

    def is_alive(self):
        """
        Check if the child process is alive
        """
        try:
            os.waitpid(self.pid, os.WNOHANG)
            return True
        except OSError:
            return False
        except KeyboardInterrupt:
            print "WAA" * 100
            self.force_exit()

    def force_exit(self):
        """
        Send a SIGKILL to the child process
        """
        print "Killing Process %s" % self.pid
        try:
            os.kill(self.pid, signal.SIGKILL)
        except OSError:
            # Already dead
            pass

        try:
            os.killpg(self.pid, signal.SIGKILL)
        except OSError:
            # Already dead
            pass

    def wait_for_completion(self):
        """
        Wait for the child process to finish.
        Returns:
         None, None if the child is already dead
         return value of os.waitpid() if we wait for it to exit
        """
        if not self.is_alive():
            return None, None

        try:
            return os.waitpid(self.pid, 0)
        except OSError:
            # child already exited
            return 0, 0

    @classmethod
    def get_system_cpu_usage(cls):
        """
        Read out an array of info from /proc/stat
        utime, nicetime, stime, idle, iowate, irq, softiq
        """

        cpu_stats = file("/proc/stat", "r").readline()
        columns = cpu_stats.replace("cpu", "").split(" ")
        return [int(a) for a in columns if a]

    def get_proc_cpu_usage(self, deep=False):
        """
        Read out an array of info from /proc/pid/stat
        utime, nicetime, stime, idle, iowate, irq, softiq
        """

        def get_proc_cpu(pid, pgrp=None):
            columns = self.proc_stats[pid]
            if not pgrp or pgrp == int(columns[4]):
                return [int(c) for c in
                        [columns[13], 0, columns[14], 0, 0, 0, 0]]
            else:
                return [0] * 7

        if not deep:
            return get_proc_cpu(self.pid)

        # Deep means get the cpu usage for all processes in our pgrp
        # Best way I can figure to do this is to check ALL procs in
        # the kernel process table, sadface.
        cpu_usage = [0] * 7
        for proc in self.proc_stats.keys():
            usage = get_proc_cpu(int(proc), self.pid)
            for index, i in enumerate(usage):
                cpu_usage[index] += i

        return cpu_usage

    def get_proc_stats(self):
        """
        Cache /proc/ID/stat so we only have to read it once
        """
        stats = {}
        for proc in os.listdir('/proc'):
            try:
                raw_stats = file("/proc/%d/stat" % int(proc), "r").readline()
                stats[int(proc)] = raw_stats.split(" ")
            except ValueError:
                # proc isn't a pid
                pass

        return stats

    def get_proc_mem_usage(self, deep=False):
        """
        Get memory usage of the child process
        """

        def get_proc_mem(pid, pgrp=None):
            columns = self.proc_stats[pid]
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
        for proc in self.proc_stats.keys():
            usage = get_proc_mem(proc, self.pid)
            mem_usage[0] += usage[0]
            mem_usage[1] += usage[1]

        # Returns vmem, res
        return mem_usage

    def calculate_cpu_usage(self):
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

        # Include system time and idle time
        system_sys_time_diff = (sum(self.system_usage[2:3]) -
                                sum(self.last_system_usage[2:3])) or 1

        #print "system:", system_sys_time_diff, system_user_time_diff
        #print "proc:", proc_sys_time_diff, proc_user_time_diff

        user_time_perc = float(proc_user_time_diff) / system_user_time_diff
        sys_time_perc = float(proc_sys_time_diff) / system_sys_time_diff

        self.cpu_usage = user_time_perc + sys_time_perc

        #print self.cpu_usage
        return self.cpu_usage

    def update_usage(self, deep=False):
        """Update process usage.

        Only updates at most once per 0.1 seconds

        Return: True if we updated, otherwise False
        """

        now = time.time()
        if now - self.last_usage_update > 0.1:
            self.previous_update_time = self.last_usage_update
            self.last_usage = self.usage

            try:
                self.proc_stats = self.get_proc_stats()
                self.usage = self.get_proc_cpu_usage(deep)
                self.last_system_usage = self.system_usage
                self.system_usage = Process.get_system_cpu_usage()
                self.mem_usage = self.get_proc_mem_usage(deep)
            except IOError:
                # Process died and /proc/PID no longer exists
                return

            self.calculate_cpu_usage()
            self.last_usage_update = time.time()
            return True

        return False
