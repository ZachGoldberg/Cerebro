"""
A class to encapsulate data about a process
"""
import os


class Process(object):
    def __init__(self, pid):
        self.pid = pid

    def IsAlive(self):
        return os.WIFEXITED(self.pid)

    def WaitForCompletion(self):
        return os.waitpid(self.pid, 0)
