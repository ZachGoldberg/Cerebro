"""
A class to encapsulate data about a process
"""
import os


class Process(object):
    def __init__(self, pid):
        self.pid = pid

    def IsAlive(self):
        try:
            os.waitpid(self.pid, os.WNOHANG)
            return True
        except OSError:
            return False

    def WaitForCompletion(self):
        return os.waitpid(self.pid, 0)
