"""
Manage the various logging facilities for a child process.
"""
import md5
import os
import random


class LogManager(object):
    """
    An object to manage logging facilities for the child
    """
    def __init__(self, stdout_location='-', stderr_location='-'):
        self.stdout_location = stdout_location
        self.stderr_location = stderr_location

        # Enable others to write to the log locations incase the child
        # Process is run as another user.
        os.system("chmod o+rwx %s" % stdout_location)
        os.system("chmod o+rwx %s" % stderr_location)
        self.harness = None
        self.extra_logfiles = {}

    def set_harness(self, harness):
        self.harness = harness

    def add_logfile(self, name, filename):
        self.extra_logfiles[name] = filename

    def setup_all(self):
        stdout_loc = self._calculate_filename(
            self.stdout_location)
        stderr_loc = self._calculate_filename(
            self.stderr_location, True)
        print "Redirecting sitter output to %s, stderr: %s" % (
            stdout_loc, stderr_loc)

        self.add_logfile("stdout", stdout_loc)
        self.add_logfile("stderr", stderr_loc)

        self.setup_stdout()
        self.setup_stderr()

    def setup_stdout(self):
        if self.stdout_location != '-':
            stdout = open(self._calculate_filename(self.stdout_location),
                          'w')
            stdout_fileno = stdout.fileno()
            os.dup2(stdout_fileno, 1)

    def setup_stderr(self):
        if self.stderr_location != '-':
            stderr = open(self._calculate_filename(self.stderr_location,
                                                   True),
                          'w')
            stderr_fileno = stderr.fileno()
            os.dup2(stderr_fileno, 2)

    def get_logfile_names(self):
        filenames = {}
        for i in range(self.harness.start_count):
            if self.stdout_location != "-":
                filenames["stdout.%d" % i] = self._calculate_filename(
                    self.stdout_location,
                    False,
                    str(i))

            if self.stderr_location != "-":
                filenames["stderr.%d" % i] = self._calculate_filename(
                    self.stderr_location,
                    True,
                    str(i))

        filenames.update(self.extra_logfiles)
        return filenames

    def _calculate_filename(self, directory, stderr=False,
                            number=None):
        if not os.path.exists(directory):
            os.makedirs(directory)

        filenum = number
        if not number and self.harness:
            filenum = self.harness.start_count

        parent_pid = os.getpid()

        if self.harness:
            payload = self.harness.command
            parent_pid = self.harness.parent_pid
        else:
            payload = str(random.random())

        payload += str(parent_pid)

        if stderr:
            payload += "err"

        name = "%s/%s.%s" % (directory,
                             md5.md5(payload).hexdigest(),
                             filenum)
        return name
