"""
A class which knows how to pull information out of a process harness
and process.  Exposes a simple interface to get all this information.
"""
import threading


class StatsCollector(object):
    """
    Collect various kinds of statistics about running the child process.
    """
    def __init__(self, harness):
        self.harness = harness
        self.thread = None

    def start(self):
        """
        Public interface to start the collection thread
        """
        self.thread = threading.Thread(target=self._start_collecting)
        self.thread.start()

    def _start_collecting(self):
        """
        Collect stats as the process is running.
        """
        pass

    def get_logfile_names(self):
        filenames = {}
        for i in range(self.harness.start_count):
            if self.harness.stdout_location != "-":
                filenames["stdout.%d" % i] = self.harness._calculate_filename(
                    self.harness.stdout_location,
                    False,
                    str(i))

            if self.harness.stderr_location != "-":
                filenames["stderr.%d" % i] = self.harness._calculate_filename(
                    self.harness.stderr_location,
                    True,
                    str(i))

        return filenames

    def get_metadata(self):
        """
        Return metadata about the child process
        """
        data = {'child_pid': self.harness.child_proc.pid,
                'task_start_time': str(self.harness.task_start),
                'process_start_time': str(self.harness.process_start),
                'num_task_starts': self.harness.start_count,
                'max_restarts': self.harness.max_restarts,
                'command': self.harness.command,
                'restart': self.harness.restart,
                'constraints': ','.join(
                [str(c) for c in self.harness.constraints])
                }

        return data
