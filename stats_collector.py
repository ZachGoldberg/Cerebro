"""
A class which knows how to pull information out of a process harness
and process.  Exposes a simple interface to get all this information.
"""
import threading

class StatsCollector(object):
    def __init__(self, harness):
        self.harness = harness
        self.thread = None

    def start(self):
        self.thread = threading.Thread(target=self._start_collecting)
        self.thread.start()

    def _start_collecting(self):
        pass

    def get_metadata(self):
        data = {'child_pid': self.harness.child_proc.pid,
                'task_start_time': str(self.harness.task_start),
                'process_start_time': str(self.harness.process_start),
                'num_task_starts': self.harness.start_count,
                'max_restarts': self.harness.max_restarts,
                'command': self.harness.command
                }

        return data
