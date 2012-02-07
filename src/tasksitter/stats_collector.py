"""
A class which knows how to pull information out of a process harness
and process.  Exposes a simple interface to get all this information.
"""
import os
import socket
import threading


class StatsCollector(object):
    """
    Collect various kinds of statistics about running the child process.
    """
    def __init__(self, harness):
        self.hostname = socket.gethostname()
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

    def get_live_data(self):
        """
        Return live statistics about the harness and child process
        """
        data = {
            'process_start_time': str(self.harness.child_proc.start_time),
            'num_task_starts': self.harness.start_count,
            }

        for constraint, count in self.harness.violations.items():
            data['violated_%s' % constraint] = count

        data['cpu_usage'] = self.harness.child_proc.cpu_usage
        data['mem_usage_vmem'] = self.harness.child_proc.mem_usage[0]
        data['mem_usage_res'] = self.harness.child_proc.mem_usage[1]
        data['system_usage'] = self.harness.child_proc.system_usage

        return data

    def get_metadata(self):
        """
        Return fixed metadata about the child process
        """
        # Collect version data
        version_file = os.path.join(self.harness.launch_location, 'VERSION')
        file_version = None
        if os.path.exists(version_file):
            filedata = open(version_file)
            file_version = filedata.read()
            filedata.close()

        dir_version = os.path.basename(self.harness.launch_location)

        data = {'child_pid': self.harness.child_proc.pid,
                'task_start_time': str(self.harness.task_start),
                'max_restarts': self.harness.max_restarts,
                'command': self.harness.command,
                'restart': self.harness.restart,
                'file_version': file_version,
                'dir_version': dir_version,
                'launch_location': self.harness.launch_location,
                'constraints': ','.join(
                [str(c) for c in self.harness.constraints])
                }

        return data
