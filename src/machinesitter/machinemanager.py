import sittercommon.http_monitor as http_monitor
import sittercommon.logmanager as logmanager
import machinestats
import taskmanager

import os
import threading
import time
import socket


class MachineManager(object):

    def __init__(self, log_location, starting_port=40000):
        self.tasks = {}
        self.thread = None
        self.should_stop = False
        self.starting_port = starting_port
        self.log_location = log_location
        self.start_count = 0
        self.logmanager = logmanager.LogManager(stdout_location=log_location,
                                                stderr_location=log_location)
        self.logmanager.set_harness(self)

        self.parent_pid = os.getpid()
        self.stats = machinestats.MachineStats(self)
        self.http_monitor = http_monitor.HTTPMonitor(self.stats,
                                                     self,
                                                     self.next_port())

        self.http_monitor.add_handler('/start_task', self.remote_start_task)

    def remote_start_task(self, args):
        if not 'task_id' in args:
            return "Error"

        if not args['task_id'] in self.tasks:
            return "Error"

        if not self.tasks[args['task_id']].is_running():
            self.tasks[args['task_id']].start()
            return "%s started" % args['task_id']
        else:
            return "Already running"

    def next_port(self):
        works = None
        while not works:
            try:
                sok = socket.socket(socket.AF_INET)
                sok.bind(("0.0.0.0", self.starting_port))
                sok.close()
                works = self.starting_port
                self.starting_port += 1
            except:
                self.starting_port += 1
                if self.starting_port > 50000:
                    self.starting_port = 40000

        return works

    def add_new_task(self, task_definition):
        task = taskmanager.TaskManager(task_definition,
                                       self.log_location)
        task.set_port(self.next_port())

        self.tasks[task.id] = task

    def start_task(self, task_id):
        self.tasks[task_id].start()

    def start(self):
        self.thread = threading.Thread(target=self._run)
        self.thread.start()

    def stop(self):
        self.should_stop = True

    def restart_task(self, task_id):
        task = self.tasks[task_id]
        print "Task %s died" % task_id
        logs = task.stdall()
        print "Task Stdout:\n %s" % logs[0]
        print "Task Stderr:\n %s" % logs[1]

        task.set_port(self.next_port())
        task.start()

    def _run(self):
        self.http_monitor.start()
        print "Machine Sitter Monitor started at " + \
            "http://localhost:%s" % self.http_monitor.port

        for task in self.tasks.values():
            print "Initializing %s" % task.id
            task.initialize()

        while True:
            for task in self.tasks.values():
                if task.was_started and not task.is_running():
                    self.restart_task(task.id)
            time.sleep(1)
