import taskmanager

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

    def next_port(self):
        bound = False
        while not bound:
            try:
                sok = socket.socket(socket.AF_INET)
                sok.bind(("0.0.0.0", self.starting_port))
                sok.close()
                bound = True
            except:
                self.starting_port += 1
                if self.starting_port > 50000:
                    self.starting_port = 40000

        return self.starting_port

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
        for task in self.tasks.values():
            print "Initializing %s" % task.id
            task.initialize()

        while True:
            for task in self.tasks.values():
                if task.was_started and not task.is_running():
                    self.restart_task(task.id)
            time.sleep(1)
