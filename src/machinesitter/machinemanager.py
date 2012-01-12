import sittercommon.http_monitor as http_monitor
import sittercommon.logmanager as logmanager
import machinestats
import taskmanager

import json
import os
import signal
import socket
import threading
import time


class MachineManager(object):

    def __init__(self, task_definition_file, log_location,
                 starting_port=40000, daemon=False):
        self.tasks = {}
        self.task_definition_file = task_definition_file
        self.daemon = daemon
        self.thread = None
        self.should_stop = False
        self.starting_port = starting_port
        self.log_location = log_location
        self.start_count = 0
        self.command = "machinemanager"
        self.logmanager = logmanager.LogManager(stdout_location=log_location,
                                                stderr_location=log_location)
        self.logmanager.set_harness(self)

        self.parent_pid = os.getpid()
        self.stats = machinestats.MachineStats(self)
        self.http_monitor = http_monitor.HTTPMonitor(self.stats,
                                                     self,
                                                     self.next_port())

        self.http_monitor.add_handler('/start_task', self.remote_start_task)
        self.http_monitor.add_handler('/stop_task', self.remote_stop_task)
        self.http_monitor.add_handler('/add_task', self.remote_add_task)

        print "Adding signals"
        signal.signal(signal.SIGTERM, self.exit_now)
        signal.signal(signal.SIGINT, self.exit_now)

    def exit_now(self, *_):
        print "Caught Control-C, killing children and exiting"
        for task in self.tasks.values():
            if task.is_running():
                task.stop()

        os._exit(0)

    def remote_add_task(self, args):
        definition = {}
        for opt in taskmanager.TaskManager.required_fields:
            definition[opt] = args.get(opt)
            if not definition[opt]:
                return "Required argument %s missing" % opt
        for opt in taskmanager.TaskManager.optional_fields:
            if opt in args:
                definition[opt] = args[opt]

        for k, v in definition.items():
            if v.lower() == 'true':
                definition[k] = True
            if v.lower() == 'false':
                definition[k] = False

        task = self.add_new_task(definition)
        task.initialize()

        config = self.write_out_task_definitions()

        return "OK | %s" % json.dumps(config)

    def remote_stop_task(self, args):
        if not 'task_name' in args:
            return "Error"

        if not args['task_name'] in self.tasks:
            return "Error"

        task = self.tasks[args['task_name']]
        if not task.was_started:
            return "Already stopped"

        task.stop()
        self.collect_old_task_logs(task)
        return "Stopped"

    def remote_start_task(self, args):
        if not 'task_name' in args:
            return "Error"

        if not args['task_name'] in self.tasks:
            return "Error"

        task = self.tasks[args['task_name']]
        if not task.is_running():
            task.set_port(self.next_port())
            task.start()
            return "%s started" % args['task_name']
        else:
            return "Already running"

    def write_out_task_definitions(self):
        config = {'log_location': self.log_location}
        task_definitions = []
        for task in self.tasks:
            task_definitions.append(self.tasks[task].to_dict())

        config['task_definitions'] = task_definitions

        file = open(self.task_definition_file, 'w')
        file.write(json.dumps(config))
        file.close()

        return config

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

        self.tasks[task.name] = task
        return task

    def start_task(self, task_name):
        self.tasks[task_name].start()

    def start(self):
        self.thread = threading.Thread(target=self._run)
        self.thread.start()

    def stop(self):
        self.should_stop = True

    def collect_old_task_logs(self, task):
        for filename, fileloc in task.get_old_logfilenames().items():
            self.logmanager.add_logfile("Terminated %s (%s @ %s) %s" % (
                    task.name,
                    task.command,
                    task.get_last_pid(),
                    filename
                    ), fileloc)

    def task_finished(self, task):
        print "Task %s finished" % task.name
        logs = task.stdall()
        print "Task Stdout:\n %s" % logs[0]
        print "Task Stderr:\n %s" % logs[1]
        self.collect_old_task_logs(task)

    def restart_task(self, task):
        task.set_port(self.next_port())
        task.start()

    def _run(self):
        self.http_monitor.start()
        print "Machine Sitter Monitor started at " + \
            "http://localhost:%s" % self.http_monitor.port

        stdout_loc = self.logmanager._calculate_filename(
            self.logmanager.stdout_location)
        stderr_loc = self.logmanager._calculate_filename(
            self.logmanager.stderr_location, True)
        self.logmanager.add_logfile("machinemanager-stdout", stdout_loc)
        self.logmanager.add_logfile("machinemanager-stderr", stderr_loc)

        if self.daemon:
            print "Redirecting machine sitter output to %s, stderr: %s" % (
                stdout_loc, stderr_loc)

            self.logmanager.setup_stdout()
            self.logmanager.setup_stderr()

        for task in self.tasks.values():
            print "Initializing %s" % task.name
            task.initialize()
            self.logmanager.add_logfile(
                "%s-stdout" % task.name, task.sitter_stdout)

            self.logmanager.add_logfile(
                "%s-stderr" % task.name, task.sitter_stderr)

        while True:
            for task in self.tasks.values():
                if task.was_started and not task.is_running():
                    self.task_finished(task)
                    if not task.allow_exit:
                        self.restart_task(task)
                    else:
                        task.was_started = False

            time.sleep(1)