import json
import simplejson
import subprocess
import time


class TaskManager(object):
    required_fields = ['name', 'command']
    optional_fields = [
        'auto_start',
        'restart',
        'max_restarts',
        'ensure_alive',
        'poll_interval',
        'allow_exit',
        'cpu',
        'mem',
        'time_limit',
        'uid']

    def __init__(self, task_definition, log_location, launch_location):
        self.auto_start = task_definition.get('auto_start', False)
        self.restart = task_definition.get('restart', False)
        self.max_restarts = task_definition.get('max_restarts', -1)
        self.ensure_alive = task_definition.get('ensure_alive', False)
        self.poll_interval = task_definition.get('poll_interval', 0.1)
        self.allow_exit = task_definition.get('allow_exit', False)
        self.cpu = task_definition.get('cpu')
        self.mem = task_definition.get('mem')
        self.time_limit = task_definition.get('time_limit')
        self.uid = task_definition.get('uid')
        self.command = task_definition['command']
        self.name = task_definition['name']
        self.launch_location = launch_location

        self.http_monitoring = True
        self.http_monitoring_port = None
        self.stdout_location = log_location
        self.stderr_location = log_location

        self.was_started = False

        self.sitter_stdout = "%s/%s.stdout" % (log_location, self.name)
        self.sitter_stderr = "%s/%s.stderr" % (log_location, self.name)

        self.process = None
        self.used_pids = []

    def __repr__(self):
        return json.dumps(self.to_dict())

    def to_dict(self):
        data = {}
        for opt in self.required_fields:
            data[opt] = getattr(self, opt)

        for opt in self.optional_fields:
            val = getattr(self, opt)
            if val != None:
                data[opt] = val

        return data

    def set_port(self, port):
        self.http_monitoring_port = port

    def initialize(self):
        if self.auto_start:
            self.start()

    def stdall(self):
        stdout = open(self.sitter_stdout).read()
        stderr = open(self.sitter_stderr).read()
        return stdout, stderr

    def get_old_logfilenames(self):
        stdout = open(self.sitter_stdout).readlines()
        print stdout
        try:
            return simplejson.loads(stdout[-1])
        except:
            return {}

    def is_running(self):
        if not self.process:
            return False

        return self.process.poll() == None

    def get_last_pid(self):
        return self.used_pids[-1]

    def stop(self):
        print "Stopping %s" % self.name
        self.was_started = False

        while self.is_running():
            if self.process:
                self.process.terminate()
            time.sleep(.1)

    def start(self):
        args = ["tasksitter"]

        if self.restart:
            args.append("--restart")

        if self.max_restarts:
            args.append("--max-restarts=%s" % self.max_restarts)

        if self.ensure_alive:
            args.append("--ensure-alive")

        if self.poll_interval:
            args.append("--poll-interval=%s" % self.poll_interval)

        if self.cpu:
            args.append("--cpu=%s" % self.cpu)

        if self.mem:
            args.append("--mem=%s" % self.mem)

        if self.time_limit:
            args.append("--time-limit=%s" % self.time_limit)

        if self.uid:
            args.append("--uid=%s" % self.uid)

        args.append("--command")
        args.append(self.command)

        args.append("--http-monitoring")
        args.append("--http-monitoring-port=%s" % self.http_monitoring_port)

        args.append("--stdout-location=%s" % self.stdout_location)
        args.append("--stderr-location=%s" % self.stderr_location)

        print "Executing command %s" % args

        self.process = subprocess.Popen(
            args,
            stdout=open(self.sitter_stdout, 'a'),
            stderr=open(self.sitter_stderr, 'a'))

        self.was_started = True
        self.used_pids.append(self.process.pid)

        print "Task '%s' Started.  Task Monitoring At: http://localhost:%s" % (
            self.command,
            self.http_monitoring_port)
