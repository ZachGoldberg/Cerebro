import psi.process
import random
import subprocess


class TaskManager(object):

    def __init__(self, task_definition, log_location):
        self.auto_start = task_definition.get('auto_start', False)
        self.restart = task_definition.get('restart', False)
        self.max_restarts = task_definition.get('max_restarts', -1)
        self.ensure_alive = task_definition.get('ensure_alive', False)
        self.poll_interval = task_definition.get('poll_interval', 0.1)
        self.cpu = task_definition.get('cpu')
        self.mem = task_definition.get('mem')
        self.time_limit = task_definition.get('time_limit')
        self.uid = task_definition.get('uid')
        self.command = task_definition['command']

        self.http_monitoring = True
        self.http_monitoring_port = None
        self.stdout_location = log_location
        self.stderr_location = log_location

        self.was_started = False

        self.id = "%s (%s)" % (self.command, int(10000 * random.random()))
        self.sitter_stdout = "%s/%s.stdout" % (log_location, self.id)
        self.sitter_stderr = "%s/%s.stderr" % (log_location, self.id)

        self.process = None

    def set_port(self, port):
        self.http_monitoring_port = port

    def initialize(self):
        if self.auto_start:
            self.start()

    def stdall(self):
        stdout = open(self.sitter_stdout).read()
        stderr = open(self.sitter_stderr).read()
        return stdout, stderr

    def is_running(self):
        return not bool(self.process.poll())

    def start(self):
        args = ["run_tasksitter"]

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
        args.append("\"%s\"" % self.command)

        args.append("--http-monitoring")
        args.append("--http-monitoring-port=%s" % self.http_monitoring_port)

        args.append("--stdout-location=%s" % self.stdout_location)
        args.append("--stderr-location=%s" % self.stderr_location)

        print "Executing command %s" % args

        self.process = subprocess.Popen(
            args,
            stdout=open(self.sitter_stdout, 'w'),
            stderr=open(self.sitter_stderr, 'w'))

        self.was_started = True

        print "%s Started.  HTTP Monitoring: http://localhost:%s" % (
            self.command,
            self.http_monitoring_port)
