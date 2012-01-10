import threading
from requests import async


class ClusterSitter:
    def __init__(self):
        self.worker_thread_count = 1

    def start(self):
        # Spin up all the monitoring threads
        self.thread = threading.Thread(target=self._run)
        self.thread.start()

    def _run(self):
        pass


class ProductionJob(object):
    def __init__(self,
                 task_configuration,
                 deployment_layout):
        # The config to pass to a machinesitter / tasksitter
        self.task_configuration = task_configuration

        # A mapping of SharedFateZoneObj : # Jobs
        self.deployment_layout = deployment_layout


class MonitoredMachine(object):
    """
    An abstract interface for a single
    machine to monitor.  Should be implemented
    per cloud provider
    """
    def __init__(self, jobname, machine_number):
        self.jobname = jobname
        self.machine_number = machine_number
        self.machinesitter_port = None
        self.intialized = False

    def get_endpoint(self, path):
        return "http://%s:%s/%s" % (self.hostname,
                                    self.port,
                                    path)

    def begin_initialization(self):
        # Start an async request to find the 
        # machinesitter port number
        # and load basic configuration
        pass

    def is_initalized(self):
        return self.initialized

class MachineMonitor:
    def __init__(self, monitored_machines):
        self.monitored_machines = monitored_machines

    def start(self):
        for machine in machines:
            machine.begin_initialization()

        requests = []
        for machine in machines:
            if machine.is_initalized():
                requests.append(async.get(machine.get_endpoint('stats')))

        # Run all requests with a time limit
                                          
