import logging
import random
import socket
import threading
import time
import requests

from sittercommon.machinedata import MachineData


class ClusterSitter(object):
    def __init__(self):
        self.worker_thread_count = 1
        # list of tuples (MachineMonitor, ThreadObj)
        self.monitors = []
        self.machines = {}
        self.zones = []

    def add_machines(self, machines):
        monitored_machines = [MonitoredMachine(m) for m in machines]

        # Spread them out evenly across threads
        threads_to_use = self.worker_thread_count
        machines_per_thread = int(len(machines) / threads_to_use) or 1

        if machines_per_thread * self.worker_thread_count > len(machines):
            threads_to_use = random.sample(self.monitors, len(machines))

        for index, monitor in enumerate(self.monitors[:threads_to_use]):
            monitor[0].add_machines(
                monitored_machines[index * machines_per_thread:(index + 1): \
                             machines_per_thread])

    def start(self):
        logging.info("Spinning up %s monitoring threads" % (
                self.worker_thread_count))
        # Spin up all the monitoring threads
        for threadnum in range(self.worker_thread_count):
            machinemonitor = MachineMonitor(parent=self,
                                            number=threadnum)
            thread = threading.Thread(target=self._run_monitor,
                                      args=(machinemonitor, ))
            thread.start()
            self.monitors.append((machinemonitor, thread))

        self.calculator = threading.Thread(target=self._calculator)
        self.calculator.start()

    def _calculator(self):
        self.calculate_idle_machines()
        time.sleep(500)

    def add_job(self, job):
        # Step 1: Ensure we have enough machines in each SFZ
        # Step 1a: Check for idle machines and reserve as we find them
        new_machines = []
        reserved_machines = []
        for zone in job.get_shared_fate_zones():
            idle_available = self.get_idle_machines_in_zone(zone)
            idle_required = job.get_required_machines_in_zone(zone)
            required_new_machine_count = (len(idle_required) -
                                          len(idle_available))
            usable_machines = []
            if required_new_machine_count > 0:
                spawned_machines = self.begin_adding_machines(
                    zone,
                    required_new_machine_count)

                # len(spawned) + len(idle_available) = len(idle_required)
                usable_machines.extend(spawned_machines)
                usable_machines.extend(idle_available)
                new_machines.extend(spawned_machines)
            else:
                # idle_available > idle_required, so use just as many
                # as we need
                usable_machines = idle_available[:len(idle_required)]

            # Now reserve part of the machine for this job
            reserved_machines.extend(usable_machines)
            for machine in usable_machines:
                machine.start_task(job)

        # Step 2: Wait for new machines to be available
        ready = False
        while not ready:
            not_ready = 0
            for machine in new_machines:
                if not machine.is_available():
                    not_ready += 1

            if not_ready == 0:
                ready = True
                break

            logging.info("Waiting for %s more machines to be available" % \
                             not_ready)
            time.sleep(5)

        # Done!
        return reserved_machines

    def begin_adding_machines(self, zone, count):
        # This should run some kind of modular procedure
        # to bring up the machines, ASYNCHRONOUSLY (in a new thread?)
        # and return objects representing the machiens on their way up.
        pass

    def get_idle_machines_in_zone(self, zone):
        return self.idle_machines[zone]

    def calculate_idle_machines(self):
        idle_machines = {}
        for zone in self.zones:
            idle_machines[zone] = []
            for machine in self.machines[zone]:
                tasks = machine.get_running_tasks()
                if not tasks:
                    idle_machines[zone].append(machine)

        # The DICT swap must be atomic, or else another
        # thread could get a bad value during calculation.
        self.idle_machines = idle_machines

    def _run_monitor(self, monitor):
        # Assume we're in our own thread here
        monitor.start()


class MachineProfile(object):
    def __init__(self, cpu=None, mem=None):
        self.cpu = cpu
        self.mem = mem


class ProductionJob(object):
    def __init__(self,
                 task_configuration,
                 deployment_layout):
        # The config to pass to a machinesitter / tasksitter
        self.task_configuration = task_configuration

        # A mapping of SharedFateZoneObj : (# Jobs, CPU, Mem)
        self.deployment_layout = deployment_layout

    def get_required_machines_in_zone(self, zone):
        zoneinfo = self.deployment_layout[zone]
        profiles = []
        for _ in range(zoneinfo[0]):
            profiles.append(MachineProfile(cpu=zoneinfo[1],
                                           mem=zoneinfo[2]))

        return profiles

    def get_name(self):
        return self.task_configuration['name']


class HasMachineSitter(object):
    """
    Everything is asynchronous -- always returns a request
    object that can be run later.
    """
    def __init__(self):
        self.machinesitter_port = None
        self.hostname = None
        self.datamanager = None
        self.historic_data = []

    def _api_start_task(self, name):
        pass

    def _api_identify_sitter(self, port):
        logging.info("Attempting to find a machinesitter at %s:%s" % (
                self.hostname, port))
        try:
            sock = socket.socket(socket.AF_INET)
            sock.connect(("localhost", port))
            sock.close()
            logging.info("Connected successfully to %s:%s" % (
                    self.hostname, port))

            self.machinesitter_port = port
            self.datamanager = MachineData("http://%s:%s" % (self.hostname,
                                                      port))
            return True
        except:
            logging.info("Connection failed to %s:%s" % (self.hostname, port))
            return False

    def _api_run_request(self, request):
        """
        Explicitly run the async object
        """
        #result = async.map(request)

    def _api_get_endpoint(self, path):
        return "http://%s:%s/%s" % (self.hostname,
                                    self.machinesitter_port,
                                    path)

    def _api_get_stats(self):
        print self.datamanager.reload().keys()
        self.historic_data.append(self.datamanager.tasks.copy())


class MonitoredMachine(HasMachineSitter):
    """
    An interface for a single
    machine to monitor.  Some functions
    Should be implemented per cloud provider.
    Note: It it assumed that the MachineMonitor
    keeps all MonitoredMachines up to date, and that
    with the exception of functions explicitly about
    downloading data, all calls are accessing LOCAL
    CACHED data and NOT making network calls.
    """
    def __init__(self, hostname, machine_number=0, *args, **kwargs):
        super(MonitoredMachine, self).__init__(*args, **kwargs)
        self.hostname = hostname
        self.running_tasks = []
        self.machine_number = machine_number

    def get_running_tasks(self):
        """
        Return cached data about running task status
        """
        pass

    def start_task(self, job):
        # If the machine is up and initalized, make the API call
        # Otherwise, spawn a thread to wait for the machine to be up
        # and then make the call
        if self.is_initialized():
            self._api_run_request(self._api_start_task(job.get_name()))

    def begin_initialization(self):
        # Start an async request to find the
        # machinesitter port number
        # and load basic configuration
        pass

    def is_initialized(self):
        return self.machinesitter_port != None

    def __str__(self):
        return "%s:%s" % (self.hostname, self.machinesitter_port)


class MachineMonitor:
    def __init__(self, parent, number, monitored_machines=[]):
        self.clustersitter = parent
        self.number = number
        self.monitored_machines = monitored_machines
        logging.info("Initialized a machine monitor for %s" % (
                str(self.monitored_machines)))

    def add_machines(self, monitored_machines):
        self.initialize_machines(monitored_machines)
        self.monitored_machines.extend(monitored_machines)

    def initialize_machines(self, monitored_machines):
        # Find the sitter port for each machine, since it
        # is assigned in an incremental fashion depending
        # on what ports are available / how many sitters
        # on the machine etc.
        remaining_machines = [m for m in monitored_machines]
        next_port = 40000
        while remaining_machines != []:
            for machine in remaining_machines:
                found = machine._api_identify_sitter(next_port)
                if found:
                    remaining_machines.remove(machine)

            next_port += 1

    def start(self):
        self.initialize_machines(self.monitored_machines)

        while True:
            logging.info("Beggining machine monitoring poll for %s" % (
                    [str(a) for a in self.monitored_machines]))
            for machine in self.monitored_machines:
                if machine.is_initialized():
                    machine._api_get_stats()
            time.sleep(1)

