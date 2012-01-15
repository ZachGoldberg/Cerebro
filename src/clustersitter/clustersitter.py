import logging
import os
import random
import socket
import threading
import time
import requests
from datetime import datetime

from providers.aws import AmazonEC2
from deploymentrecipe import DeploymentRecipe
from machineconfig import MachineConfig
from machinemonitor import MachineMonitor
from monitoredmachine import MonitoredMachine
from sittercommon import http_monitor
from sittercommon import logmanager
from sittercommon.machinedata import MachineData



"""
We have 3 kinds of data
1) Historical Performance Data
 -- This is monstrous, and timeseries based.  It makes sense
   to put this in some kind of RRDTool

2) Cluster Metadata -- what tasks go where
 -- Super critical,  pretty small.  Store in config and memory

3) Event level data -- what happpened
  -- Store in Django (SQL or Mongo)
"""

"""
In an ideal world this is written such that we always speak
in terms of CPUs and RAM requirements for each job.  This allows
us to put multiple jobs on a machine whilst keeping track
of total theoretical machine utilization.  However, for simplicities
sake we're cutting some corners (FOR NOW) and assuming one job
per machine.  These sections are noted with the comment
#!MACHINEASSUMPTION!
"""

class ProductionJob(object):
    def __init__(self,
                 task_configuration,
                 deployment_layout,
                 deployment_recipe,
                 recipe_options={}):
        # The config to pass to a machinesitter / tasksitter
        self.task_configuration = task_configuration

        # A mapping of SharedFateZoneObj : {'cpu': #CPU, 'mem': MB_Mem_Per_CPU}
        self.deployment_layout = deployment_layout

        #!MACHINEASSUMPTION!
        # Hack to make num_machines == num_cpu, for now.
        for zone in self.deployment_layout.keys():
            self.deployment_layout[zone]['num_machines'] = \
                self.deployment_layout[zone]['cpu']


    def get_shared_fate_zones(self):
        return self.deployment_layout.keys()

    def get_required_machines_in_zone(self, zone):
        """
        Note: this is not the TOTAL machines needed in this zone,
        just the total REMAINING machines in the zone.
        For the total machines in the zone do:
        job.deployment_layout[zonename]['num_machines']
        """
        #!MACHINEASSUMPTION!
        zoneinfo = self.deployment_layout[zone]
        profiles = []
#        for _ in range(zoneinfo['num_machines']):
#            profiles.append(MachineProfile(cpu=zoneinfo[1],
#                                           mem=zoneinfo[2]))

        return profiles

    def get_name(self):
        return self.task_configuration['name']



class ClusterSitter(object):
    def __init__(self, log_location, daemon, starting_port=30000):
        self.worker_thread_count = 2
        self.daemon = daemon

        # list of tuples (MachineMonitor, ThreadObj)
        self.monitors = []
        self.machines_by_zone = {}
        self.zones = []
        self.jobcatalog = {"byjob": {}, "bymachine": {}}
        self.jobs = []
        self.providers = {}

        self.orig_starting_port = starting_port
        self.next_port = starting_port
        self.start_count = 1
        self.command = "clustersitter"
        self.parent_pid = os.getpid()
        self.logmanager = logmanager.LogManager(stdout_location=log_location,
                                                stderr_location=log_location)
        self.logmanager.set_harness(self)

        # In seconds
        self.stats_poll_interval = 2
        self.stats = None
        self.http_monitor = http_monitor.HTTPMonitor(self.stats,
                                                     self,
                                                     self.get_next_port())

    def get_next_port(self):
        works = None
        while not works:
            try:
                sok = socket.socket(socket.AF_INET)
                sok.bind(("0.0.0.0", self.next_port))
                sok.close()
                works = self.next_port
                self.next_port += 1
            except:
                self.next_port += 1
                if self.next_port > self.orig_starting_port + 10000:
                    self.next_port = self.orig_starting_port
        return works

    def add_machines(self, machines):
        """
        """
        monitored_machines = []

        for m in machines:
            mm = MonitoredMachine(m)
            if not m.shared_fate_zone in self.machines_by_zone:
                self.machines_by_zone[m.shared_fate_zone] = []

            self.machines_by_zone[m.shared_fate_zone].append(mm)
            monitored_machines.append(mm)

        # Spread them out evenly across threads
        num_threads_to_use = self.worker_thread_count
        machines_per_thread = int(len(machines) / num_threads_to_use) or 1

        for index, monitor in enumerate(self.monitors[:num_threads_to_use]):
            start_index = index * machines_per_thread
            end_index = (index + 1) * machines_per_thread

            if end_index > len(machines):
                end_index = len(machines)

            if start_index > len(machines):
                start_index = end_index

            if start_index != end_index:
                monitor[0].add_machines(
                    monitored_machines[start_index:end_index])

    def start(self):
        logging.info("Initializing MachineProviders")
        # Initialize (non-started) machine monitors first
        # Then download machine data, populate data structure and monitors
        # then kickoff everything.

        logging.info("Initializing %s MachineMonitors" % self.worker_thread_count)
        # Spin up all the monitoring threads
        for threadnum in range(self.worker_thread_count):
            machinemonitor = MachineMonitor(parent=self,
                                            number=threadnum)
            thread = threading.Thread(target=self._run_monitor,
                                      args=(machinemonitor, ))
            self.monitors.append((machinemonitor, thread))

        self.calculator = threading.Thread(target=self._calculator)

        aws = AmazonEC2()
        if aws.usable():
            self.providers['aws'] = aws

        for provider in self.providers.values():
            self.zones.extend(provider.get_all_shared_fate_zones())
            # Note: add_machines() has to be called AFTER the monitors
            # are initialized.
            self.add_machines(provider.get_machine_list())

        logging.info("Zone List: %s" % self.zones)

        # Kickoff all the threads at once
        self.http_monitor.start()
        logging.info("Cluster Sitter Monitor started at " + \
            "http://localhost:%s" % self.http_monitor.port)

        if self.daemon:
            self.logmanager.setup_all()

        logging.info("Spinning up %s monitoring threads" % (
                self.worker_thread_count))
        for monitor in self.monitors:
            monitor[1].start()

        logging.info("Starting metadata calculator")
        self.calculator.start()


    def add_job(self, job):
        for zone in job.get_shared_fate_zones():
            if not zone in self.zones:
                logging.warn("Tried to add a job with an unknown SFZ %s" % zone)
                return

        self.jobs.append(job)
        self.refill_job(job)

    def refill_job(self, job):
        #!MACHINEASSUMPTION!
        # Step 1: Ensure we have enough machines in each SFZ
        # Step 1a: Check for idle machines and reserve as we find them
        new_machines = []
        reserved_machines = []
        for zone in job.get_shared_fate_zones():
            idle_available = self.get_idle_machines_in_zone(zone)
            idle_required = job.get_required_machines_in_zone(zone)
            # !MACHINEASSUMPTION! Ideally we're counting resources here not machines
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

        # Step 3: Add this job and the associated machines to the catalog
        # !MACHINEASSUMPTION! Should just allocate RESOURCES to the job,
        # in the catalog, not the whole machines
        job.set_monitored_machines(reserved_machines)
        self.jobcatalog['byjob'][job] = reserved_machines
        for machine in reserved_machines:
            if not self.jobcatalog['bymachine'][machine]:
                self.jobcatalog['bymachine'][machine] = []

            self.jobcatalog['bymachine'][machine].append(job)

        # Done!
        return reserved_machines

    def begin_adding_machines(self, zone, count):
        # This should run some kind of modular procedure
        # to bring up the machines, ASYNCHRONOUSLY (in a new thread?)
        # and return objects representing the machiens on their way up.
        pass

    def _register_sitter_failure(self, monitored_machine):
        """
        Try and SSH into the machine and see whats up.  If we can't
        get to it and reboot a sitter than decomission it.

        Maybe do all this in a thread?  SSH etc. could take a while.

        !! Fabric is not threadsafe.  Do it in the main clustersitter
        thread !!


        NOTE: We should do some SERIOUS rate limiting here.
        If we just have a 10 minute network hiccup we *should*
        try and replace those machines, but we should continue
        to check for the old ones for *A LONG TIME*
        to see if they come back.  After that formally decomission
        them.  If they do come back after we've moved their jobs around
        then simply remove the jobs from the machine and add them
        to the idle resources pool.
        """

        # For now just assume its dead, johnny.
        # Find which jobs this was running
        jobs = self.jobcatalog['bymachine'].get(monitored_machine, [])
        for job in jobs:
            job.remove_monitored_machine(monitored_machine)
            if job.needs_machines():
                self.refill_job(job)


    def _calculator(self):
        while True:
            self.calculate_idle_machines()
            time.sleep(self.stats_poll_interval)

    def get_idle_machines_in_zone(self, zone):
        """
        @ TODO Do some sort of calculation -- if we have too many idle
        machines we should decomission them.  Define a configurable
        threshold somewhere.
        """
        return self.idle_machines[zone]

    def calculate_idle_machines(self):
        idle_machines = {}

        for zone in self.zones:
            idle_machines[zone] = []
            for machine in self.machines_by_zone.get(zone, []):
                tasks = machine.get_running_tasks()

                #!MACHINEASSUMPTION! Here we assume no tasks == idle,
                # not sum(jobs.cpu) < machine.cpu etc.
                if not tasks and machine.has_loaded_data():
                    idle_machines[zone].append(machine)

        # The DICT swap must be atomic, or else another
        # thread could get a bad value during calculation.
        self.idle_machines = idle_machines
        logging.info("Calculated idle machines: %s" % str(self.idle_machines))

    def _run_monitor(self, monitor):
        # Assume we're in our own thread here
        monitor.start()


class MachineProfile(object):
    def __init__(self, cpu=None, mem=None):
        self.cpu = cpu
        self.mem = mem
