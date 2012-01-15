import logging
import os
import random
import socket
import threading
import time
import requests
from datetime import datetime

from providers.aws import AmazonEC2
from deploymentrecipe import DeploymentRecipe, MachineSitterRecipe
from machineconfig import MachineConfig
from machinemonitor import MachineMonitor
from monitoredmachine import MonitoredMachine
from productionjob import ProductionJob
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
class ClusterState(object):
    def __init__(self):
        # list of tuples (MachineMonitor, ThreadObj)
        self.monitors = []
        self.machines_by_zone = {}
        self.zones = []
        self.jobs = []
        self.job_fill = {}
        self.providers = {}
        self.unreachable_machines = []
        self.machine_spawn_threads = []
        self.spawning_machines = {}
        self.pending_recipes = []
        self.idle_machines = []

    def get_idle_machines_in_zone(self, zone):
        """
        @ TODO Do some sort of calculation -- if we have too many idle
        machines we should decomission them.  Define a configurable
        threshold somewhere.
        """
        return self.idle_machines[zone]

    def calculate_job_fill(self):
        job_fill = {}
        #!MACHINEASSUMPTION! Should be cpu_count not machine_count
        # Fill out a mapping of [job][task] -> machine_count
        for job in self.jobs:
            job_fill[job.name] = {}
            if not job.name in self.spawning_machines:
                self.spawning_machines[job.name] = {}

            for zone in job.get_shared_fate_zones():
                job_fill[job.name][zone] = 0
                if not zone in self.spawning_machines[job.name]:
                    self.spawning_machines[job.name][zone] = 0

        # Actually do the counting
        for zone, machines in self.machines_by_zone.items():
            for machine in machines:
                for task in machine.get_running_tasks():
                    job_fill[task['name']][zone] += 1

        self.job_fill = job_fill


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


class ClusterSitter(object):
    def __init__(self, log_location, daemon,
                 keys=None, user=None,
                 starting_port=30000):
        self.worker_thread_count = 2
        self.daemon = daemon

        self.keys = keys
        self.user = user

        self.state = ClusterState()

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

    def build_recipe(self, recipe_class, machine, post_callback, options):
        username = self.user
        keys = self.keys
        if machine.config.login_name:
            username = machine.config.login_name

        if machine.config.login_key:
            keys.append(machine.config.login_key)

        return recipe_class(machine.config.hostname,
                            username,
                            keys,
                            post_callback,
                            options)

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
            if not m.shared_fate_zone in self.state.machines_by_zone:
                self.state.machines_by_zone[m.shared_fate_zone] = []

            self.state.machines_by_zone[m.shared_fate_zone].append(mm)
            monitored_machines.append(mm)

        # Spread them out evenly across threads
        num_threads_to_use = min(self.worker_thread_count, len(machines)) or 1
        machines_per_thread = int(len(machines) / num_threads_to_use) or 1
        self.state.monitors.sort(key=lambda x: x[0].num_monitored_machines())
        monitors_to_use = self.state.monitors[:num_threads_to_use]

        for index, monitor in enumerate(monitors_to_use):
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
            self.state.monitors.append((machinemonitor, thread))

        aws = AmazonEC2()
        if aws.usable():
            self.state.providers['aws'] = aws

        for provider in self.state.providers.values():
            self.state.zones.extend(provider.get_all_shared_fate_zones())
            # Note: add_machines() has to be called AFTER the monitors
            # are initialized.
            self.add_machines(provider.get_machine_list())

        logging.info("Zone List: %s" % self.state.zones)

        # Kickoff all the threads at once
        self.http_monitor.start()
        logging.info("Cluster Sitter Monitor started at " + \
            "http://localhost:%s" % self.http_monitor.port)

        if self.daemon:
            self.logmanager.setup_all()

        logging.info("Spinning up %s monitoring threads" % (
                self.worker_thread_count))
        for monitor in self.state.monitors:
            monitor[1].start()

        logging.info("Starting metadata calculator")
        self.calculator = threading.Thread(target=self._calculator)
        self.calculator.start()

        logging.info("Starting machine recovery thread")
        self.machine_doctor = threading.Thread(target=self._machine_doctor)
        self.machine_doctor.start()

    def add_job(self, job):
        logging.info("Add Job: %s" % job.name)
        for zone in job.get_shared_fate_zones():
            if not zone in self.state.zones:
                logging.warn("Tried to add a job with an unknown SFZ %s" % zone)
                return

        self.state.jobs.append(job)
        job.refill(self.state, self)

    def spawn_machines(self, zone, count, job):
        # This should run some kind of modular procedure
        # to bring up the machines, ASYNCHRONOUSLY (in a new thread?)
        # and return objects representing the machiens on their way up.
        print "UNIMPLEMENTED " * 10
        print zone, count
        pass

    def _register_sitter_failure(self, monitored_machine, monitor):
        """
        !! Fabric is not threadsafe.  Do all work in one thread by appending to
        a queue !!
        """
        logging.info("Registering an unreachable machine %s" % monitored_machine)
        self.state.unreachable_machines.append((monitored_machine, monitor))

    def _machine_doctor(self):
        """
        !! Note: This is the ONLY place where we can run Fabric !!

        Try and SSH into the machine and see whats up.  If we can't
        get to it and reboot a sitter than decomission it.

        NOTE: We should do some SERIOUS rate limiting here.
        If we just have a 10 minute network hiccup we *should*
        try and replace those machines, but we should continue
        to check for the old ones for *A LONG TIME*
        to see if they come back.  After that formally decomission
        them.  If they do come back after we've moved their jobs around
        then simply remove the jobs from the machine and add them
        to the idle resources pool.
        """
        while True:
            start_time = datetime.now()
            for machine, monitor in self.state.unreachable_machines:
                # This is strange indeed!  Try reinstalling the clustersitter
                recipe = self.build_recipe(MachineSitterRecipe, machine)

                logging.info("Attempting to reploy to %s" % machine)
                val = recipe.deploy()
                if val:
                    # We were able to successfully reploy to the machine
                    # so readd it to the monitor
                    logging.info("Successful redeploy of %s!" % machine)
                    monitor.add_machines([machine])
                else:
                    logging.info("Redeploy failed!  Decomissioning %s" % machine)
                    # Decomission time!
                    # For now just assume its dead, johnny.

                    # TODO: Write machine decomission logic
                    pass

                self.state.unreachable_machines.remove((machine, monitor))

            # Now see if we need to add any new machines to any jobs
            for job in self.state.jobs:
                for zone in job.get_shared_fate_zones():
                    while job.name not in self.state.job_fill:
                        logging.info("Doctor waiting for calculator thread"
                                     "to kick in before filling jobs")
                        time.sleep(0.5)

                    """
                    Always refill the job -- if we're fill then
                    this is a noop.  The real reason for this is that inside
                    refill_job we have a "spawning_machines" tracker.
                    If we did a comparison here, active_machines == job.machines
                    then spawning_machines would always have one left,
                    and would never be zeroe'd out after an initial spawn.
                    This is because idle_required is what decrements the spawn
                    count, which only happens if we call refill job when there
                    are (active_machines + spawning_machines > needed_machines)
                    If we had a condition here refill IFF active machines < needed_machines
                    then the above condition would never be met when spawning_machines = 1
                    and active_machines == needed_machines (aka the last machine came up
                    """
                    job.refill(self.state, self)

            # Now do any other fabric type work
            for recipe in self.state.pending_recipes:
                recipe.deploy()
                self.state.pending_recipes.remove(recipe)

            time_spent = datetime.now() - start_time
            sleep_time = self.stats_poll_interval - \
                time_spent.seconds
            logging.info(
                "Finished Machine Doctor run.  Time_spent: %s, sleep_time: %s" % (
                    time_spent,
                    sleep_time))

            if sleep_time > 0:
                time.sleep(sleep_time)

    def _calculator(self):
        while True:
            self.state.calculate_idle_machines()
            self.state.calculate_job_fill()

            time.sleep(self.stats_poll_interval)

    def _run_monitor(self, monitor):
        # Assume we're in our own thread here
        monitor.start()


class MachineProfile(object):
    def __init__(self, cpu=None, mem=None):
        self.cpu = cpu
        self.mem = mem
