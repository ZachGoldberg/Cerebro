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


class ClusterSitter(object):
    def __init__(self, log_location, daemon,
                 keys=None, user=None,
                 starting_port=30000):
        self.worker_thread_count = 2
        self.daemon = daemon

        self.keys = keys
        self.user = user

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

    def build_recipe(self, recipe_class, machine, post_callback):
        username = self.user
        keys = self.keys
        if machine.config.login_name:
            username = machine.config.login_name

        if machine.config.login_key:
            keys.append(machine.config.login_key)

        return recipe_class(machine.config.hostname,
                            username,
                            keys,
                            post_callback)

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
        num_threads_to_use = min(self.worker_thread_count, len(machines)) or 1
        machines_per_thread = int(len(machines) / num_threads_to_use) or 1
        self.monitors.sort(key=lambda x: x[0].num_monitored_machines())
        monitors_to_use = self.monitors[:num_threads_to_use]

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
            self.monitors.append((machinemonitor, thread))

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
        self.calculator = threading.Thread(target=self._calculator)
        self.calculator.start()

        logging.info("Starting machine recovery thread")
        self.machine_doctor = threading.Thread(target=self._machine_doctor)
        self.machine_doctor.start()



    def add_job(self, job):
        logging.info("Add Job: %s" % job.name)
        for zone in job.get_shared_fate_zones():
            if not zone in self.zones:
                logging.warn("Tried to add a job with an unknown SFZ %s" % zone)
                return

        self.jobs.append(job)
        self.refill_job(job)

    def refill_job(self, job):
        while not self.job_fill:
            # 1) Assume this job has already been added to self.jobs
            # 2) Want to ensure calculator has run at least once to find out
            #    if this job already exists throughout the cluster
            logging.info("Waiting for calculator thread to kick in before "
                         "filling jobs")
            time.sleep(0.5)

        #!MACHINEASSUMPTION!
        # Step 1: Ensure we have enough machines in each SFZ
        # Step 1a: Check for idle machines and reserve as we find them
        for zone in job.get_shared_fate_zones():
            idle_available = self.get_idle_machines_in_zone(zone)
            total_required = job.get_num_required_machines_in_zone(zone)
            idle_required = total_required - self.job_fill[job.name][zone]
            currently_spawning = self.spawning_machines[job.name][zone]
            idle_required -= currently_spawning

            # !MACHINEASSUMPTION! Ideally we're counting resources here not machines
            required_new_machine_count = (idle_required -
                                          len(idle_available))
            logging.info(
                ("Calculated job requirements for %s in %s: " % (job.name,
                                                                 zone)) +
                "Total Required: %s, Total New: %s" % (
                    idle_required,
                    required_new_machine_count))

            # For the machines we have idle now, use those immediately
            # For the others, spinup a thread to launch machines (which takes time)
            # and do the deployment

            # Now reserve part of the machine for this job
            usable_machines = []
            if required_new_machine_count == 0:
                # idle_available > idle_required, so use just as many
                # as we need
                usable_machines = idle_available[:idle_required]
            else:
                usable_machines.extend(idle_available)


            for machine in usable_machines:
                # Have the recipe deploy the job then set the callback
                # to be for the monitoredmachine to trigger the machinesitter
                # to actually start the job
                recipe = self.build_recipe(job.deployment_layout,
                                           machine,
                                           lambda: machine.start_task(job.name))

                # TODO - Mark this machine as no longer idle
                # so another job doesn't pick it up while we're deploying
                self.pending_recipes.add(recipe)

            if required_new_machine_count:
                spawn_thread = threading.Thread(target=self.spawn_machines,
                                                args=(zone, required_new_machine_count, job))
                spawn_thread.start()

            # TODO -- When does this get decremented?
            self.spawning_machines[job.name][zone] += idle_required


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
        self.unreachable_machines.append((monitored_machine, monitor))

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
            for machine, monitor in self.unreachable_machines:
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

                self.unreachable_machines.remove((machine, monitor))

            # Now see if we need to add any new machines to any jobs
            for job in self.jobs:
                for zone in job.get_shared_fate_zones():
                    while job.name not in self.job_fill:
                        logging.info("Doctor waiting for calculator thread"
                                     "to kick in before filling jobs")
                        time.sleep(0.5)

                    if (self.job_fill[job.name][zone] !=
                        job.deployment_layout[zone]['num_machines']):
                        self.refill_job(job)

            # Now do any other fabric type work
            for recipe in self.pending_recipes:
                recipe.deploy()
                self.pending_recipes.remove(recipe)

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
            self.calculate_idle_machines()
            self.calculate_job_fill()

            time.sleep(self.stats_poll_interval)

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

    def _run_monitor(self, monitor):
        # Assume we're in our own thread here
        monitor.start()


class MachineProfile(object):
    def __init__(self, cpu=None, mem=None):
        self.cpu = cpu
        self.mem = mem
