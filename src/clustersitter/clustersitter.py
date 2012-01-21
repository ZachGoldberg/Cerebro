import logging
import os
import random
import socket
import sys
import threading
import time
import requests
from datetime import datetime
from logging import FileHandler

import providers.aws
import deploymentrecipe
import eventmanager
import jobfiller
import machineconfig
import machinemonitor
import monitoredmachine
import productionjob
import sittercommon.machinedata

from clusterstats import ClusterStats
from deploymentrecipe import DeploymentRecipe, MachineSitterRecipe
from eventmanager import ClusterEventManager
from machineconfig import MachineConfig
from machinemonitor import MachineMonitor
from monitoredmachine import MonitoredMachine
from productionjob import ProductionJob
from providers.aws import AmazonEC2
from sittercommon import http_monitor
from sittercommon import logmanager
from sittercommon.machinedata import MachineData

logger = logging.getLogger(__name__)

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
    def __init__(self, parent):
        # list of tuples (MachineMonitor, ThreadObj)
        self.monitors = []
        self.machines_by_zone = {}
        self.zones = []
        self.provider_by_zone = {}
        self.jobs = []
        self.job_fill = {}
        self.providers = {}
        self.unreachable_machines = []
        self.machine_spawn_threads = []
        self.idle_machines = []
        self.sitter = parent

    def get_idle_machines_in_zone(self, zone):
        """
        @ TODO Do some sort of calculation -- if we have too many idle
        machines we should decomission them.  Define a configurable
        threshold somewhere.
        """
        return self.idle_machines[zone]

    def calculate_job_fill(self):
        # If we find out that a job has TOO MANY tasks,
        # then we should decomission some machines or make
        # them idle

        job_fill = {}
        #!MACHINEASSUMPTION! Should be cpu_count not machine_count
        # Fill out a mapping of [job][task] -> machine_count
        logger.info("Calculating job fill for jobs: %s" % self.jobs)
        for job in self.jobs:
            job_fill[job.name] = {}

            for zone in job.get_shared_fate_zones():
                job_fill[job.name][zone] = 0

        # Actually do the counting
        for zone, machines in self.machines_by_zone.items():
            for machine in machines:
                for task in machine.get_running_tasks():
                    # Don't add tasks from machines to the job_fill
                    # dict unless we already know about the job
                    if not task['name'] in job_fill:
                        continue

                    job_fill[task['name']][zone] += 1

        self.job_fill = job_fill
        logger.info("Calculated job fill: %s" % self.job_fill)

    def calculate_job_refill(self):
        logger.info("Calculating job refill for jobs: %s" % self.jobs)
        # Now see if we need to add any new machines to any jobs
        for job in self.jobs:
            if job.name in self.job_fill:
                job.refill(self, self.sitter)

        logger.info("Calculated job refill: %s" % self.job_fill)

    def calculate_idle_machines(self):
        idle_machines = {}
        for zone in self.zones:
            idle_machines[zone] = []
            for machine in self.machines_by_zone.get(zone, []):
                tasks = machine.get_running_tasks()

                #!MACHINEASSUMPTION! Here we assume no tasks == idle,
                # not sum(jobs.cpu) < machine.cpu etc.
                idle = bool(not tasks)
                idle = idle and machine.has_loaded_data()
                idle = idle and not machine.is_in_deployment()

                if idle:
                    idle_machines[zone].append(machine)

        # The DICT swap must be atomic, or else another
        # thread could get a bad value during calculation.
        self.idle_machines = idle_machines
        logger.info("Calculated idle machines: %s" % str(self.idle_machines))


class ClusterSitter(object):
    def __init__(self, log_location, daemon,
                 provider_config,
                 keys=None, user=None,
                 starting_port=30000):
        self.worker_thread_count = 2
        self.daemon = daemon
        self.keys = keys
        self.user = user
        self.provider_config = provider_config
        self.log_location = log_location
        self.launch_time = datetime.now()
        self.start_state = "Not Started"

        self.state = ClusterState(self)

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

        self.stats = ClusterStats(self)
        self.http_monitor = http_monitor.HTTPMonitor(self.stats,
                                                     self,
                                                     self.get_next_port())

        self.http_monitor.add_handler('/overview', self.stats.overview)

        # Do lots of logging configuration
        modules = [sys.modules[__name__],
                   machinemonitor,
                   monitoredmachine,
                   productionjob,
                   providers.aws,
                   deploymentrecipe,
                   sittercommon.machinedata,
                   jobfiller,
                   eventmanager]

        formatter = logging.Formatter(
            '%(asctime)s - %(name)s:%(threadName)s'
            '.%(levelname)s - %(message)s')

        if not os.path.exists(self.log_location):
            os.makedirs(self.log_location)

        all_file = FileHandler("%s/all.log" % self.log_location)
        all_file.setFormatter(formatter)

        self.logfiles = ["%s/all.log" % self.log_location]

        for module in modules:
            name = module.__name__.split('.')[-1]
            logfile = "%s/%s.log" % (self.log_location,
                                                 name)
            self.logfiles.append(logfile)
            handler = FileHandler(logfile)
            handler.setFormatter(formatter)
            module.logger.addHandler(all_file)
            module.logger.addHandler(handler)

        socket.setdefaulttimeout(2)

    def build_recipe(self, recipe_class, machine,
                     post_callback=None, options=None):
        username = self.user
        keys = self.keys
        if machine.config.login_name:
            username = machine.config.login_name

        if machine.config.login_key:
            keys.append(machine.config.login_key)

        return recipe_class(machine.config.hostname,
                            username,
                            keys,
                            post_callback=post_callback,
                            options=options)

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
        if not machines:
            return

        monitored_machines = []
        logger.info("Adding machines to monitoring: %s" % machines)
        for m in machines:
            mm = m
            if not isinstance(mm, MonitoredMachine):
                mm = MonitoredMachine(m)

            if not mm.config.shared_fate_zone in self.state.machines_by_zone:
                self.state.machines_by_zone[mm.config.shared_fate_zone] = []

            self.state.machines_by_zone[mm.config.shared_fate_zone].append(mm)
            monitored_machines.append(mm)

        # Spread the machines out evenly across threads
        num_threads_to_use = min(self.worker_thread_count, len(machines)) or 1
        machines_per_thread = int(len(machines) / num_threads_to_use) or 1

        # Take care of individisable # of machines by thread
        if machines_per_thread * num_threads_to_use != len(machines):
            machines_per_thread += 1

        self.state.monitors.sort(key=lambda x: x[0].num_monitored_machines())
        monitors_to_use = self.state.monitors[:num_threads_to_use]

        logger.info("Add to Monitoring, num_threads_to_use: %s" % num_threads_to_use +
                    "machines_per_thread: %s, " % machines_per_thread)

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
        self.http_monitor.start()
        logger.info("Cluster Sitter Monitor started at " + \
            "http://localhost:%s" % self.http_monitor.port)

        self.start_state = "Starting Up"

        logger.info("Initializing MachineProviders")
        # Initialize (non-started) machine monitors first
        # Then download machine data, populate data structure and monitors
        # then kickoff everything.

        logger.info("Initializing %s MachineMonitors" % self.worker_thread_count)
        # Spin up all the monitoring threads
        for threadnum in range(self.worker_thread_count):
            machinemonitor = MachineMonitor(parent=self,
                                            number=threadnum)
            thread = threading.Thread(target=self._run_monitor,
                                      args=(machinemonitor, ),
                                      name='Monitoring-%s' % threadnum)
            self.state.monitors.append((machinemonitor, thread))

        aws = AmazonEC2(self.provider_config['aws'])
        if aws.usable():
            self.state.providers['aws'] = aws

        for provider in self.state.providers.values():
            newzones = provider.get_all_shared_fate_zones()
            self.state.zones.extend(newzones)
            for zone in newzones:
                self.state.provider_by_zone[zone] = provider

            # Note: add_machines() has to be called AFTER the monitors
            # are initialized.
            self.add_machines(provider.get_machine_list())

        logger.info("Zone List: %s" % self.state.zones)

        # Kickoff all the threads at once
        if self.daemon:
            self.logmanager.setup_all()

        logger.info("Spinning up %s monitoring threads" % (
                self.worker_thread_count))
        for monitor in self.state.monitors:
            monitor[1].start()

        logger.info("Starting metadata calculator")
        self.calculator = threading.Thread(target=self._calculator,
                                           name="Calculator")
        self.calculator.start()

        logger.info("Starting machine recovery thread")
        self.machine_doctor = threading.Thread(target=self._machine_doctor,
                                               name="MachineDoctor")
        self.machine_doctor.start()

        self.start_state = "Started"

    def add_job(self, job):
        logger.info("Add Job: %s" % job.name)
        for zone in job.get_shared_fate_zones():
            if not zone in self.state.zones:
                logger.warn("Tried to add a job with an unknown SFZ %s" % zone)
                return

        self.state.jobs.append(job)

    def _register_sitter_failure(self, monitored_machine, monitor):
        """
        !! Fabric is not threadsafe.  Do all work in one thread by appending to
        a queue !!
        # Update -- not using fabric anymore, but we still don't want
        to be mucking with machines in the machinemonitor thread,
        still a good idea to shift to machinedoctor

        """
        logger.info("Registering an unreachable machine %s" % monitored_machine)
        self.state.unreachable_machines.append((monitored_machine, monitor))

    def _machine_doctor(self):
        """
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
            logger.info("Begin machine doctor run.  Unreachables: %s"
                        % (self.state.unreachable_machines))
            try:
                for machine, monitor in self.state.unreachable_machines:
                    logger.info("Attempting to redeploy to %s" %
                                machine)

                    recipe = self.build_recipe(MachineSitterRecipe, machine)
                    logger.info("Recipe for redeploy built, running it now")
                    val = recipe.deploy()
                    if val:
                        # We were able to successfully reploy to the machine
                        # so readd it to the monitor
                        logger.info("Successful redeploy of %s!" % machine)
                        monitor.add_machines([machine])
                    else:
                        logger.info("Redeploy failed!  Decomissioning %s" % machine)
                        # Decomission time!
                        # For now just assume its dead, johnny.
                        ClusterEventManager.handle(
                            "Decomissioning %s" % machine)
                        # TODO: Write machine decomission logic

                    self.state.unreachable_machines.remove((machine, monitor))
            except:
                # Der?  Not sure what this could be...
                import traceback
                traceback.print_exc()
                logger.error(traceback.format_exc())

            time_spent = datetime.now() - start_time
            sleep_time = self.stats_poll_interval - \
                time_spent.seconds
            logger.info(
                "Finished Machine Doctor run.  Time_spent: %s, sleep_time: %s" % (
                    time_spent,
                    sleep_time))

            if sleep_time > 0:
                time.sleep(sleep_time)

    def _calculator(self):
        while True:
            self.state.calculate_idle_machines()
            self.state.calculate_job_fill()
            self.state.calculate_job_refill()
            time.sleep(self.stats_poll_interval)

    def _run_monitor(self, monitor):
        # Assume we're in our own thread here
        monitor.start()
