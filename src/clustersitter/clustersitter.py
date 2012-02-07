import json
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

import deploymentrecipe
import dreamhost
import dynect
import eventmanager
import jobfiller
import machineconfig
import machinemonitor
import monitoredmachine
import productionjob
import providers.aws
import sittercommon.machinedata

from clusterstats import ClusterStats
from deploymentrecipe import DeploymentRecipe, MachineSitterRecipe
from eventmanager import ClusterEventManager
from jobfiller import JobFiller
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
        self.job_fill_machines = {}
        self.providers = {}
        self.job_overflow = {}
        self.unreachable_machines = []
        self.machine_spawn_threads = []
        self.idle_machines = []
        self.sitter = parent
        self.job_file = "%s/jobs.json" % self.sitter.log_location
        self.repair_jobs = []
        self.max_idle_per_zone = -1
        self.loggers = []

    def add_job(self, job):
        logger.info("Add Job: %s" % job.name)
        for zone in job.get_shared_fate_zones():
            if not zone in self.zones:
                logger.warn("Tried to add a job with an unknown SFZ %s" % zone)
                return False

        # Ensure we can find the job's recipe
        if (job.deployment_recipe and
            not self.sitter._get_recipe_class(job.deployment_recipe)):
            logger.warn(
                "Tried to add a job with an invalid recipe class: %s" % (
                    job.deployment_recipe))

            return False

        # Ensure we don't already have a job with this name,
        # if we do, replace it
        for existing_job in self.jobs:
            if existing_job.name == job.name:
                self.jobs.remove(existing_job)

        self.jobs.append(job)
        self.persist_jobs()
        return True

    def remove_job(self, jobname):
        removed = False
        for job in self.jobs:
            if job.name == jobname:
                self.jobs.remove(job)
                removed = True

        self.persist_jobs()
        return removed

    def persist_jobs(self):
        """
        Naively just rewrite the whole "job db"
        """
        jobs_to_write = [j for j in self.jobs if j.persistent]
        f = open(self.job_file, 'w')
        f.write(json.dumps([j.to_dict() for j in jobs_to_write]))
        f.close()

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
        job_fill_machines = {}
        #!MACHINEASSUMPTION! Should be cpu_count not machine_count
        # Fill out a mapping of [job][task] -> machine_count
        logger.debug("Calculating job fill for jobs: %s" % self.jobs)
        for job in self.jobs:
            job_fill[job.name] = {}
            job_fill_machines[job.name] = {}

            for zone in job.get_shared_fate_zones():
                job_fill[job.name][zone] = 0
                job_fill_machines[job.name][zone] = []

        # Actually do the counting
        for zone, machines in self.machines_by_zone.items():
            for machine in machines:
                for task in machine.get_running_tasks():
                    # Don't add tasks from machines to the job_fill
                    # dict unless we already know about the job
                    if not task['name'] in job_fill:
                        continue

                    job_fill[task['name']][zone] += 1
                    job_fill_machines[task['name']][zone].append(machine)

        self.job_fill = job_fill
        self.job_fill_machines = job_fill_machines
        logger.debug("Calculated job fill: %s" % self.job_fill)

    def calculate_job_refill(self):
        logger.debug("Calculating job refill for jobs: %s" % self.jobs)
        # Now see if we need to add any new machines to any jobs
        for job in self.jobs:
            if job.name in self.job_fill:
                if not job.refill(self, self.sitter):
                    # refill returning false means
                    # we need to wait for another calculation run before
                    # we can keep working.
                    break

        logger.debug("Calculated job refill: %s" % self.job_fill)

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
        logger.debug("Calculated idle machines: %s" % str(self.idle_machines))

    def calculate_job_overfill(self):
        # TODO we really should just do the calculating here
        # and let the machinedoctor spin down the task
        job_overflow = {}
        for job in self.jobs:
            zone_overflow = job.get_zone_overflow(self)
            job_overflow[job.name] = zone_overflow

        self.job_overflow = job_overflow
        logger.debug("Calculated job overflow: %s" % self.job_overflow)

    def _calculator(self):
        while True:
            def run_job(job, name):
                # Since all state is accessed and shared there
                # are all sorts of race conditions if a calculator
                # is running and a job is added or removed.
                # If one calculator run crashes because of this
                # thats OK.
                try:
                    job()
                except:
                    import traceback
                    logger.warn("Crash in %s" % name)
                    logger.warn(traceback.format_exc())

            run_job(self.calculate_idle_machines, "Calculate Idle Machines")
            run_job(self.calculate_job_fill, "Calculate Job Fill")
            run_job(self.calculate_job_refill, "Calculate Job ReFill")
            run_job(self.calculate_job_overfill, "Calculate Job OverFill")
            time.sleep(self.sitter.stats_poll_interval)


class ClusterSitter(object):
    def __init__(self, log_location, daemon,
                 provider_config,
                 dns_provider_config,
                 keys=None, user=None,
                 starting_port=30000):
        self.worker_thread_count = 4
        self.daemon = daemon
        self.keys = keys
        self.user = user
        self.provider_config = provider_config
        self.dns_provider_config = dns_provider_config
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

        self.dns_provider = None
        if 'class' in dns_provider_config:
            (module, clsname) = dns_provider_config['class'].split(':')
            clz = self._get_recipe_class(module, find_func=clsname)
            self.dns_provider = getattr(clz, clsname)(
                dns_provider_config)

        # In seconds
        self.stats_poll_interval = 2

        self.stats = ClusterStats(self)
        self.http_monitor = http_monitor.HTTPMonitor(self.stats,
                                                     self,
                                                     self.get_next_port())

        self.http_monitor.add_handler('/overview', self.stats.overview)
        self.http_monitor.add_handler('/add_job', self.api_add_job)
        self.http_monitor.add_handler('/remove_job', self.api_remove_job)
        self.http_monitor.add_handler('/update_idle_limit',
                                      self.api_enforce_idle)
        self.http_monitor.add_handler('/update_logging_level',
                                      self.api_update_logging_level)
        self.http_monitor.add_handler('/update_job',
                                      self.api_update_job)

        # Do lots of logging configuration
        modules = [sys.modules[__name__],
                   machinemonitor,
                   monitoredmachine,
                   productionjob,
                   providers.aws,
                   deploymentrecipe,
                   sittercommon.machinedata,
                   jobfiller,
                   eventmanager,
                   dynect]

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
            self.state.loggers.append(module.logger)

        socket.setdefaulttimeout(2)

    # ----------- API ----------------
    def _api_check(self, args, required_fields):
        if self.start_state != "Started":
            return "Not ready to recieve API calls"

        for field in required_fields:
            if not field in args:
                return "Missing Field %s" % field

        return

    def api_update_logging_level(self, args):
        # Default level is INFO
        level = args.get('level', 20)
        for logger in self.state.loggers:
            logger.setLevel(level)

        logger.info('Updated logging level to %s' % level)
        return "Level set to %s" % level

    def api_enforce_idle(self, args):
        # Really naive right now, a global # of
        # max idle per zone.  Could do a lot more here.
        check = self._api_check(args, ['idle_count_per_zone'])

        if check:
            return check

        try:
            self.state.max_idle_per_zone = int(args['idle_count_per_zone'])
        except:
            return "Invalid limit"

        return "Limit set"

    def api_update_job(self, args):
        check = self._api_check(args,
                                ['job_name'])

        job_name = args['job_name']
        job = None
        for state_job in self.state.jobs:
            if state_job.name == job_name:
                job = state_job
                break

        if not job:
            return "Error updating job: %s doesn't exist" % job_name

        job.do_update_deployment(self.state, args.get('version'))
        # Now build a deployment recipe for this job
        return "Job update initiated"

    def api_add_job(self, args):
        check = self._api_check(args,
                                ['dns_basename',
                                 'task_configuration',
                                 'deployment_layout',
                                 'deployment_recipe',
                                 'recipe_options',
                                 'persistent'])
        if check:
            return check

        if self.state.add_job(ProductionJob(
                args['dns_basename'],
                args['task_configuration'],
                args['deployment_layout'],
                args['deployment_recipe'],
                args['recipe_options'],
                args['persistent'],
                args.get('linked_job'))):
            return "Job Added"
        else:
            return "Error adding job, see logs"

    def api_remove_job(self, args):
        check = self._api_check(args, ['name'])

        if check:
            return check

        if self.state.remove_job(args['name']):
            return "Removal OK"
        else:
            return "Couldn't find job to remove"

    # ----------- END API ----------

    def machines_in_queue(self):
        for monitor, thread in self.state.monitors:
            if monitor.processing_new_machines():
                return True

        return False

    def _get_recipe_class(self, recipe_class, find_func="run_deploy"):
        if isinstance(recipe_class, type):
            return recipe_class

        recipe_cls = None
        if isinstance(recipe_class, basestring):
            try:
                recipe_cls = __import__(recipe_class,
                                          globals(),
                                          locals())
            except:
                # Odd?
                logger.warn("Not sure what %s is..." % recipe_class)
        else:
            logger.warn("Not sure what %s is..." % recipe_class)
            return None

        if hasattr(recipe_cls, find_func):
            return recipe_cls

        exports = dir(recipe_cls)
        for export in exports:
            if hasattr(getattr(recipe_cls, export), find_func):
                return getattr(recipe_cls, export)

        try:
            module = sys.modules[recipe_class]
        except:
            return None

        exports = dir(module)
        for export in exports:
            if hasattr(getattr(module, export), find_func):
                return getattr(module, export)

    def build_recipe(self, recipe_class, machine,
                     post_callback=None, options=None,
                     given_logger=None):
        username = self.user
        keys = self.keys
        if machine.config.login_name:
            username = machine.config.login_name

        if machine.config.login_key:
            keys.append(machine.config.login_key)

        recipe_cls = self._get_recipe_class(recipe_class)
        if not recipe_cls:
            return None

        return recipe_cls(machine.config.hostname,
                          username,
                          keys,
                          post_callback=post_callback,
                          options=options,
                          given_logger=given_logger,
                          dns_hostname=machine.config.dns_name)

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

    def remove_machine(self, machine):
        self.state.machines_by_zone[
            machine.config.shared_fate_zone].remove(machine)

        for monitor in self.state.monitors:
            monitor[0].remove_machine(machine)

    def decomission_machine(self, machine):
        self.remove_machine(machine)
        provider = self.state.provider_by_zone[machine.config.shared_fate_zone]

        ClusterEventManager.handle(
            "Decomissioning %s" % str(machine))

        provider.decomission(machine)
        if machine.config.dns_name:
            self.dns_provider.remove_record(data=machine.config.ip,
                                            hostName=machine.config.dns_name)

            # Strip off the leading number, e.g.
            # 12.bar.mydomain.com -> bar.mydomain.com
            root_name = '.'.join(machine.config.dns_name.split('.')[1:])

            self.dns_provider.remove_record(data=machine.config.ip,
                                            hostName=root_name)

    def add_machines(self, machines, update_dns=True):
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

            # We can re-add machines after they temporarily disappear
            if mm not in self.state.machines_by_zone[
                mm.config.shared_fate_zone]:
                self.state.machines_by_zone[
                    mm.config.shared_fate_zone].append(mm)

            monitored_machines.append(mm)

        # Spread the machines out evenly across threads
        num_threads_to_use = min(self.worker_thread_count, len(machines)) or 1
        machines_per_thread = int(len(machines) / num_threads_to_use) or 1

        # Take care of individisable # of machines by thread
        if machines_per_thread * num_threads_to_use != len(machines):
            machines_per_thread += 1

        self.state.monitors.sort(key=lambda x: x[0].num_monitored_machines())
        monitors_to_use = self.state.monitors[:num_threads_to_use]

        logger.info(
            "Add to Monitoring, num_threads_to_use: %s" % num_threads_to_use +
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

        if not update_dns:
            return

        # Ensure we have up to date DNS names for each machine
        logger.info("Loading DNS Records...")

        records = self.dns_provider.get_records()
        record_by_ip = {}
        for record in records:
            record_by_ip[record['value']] = record['record']

        for machine in monitored_machines:
            if not machine.config.dns_name:
                ip = socket.gethostbyname(machine.config.hostname)
                if ip in record_by_ip:
                    machine.config.dns_name = record_by_ip[ip]
                    logger.info("Found name %s for %s" % (
                            machine.config.dns_name,
                            ip))

    def start(self):
        self.http_monitor.start()
        logger.info("Cluster Sitter Monitor started at " + \
            "http://localhost:%s" % self.http_monitor.port)

        self.start_state = "Starting Up"

        logger.info("Initializing MachineProviders")
        # Initialize (non-started) machine monitors first
        # Then download machine data, populate data structure and monitors
        # then kickoff everything.

        logger.info(
            "Initializing %s MachineMonitors" % self.worker_thread_count)
        # Spin up all the monitoring threads
        for threadnum in range(self.worker_thread_count):
            machinemonitor = MachineMonitor(parent=self,
                                            number=threadnum)
            thread = threading.Thread(target=machinemonitor.start,
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
        self.calculator = threading.Thread(target=self.state._calculator,
                                           name="Calculator")
        self.calculator.start()

        logger.info("Starting machine recovery thread")
        self.machine_doctor = threading.Thread(target=self._machine_doctor,
                                               name="MachineDoctor")
        self.machine_doctor.start()

        self.start_state = "Started"

    def _register_sitter_failure(self, monitored_machine, monitor):
        """
        !! Fabric is not threadsafe.  Do all work in one thread by appending to
        a queue !!
        # Update -- not using fabric anymore, but we still don't want
        to be mucking with machines in the machinemonitor thread,
        still a good idea to shift to machinedoctor

        """
        logger.info(
            "Registering an unreachable machine %s" % monitored_machine)
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
            logger.debug("Begin machine doctor run.  Unreachables: %s"
                        % (self.state.unreachable_machines))
            try:
                # TODO - Break out these three actions into sub-functions

                """
                Try and fix unreachable machines
                """
                for machine, monitor in self.state.unreachable_machines:
                    ClusterEventManager.handle("Attempting to redeploy to %s" %
                                               machine)

                    # Build a 'fake' job for the doctor to run
                    job = ProductionJob(
                        "",
                        {'name': 'Machine Doctor Redeployer'},
                        {
                            machine.config.shared_fate_zone: {
                                'mem': machine.config.mem,
                                'cpu': machine.config.cpus
                                }
                            }, None)

                    job.sitter = self
                    self.state.repair_jobs.append(job)

                    # Run in thread for now
                    # TODO run not in doctor thread
                    filler = JobFiller(1, job, machine.config.shared_fate_zone,
                                       raw_machines=[machine])

                    job.fillers[machine.config.shared_fate_zone] = [filler]

                    val = filler.run(fail_on_error=True)

                    # Remove the job from the job list, now that its finished
                    self.state.repair_jobs.remove(job)

                    if val:
                        # We were able to successfully reploy to the machine
                        logger.info("Successful redeploy of %s!" % machine)
                    else:
                        logger.info(
                            "Redeploy failed!  Decomissioning %s" % machine)
                        # Decomission time!
                        # For now just assume its dead, johnny.
                        self.state.machines_by_zone[
                            machine.config.shared_fate_zone].remove(machine)
                        ClusterEventManager.handle(
                            "Decomissioning %s" % machine)

                        self.decomission_machine(machine)

                    self.state.unreachable_machines.remove((machine, monitor))

                """
                Turn off any overflowed jobs
                """
                for jobname, zone_overflow in self.state.job_overflow.items():
                    for zone, count in zone_overflow.items():
                        if count <= 0:
                            continue

                        ClusterEventManager.handle(
                            "Detected job overflow:" +
                            "Job: %s, Zone: %s, Count: %s" % (jobname,
                                                              zone,
                                                              count))

                        decomissioned = 0
                        for machine in self.state.machines_by_zone[zone]:
                            if decomissioned == count:
                                break

                            for task in machine.get_running_tasks():
                                if task['name'] == jobname:
                                    ClusterEventManager.handle(
                                        "Stopping %s on %s" % (jobname,
                                                               str(machine)))
                                    machine.datamanager.stop_task(jobname)
                                    decomissioned += 1
                                    break

                """
                Enforce idle machine limits
                """
                if self.state.max_idle_per_zone != -1:
                    logger.info("Enforcing an idle limit")
                    idle_limit = self.state.max_idle_per_zone
                    self.state.max_idle_per_zone = -1
                    for zone, machines in self.state.idle_machines.items():
                        provider = self.state.provider_by_zone[zone]

                        if len(machines) > idle_limit:
                            decomission_targets = [
                                m for m in machines[idle_limit:]]
                            for machine in decomission_targets:
                                self.decomission_machine(machine)

            except:
                # Der?  Not sure what this could be...
                import traceback
                traceback.print_exc()
                logger.error(traceback.format_exc())

            time_spent = datetime.now() - start_time
            sleep_time = self.stats_poll_interval - \
                time_spent.seconds
            logger.debug(
                "Finished Machine Doctor run. " +
                "Time_spent: %s, sleep_time: %s" % (
                    time_spent,
                    sleep_time))

            if sleep_time > 0:
                time.sleep(sleep_time)
