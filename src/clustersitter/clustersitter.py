import logging
import os
import socket
import sys
import threading
from datetime import datetime
from logging import FileHandler

import clusterstate
import deploymentrecipe
import dynect
import eventmanager
import jobfiller
import machinedoctor
import machinemonitor
import monitoredmachine
import productionjob
import providers.aws
import sittercommon.machinedata


from clusterstate import ClusterState
from clusterstats import ClusterStats
from eventmanager import ClusterEventManager
from machinedoctor import MachineDoctor
from machinemonitor import MachineMonitor
from monitoredmachine import MonitoredMachine
from productionjob import ProductionJob
from providers.aws import AmazonEC2
from sittercommon import http_monitor
from sittercommon import logmanager

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


class ClusterSitter(object):
    def __init__(self, log_location, daemon,
                 provider_config,
                 dns_provider_config,
                 keys=None, user=None,
                 starting_port=30000,
                 launch_location=None):
        self.worker_thread_count = 4
        self.daemon = daemon
        self.keys = keys
        self.user = user
        self.provider_config = provider_config
        self.dns_provider_config = dns_provider_config
        self.log_location = log_location
        self.launch_location = launch_location
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
        self.stats_poll_interval = 5

        self.stats = ClusterStats(self)
        self.http_monitor = http_monitor.HTTPMonitor(self.stats,
                                                     self,
                                                     30000)

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
                   clusterstate,
                   machinedoctor,
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
            self.logmanager.add_logfile(name, logfile)
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

        ClusterEventManager.handle(
                'Updated logging level to %s' % level)

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

        ClusterEventManager.handle(
                'Enforce Idle Limit at %s' % int(args['idle_count_per_zone']))
        return "Limit set"

    def api_update_job(self, args):
        check = self._api_check(args,
                                ['job_name'])

        if check:
            return check

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
        ClusterEventManager.handle(
            'Update %s started' % job_name)

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
        job = ProductionJob(
                args['dns_basename'],
                args['task_configuration'],
                args['deployment_layout'],
                args['deployment_recipe'],
                args['recipe_options'],
                args['persistent'],
                args.get('linked_job'))

        if args.get('linked_job'):
            job.find_linked_job(self.state)
            if not job.linked_job_object:
                return "Couldn't find linked job!"

        if self.state.add_job(job):
            ClusterEventManager.handle(
                "Added a job: %s" % job.get_name())

            return "Job Added"
        else:
            return "Error adding job, see logs"

    def api_remove_job(self, args):
        check = self._api_check(args, ['name'])

        if check:
            return check

        if self.state.remove_job(args['name']):
            ClusterEventManager.handle(
                "Removed a job: %s" % args['name'])

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
                value = getattr(module, export)
                if value.__module__ == module.__name__:
                    return value

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
                          dns_hostname=machine.config.dns_name,
                          launch_location=self.launch_location)

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
        if not machine.config.shared_fate_zone in self.state.provider_by_zone:
            logger.warn(
                "No provider found for %s?" % machine.config.shared_fate_zone)
            return

        provider = self.state.provider_by_zone[machine.config.shared_fate_zone]

        ClusterEventManager.handle(
            "Decomissioning %s" % str(machine))

        if not provider.decomission(machine):
            # If we can't decomission it then perhaps its locked
            # and we should leave well enough alone at this point,
            # just remove it from monitoring etc.
            return

        if machine.config.dns_name:
            self.dns_provider.remove_record(data=machine.config.ip,
                                            hostName=machine.config.dns_name)

            # Strip off the leading number, e.g.
            # 12.bar.mydomain.com -> bar.mydomain.com
            root_name = '.'.join(machine.config.dns_name.split('.')[1:])

            self.dns_provider.remove_record(data=machine.config.ip,
                                            hostName=root_name)

        # Now look for other dangling records pointing to this machine
        # and delete those too.
        records = self.dns_provider.get_records()
        for record in records:
            if record['value'] == machine.config.ip:
                logger.info("Removing %s from %s" % (
                        machine.config.ip,
                        record['record']))
                self.dns_provider.remove_record(data=machine.config.ip,
                                                hostName=record['record'])

        ClusterEventManager.handle(
            "Decomissioning of %s complete!" % str(machine))

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
        self.machine_doctor = MachineDoctor(self)
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
