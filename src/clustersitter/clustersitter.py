import logging
import os
import socket
import sys
import threading
from datetime import datetime
from logging import FileHandler

import actions
import clusterstate
import deploymentrecipe
import dynect
import eventmanager
import jobfiller
import machinemonitor
import monitoredmachine
import productionjob
import providers.aws
import sittercommon.machinedata

from clusterstate import ClusterState
from clusterstats import ClusterStats
from eventmanager import ClusterEventManager
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

        # In seconds
        self.stats_poll_interval = 5

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
        modules = [
            sys.modules[__name__],
            actions,
            clusterstate,
            deploymentrecipe,
            dynect,
            eventmanager,
            jobfiller,
            machinemonitor,
            monitoredmachine,
            productionjob,
            providers.aws,
            sittercommon.machinedata,
        ]

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
            logfile = "%s/%s.log" % (
                self.log_location, name)
            self.logfiles.append(logfile)
            self.logmanager.add_logfile(name, logfile)
            handler = FileHandler(logfile)
            handler.setFormatter(formatter)
            module.logger.addHandler(all_file)
            module.logger.addHandler(handler)
            self.state.loggers.append(module.logger)

        requests_logger = logging.getLogger(
            'requests.packages.urllib3.connectionpool')
        requests_logger.setLevel(logging.WARNING)

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
            "Updated logging level to %s" % level)

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
            "Enforce Idle Limit at %s" % int(args['idle_count_per_zone']))
        return "Limit set"

    def api_update_job(self, args):
        check = self._api_check(args,
                                ['job_name'])

        if check:
            return check

        job_name = args['job_name']
        if not self.state.update_job(job_name):
            return "Error updating job: %s doesn't exist" % job_name

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
            self,
            args['dns_basename'],
            args['task_configuration'],
            args['deployment_layout'],
            args['deployment_recipe'],
            args['recipe_options'],
            args['persistent'],
            args.get('linked_job'))

        if args.get('linked_job'):
            job.find_linked_job()
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

        jobs = self.state.remove_job(args['name'])
        ClusterEventManager.handle(
            "Removed jobs: %s" % ', '.join(jobs))
        if jobs:
            return "Removed: %s" % ', '.join(jobs)
        else:
            return "Job Not Found"

    # ----------- END API ----------

    def machines_in_queue(self):
        #TODO: Replace with an equivelent method in ClusterState.
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
                recipe_cls = __import__(
                    recipe_class, globals(), locals())
            except:
                # Odd?
                logger.warn("Unable to load recipe %s..." % recipe_class)
        else:
            logger.warn("Invalid recipe class %s..." % recipe_class)
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

    def decomission_machine(self, machine):
        self.state.remove_machine(machine)
        provider = self.state.get_zone_provider(
            machine.config.shared_fate_zone)
        if not provider:
            logger.warn(
                "No provider found for %s?" % machine.config.shared_fate_zone)
            return

        ClusterEventManager.handle(
            "Decomissioning %s" % str(machine))

        if not provider.decomission(machine):
            # If we can't decomission it then perhaps its locked
            # and we should leave well enough alone at this point,
            # just remove it from monitoring etc.
            ClusterEventManager.handle(
                "Provider doesn't allow decomissioning of %s" % str(machine))
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
                logger.info(
                    "Removing %s from %s" % (
                    machine.config.ip, record['record']))
                self.dns_provider.remove_record(data=machine.config.ip,
                                                hostName=record['record'])

        ClusterEventManager.handle(
            "Decomissioning of %s complete!" % str(machine))

    def add_machines(self, machines, update_dns=True, deploying=False):
        """
        Add machines to the sitter.

        @param machines A list of machines to add.
        @param update_dns Whethor or not to update the DNS for the machines.
            Defaults to True.
        @param deploying Whether or not to flag the machines as currently being
            deployed. Defaults to False.
        """
        if not machines:
            return

        monitored_machines = []
        logger.info("Adding machines to monitoring: %s" % machines)
        for m in machines:
            mm = m
            if not isinstance(mm, MonitoredMachine):
                mm = MonitoredMachine(m)
            status = None
            if deploying:
                status = self.state.Deploying
            self.state.monitor_machine(mm)
            self.state.add_machine(mm, status=status, existing=True)
            monitored_machines.append(mm)

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
                    logger.info(
                        "Found name %s for %s" % (
                        machine.config.dns_name, ip))

    def start(self):
        self.http_monitor.start()
        logger.info((
            "Cluster Sitter Monitor started at "
            "http://localhost:%s") % self.http_monitor.port)

        self.start_state = "Starting Up"

        logger.info("Initializing MachineProviders")
        # Initialize (non-started) machine monitors first
        # Then download machine data, populate data structure and monitors
        # then kickoff everything.

        logger.info(
            "Initializing %s MachineMonitors" % self.worker_thread_count)
        # Spin up all the monitoring threads
        #TODO: Move monitor thread spinup to ClusterState.
        for threadnum in range(self.worker_thread_count):
            machinemonitor = MachineMonitor(parent=self,
                                            number=threadnum)
            thread = threading.Thread(target=machinemonitor.start,
                                      name='Monitoring-%s' % threadnum)
            self.state.monitors.append((machinemonitor, thread))

        #TODO: Don't hard code providers.
        aws = AmazonEC2(self.provider_config['aws'])
        if aws.usable():
            self.state.add_provider('aws', aws)

        for provider in self.state.get_providers().values():
            # Note: add_machines() has to be called AFTER the monitors
            # are initialized.
            logger.info(
                "adding %d machines for provider %s" % (
                len(provider.get_machine_list()), 'aws'))
            self.add_machines(provider.get_machine_list())

        logger.info("Zone List: %s" % self.state.get_zones())

        # Kickoff all the threads at once
        if self.daemon:
            self.logmanager.setup_all()

        logger.info(
            "Spinning up %s monitoring threads" %
            self.worker_thread_count)
        for monitor in self.state.monitors:
            monitor[1].start()

        logger.info("Starting metadata calculator")
        self.state.start()
        self.start_state = "Started"
