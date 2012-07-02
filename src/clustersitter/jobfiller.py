import logging
import socket
import threading
import time
from datetime import datetime, timedelta

from deploymentrecipe import MachineSitterRecipe
from monitoredmachine import MonitoredMachine
from eventmanager import ClusterEventManager

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


class StateMachine(object):
    states = {}

    def __init__(self):
        self.state = 0
        self.last_state = 0

    def get_state(self):
        return self.state

    def set_error(self):
        self.set_state(-1)

    def set_state(self, new_state):
        self.last_state = self.state
        self.state = new_state

    def next(self):
        self.set_state(self.state + 1)

    def __str__(self):
        return "%s: %s" % (self.state, self.states[self.state])


class MachineDeploymentState(StateMachine):
    states = {
        -1: 'Error',
         0: 'None',
         1: 'Provider.Launching',
         2: 'TaskSitter.Deployment',
         3: 'Job.Deployment',
         4: 'Job.Launching',
         5: 'AddingMonitoring',
         6: 'Ready'
        }


class JobFillerState(StateMachine):
    states = {
        -1: 'Error',
         0: 'CreatingResources',
         1: 'EnsureDNS',
         2: 'DeployingMonitoringCode',
         3: 'DeployingJobCode',
         4: 'LaunchingTasks',
         5: 'AddingtoMonitoring',
         6: 'EnsureBaseDNS',
         7: 'Reboot Dependent Jobs',
         8: 'Done'
         }


class JobFiller(object):
    def __init__(self, num_cores, job, zone, idle_machines=None,
                 raw_machines=None, reboot_task=False, post_callback=None,
                 fail_on_error=False):
        self.num_cores = num_cores
        self.job = job
        self.zone = zone
        self.machines = idle_machines or []
        self.state = JobFillerState()
        self.thread = None
        self.end_time = None
        self.reboot_task = reboot_task
        self.post_callback = post_callback
        self.fail_on_error = fail_on_error

        if not raw_machines:
            raw_machines = []

        self.machine_states = {}

        for machine in self.machines:
            self.machine_states[machine] = MachineDeploymentState()
            self.machine_states[machine].set_state(3)

        for machine in raw_machines:
            self.machine_states[machine] = MachineDeploymentState()
            self.machine_states[machine].set_state(1)
            self.machines.append(machine)

    def __str__(self):
        return "%s cores in %s for %s" % (self.num_cores,
                                          self.zone, self.job)

    def num_remaining(self):
        if self.is_done():
            return 0
        else:
            return self.num_cores

    def start(self):
        self.thread = threading.Thread(
            target=self.run,
            name="JobFiller-%s:%s:%s" % (
                self.job.name,
                self.zone,
                self.num_cores))

        self.thread.start()

    def is_done(self):
        return self.state.get_state() == 8

    def run(self):
        logger.info("Starting JobFiller")
        release_attempts = 1
        while self.state.get_state() != 8:
            state = self.state.get_state()
            logger.info("Running State: %s, attempt #%s" % (
                    str(self.state),
                    release_attempts))

            try:
                if state == 0:
                    self.run_create_resources()
                elif state == 1:
                    self.ensure_dns()
                elif state == 2:
                    self.deploy_monitoring_code()
                elif state == 3:
                    self.deploy_job_code()
                elif state == 4:
                    self.launch_tasks()
                elif state == 5:
                    self.add_to_monitoring()
                elif state == 6:
                    self.ensure_dns(do_basename=True)
                elif state == 7:
                    self.reboot_dependent_jobs()
            except:
                release_attempts += 1
                import traceback
                traceback.print_exc()
                logger.error(traceback.format_exc())

                if release_attempts > 10 or self.fail_on_error:
                    logger.info("Job Filler: Failed")
                    ClusterEventManager.handle(
                        "Failed Filling: %s" % str(self))

                    if self.post_callback:
                        self.post_callback(success=False)

                    return False

        ClusterEventManager.handle(
            "Completed Filling: %s" % str(self))
        logger.info("Job Filler: Done!")
        self.end_time = datetime.now()

        if self.post_callback:
            self.post_callback(success=True)

        return True

    def run_create_resources(self):
        if len(self.machines) < self.num_cores:
            #!MACHINEASSUMPTION! Should calculate machines/cores
            # (input_machines.num_cores).
            new_machine_count = self.num_cores - len(self.machines)
            # Then spin some up!
            if self.launch_machines(new_machine_count):
                self.state.next()
            else:
                logger.warn(
                    "Couldn't launch machines for %s?" % self.job.name)
        else:
            self.state.next()

    def ensure_dns(self, do_basename=False):
        basename = "%s.%s" % (self.zone, self.job.dns_basename)
        provider = self.job.sitter.dns_provider
        if not self.job.dns_basename or not provider:
            self.state.next()
            return

        num_machines_total = self.job.get_num_required_machines_in_zone(
            self.zone, self.state)

        try:
            records = provider.get_records(hostName=basename)
        except:
            import traceback
            logger.error("Couldn't download records for %s" % basename)
            logger.error(traceback.format_exc())
            records = []

        # Find out what records exist underneath this basename
        # and ensure the machine objects have them
        record_by_ip = {}
        used_prefixes = []
        records_for_basename = []
        for record in records:
            if record['record'] == basename:
                records_for_basename.append(record['value'])

            pieces = record['record'].split('.')
            if pieces[0].isdigit() and '.'.join(pieces[1:]) == basename:
                used_prefixes.append(int(pieces[0]))
                record_by_ip[record['value']] = record['record']

        for machine in self.machines:
            ip = socket.gethostbyname(machine.config.hostname)
            machine.config.ip = ip
            # Ensure 2 things:
            # 1) x.basename exists for this machine
            # 2) basename includes this machine's IP as an a record

            # Part 1
            if ip in record_by_ip:
                machine.config.dns_name = record_by_ip[ip]
            else:
                # We need a new name!  Get the first valid one
                new_prefix = None

                for i in range(num_machines_total):
                    if i not in used_prefixes:
                        new_prefix = i
                        used_prefixes.append(i)
                        break

                if new_prefix == None:
                    # This only happens if the state of DNS
                    # records somehow doesn't match the state
                    # of this job, which shouldn't happen
                    if used_prefixes:
                        new_prefix = max(used_prefixes) + 1
                    else:
                        new_prefix = 0

                machine.config.dns_name = "%s.%s" % (new_prefix,
                                                     basename)
                logger.info("Assigning %s to %s" % (
                        machine.config.dns_name, ip))

                if not provider.add_record(ip,
                                          machine.config.dns_name):
                    logger.error("Couldn't assign DNS for %s" % (
                            machine.config.dns_name))

            # Part 2
            if do_basename:
                """
                We should only add to the basename once the machine
                is up and running.  Otherwise we're effectively adding
                this machine 'to the pool' before its ready, potentially
                causing problems.
                """
                if ip not in records_for_basename:
                    logger.info("Adding %s to %s" % (ip, basename))
                    if not provider.add_record(ip, basename):
                        logger.error("Couldn't assign DNS for %s -> %s" % (
                                ip,
                                basename))

        self.state.next()

    def deploy_monitoring_code(self):
        # TODO Parallelize this somehow
        for machine in self.machines:
            recipe = self.job.sitter.build_recipe(
                MachineSitterRecipe,
                machine,
                post_callback=None,
                options={'log_location': self.job.sitter.log_location})

            self._do_recipe_deployment(2, 3, machine,
                                       recipe)
        self.state.next()

    def _do_recipe_deployment(self, old_state,
                              new_state,
                              machine,
                              recipe):
        # We'll try and deploy to a machine
        # for a total of 2 minutes.  This is random
        # and probably a terrible way to limit
        # how many times we try and deploy, but
        # I can't think of something better right now.  *shrug*.
        start_time = datetime.now()

        while self.machine_states[machine].get_state() <= old_state:
            self.machine_states[machine].set_state(old_state)
            recipe.connect()
            val = recipe.deploy()
            if val:
                self.machine_states[machine].set_state(new_state)
            else:
                logger.warn(
                    "Couldn't deploy code to %s?" % (
                        str(machine)))

                if datetime.now() - start_time > timedelta(minutes=2):
                    self.machine_states[machine].set_state(new_state)
                    logger.error("Giving up deploying to %s" % str(
                            machine))

                    raise Exception("Failed to deploy!")

                time.sleep(1)

    def deploy_job_code(self):
        # TODO Parallelize this somehow
        for machine in self.machines:
            while self.machine_states[machine].get_state() <= 3:
                self.machine_states[machine].set_state(3)

                if not self.job.deployment_recipe:
                    self.machine_states[machine].set_state(4)
                    continue

                recipe = self.job.sitter.build_recipe(
                    self.job.deployment_recipe,
                    machine,
                    post_callback=None,
                    options=self.job.recipe_options,
                    given_logger=logger)

                self._do_recipe_deployment(3, 4,
                                           machine,
                                           recipe)

        self.state.next()

    def reboot_dependent_jobs(self):
        if not self.reboot_task:
            self.state.next()
            return

        jobs = self.job.find_dependent_jobs()
        for machine in self.machines:
            task_names = [task['name'] for task in machine.get_running_tasks()]
            for job in jobs:
                if job.name in task_names:
                    machine.stop_task(job)
                    machine.start_task(job)

        self.state.next()

    def launch_tasks(self):
        # TODO Parallelize this somehow
        for machine in self.machines:
            while self.machine_states[machine].get_state() <= 4:
                self.machine_states[machine].set_state(4)
                machine.initialize()
                if self.reboot_task:
                    machine.stop_task(self.job)

                val = machine.start_task(self.job)

                if not val:
                    # Couldn't start the task, which is odd
                    # Should bail out?
                    break

                # Now verify that its started
                machine._api_get_stats()
                tasks = machine.get_running_tasks()
                task_names = [task['name'] for task in tasks]
                if self.job.name in task_names:
                    self.machine_states[machine].set_state(5)
                else:
                    logger.warn(
                        "Tried to start %s on %s but failed?" % (
                            self.job.name, str(machine)))

        self.state.next()

    def add_to_monitoring(self):
        # Ensure the machines aren't already monitored
        machines_to_add = []
        # TODO move this to a function in clustersitter.py
        # def check_already_monitored or some such
        for machine in self.machines:
            found = False
            for monitor, _ in self.job.sitter.state.monitors:
                if machine in monitor.monitored_machines:
                    found = True
                    break

            if not found:
                machines_to_add.append(machine)

        if machines_to_add:
            self.job.sitter.add_machines(machines_to_add, update_dns=False)

        # Now wait for the machines to actually be monitored
        for machine in self.machines:
            ready = False
            while not ready:
                loaded_data = machine.has_loaded_data()
                has_task = self.job.name in machine.get_tasks()
                if self.job.name == "Machine Doctor Redeployer":
                    has_task = True

                ready = loaded_data and has_task
                if not ready:
                    logger.debug(
                        "Waiting for machine to be actually monitored...")
                    time.sleep(0.1)
                else:
                    logger.debug("%s has our task, good to go!" % machine)

        self.state.next()

    def launch_machines(self, new_machine_count):
        provider = self.job.sitter.state.provider_by_zone[self.zone]
        mem_per_job = self.job.deployment_layout[self.zone]['mem']

        machineconfigs = provider.fill_request(zone=self.zone,
                                               cpus=new_machine_count,
                                               mem_per_job=mem_per_job)
        if not machineconfigs:
            return False

        machines = [MonitoredMachine(m) for m in machineconfigs]

        for machine in machines:
            self.machine_states[machine] = MachineDeploymentState()

        self.machines.extend(machines)

        return True
