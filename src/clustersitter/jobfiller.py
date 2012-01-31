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
         6: 'Done'
         }


class JobFiller(object):
    def __init__(self, num_cores, job, zone, idle_machines=None,
                 raw_machines=None):
        self.num_cores = num_cores
        self.job = job
        self.zone = zone
        self.machines = idle_machines or []
        self.state = JobFillerState()
        self.thread = None

        if not raw_machines:
            raw_machines = []

        for machine in self.machines:
            machine.state = MachineDeploymentState()
            machine.state.set_state(3)

        for machine in raw_machines:
            machine.state = MachineDeploymentState()
            machine.state.set_state(1)
            self.machines.append(machine)

    def __str__(self):
        return "%s cores in %s for %s" % (self.num_cores,
                                          self.zone, self.job)

    def num_remaining(self):
        if self.state.get_state() < 3:
            return self.num_cores
        else:
            count = 0
            for machine in self.machines:
                if not machine.state or machine.state.get_state() != 6:
                    count += 1

            return count

    def start_fill(self):
        self.thread = threading.Thread(
            target=self.run,
            name="JobFiller-%s:%s:%s" % (
                self.job.name,
                self.zone,
                self.num_cores))

        self.thread.start()

    def is_done(self):
        return self.state.get_state() == 6

    def run(self, fail_on_error=False):
        logger.info("Starting JobFiller")
        while self.state.get_state() != 6:
            state = self.state.get_state()
            logger.info("Running State: %s" % str(self.state))

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
            except:
                import traceback
                traceback.print_exc()
                logger.error(traceback.format_exc())
                if fail_on_error:
                    return False

        # We're done, so clear the deployment states
        for machinedata in self.machines:
            machine.state = None

        ClusterEventManager.handle("Completed Filling: %s" % str(self))
        logger.info("Job Filler: Done!")
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

    def ensure_dns(self):
        basename = "%s.%s" % (self.zone, self.job.dns_basename)
        provider = self.job.sitter.dns_provider
        if not basename or not provider:
            self.state.next()
            return

        num_machines_total = self.job.get_num_required_machines_in_zone(
            self.zone)

        records = provider.get_records(hostName=basename)
        # Find out what records exist underneath this basename
        # and ensure the machine objects have them
        record_by_ip = {}
        used_prefixes = []
        records_for_basename = []
        for record in records:
            if record['record'] == basename:
                records_for_basename.append(record['value'])

            pieces = record['record'].split('.')
            if pieces[0].isdigit() and pieces[1:] == basename:
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

                machine.config.dns_name = "%s.%s" % (new_prefix,
                                                     basename)
                logger.info("Assigning %s to %s" % (
                        machine.config.dns_name, ip))
                try:
                    ret = provider.add_record(ip,
                                              machine.config.dns_name)
                except:
                    ret = None

                if not provider.valid_response(ret):
                    logger.error("Couldn't assign DNS for %s: %s" % (
                            machine.config.dns_name,
                            str(ret)))

            # Part 2
            if ip not in records_for_basename:
                logger.info("Adding %s to %s" % (ip, basename))
                ret = None
                try:
                    ret = provider.add_record(ip, basename)
                except:
                    pass

                if not provider.valid_response(ret):
                    logger.error("Couldn't assign DNS for %s -> %s: %s" % (
                            ip,
                            basename,
                            str(ret)))

        self.state.next()

    def deploy_monitoring_code(self):
        # TODO Parallelize this somehow
        for machine in self.machines:
            recipe = self.job.sitter.build_recipe(
                MachineSitterRecipe,
                machine,
                post_callback=None,
                options=None)

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
        while machine.state.get_state() <= old_state:
            machine.state.set_state(old_state)
            recipe.connect()
            val = recipe.deploy()
            if val:
                machine.state.set_state(new_state)
            else:
                logger.warn(
                    "Couldn't deploy monitoring code to %s?" % (
                        str(machine)))

                if datetime.now() - start_time > timedelta(minutes=2):
                    machine.state.set_state(new_state)
                    logger.error("Giving up deploying to %s" % str(
                            machine))

                    raise Exception("Failed to deploy!")

                time.sleep(1)

    def deploy_job_code(self):
        # TODO Parallelize this somehow
        for machine in self.machines:
            while machine.state.get_state() <= 3:
                machine.state.set_state(3)

                if not self.job.deployment_recipe:
                    machine.state.set_state(4)
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

    def launch_tasks(self):
        # TODO Parallelize this somehow
        for machine in self.machines:
            while machine.state.get_state() <= 4:
                machine.state.set_state(4)
                machine.initialize()
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
                    machine.state.set_state(5)
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
            self.job.sitter.add_machines(machines_to_add)

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
            machine.state = MachineDeploymentState()

        self.machines.extend(machines)

        return True
