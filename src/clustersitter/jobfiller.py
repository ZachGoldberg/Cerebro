import logging
import threading

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
         1: 'DeployingMonitoringCode',
         2: 'DeployingJobCode',
         3: 'LaunchingTasks',
         4: 'AddingtoMonitoring',
         5: 'Done'
         }


class JobFiller(object):
    def __init__(self, num_cores, job, zone, idle_machines):
        self.num_cores = num_cores
        self.job = job
        self.zone = zone
        self.machines = idle_machines
        self.state = JobFillerState()
        self.thread = None

        for machine in self.machines:
            machine.state = MachineDeploymentState()
            machine.state.set_state(3)

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
        return self.state.get_state() == 5

    def run(self):
        logger.info("Starting JobFiller")
        while self.state.get_state() != 5:
            state = self.state.get_state()
            logger.info("Running State: %s" % str(self.state))

            try:
                if state == 0:
                    self.run_create_resources()
                elif state == 1:
                    self.deploy_monitoring_code()
                elif state == 2:
                    self.deploy_job_code()
                elif state == 3:
                    self.launch_tasks()
                elif state == 4:
                    self.add_to_monitoring()
            except:
                import traceback
                traceback.print_exc()
                logger.error(traceback.format_exc())

        # We're done, so clear the deployment states
        for machine in self.machines:
            machine.state = None

        ClusterEventManager.handle("Job %s completed" % str(self))
        logger.info("Job Filler: Done!")

    def run_create_resources(self):
        #!MACHINEASSUMPTION! Should calculate core (input_machines.num_cores).
        if len(self.machines) < self.num_cores:
            new_machine_count = self.num_cores - len(self.machines)
            # Then spin some up!
            if self.launch_machines(new_machine_count):
                self.state.next()
            else:
                logger.warn(
                    "Couldn't launch machines for %s?" % self.job.name)
        else:
            self.state.next()

    def deploy_monitoring_code(self):
        # TODO Parallelize this somehow
        for machine in self.machines:
            while machine.state.get_state() <= 2:
                machine.state.set_state(2)
                recipe = self.job.sitter.build_recipe(
                    MachineSitterRecipe,
                    machine,
                    post_callback=None,
                    options=None)

                val = recipe.deploy()
                if val:
                    machine.state.set_state(3)
                else:
                    logger.warn(
                        "Couldn't deploy monitoring code to %s?" % (
                            str(machine)))
        self.state.next()

    def deploy_job_code(self):
        # TODO Parallelize this somehow
        for machine in self.machines:
            while machine.state.get_state() <= 3:
                machine.state.set_state(3)
                val = True
                if self.job.deployment_recipe:
                    recipe = self.job.sitter.build_recipe(
                        self.job.deployment_recipe,
                        machine,
                        post_callback=None,
                        options=self.job.recipe_options)

                    val = recipe.deploy()

                if val:
                    machine.state.set_state(4)
                else:
                    logger.warn(
                        "Couldn't deploy job code to %s?" % (
                            str(machine)))

        self.state.next()

    def launch_tasks(self):
        # TODO Parallelize this somehow
        for machine in self.machines:
            while machine.state.get_state() <= 4:
                machine.state.set_state(4)
                machine.initialize()
                machine.start_task(self.job)

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
