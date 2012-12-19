
import logging
from jobfiller import JobFiller
from productionjob import ProductionJob
from threading import Thread

logger = logging.getLogger(__name__)


class ClusterActionError(Exception):
    """Raised on Action error."""


class ClusterActionStatus(object):
    """
    Action status.

    Pending -- Has not been run.
    Running -- Is running,
    Finished -- Finished running.
    """
    Pending = 'pending'
    Running = 'running'
    Finished = 'finished'


class ClusterActionGenerator(object):
    """
    Generate optimized actions for a state diff.
    """

    def __init__(self, sitter):
        """Initialize the generator."""
        self.update_actions = {}
        self.deploy_actions = {}
        self.sitter = sitter

    def _ensure_update(self, zone, machine):
        """Ensure there is space in the update_actions dict."""
        if zone not in self.update_actions:
            self.update_actions[zone] = {}
        if machine.hostname not in self.update_actions[zone]:
            self.update_actions[zone][machine.hostname] = {
                'machine': machine,
                'add': [],
                'remove': [],
                'update': {},
            }

    def _ensure_deploy(self, zone):
        """Ensure there is space in the deploy_actions dict."""
        if zone not in self.deploy_actions:
            self.deploy_actions[zone] = []

    def add_task(self, zone, machine, task):
        """
        Add an "add task" action.

        @param zone The zone the task should run in.
        @param machine The machine to add the task to.
        @param task The name of the task to add.
        """
        self._ensure_update(zone, machine)
        self.update_actions[zone][machine.hostname]['add'].append(task)

    def remove_task(self, zone, machine, task):
        """
        Add a "remove task" action.

        @param zone The zone the task is running in.
        @param machine The machine the task is running on.
        @param task The name of the task to remove.
        """
        self._ensure_update(zone, machine)
        self.update_actions[zone][machine.hostname]['remove'].append(task)

    def update_task(self, zone, machine, task, status):
        """
        Add an "update task" action.

        @param zone The zone the task is running in.
        @param machine The machine the task is running on.
        @param task The name of the task to update.
        @param status The new status of the task.
        """
        self._ensure_update(zone, machine)
        self.update_actions[zone][machine.hostname]['update'][task] = status

    def deploy_machines(self, zone, tasks, machines):
        """
        Deploy new machines to run a task.

        @param zone The zone to deploy the machines to.
        @param tasks The tasks to deploy to the machine.
        @param machines The number of new machines to deploy.
        """
        self._ensure_deploy(zone)
        self.deploy_actions[zone].append({
            'tasks': tasks,
            'machines': machines,
        })

    def generate(self):
        """
        Generate the actions. Each generated action will be a SequentialAction
        containing each of the actions for a particular machine.

        @return A list of generated actions.
        """
        actions = []
        for zone, machines in self.update_actions.iteritems():
            for machine in machines.values():
                sequence = []
                for task, status in machine['update'].iteritems():
                    if status == self.sitter.state.desired_jobs.Running:
                        sequence.append(StartTaskAction(
                            self.sitter, zone, machine['machine'], task))
                    else:
                        sequence.append(StopTaskAction(
                            self.sitter, zone, machine['machine'], task))

                for task in machine['remove']:
                    sequence.append(RemoveTaskAction(
                        self.sitter, zone, machine['machine'], task))

                for task in machine['add']:
                    sequence.append(AddTaskAction(
                        self.sitter, zone, machine['machine'], task))

                sequence_action = SequentialMachineAction(
                    self.sitter, machine['machine'], sequence)
                actions.append(sequence_action)

        for zone, items in self.deploy_actions.iteritems():
            for item in items:
                for n in range(item['machines']):
                    actions.append(
                        DeployMachineAction(self.sitter, zone, item['tasks']))

        return actions


class ClusterAction(object):
    """
    Base class for threaded state actions.
    """

    def __init__(self, sitter):
        """
        Initialize the action.

        @param sitter The cluster sitter object.
        """
        self.sitter = sitter
        self.state = sitter.state
        self.status = ClusterActionStatus.Pending
        self.thread = None

    def is_running(self):
        """
        Check if the action is running.

        @return True if the action is running or False.
        """
        return self.status == ClusterActionStatus.Running

    def is_finished(self):
        """
        Check if the action has completed.

        @return True if the action has finished or False.
        """
        return self.status == ClusterActionStatus.Finished

    def execute(self):
        """
        Run the action synchronously.

        @raise ClusterActionError If the action has already been run.
        """
        if self.status != ClusterActionStatus.Pending:
            raise ClusterActionError("action may not be run more than once")
        self.status = ClusterActionStatus.Running
        try:
            self.run()
        finally:
            self.status = ClusterActionStatus.Finished

    def start(self):
        """
        Run the action asynchronously.

        @raise ClusterActionError If the action has already been run.
        """
        if self.status != ClusterActionStatus.Pending:
            raise ClusterActionError("action may not be run more than once")
        if hasattr(self, 'name'):
            name = self.name
        else:
            name = self.__class__.__name__
        self.thread = Thread(name=name, target=self.execute)
        self.thread.start()

    def join(self):
        """
        Wait for the asyncrhonous running of the action to complete.

        @raise ClusterActionError If the action is not being run
            asynchronously.
        """
        if self.thread is None:
            raise ClusterActionError("action is not being run asynchronously")
        self.thread.join()


class SequentialAction(ClusterAction):
    """
    Execute a series of actions in sequence.
    """

    def __init__(self, sitter, actions):
        """
        Initialize the action.

        @param sitter The cluster sitter object.
        @param actions The actions to execute.
        """
        super(SequentialAction, self).__init__(sitter)
        self.actions = actions

    def run(self):
        """Run the action."""
        for action in self.actions:
            try:
                action.execute()
            except:
                import traceback
                logger.error("failure in sequential action, continuing")
                logger.error(traceback.format_exc())


class SequentialMachineAction(SequentialAction):
    """
    Execute a series of actions in sequence on a single machine. Puts the
    machine in maintenance mode during the run.
    """

    def __init__(self, sitter, machine, actions):
        """
        Initialize the action.

        @param sitter The cluster sitter object.
        @param machine The machine to operate on.
        @param actions The actions to execute.
        """
        super(SequentialMachineAction, self).__init__(sitter, actions)
        self.name = "%s %s" % (self.__class__.__name__, machine.hostname)
        self.machine = machine
        self.initial_status = self.state.get_machine_status(machine)

    def run(self):
        """Run the action sequence in machine maintenance mode."""
        self.state.update_machine(self.machine, self.state.Maintenance)
        try:
            super(SequentialMachineAction, self).run()
        finally:
            self.state.update_machine(self.machine, self.initial_status)


class MachineAction(ClusterAction):
    """
    Base class for running action against a machine.
    """

    def __init__(self, sitter, zone, machine):
        """
        Initialize the action.

        @param sitter The cluster sitter object.
        @param zone The zone the job is being deployed to.
        @param machine The machine to manage the job on.
        """
        super(MachineAction, self).__init__(sitter)
        self.name = "%s %s" % (self.__class__.__name__, machine.hostname)
        self.zone = zone
        self.machine = machine
        self.initial_state = self.state.get_machine_status(machine)

    def run(self):
        """
        Run the action. Sets all machines to maintenance and calls
        run_maintenance.
        """
        try:
            self.state.update_machine(self.machine, self.state.Maintenance)
            self.run_maintenance()
        finally:
            self.state.update_machine(self.machine, self.initial_state)


class TaskAction(MachineAction):
    """
    Base class for managing a task on a machine.
    """

    def __init__(self, sitter, zone, machine, task):
        """
        Initialize the action.

        @param sitter The cluster sitter object.
        @param zone The zone the job is being deployed to.
        @param machine The machine to manage the job on.
        @param task The task to run the action against.
        """
        super(TaskAction, self).__init__(sitter, zone, machine)
        self.name = "%s %s %s" % (
            self.__class__.__name__, machine.hostname, task)
        self.job = self.state.get_job(task)


class StartTaskAction(TaskAction):
    """Start a task."""

    def run_maintenance(self):
        """Run the action in maintenance mode."""
        if not self.machine.start_task(self.job):
            raise ClusterActionError(
                "failed to start job '%s' on machine '%s'" %
                (self.job.name, self.machine.hostname))


class RestartTaskAction(TaskAction):
    """Restart a task."""

    def run_maintenance(self):
        """Run the action in maintenance mode."""
        if not self.machine.restart_task(self.job):
            raise ClusterActionError(
                "failed to restart job '%s' on machine '%s'" %
                (self.job.name, self.machine.hostname))


class StopTaskAction(TaskAction):
    """Stop a task."""

    def run_maintenance(self):
        """Run the action."""
        if not self.machine.stop_task(self.job):
            raise ClusterActionError(
                "failed to stop job '%s' on machine '%s'" %
                (self.job.name, self.machine.hostname))


class AddTaskAction(TaskAction):
    """Add a task to a machine."""

    def run_maintenance(self):
        """Run the action in maintenance mode."""
        if not self.job.deploy(self.zone, self.machine):
            logger.error(
                "failed to deploy job '%s' to machine '%s'" %
                (self.job.name, self.machine.hostname))


class RemoveTaskAction(TaskAction):
    """Remove a task from a machine."""

    def run_maintenance(self):
        """Run the action in maintenance mode."""
        #TODO: Undeploy the job code.
        if not self.machine.stop_task(self.job):
            raise ClusterActionError(
                "failed to stop job '%s' on machine '%s'" %
                (self.job.name, self.machine.hostname))


class DeployMachineAction(ClusterAction):
    """Deploy a set of tasks to a new machine."""

    def __init__(self, sitter, zone, tasks):
        """
        Initialize the action.

        @param zone The zone to deploy the tasks to.
        @param tasks The tasks to deploy.
        """
        super(DeployMachineAction, self).__init__(sitter)
        self.name = "%s %s" % (self.__class__.__name__, ",".join(tasks))
        self.zone = zone
        self.jobs = [self.state.get_job(t) for t in tasks]

    def run(self):
        """Run the action."""
        def fail(job):
            for job in self.jobs:
                self.state.remove_job(job)
            raise ClusterActionError(
                "failed to deploy job '%s' on new machine" %
                job.name)

        # Perform first run and grab the new machine.
        job = self.jobs[0]
        machine = self.jobs[0].deploy(self.zone)
        if not machine:
            fail(self.jobs[0])

        self.state.desired_jobs.set_pending_machines(
            self.zone, self.jobs[0].name, [machine], True)

        for job in self.jobs[1:]:
            if not job.deploy(self.zone, machine):
                fail(job)


class RedeployMachineAction(MachineAction):
    """Redeploy an unreachable machine."""

    def run_maintenance(self):
        """Run the action in maintenance mode."""
        #TODO: Make redeploying less hacky.
        job = ProductionJob(
            self.sitter,
            '', {'name': 'Machine Redeployer'}, {
                self.zone: {
                    'mem': self.machine.config.mem,
                    'cpu': self.machine.config.cpus,
                },
            }, None)

        filler = JobFiller(
            1, job, self.zone, raw_machines=[self.machine], fail_on_error=True)
        if not filler.run():
            self.sitter.decomission_machine(self.machine)


class DecomissionMachineAction(MachineAction):
    """Decomission an existing machine."""

    def run_maintenance(self):
        """Run the action in maintenance mode."""
        self.sitter.decomission_machine(self.machine)
