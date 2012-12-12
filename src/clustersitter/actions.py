
import logging
from managedstate import ManagedState
from jobfiller import JobFiller
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
        self.thread = Thread(target=self.execute)
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


class MachineAction(ClusterAction):
    """
    Base class for running something against a machine.
    """

    def __init__(self, sitter, machine):
        """
        Initialize the action.

        @param state The cluster state. Updated by the action.
        @param machine The machine to remove the task from.
        """
        super(MachineAction, self).__init__(sitter)
        self.machine = machine

    def run(self):
        """
        Run the action. Sets the machine status to maintenance and calls
        run_maintenance.
        """
        try:
            self.state.current.update_machine(
                self.machine, ManagedState.Maintenance)
            return self.run_maintenance()
        finally:
            self.state.current.update_machine(
                self.machine, ManagedState.Active)


class TaskAction(ClusterAction):
    """
    Base class for running something against a task.
    """

    def __init__(self, sitter, machine, job):
        """
        Initialize the action.

        @param state The cluster state. Updated by the action.
        @param machine The machine to manage the task from.
        @param job The job to run the action against.
        """
        super(TaskAction, self).__init__(sitter)
        self.machine = machine
        self.job = job

    def run(self):
        """
        Run the action. Sets the task status to maintenance and calls
        run_maintenance.
        """
        task = self.state.get_task(self.machine, self.job)
        status = task['status']
        try:
            self.state.current.update_task(
                self.machine, self.job, ManagedState.Maintenance)
            status = self.run_maintenance()
        finally:
            self.state.current.update_task(
                self.machine, self.job, status)


class JobAction(ClusterAction):
    """
    Base class for managing a job on a series of machines.
    """

    def __init__(self, sitter, zone, machines, job):
        """
        Initialize the action.

        @param state The cluster state. Updated by the action.
        @param zone The zone the job is being deployed to.
        @param machines The machines to manage the job on.
        @param job The job to run the action against.
        """
        super(JobAction, self).__init__(sitter)
        self.zone = zone
        self.machines = machines
        self.job = job

    def run(self):
        """
        Run the action. Sets all machines to maintenance and calls
        run_maintenance.
        """
        try:
            for machine in self.machines:
                self.state.current.update_machine(
                    machine, status=ManagedState.Maintenance)
            self.run_maintenance()
        finally:
            for machine in self.machines:
                self.state.current.update_machine(
                    machine, status=ManagedState.Active)


class StartTaskAction(TaskAction):
    """Start a task."""

    def run_maintenance(self):
        """Run the action in maintenance mode."""
        if not self.machine['machine'].start_task(self.task['job']):
            raise ClusterActionError("failed to start job '%s' on machine '%s'" % (
                self.job.name, self.machine.hostname))
        return ManagedState.Running


class RestartTaskAction(TaskAction):
    """Restart a task."""

    def run_maintenance(self):
        """Run the action in maintenance mode."""
        if not self.machine['machine'].restart_task(self.task['job']):
            raise ClusterActionError("failed to restart job '%s' on machine '%s'" % (
                self.job.name, self.machine.hostname))
        return ManagedState.Running


class StopTaskAction(TaskAction):
    """Stop a task."""

    def run_maintenance(self):
        """Run the action."""
        if not self.machine['machine'].stop_task(self.task['job']):
            raise ClusterActionError("failed to stop job '%s' on machine '%s'" % (
                self.job.name, self.machine.hostname))
        return ManagedState.Stopped


class DeployJobAction(JobAction):
    """Deploy a job."""

    def run_maintenance(self):
        """Run the action in maintenance mode."""
        idle_machines = []
        pending_machines = []
        for machine in self.machines:
            if self.state.is_pending_machine(machine):
                pending_machines.append(machine)
            else:
                idle_machines.append(machine)

        def filler_callback(filler, success):
            """Update pending machines with real machines."""
            new_machines = list(filler.new_machines)
            while len(pending_machines) > 0 and len(new_machines) > 0:
                new = new_machines.pop(0)
                pending = pending_machines.pop(0)
                self.state.desired.update_machine(pending, obj=new)

            while len(new_machines) > 0:
                self.state.desired.add_machine(self.zone, new_machines.pop(0))

            for machine in filler.new_machines:
                self.state.current.add_machine(self.zone, machine)
            for machine in filler.machines:
                self.state.current.add_task(machine, self.job)

        filler = JobFiller(
            num_cores=len(self.machines), job=self.job, zone=self.zone,
            idle_machines=idle_machines, post_callback=filler_callback)
        filler.run()

class UndeployJobAction(JobAction):
    """Undeploy a job."""

    def run_maintenance(self):
        """Run the action in maintenance mode."""


class DecomissionMachineAction(MachineAction):
    """Decomission an existing machine."""

    def run_maintenance(self):
        """Run the action in maintenance mode."""
        for machine in self.machines:
            self.state.remove_machine(machine)
            self.sitter.decomission_machine(machine)
