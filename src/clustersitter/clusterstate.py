
import logging
import time
from actions import (
    DecomissionMachineAction, DeployJobAction, UndeployJobAction,
    StartTaskAction, StopTaskAction)
from managedstate import ManagedState, PendingMachine
from threading import Thread

logger = logging.getLogger(__name__)


class ClusterState(object):
    """
    Cluster state. Manages the state of the cluster and generates actions as a
    result of state changes.
    """

    def __init__(self, sitter):
        """
        Initialize an empty state object.

        @param sitter The cluster sitter object.
        """
        self.thread = None
        self.running = False
        self.sleep = sitter.stats_poll_interval
        self.sitter = sitter
        self.monitors = []
        self.loggers = []
        self.providers = {}
        self.jobs = {}
        self.desired = ManagedState()
        self.current = ManagedState()
        self.max_idle_per_zone = -1
        self.pending_actions = []
        self.running_actions = []

    def get_provider(self, zone):
        """
        Get the provider for a zone.

        @param zone The name of the zone.
        """
        return self.providers.get(zone)

    def set_provider(self, zone, provider):
        """
        Set the provider for a zone.

        @param zone The name of the zone.
        @param provider The provider for the zone.
        """
        self.providers[zone] = provider

    def has_provider(self, zone):
        """
        Check if the zone has a provider.

        @param zone The name of the zone.
        """
        return zone in self.providers

    def get_machines(self, zones=None):
        """
        Get machines. Machines are returned in a dict categorized by zone.

        @param zones Only return machines in these zones.
        @return A dictionary of machines categorized by zone. Each key is the
            zone name. The value is a list of machines by zone.
        """
        if isinstance(zones, basestring):
            zones = [zones]
        machines = {}

        def map_machine(state, machine):
            if zones is None or machine['zone'] in zones:
                if machine['zone'] not in machines:
                    machines[machine['zone']] = []
                machines[machine['zone']].append(machine['machine'])

        self.current.map_machines(map_machine)
        return machines

    def get_idle_machines(self, zones=None):
        """
        Get idle machines.

        @param zones Only return machines in these zones.
        @return A dictionary of zone/idle machine mappings. The keys are the
            zone names and the values are a list of idle machines in that zone.
        """
        if isinstance(zones, basestring):
            zones = [zones]
        machines = {}

        def map_idle_machine(state, machine):
            if (zones is None or machine['zone'] in zones and
                    state.is_idle_machine(machine)):
                if machine['zone'] not in machines:
                    machines[machine['zone']] = []
                machines[machine['zone']].append(machine['machine'])

        self.current.map_machines(map_idle_machine)
        return machines

    def get_job_machines(self, job, zones=None):
        """
        Get the machines running a job.

        @param job The job to find machines for.
        @return A list of machines running the job.
        """
        if isinstance(job, basestring):
            job = self.jobs.get(job)
        if isinstance(zones, basestring):
            zones = [zones]
        machines = {}

        if job is None:
            return []

        def map_job_machine(state, machine, task):
            if (zones is None or machine['zone'] in zones and
                    job == task['job']):
                if machine['zone'] not in machines:
                    machines[machine['zone']] = []
                machines[machine['zone']].append(machine['machine'])

        self.current.map_tasks(map_job_machine)
        return machines

    def add_job(self, job):
        """
        Add a job to the cluster. The job is allocated to idle machines.
        New machines are added to the state if necessary. If a job exists with
        the same name it will be replaced. Exact jobs will not be replaced.

        @param job The job to add to the cluster. The job is configured with
            the required zones and number of machines.
        @return True if the job was added. False if it was not.
        @raise ClusterStateError If a child job is added before its parent.
        """
        if job.name in self.jobs:
            if job == self.jobs[job.name]:
                return
            self.remove_job(self.jobs[job.name])

        zones = job.get_shared_fate_zones()
        idle_machine_zones = self.get_idle_machines(zones=zones)
        for zone in zones:
            num_required_machines = job.get_num_required_machines_in_zone(zone)
            idle_machines = idle_machine_zones.get(
                zone, [])[:num_required_machines]
            num_pending_machines = num_required_machines - len(idle_machines)
            for machine in idle_machines:
                self.desired.add_task(machine, job)
            for n in range(num_pending_machines):
                machine = self.desired.add_machine(zone)
                self.desires.add_task(machine, job)

    def remove_job(self, job):
        """
        Remove a job from the cluster. Child jobs will be removed as well.

        @param job The job to remove.
        """
        if isinstance(job, basestring):
            job = self.jobs.get(job)
        if job is not None:
            jobs = [job]
            jobs.extend(job.find_dependent_jobs())
            while jobs:
                self.desired.remove_tasks(jobs.pop(0))

    def get_zones(self):
        """
        Get a list of active zones.
        
        @return A list of active zone names.
        """
        return self.providers.keys()

    def add_machine(self, zone, machine):
        """
        Add a machine to the cluster.

        @param machine The machine to add to the cluster.
        """
        self.desired.add_machine(zone, machine)
        self.current.add_machine(zone, machine)

    def remove_machine(self, machine):
        """
        Remove a machine from the cluster.

        @param machine The machine to remove.
        """
        self.desired.remove_machine(machine)
        for monitor in self.monitors:
            monitor[0].remove_machine(machine)

    def start_task(self, machine, task):
        """
        Start a task on a machine.

        @param machine The machine on which the task is running.
        @param task The name of the task to start.
        """
        if task in self.jobs:
            self.desired.update_task(
                machine, self.jobs[task], ManagedState.Running)

    def stop_task(self, machine, task):
        """
        Stop a task on a machine.

        @param machine The machine on which the task is running.
        @param task The name of the task to stop.
        """
        if task in self.jobs:
            self.desired.update_task(
                machine, self.jobs[task], ManagedState.Stopped)

    def restart_task(self, machine, task):
        """
        Restart a task on a machine.

        @param machine The machine on which the task is running.
        @param task The name of the task to restart.
        """
        if task in self.jobs:
            self.desired.update_task(
                machine, self.jobs[task], ManagedState.Restart)

    def pause_machine(self, machine):
        """
        Pause all maintenance on a machine. State changes will not be
        propogated to the machine while it is pause.

        @param machine The machine to pause.
        @return True if the machine was paused, False if it was not (machine
            does not exist).
        """
        self.desired.update_machine(machine, ManagedState.Paused)

    def move_task(self, from_machine, to_machine, task):
        """
        Move a task from one machine to another. Will also move child tasks.

        @param from_machine The machine currently running the task.
        @param to_machine The machine that should be running the task.
        @param task The task to move.
        """
        if task in self.jobs:
            jobs = [self.jobs[task]]
            jobs.extend(self.jobs[task].find_dependent_jobs())
            for job in jobs:
                self.desired.move(from_machine, to_machine, job)

    def calculate_current_state(self):
        """
        Update the current state. Machine and task states are updated to
        reflect current real world values.
        """
        def map_machine_state(state, machine):
            # Generate current task list
            current_tasks = {}
            for task in machine['tasks']:
                if task['job'] is not None:
                    current_tasks[task['job'].name] = (task['status'], task['job'])

            # Generate monitored task list
            monitored_tasks = {}
            running_tasks = machine['machine'].get_running_tasks()
            for task in machine['machine'].get_tasks():
                status = ManagedState.Stopped
                if task in running_tasks:
                    status = ManagedState.Running
                monitored_tasks[task] = (status, self.jobs.get(task))

            # We're going to do some easy set math.
            monitored_task_set = set(monitored_tasks.keys())
            current_task_set = set(current_tasks.keys())

            # Add missing tasks
            for task in monitored_task_set - current_task_set:
                state.add_task(
                    machine, monitored_tasks[task][1],
                    monitored_tasks[task][0])

            # Remove extra tasks
            for task in current_task_set - monitored_task_set:
                state.remove_task(machine, current_tasks[task][1])

            # Update task state
            for task in current_task_set & monitored_task_set:
                state.update_task(
                    machine, monitored_tasks[task][1],
                    monitored_tasks[task][0])

        self.current.map_machines(map_machine_state)

    def calculate_actions(self):
        """
        Diff the desired (self) state against the current (provided) state. The
        resulting actions will perform the changes that need to be made to move
        the current state to the desired state.

        @return A list of actions.
        """
        actions = []
        idle_machines = {}

        # Pause and activate machines.
        def map_pause_machine(current, current_machine):
            if current_machine['status'] == ManagedState.Maintenance:
                return
            desired_machine = self.desired.get_machine(current_machine)
            if (desired_machine is not None and
                    current_machine['status'] != desired_machine['status']):
                current.update_machine(
                    current_machine, status=desired_machine['status'])

        self.current.map_machines(map_pause_machine)

        # Remove tasks that need removing.
        def map_remove_task(current, machine, task):
            if (task['job'] is None or
                    not self.current.is_mutable_task(machine, task)):
                return
            if not self.desired.has_task(machine, task):
                action = UndeployJobAction(
                    self.sitter, machine['zone'], [machine['machine']],
                    task['job'])
                # We need to know if this will create an idle machine.
                if self.current.is_idle_machine(machine, exclude=task):
                    if machine['zone'] not in idle_machines:
                        machine['zone'] = []
                    idle_machines['zone'].append(machine)
                actions.append(action)

        self.current.map_tasks(map_remove_task)

        # Start, stop, or restart tasks that aren't in the right state.
        def map_task_status(current, machine, current_task):
            if (current_task['job'] is None or
                    not current.is_mutable_task(machine, current_task)):
                return
            desired_task = self.desired.get_task(current_task)
            if desired_task is None:
                return
            if current_task['status'] != desired_task['status']:
                if desired_task['status'] == ManagedState.Running:
                    actions.append(StartTaskAction(
                        self.sitter, machine['machine'], current_task['job']))
                elif desired_task['status'] == ManagedState.Stopped:
                    actions.append(StopTaskAction(
                        self.sitter, machine['machine'], current_task['job']))

        self.current.map_tasks(map_task_status)

        # Calculate idle machines.
        def map_idle_machine(state, machine):
            if state.is_idle_machine(machine):
                zone = machine['zone']
                if zone not in idle_machines:
                    idle_machines[zone] = []
                idle_machines[zone].append(machine)

        self.current.map_machines(map_idle_machine)

        # Resolve pending machines with available idle machines.
        def map_pending_with_idle(desired, machine):
            if not self.current.is_mutable_machine(machine):
                return
            if desired.is_pending_machine(machine):
                if zone in machine['zone']:
                    idle_zone = idle_machines[machine['zone']]
                    if len(idle_zone) > 0:
                        idle_machine = idle_zone.pop(0)
                        desired.update_machine(
                            machine, idle_machine['machine'])

        self.desired.map_machines(map_pending_with_idle)

        # Deploy missing tasks.
        deploy_jobs = {}

        def map_missing_task(desired, desired_machine, task):
            current_machine = self.current.get_machine(desired_machine)
            if (current_machine is None or
                    not self.current.is_mutable_machine(current_machine)):
                return
            if not self.current.has_task(current_machine, task):
                if task['job'].name not in deploy_jobs:
                    deploy_jobs[task['job'].name] = (task['job'], [])
                deploy_jobs[task['job'].name][1].append(
                    current_machine['machine'])

        self.desired.map_tasks(map_missing_task)
        for job, machines in deploy_jobs.values():
            actions.append(DeployJobAction(self.sitter, machines, job))

        # Resolve pending machines with new machines.
        """Not sure if necessary? Breaks job filler I think.
        def map_pending_with_new(desired, desired_machine):
            if (self.current.has_machine(desired_machine) and
                    not self.current.is_mutable_machine(desired_machine)):
                return
            if desired.is_pending_machine(desired_machine):
                actions.append(DeployMachineAction(
                    self, desired_machine['machine']))

        self.desired.map_machines(map_pending_with_new)
        """

        # Decomission extra machines.
        if self.max_idle_per_zone >= 0:
            for zone, idle_zone in idle_machines.iteritems():
                while len(idle_zone) > self.max_idle_per_zone:
                    self.desired.remove_machine(idle_zone.pop())

        def map_decomission_machine(current, current_machine):
            if not current.is_mutable_machine(current_machine):
                return
            if not self.desired.has_machine(current_machine):
                actions.append(
                    DecomissionMachineAction(
                        self.sitter, current_machine['machine']))

        self.current.map_machines(map_decomission_machine)

        return actions

    def calculate(self):
        """
        Handle all state calculations.

        @return Actions generated by the state calculations.
        """
        print("Desired state: %s" % self.desired.state)
        print("Current state: %s" % self.current.state)
        self.calculate_current_state()
        self.pending_actions.extend(self.calculate_actions())

    def process(self):
        """
        Process actions in their own threads.
        """
        while len(self.pending_actions) > 0:
            action = self.pending_actions.pop(0)
            action.start()
            self.running_actions.append(action)

    def run(self):
        """
        Run a full calculate/process cycle.
        """
        try:
            self.calculate()
            self.process()
        except:
            import traceback
            logger.error("failure during state cycle")
            logger.error(traceback.format_exc())

    def start(self):
        """
        Run the state calculator until stopped.
        """
        def run_loop():
            while self.running:
                self.run()
                time.sleep(self.sleep)

        if self.thread is None or not thread.is_alive():
            self.thread = Thread(target=run_loop, name="Calculator")
        self.running = True
        self.thread.start()

    def stop(self, timeout=None):
        """
        Stop the state calculator.
        """
        self.running = False
        self.thread.join()
