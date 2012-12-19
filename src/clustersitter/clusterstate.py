"""
Cluster state tracking and maintenance.
"""

import logging
import simplejson
import time
from actions import (
    ClusterActionGenerator, DecomissionMachineAction, RestartTaskAction)
from threading import Thread

logger = logging.getLogger(__name__)


class JobState(object):
    """
    Job state. Manages the state of all running jobs on the cluster.

    Some static status attributes are defined as part of the class. These are:

    Running -- The task is running.
    Stopped -- The task is stopped.
    """

    Running = 'running'
    Stopped = 'stopped'

    def __init__(self):
        """
        Initialize an empty state object.

        State attributes are structured as follows:

            tasks = {
                task_name: [
                    {
                        'machine': machine_object,
                        'status': task_status,
                        'zone': zone,
                    },
                ],
            }
        """
        self.tasks = {}

    def _get_task_item(self, name, machine):
        """
        Get the list item from self.tasks[name] that contains the given task.
        """
        if name in self.tasks:
            for task in self.tasks[name]:
                if task['machine'] == machine:
                    return task
        return None

    def flatten(self):
        """
        Flatten the job state. The result of the flattening is a set of
        three-tuples representing all of the tasks present in the state. Each
        tuple contains the zone, machine, and task name. Tasks that do not have
        machines assigned will not be included. This method is meant to ease
        comparisons of job state objects.

        @return The flattened job set.
        """
        flat = set()
        for name, task_items in self.tasks.iteritems():
            for task in task_items:
                if task['machine'] is not None:
                    flat.add((task['zone'], task['machine'], name))
        return flat

    def is_machine_idle(self, machine):
        """
        Check if a machine has any tasks assigned to it.

        @param machine The machine to check.
        @return True if the machine is idle or False.
        """
        for task_items in self.tasks.values():
            for task in task_items:
                if task['machine'] == machine:
                    return False
        return True

    def remove_machine(self, machine):
        """
        Remove tasks running on the given machine.

        @param machine The machine to remove tasks from.
        """
        for name in self.tasks.keys():
            for task in list(self.tasks[name]):
                if task['machine'] == machine:
                    self.tasks[name].remove(task)
            if not self.tasks[name]:
                del self.tasks[name]

    def has_task(self, name, machine=None):
        """
        Check if a task is deployed to the cluster.

        @param name The name of the task.
        @param machine Check if the task is deployed to this machine.
        @return True if the task is deployed or False.
        """
        if machine:
            return self._get_task_item(name, machine) is not None
        else:
            return name in self.tasks

    def get_task_status(self, name, machine):
        """
        Get the status of the task on the given machine.

        @param name The name of the task.
        @param machine The machine to check in.
        @return The status of the task or None if not present.
        """
        item = self._get_task_item(name, machine)
        if item:
            return item['status']
        return None

    def get_task_machines(self, name=None, zones=None):
        """
        Get machines that have tasks allocated to them.

        @param name Only return machines with this task running on them.
            Optional.
        @param zones Only return machines in these zones. Optional.
        @return A dictionary of zone/machine lists representing the found
            machines.
        """
        if zones:
            zoned_machines = {z: [] for z in zones}
        else:
            zoned_machines = {}

        if name is not None:
            if name not in self.tasks:
                task_items = {}
            else:
                task_items = {name: self.tasks[name]}
        else:
            task_items = self.tasks

        for name, tasks in task_items.iteritems():
            for task in tasks:
                zone = task['zone']
                machine = task['machine']
                if machine:
                    if zone not in zoned_machines:
                        zoned_machines[zone] = []
                    zoned_machines[zone].append(machine)
        return zoned_machines

    def get_machine_tasks(self, machine, status=None):
        """
        Get the tasks running on a machine.

        @param machine The machine to get the running tasks on.
        @param status Only get tasks matching this status. Optional.
        @return A set of task names running on the machine. An empty list if
            the machine is not present.
        """
        names = set()
        for name, tasks in self.tasks.iteritems():
            for task in tasks:
                if task['machine'] == machine:
                    if status is None or status == task['status']:
                        names.add(name)
        return names

    def add_tasks(self, name, zone, machines, create=None, status=None):
        """
        Add tasks to state tracking. The provided machines will be added if
        they are not present. More tasks than available machines may be added
        using the create parameter. Machines should be assigned to these tasks
        later using the set_pending_machines method.

        @param name The name of the task.
        @param zone The zone the tasks are being added to. Machines must be in
            this zone.
        @param machines The machines available to assign to the tasks.
        @param create A count of empty/undeployed machines to fill the task on.
        @param status The initial status of the tasks.
        """
        if create is None:
            create = 0
        if not status:
            status = self.Running

        if name not in self.tasks:
            self.tasks[name] = []

        for machine in machines:
            if not self.has_task(name, machine):
                self.tasks[name].append({
                    'machine': machine,
                    'status': status,
                    'zone': zone})
        for n in range(create):
            self.tasks[name].append({
                'machine': None,
                'status': status,
                'zone': zone})

    def remove_tasks(self, name, machines=None):
        """
        Remove tasks from machines.

        @param name The name of the tasks.
        @param machines The machines to remove the tasks from. Defaults to all
            machines.
        @return A list of tasks that were removed.
        """
        removed = []
        if name in self.tasks:
            removed.append(name)
            if not machines:
                del self.tasks[name]
            else:
                for task in list(self.tasks[name]):
                    if task['machine'] in machines:
                        self.tasks[name].remove(task)
                if not self.tasks[name]:
                    del self.tasks[name]
        return removed

    def update_tasks(self, name, status, machines=None):
        """
        Update the status on a set of tasks.

        @param name The name of the task to update.
        @param machines The machines to update the task status on.
        """
        if name in self.tasks:
            for task in self.tasks[name]:
                if not machines or task['machine'] in machines:
                    task['status'] = status

    def set_pending_deploying(self, zone, name, count):
        """
        Flag a pending task as currently being deployed.

        @param zone The zone the task is running in.
        @param name The name of the task to flag.
        @param count The max number of task in a zone to flag.
        """
        if name in self.tasks:
            flagged = 0
            for task in self.tasks[name]:
                if not task['machine'] and task['zone'] == zone:
                    flagged += 1
                    task['deploying'] = True
                    if flagged >= count:
                        break

    def get_pending_tasks(self):
        """
        Find the tasks that require machines.

        @param zones Only return required machines for these zones.
        @param tasks Only return required machines for these tasks.
        @return A dictionary describing what tasks in which zones require
            machines. The key is the zone name and the value is another
            dictionary. That dictionary's keys are the task names and the
            values are the number of machines required.
        """
        required = {}
        for name, tasks in self.tasks.iteritems():
            for task in tasks:
                zone = task['zone']
                if zone not in required:
                    required[zone] = {}
                if name not in required[zone]:
                    required[zone][name] = 0
                if not task['machine'] and not task.get('deploying', False):
                    required[zone][name] += 1
        return required

    def set_pending_machines(self, zone, name, machines, deploying=False):
        """
        Assign machines to tasks that do not have machines. Each machine will
        be assigned to the given task.

        @param zone The zone to assign to.
        @param name The name of the task to assign machines to.
        @param machines The machines to assign to the task.
        @param deploying True to replace machines that have the deploying flag
        set. Defaults to False.
        """
        for machine in machines:
            for task in self.tasks.get(name, []):
                if (not task['machine'] and task['zone'] == zone and
                        task.get('deploying', False) == deploying):
                    task['machine'] = machine
                    break

    def get_job_fill(self):
        """
        Calculate the job fill. returns a two-tuple with the job fill and job
        machine fill. These in turn look like this:

        job_fill = {
            job_name: {
                zone: task_count,
            },
        }

        job_fill_machines = {
            job_name: {
                zone: [machines],
            },
        }

        @return The job fill.
        """
        job_fill = {}
        job_fill_machines = {}
        for name, tasks in self.tasks.iteritems():
            job_fill[name] = {}
            job_fill_machines[name] = {}
            for task in tasks:
                if task['zone'] not in job_fill[name]:
                    job_fill[name][task['zone']] = 0
                    job_fill_machines[name][task['zone']] = []
                job_fill[name][task['zone']] += 1
                job_fill_machines[name][task['zone']].append(task['machine'])
        return (job_fill, job_fill_machines)


class ClusterState(object):
    """
    Cluster state. Manages the state of the cluster and generates actions as a
    result of state changes.

    Some static status attributes are defined as part of the class. These are:

    Active -- Machine is operating normally.
    Paused -- Machine is paused by admin for maintenance.
    Maintenance -- Maintenance is being performed by the cluster sitter on the
        machine.
    Pending -- Waiting for the machine monitor to initialize the machine so
        that tasks can be added to state tracking.
    Unreachable -- Machine is unreachable.
    """

    Active = 'active'
    Maintenance = 'maint'
    Paused = 'paused'
    Pending = 'pending'
    Unreachable = 'unreachable'

    def __init__(self, sitter):
        """
        Initialize an empty state object.

        State attributes are structured as follows:

            zones = {
                name: provider,
            }

            machines = [
                {
                    'machine': machine_status,
                    'status': machine_status,
                    'zone': zone,
                },
            ]

        @param sitter The cluster sitter object.
        """
        self.thread = None
        self.running = False
        self.sleep = sitter.stats_poll_interval
        self.sitter = sitter
        self.monitors = []
        self.zones = {}
        self.providers = {}
        self.max_idle_per_zone = -1
        self.machines = []
        self.jobs = {}
        self.job_file = "%s/jobs.json" % sitter.log_location
        self.desired_jobs = JobState()
        self.current_jobs = JobState()
        self.pending_actions = []
        self.running_actions = []

        #TODO: Move this to the sitter. State is not a catch-all.
        self.loggers = []

    def _get_machine_item(self, machine):
        """
        Get the list item from self.machines that contains the given machine.
        """
        for item in self.machines:
            if item['machine'] == machine:
                return item
        return None

    def _get_master_job(self, job):
        """Get the master job in a job chain."""
        if job.name not in self.jobs:
            return None
        current_job = job
        next_job = job.find_linked_job()
        while next_job:
            current_job = next_job
            next_job = current_job.find_linked_job()
        return current_job

    def get_job(self, name):
        """
        Get a job by name.

        @return The job object or None if it does not exist.
        """
        return self.jobs.get(name)

    def get_zones(self):
        """
        Get the active zones.

        @return A list of active zone names.
        """
        return self.zones.keys()

    def get_providers(self):
        """
        Get the machine providers.

        @return A dictionary of name/provider mappings.
        """
        return dict(self.providers)

    def add_provider(self, name, provider):
        """
        Add a machine provider to state tracking. Zones in the provider will
        automatically be added.

        @param name The name of the provider.
        @param provider The provider object.
        """
        if name not in self.providers:
            self.providers[name] = provider
            for zone in provider.get_all_shared_fate_zones():
                self.zones[zone] = name
            return True
        return False

    def get_zone_provider(self, zone):
        """
        Get the provider for a zone.

        @param zone The name of the zone.
        @return The provider for the zone or None if not present.
        """
        if zone in self.zones:
            return self.providers[self.zones[zone]]
        return None

    def has_machine(self, machine):
        """
        Check if the machine is present.

        @param machine The machine to check.
        @return True if present or False.
        """
        return self._get_machine_item(machine) is not None

    def is_machine_mutable(self, machine):
        """
        Check if a machine is allowed to have actions run against it.

        @param machine The machine to check.
        @return True if the machine is mutable or False.
        """
        item = self._get_machine_item(machine)
        allow_status = [self.Maintenance, self.Paused, self.Pending]
        if not item or item['status'] in allow_status:
            return False
        return True

    def get_machines(self, zones=None, status=None, idle=None):
        """
        Get the machines running in the given zones.

        @param zones The zones to find machines in.
        @param status Only return machines with this status.
        @param idle True to return idle machines, False to return active
            machines. Defaults to both.
        @return A dictionary of zone to machine list mappings.
        """
        if zones:
            zoned_machines = {z: [] for z in zones}
        else:
            zoned_machines = {}

        for item in self.machines:
            if zones is None or item['zone'] in zones:
                if not status or item['status'] != status:
                    if item['zone'] not in zoned_machines:
                        zoned_machines[item['zone']] = []
                    if idle is None:
                        zoned_machines[item['zone']].append(item['machine'])
                    else:
                        is_idle = self.desired_jobs.is_machine_idle(
                            item['machine'])
                        if (is_idle and idle) or (not is_idle and not idle):
                            zoned_machines[item['zone']].append(item['machine'])
        return zoned_machines

    def get_machine_zone(self, machine):
        """
        Get the name of the zone the machine is in.

        @param machine The machine to get the zone for.
        @return The name of the zone the machine is in or None if the machine
            is not present.
        """
        item = self._get_machine_item(machine)
        if not item:
            return None
        return item['zone']

    def get_machine_status(self, machine):
        """
        Get the status of the machine.

        @return The status of the machine or None if the machine is not
            present.
        """
        item = self._get_machine_item(machine)
        if not item:
            return None
        return item['status']

    def add_machine(self, machine, status=None, existing=False):
        """
        Add a machine to state tracking.

        @param machine The machine to add.
        @param zone The zone the machine is in.
        @param status The initial status of the machine. Defaults to Active.
        @param existing True if this is an existing machine with jobs deployed
            to it. Causes the machine to be scanned for task and those tasks
            added to the desired job state.
        @return True on success, False on failure (machine not initialized).
        """
        if not self.has_machine(machine):
            #TODO: Add machine to monitoring here.
            if status is None:
                status = self.Active

            if existing and not machine.is_initialized():
                logger.info(
                    "%s not initialized, marked as pending" %
                    machine.hostname)
                status = self.Pending

            zone = machine.config.shared_fate_zone
            self.machines.append({
                'machine': machine,
                'status': status,
                'zone': zone,
            })

            if existing and status != self.Pending:
                self.add_machine_tasks(machine)
            return True

    def add_machine_tasks(self, machine):
        """
        Add tasks from a running machine to state tracking.

        @param machine The machine to add tasks from.
        """
        if not machine.is_initialized():
            logger.info(
                "%s not initialized, not adding tasks" %
                machine.hostname)
            return False
        if not machine.has_loaded_data():
            machine.datamanager.reload()
        for task in machine.get_tasks().values():
            name = task['name']
            zone = self.get_machine_zone(machine)
            task_status = JobState.Running
            if not task['running']:
                task_status = JobState.Stopped
            if name not in self.jobs:
                logger.warn((
                    "task '%s' does not have a job object, "
                    "adding anyways") % name)
            self.desired_jobs.add_tasks(
                name, zone, [machine], status=task_status)
            self.current_jobs.add_tasks(
                name, zone, [machine], status=task_status)
        return True

    def remove_machine(self, machine):
        """
        Remove a machine from state tracking and monitoring. Tasks associated
        with the machine will also be removed.

        @param machine The machine to remove.
        """
        self.desired_jobs.remove_machine(machine)
        self.current_jobs.remove_machine(machine)
        item = self._get_machine_item(machine)
        if item:
            self.machines.remove(item)

        for monitor in self.monitors:
            #TODO: Verify this once monitors are managed in ClusterState.
            monitor[0].remove_machine(machine)

    def update_machine(self, machine, status):
        """
        Update the status of a machine.

        @param machine The machine to update.
        @param status The new status of the machine.
        @return True on success or False (machine not found)
        """
        item = self._get_machine_item(machine)
        if item:
            item['status'] = status

    def add_job(self, job):
        """
        Add a job to the cluster. Master jobs are immediately allocated to
        machines. Child jobs are attached to their associated master job and
        deployed simultenously as a job "chain". To deploy a job with the same
        name the existing job must first be removed. If the name of this job
        matches existing tasks that do not have jobs then those tasks will be
        assigned to this job.

        @param job The job to add to the cluster. The job is configured with
            the required zones and number of machines.
        @return True if the job was added or False if it was not.
        """
        if job.name in self.jobs:
            logger.warn(
                "job '%s' already deployed, not adding" % job.name)
            return False

        self.jobs[job.name] = job
        if not job.linked_job:
            # Allocate the master job to machines.
            zones = job.get_shared_fate_zones()
            zoned_idle_machines = self.get_machines(zones, idle=True)
            zoned_existing_machines = self.desired_jobs.get_task_machines(
                job.name, zones)

            for zone in zones:
                #TODO: !MACHINEASSUMPTION! Fill jobs based on required CPU and
                # memory.
                num_existing = len(zoned_existing_machines[zone])
                num_required = job.get_num_required_machines_in_zone(zone)
                num_needed = max(0, num_required - num_existing)
                
                idle_machines = zoned_idle_machines[zone][:num_needed]
                num_create = num_needed - len(idle_machines)
                self.desired_jobs.add_tasks(
                    job.name, zone, idle_machines, num_create)
        return True

    def remove_job(self, job):
        """
        Remove a job from the cluster. Child jobs will be removed as well.

        @param job The job or name of the job to remove.
        @return A list of job names that were removed.
        """
        tasks = []
        if isinstance(job, basestring):
            tasks.append(job)
            job = self.jobs.get(job)
        else:
            tasks.append(job.name)
            children = job.find_dependent_jobs()
            for child in children:
                tasks.append(child.name)

        removed = []
        for task in tasks:
            removed.extend(self.desired_jobs.remove_tasks(task))
        return removed

    def persist_jobs(self):
        """
        Persist jobs to the jobs file.
        """
        jobs = [j for j in self.jobs.values() if j.persistent]
        with open(self.job_file, 'w') as fd:
            fd.write(simplejson.dumps([j.to_dict() for j in jobs]))

    def start_task(self, machine, task):
        """
        Start a task on a machine.

        @param machine The machine on which the task is running.
        @param task The name of the task to start.
        @return True if successful or False (job not deployed)
        """
        if task not in self.jobs:
            logger.warn("job '%s' not deployed, not starting" % task)
            return False

        self.desired_jobs.update_tasks(task, JobState.Running, [machine])
        return True

    def stop_task(self, machine, task):
        """
        Stop a task on a machine.

        @param machine The machine on which the task is running.
        @param task The name of the task to stop.
        @return True if successful or False (job not deployed)
        """
        if task not in self.jobs:
            logger.warn("job '%s' not deployed, not stopping" % task)
            return False

        self.desired_jobs.update_tasks(task, JobState.Running, [machine])
        return True

    def restart_task(self, machine, task):
        """
        Restart a task on a machine.

        @param machine The machine on which the task is running.
        @param task The name of the task to restart.
        @return True if successful or False (job not deployed)
        """
        if task not in self.jobs:
            logger.warn("job '%s' not deployed, not restarting" % task)
            return False

        status = self.desired_jobs.get_task_status(task, machine)
        if status is None:
            logger.warn(
                "job '%s' not deployed to '%s', not restarting" %
                (task, machine.hostname))
            return False

        if status != JobState.Running:
            self.desired_jobs.update_tasks(task, JobState.Running, [machine])
        else:
            if not self.is_machine_mutable(machine):
                logger.info(
                    "machine '%s' in maintenance mode, not restarting" %
                    machine.hostname)
                return False

            self.pending_actions.append(RestartTaskAction(machine, task))
            logger.info(
                "restart action queued for task '%s' on machine '%s'" %
                (task, machine.hostname))
        return True

    def calculate_ready_machines(self):
        """
        Convert ready pending machines to active.
        """
        for machine in self.machines:
            if (machine['status'] == self.Pending and
                    machine['machine'].is_initialized()):
                if self.add_machine_tasks(machine['machine']):
                    machine['status'] = self.Active

    def calculate_current_state(self):
        """
        Update the current state of jobs on the cluster.
        """
        #TODO: Check providers for new zones.
        zoned_machines = self.get_machines()
        job_machines = self.current_jobs.get_task_machines()

        # Remove missing machines.
        for zone, machines in job_machines.iteritems():
            for machine in machines:
                if (zone not in zoned_machines or
                        machine not in zoned_machines[zone]):
                    self.current_jobs.remove_machine(machine)

        # Update tasks.
        for zone, machines in zoned_machines.iteritems():
            for machine in machines:
                if not machine.is_initialized():
                    logger.info(
                        "machine '%s' not initialized, skipping state update" %
                        machine.hostname)
                    continue

                expected_tasks = self.current_jobs.get_machine_tasks(machine)
                actual_tasks = {}
                for actual_task in machine.get_tasks().values():
                    if actual_task['running']:
                        actual_tasks[actual_task['name']] = JobState.Running
                    else:
                        actual_tasks[actual_task['name']] = JobState.Stopped

                add_tasks = set(actual_tasks.keys()) - set(expected_tasks)
                remove_tasks = set(expected_tasks) - set(actual_tasks.keys())
                update_tasks = set(expected_tasks) & set(actual_tasks.keys())

                for task in add_tasks:
                    self.current_jobs.add_tasks(
                        task, zone, [machine], status=actual_tasks[task])
                for task in remove_tasks:
                    self.current_jobs.remove_tasks(task, [machine])
                for task in update_tasks:
                    self.current_jobs.update_tasks(
                        task, actual_tasks[task], [machine])

    def calculate_job_deployment(self):
        """
        Generate the actions necessary to convert the current job state to the
        desired job state.
        """
        action_generator = ClusterActionGenerator(self.sitter)

        # Build job chains.
        chains = {}
        for name, job in self.jobs.iteritems():
            master = self._get_master_job(job)
            if not master:
                continue
            if master.name not in chains:
                chains[master.name] = []
            if master.name != job.name:
                chains[master.name].append(job.name)

        # Assign chain tasks to machines.
        for master, children in chains.iteritems():
            # Find machines where the master is running.
            zoned_master_machines = self.desired_jobs.get_task_machines(master)
            for zone, master_machines in zoned_master_machines.iteritems():
                for child in children:
                    # Assign child tasks.
                    machines = []
                    for machine in master_machines:
                        tasks = self.desired_jobs.get_machine_tasks(machine)
                        if child not in tasks:
                            machines.append(machine)
                    self.desired_jobs.add_tasks(child, zone, machines)

        # Assign idle machines to pending tasks.
        pending_tasks = self.desired_jobs.get_pending_tasks()
        for zone, tasks in pending_tasks.iteritems():
            for master, required in tasks.iteritems():
                if required > 0:
                    tasks = [master] + chains.get(master, [])

                    # Find some idle machines.
                    idle_machines = self.get_machines(
                        zones=[zone], idle=True)[zone]
                    idle_machines = idle_machines[:required]
                    required = required - len(idle_machines)

                    # Assign idle machines to task chain.
                    self.desired_jobs.set_pending_machines(
                        zone, master, idle_machines)

                    # Deploy job chain to new machines.
                    self.desired_jobs.set_pending_deploying(
                        zone, master, required)
                    action_generator.deploy_machines(
                        zone, tasks, required)

        # Calculate changes to existing tasks.
        desired_tasks = self.desired_jobs.flatten()
        current_tasks = self.current_jobs.flatten()

        add_tasks = desired_tasks - current_tasks
        remove_tasks = current_tasks - desired_tasks
        check_tasks = desired_tasks & current_tasks

        # Add actions for existing tasks.
        for task in add_tasks:
            if self.is_machine_mutable(task[1]):
                action_generator.add_task(*task)

        for task in remove_tasks:
            #TODO: Remove status check when undeploying jobs works.
            if (self.current_jobs.get_task_status(task[2], task[1]) ==
                    JobState.Stopped):
                continue
            if self.is_machine_mutable(task[1]):
                action_generator.remove_task(*task)

        for task in check_tasks:
            if self.is_machine_mutable(task[1]):
                desired_status = self.desired_jobs.get_task_status(
                    task[2], task[1])
                current_status = self.current_jobs.get_task_status(
                    task[2], task[1])
                if desired_status != current_status:
                    args = task + (desired_status,)
                    action_generator.update_task(*args)

        self.pending_actions.extend(action_generator.generate())

    def calculate_idle_cleanup(self):
        """
        Calculate the idle machines and decomission the ones we don't need.
        """
        if self.max_idle_per_zone >= 0:
            decomission = {}
            zoned_idle_machines = self.get_machines(idle=True)
            for zone, idle_machines in zoned_idle_machines.iteritems():
                if len(idle_machines) > self.max_idle_per_zone:
                    count = len(idle_machines) - self.max_idle_per_zone
                    decomission[zone] = idle_machines[-count:]

            for zone, machines in decomission.iteritems():
                for machine in machines:
                    logger.info("decomissioning idle maching '%s'" %
                        machine.hostname)
                    self.pending_actions.append(
                        DecomissionMachineAction(self.sitter, zone, machine))

    def calculate_job_cleanup(self):
        """
        Remove jobs that are no longer deployed to machines. If the job is a
        child job then we remove it if its master is no longer deployed to any
        machines.
        """
        for job in self.jobs.values():
            master = self._get_master_job(job)
            if (master and not self.desired_jobs.has_task(master.name) and
                    not self.current_jobs.has_task(master.name)):
                del self.jobs[job.name]
        self.persist_jobs()

    def calculate(self):
        """
        Handle all state calculations.

        @return Actions generated by the state calculations.
        """
        self.calculate_ready_machines()
        self.calculate_current_state()
        logger.info("machines = %s" % self.machines)
        logger.info("desired_jobs = %s" % self.desired_jobs.tasks)
        logger.info("current_jobs = %s" % self.current_jobs.tasks)
        self.calculate_job_deployment()
        self.calculate_job_cleanup()
        self.calculate_idle_cleanup()

    def process(self):
        """
        Process actions in their own threads.
        """
        # Clean up finished actions.
        for action in list(self.running_actions):
            if action.is_finished():
                self.running_actions.remove(action)

        # Run pending actions.
        while len(self.pending_actions) > 0:
            action = self.pending_actions.pop(0)
            action.start()
            self.running_actions.append(action)

    def run(self):
        """
        Run a full calculate/process cycle.
        """
        try:
            logger.info("start calculation cycle")
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

        if self.thread is None or not self.thread.is_alive():
            self.thread = Thread(target=run_loop, name="Calculator")
        self.running = True
        self.thread.start()

    def stop(self, timeout=None):
        """
        Stop the state calculator.
        """
        self.running = False
        self.thread.join()
