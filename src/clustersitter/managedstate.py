
class ManagedStateError(Exception):
    """Thrown on state errors."""


class PendingMachine(object):
    """
    A machine placeholder used prior to spinning up a machine.
    """

    def __init__(self, hostname=None):
        """
        Initialize the machine.
        """
        if hostname is None:
            self.hostname = 'pending'
        else:
            self.hostname = hostname


class ManagedState(object):
    """
    Manageable state object representing current or desired state of the
    cluster.

    Some static status attributes are defined as part of the class. These are:

    Active -- Machine is operating normally.
    Paused -- Machine is paused by admin for maintenance.
    Running -- The task is running.
    Stopped -- The task is stopped.
    Maintenance -- Maintenance is being performed by the cluster sitter on the
        machine or task.
    """

    Active = 'active'
    Paused = 'paused'
    Running = 'running'
    Stopped = 'stopped'
    Maintenance = 'maintenance'

    def __init__(self):
        """
        Initialize an empty state object.

        State attribute is structured as follows:

            state = [{
                'zone': zone_name,
                'machine': machine_object,
                'status': machine_status,
                'tasks': [{
                    'job': job_object,
                    'status': task_status,
                }],
                'zone': 'zone name',
            }]
        """
        self.state = []

    def get_machine(self, machine):
        """
        Get the dictionary representing the given machine.

        @param machine The machine to find the dict for.
        @return The machine dict if it exists or None.
        """
        if machine is not None:
            if isinstance(machine, dict):
                if machine in self.state:
                    return machine
                else:
                    machine = machine['machine']
            for machine_dict in self.state:
                if machine_dict['machine'] == machine:
                    return machine_dict
        return None

    def has_machine(self, machine, zone=None):
        """
        Check if a machine exists.

        @param machine The machine to check for.
        @param zone Ensure the machine is in this zone.
        @return True if the machine is present in the (optional) provided zone
            or False.
        """
        machine = self.get_machine(machine)
        return machine is not None and (
            zone is None or machine['zone'] == zone)

    def is_mutable_machine(self, machine):
        """
        Check if a machine may be operated upon.

        @param machine The machine to check.
        @return True if the machine may be operated upon or False.
        @raise ManagedStateError If the machine is not in this state store.
        """
        check_machine = self.get_machine(machine)
        if check_machine is None:
            raise ManagedStateError("machine '%s' not found in state store" % (
                self._get_machine_name(machine)))
        return check_machine['status'] == ManagedState.Active

    def is_pending_machine(self, machine):
        """
        Check if a machine is pending.

        @param machine The machine to check.
        @return True if machine is pending creation.
        """
        machine = self.get_machine(machine)
        return machine is not None and isinstance(machine, PendingMachine)

    def is_idle_machine(self, machine, exclude=None):
        """
        Check if a machine is idle. Idle machines don't have any tasks assigned
        to them and are kept in reserve in case new tasks need to be spun up on
        new machines.

        @param machine The machine to check.
        @param exclude A list of tasks to exclude in the idle calculation.
        @return True if the machine is idle or False.
        @raise ManagedStateError If the machine does not exist.
        """
        # Sanitize the input.
        machine = self.get_machine(machine)
        if machine is None:
            raise ManagedStateError("machine '%s' not found in state store" % (
                self._get_machine_name(machine)))

        if exclude is None:
            exclude = []
        exclude_tasks = []

        for task in exclude:
            task = self.get_task(task)
            if task is not None:
                exclude_tasks.append(task)

        # Fail on the first unexcluded task.
        for task in machine['tasks']:
            if task not in exclude_tasks:
                return False
        return True

    def map_machines(self, func):
        """
        Execute a function against each machine in the state. The function
        should take two params: the state object and the machine dictionary.

        @param func The function to call on each machine.
        @result A list of function results.
        """
        results = []
        for machine in self.state:
            results.append(func(self, machine))
        return results

    def get_task(self, machine, job):
        """
        Get the dictionary representing the given task.

        @param machine The machine the job is running on.
        @param job The job representing the task.
        @return The task dict if it exists or None.
        """
        if job is not None:
            machine = self.get_machine(machine)
            if machine is not None:
                if isinstance(job, tuple):
                    if job in machine['tasks']:
                        return job
                    else:
                        job = job['job']
                for task_dict in machine['tasks']:
                    if task_dict['job'] == job:
                        return task_dict
        return None

    def get_tasks(self, job):
        """
        Get a list of tasks that are running the given job. The result is a
        list of two tuples. The first item of the tuple is the machine
        dictionary and the second is the task dictionary.

        @param job The job to get the tasks for.
        """
        results = []
        for machine in self.state:
            for task in machine['tasks']:
                if task['job'] == job:
                    results.append(machine, task)
        return results

    def has_task(self, machine, job):
        """
        Check if a task exists.

        @param machine The machine to check for the task on.
        @param job The job with the task to run.
        @return True if the task is present, False if not.
        """
        return self.get_task(machine, job) is not None

    def is_mutable_task(self, machine, job):
        """
        Check if a task may be operated upon.

        @param machine The machine running the task.
        @param job The job with the task to check,
        @return True if the task may be operated upon or False.
        @raise ManagedStateError If the machine or task is not present.
        """
        machine = self.get_machine(machine)
        if not self.is_mutable_machine(machine):
            return False

        task = self.get_task(machine, job)
        if task is None:
            raise ManagedStateError(
                "task '%s' not found on machine '%s' in state store" % (
                self._get_task_name(job),
                self._get_machine_name(machine)))
        return task['status'] != ManagedState.Maintenance

    def map_tasks(self, func):
        """
        Execute a function against each task in the state. The function should
        take three params: the state object, the machine dictionary for the
        task, and the task dictionary for the task.

        @param func The function to call on each task.
        @return A list of function results.
        """
        results = []
        for machine in self.state:
            for task in machine['tasks']:
                results.append(func(self, machine, task))
        return results

    def _get_machine_name(self, machine):
        """
        Get a machine name. Machine can be a string, machine object, or machine
        dict.

        @param machine  The machine object/dict.
        @return A string with the machine name in it.
        """
        if isinstance(machine, dict):
            machine = machine['machine']
        if hasattr(machine, 'hostname'):
            return machine.hostname
        return str(machine)

    def _get_task_name(self, task):
        """
        Get a job name. Task can be a string, job object, or task dict.

        @param task The string, task dict,  or job object.
        @return A string with a name in it.
        """
        if isinstance(task, dict):
            task = task['job']
        if hasattr(task, 'name'):
            return task.name
        return str(task)

    def add_machine(self, zone, machine=None, status=None):
        """
        Add a machine.

        @param zone The zone to add the machine to.
        @param machine The machine to add.
        @param status The initial status of the machine. Defaults to Active.
        @return The new machine dict.
        @raise ManagedStateError If the machine already exists.
        """
        if self.get_machine(machine) is not None:
            msg = "machine '%s' already stored in state object"
            raise ManagedStateError(msg % self._get_machine_name(machine))
        if status is None:
            status = ManagedState.Active
        if machine is None:
            machine = PendingMachine()
        machine_dict = {
            'machine': machine,
            'status': status,
            'tasks': [],
            'zone': zone,
        }
        self.state.append(machine_dict)
        return machine_dict

    def remove_machine(self, machine):
        """
        Remove a machine. No error is raised if the machine does not exist.

        @param machine The machine to remove.
        """
        machine = self.get_machine(machine)
        if machine is not None:
            self.state.remove(machine)

    def update_machine(self, machine, obj=None, status=None):
        """
        Update the machine state.

        @param machine The machine to update.
        @param obj The machine object this machine.
        @param status The new status of the machine.
        @return the machine dict that was updated.
        """
        if self.has_machine(obj):
            raise ManagedStateError(
                "object to assign to machine '%s' already in state store" % (
                self._get_machine_name(machine)))
        machine = self.get_machine(machine)
        if machine is not None:
            if obj is not None:
                machine['machine'] = obj
            if status is not None:
                machine['status'] = status
        return machine

    def add_task(self, machine, job, status=None):
        """
        Add a task to a machine.

        @param machine The machine to add the task to.
        @param job The job with the task to add to the machine.
        @param status The initial status of the task. Defaults to Running.
        @return The new task dict.
        """
        if self.get_task(machine, job) is not None:
            return
        if status is None:
            status = ManagedState.Running
        machine = self.get_machine(machine)
        task_dict = {
            'job': job,
            'status': status,
        }
        machine['tasks'].append(task_dict)
        return task_dict

    def move_task(self, from_machine, to_machine, job):
        """
        Move a task between machines.

        @param from_machine The machine currently running the task.
        @param to_machine The machine that should be running the task.
        @param job The job with the task to move.
        @return The task dict for the task that was moved.
        @raise ManagedStateError If either machine or the task does not exist.
        """
        from_machine = self.get_machine(from_machine)
        if from_machine is None:
            raise ManagedStateError("machine '%s' not found in state store" % (
                self._get_machine_name(from_machine)))

        to_machine = self.get_machine(to_machine)
        if to_machine is None:
            raise ManagedStateError("machine '%s' not found in state store" % (
                self._get_machine_name(to_machine)))

        task = self.get_task(from_machine, job)
        if task is None:
            raise ManagedStateError(
                "task '%s' not found on machine '%s' in state store" % (
                self._get_task_name(task),
                self._get_machine_name(from_machine)))

        from_machine['tasks'].remove(task)
        to_machine['tasks'].append(task)
        return task

    def remove_task(self, machine, job):
        """
        Remove a task from a machine.

        @param machine The machine to remove the task from.
        @param job The job with the task to remove from the machine.
        @raise ManagedStateError If the task does not exist.
        """
        machine = self.get_machine(machine)
        if machine is None:
            raise ManagedStateError("machine '%s' not found in state store")

        task = self.get_task(machine, job)
        if task is None:
            raise ManagedStateError(
                "task '%s' not found on machine '%s' in state store" % (
                self._get_task_name(task),
                self._get_machine_name(machine)))

        machine['tasks'].remove(task)

    def remove_tasks(self, job):
        """
        Remove all job tasks from the cluster.

        @param job The job to remove tasks for.
        """
        tasks = self.get_tasks(job)
        for machine, task in tasks:
            self.remove_task(machine, task)

    def update_task(self, machine, job, status):
        """
        Update the status of a task.

        @param machine The machine running the task to update.
        @param job The job with the task that should be updated.
        @param status The new status of the task.
        @return The task dict that was updated.
        @raise ManagedStateError If the task does not exist.
        """
        task = self.get_task(machine, job)
        if task is None:
            msg = "task '%s' does not exist on machine '%s'"
            raise ManagedStateError(msg % (
                self._get_task_name(task), self._get_machine_name(machine)))

        task['status'] = status
        return task
