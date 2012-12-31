
import logging
from threading import Thread

try:
    from Queue import Empty, Full, Queue
    (Empty, Full, Queue)  # pyflakes fix
except ImportError:
    from queue import Empty, Full, Queue

logger = logging.getLogger(__name__)


class ClusterActionRunner(Thread):
    """
    Run a cluster action in a thread.
    """

    def __init__(self, action):
        """Initialize the runner."""
        self.thread = None
        self.action = action

    def is_running(self):
        """
        Check if the queue consumer is running.

        @return True if the consumer is running or False.
        """
        return self.thread is not None and self.thread.is_alive()

    def start(self):
        """
        Start the action thread.

        @return True if the thread was started or False if it was already
            running.
        """
        if self.is_running():
            return False

        def run():
            try:
                logger.info("action '%s' running" % self.action)
                self.action.run()
                logger.info("action '%s' complete" % self.action)
            except:
                import traceback
                logger.error("action '%s' failed" % self.action)
                logger.error(traceback.format_exc())
            finally:
                self.queue.task_done()

        self.thread = Thread(target=run, name=self.action)
        self.thread.start()
        return True

    def stop(self):
        """
        Signal the action to stop if supported. A method called stop() is
        checked for on the action and should return True on success. This
        method does not block.

        @return True if the action was signaled to stop or False if the thread
            was not running or the action does not support stopping.
        """
        if self.is_running() and hasattr(self.action, 'stop'):
            return self.action.stop()
        return False

    def join(self, timeout=None):
        """
        Block until the thread completes or the given timeout expires.

        @param timeout The number of seconds to wait for the thread to
            complete. Defaults to infinite.
        @return True if the thread has stopped or False if it is still running.
            If a timeout is not specified it will always be True.
        """
        if self.thread is not None:
            self.thread.join(timeout)
        return not self.is_running()


class MachineActionQueue(object):
    """
    Queue for running machine actions. Runs queues actions in a single thread
    until the queue empties.
    """

    timeout = 30

    def __init__(self, machine):
        """
        Initialize the queue.

        @param machine The machine this queue is operating on.
        """
        self.machine = machine
        self.name = "ActionQueue(%s)" % machine
        self.queue = Queue()
        self.stop = False
        self.thread = None

    def add(self, action):
        """
        Queue an action.

        @param action The action to queue.
        @return True on success or False if the action's machine does not match
        the queue's machine or if the queue add times out.
        """
        if self.machine == getattr(action, 'machine', None):
            try:
                self.queue.put(action, True, self.timeout)
                return True
            except Full:
                logger.error("%s: timed out while queuing" % action)
        return False

    def is_empty(self):
        """
        Check if the queue is empty.

        @return True if the queue is empty or False.
        """
        return self.queue.empty()

    def is_running(self):
        """
        Check if the queue consumer is running.

        @return True if the consumer is running or False.
        """
        return self.thread is not None and self.thread.is_alive()

    def start(self):
        """
        Start the queue consumer thread. The thread will only be run if the
        queue is non-empty. The thread will terminate once the queue is empty.

        @return True if the consumer thread was started or False if the thread
            is already running or the queue is empty.
        """
        if self.is_running() or self.is_empty():
            return False

        def run():
            logger.info("action queue consumer started")
            self.stop = False
            while not self.queue.empty() and not self.stop:
                try:
                    action = self.queue.get(True, self.timeout)
                except Empty:
                    logger.warn("queue timed out, retrying")
                    continue

                try:
                    logger.info("action '%s' running" % action)
                    action.run()
                    logger.info("action '%s' complete" % action)
                except:
                    import traceback
                    logger.error("action '%s' failed" % action)
                    logger.error(traceback.format_exc())
                finally:
                    self.queue.task_done()
            logger.info("action queue thread stopped")

        self.thread = Thread(target=run, name=str(self))
        self.thread.start()
        return True

    def stop(self):
        """
        Signal the queue consumer thread to stop. This method does not block.

        @return True if the consumer thread was signalled to stop or False if
            the thread is not running.
        """
        if not self.is_running():
            return False
        self.stop = True
        return True

    def join(self, timeout=None):
        """
        Block until the consumer thread completes or the given timeout expires.

        @param timeout The number of seconds to wait for the thread to
            complete. Defaults to infinite.
        @return True if the thread has stopped or False if it is still running.
            If a timeout is not specified it will always be True.
        """
        if self.thread is not None:
            self.thread.join(timeout)
        return not self.is_running()

    def __str__(self):
        """Return the name of the action queue."""
        return self.name

    def __repr__(self):
        """Return the name of the action queue."""
        return self.name


class ClusterActionManager(object):
    """
    Manage cluster actions. Each machine has a dedicated queue for actions
    which need to be performed. Each queue has a single consumer thread.
    Threads are cleaned up when their associated queues empty.
    """

    def __init__(self):
        """Initialize the manager."""
        self.pending = []
        self.queues = {}
        self.runners = []

    def add(self, action):
        """
        Add an action to the queue. The action is not actually queued until
        process() is called.
        """
        self.pending.append(action)

    def process(self):
        """
        Process pending actions. If the action is related to a machine it is
        queued in the associated machine queue. Otherwise the action is run
        immediately. This method also cleans up completed actions and empty
        machine queues.
        """
        while len(self.pending):
            action = self.pending.pop(0)
            if hasattr(action, 'machine'):
                queue = self.queues.get(action.machine)
                if not queue:
                    queue = MachineActionQueue(action.machine)
                    self.queues[action.machine] = queue
                if not queue.add(action):
                    logger.error("%s: failed to queue action" % action)
                if not queue.is_running():
                    queue.start()
            else:
                runner = ClusterActionRunner(action)
                result = runner.start()
                if result:
                    self.runners.append(runner)
                else:
                    logger.error("%: failed to start action" % action)

        for name, queue in dict(self.queues).iteritems():
            if not queue.is_running() and queue.is_empty():
                del self.queues[name]

        for runner in list(self.runners):
            if not runner.is_running():
                self.runners.remove(runner)

    def stop(self):
        """
        Signal all actions to stop.
        """
        for queue in self.queues.values():
            queue.stop()
        for runner in self.runners:
            runner.stop()

    def join(self, timeout=None):
        """
        Wait for all thread queues to complete.

        @param timeout The number of seconds to block waiting for a queue
            thread to join. This will be applied to each queue thread. Defaults
            to infinite.
        @return True if all threads are stopped or False if some threads
            continue to run.
        """
        running = True
        for queue in self.queues.values():
            running = running and queue.join(timeout)
        for runner in self.runners:
            running = running and runner.join(timeout)
        return running


class ClusterAction(object):
    """
    Base class for all actions. Inheriting classes must implement the run()
    method.
    """

    def __init__(self, sitter):
        """
        Initialize the action.

        @param sitter The cluster sitter object.
        """
        self.name = "%s" % self.__class__.__name__
        self.sitter = sitter
        self.state = sitter.state

    def run(self):
        """Run the action."""

    def __str__(self):
        """Return the name of the action."""
        return self.name

    def __repr__(self):
        """Return the name of the action."""
        return self.name


class MachineAction(ClusterAction):
    """
    Base class for action executed against a machine. Inheriting classes must
    implement the run() method.
    """

    def __init__(self, sitter, zone, machine):
        """
        Initialize the action.

        @param sitter The cluster sitter object.
        @param zone The zone the job is being deployed to.
        @param machine The machine to manage the job on.
        """
        super(MachineAction, self).__init__(sitter)
        self.machine = machine
        self.name = "%s(%s)" % (self.__class__.__name__, machine)
        self.zone = zone


class TaskAction(MachineAction):
    """
    Base class for managing a task on a machine. Inheriting classes must
    implement the run() method.
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
        self.name = "%s(%s, %s)" % (self.__class__.__name__, machine, task)
        self.task = task


class StartTaskAction(TaskAction):
    """Start a task."""

    def run(self):
        """Run the action."""
        job = self.state.get_job(self.task)
        if not job:
            logger.error("%s: job does not exist" % self)
        elif not self.machine.start_task(job):
            logger.error("%s: failed to start task" % self)


class RestartTaskAction(TaskAction):
    """Restart a task."""

    def run(self):
        """Run the action."""
        job = self.state.get_job(self.task)
        if not job:
            logger.error("%s: job does not exist" % self)
        elif not self.machine.restart_task(job):
            logger.error("%s: failed to restart task" % self)


class StopTaskAction(TaskAction):
    """Stop a task."""

    def run(self):
        """Run the action."""
        job = self.state.get_job(self.task)
        if not job:
            logger.error("%s: job does not exist" % self)
        elif not self.machine.stop_task(job):
            logger.error("%s: failed to stop task" % self)


class AddTaskAction(TaskAction):
    """Add a task to a machine."""

    def run(self):
        """Run the action."""
        job = self.state.get_job(self.task)
        if not job:
            logger.error("%s: job does not exist" % self)
        elif not job.deploy(self.zone, self.machine):
            logger.error("%s: failed to deploy task" % self)
        elif not self.state.start_task(self.machine, self.task):
            logger.error(
                "%s: failed to start task after deploying" % self)


class RemoveTaskAction(TaskAction):
    """Remove a task from a machine."""

    def run(self):
        """Run the action."""
        job = self.state.get_job(self.task)
        if not job:
            logger.error("%s: job does not exist" % self)
        elif not self.machine.stop_task(job):
            logger.error("%s: failed to remove task" % self)


class DeployMachineAction(ClusterAction):
    """Deploy a master job to a new machine."""

    def __init__(self, sitter, zone, job):
        """
        Initialize the action.

        @param sitter The cluster sitter object.
        @param zone The zone to deploy the tasks to.
        @param job The job to deploy.
        """
        super(DeployMachineAction, self).__init__(sitter)
        self.job = job
        self.name = "%s(%s, %s)" % (
            self.__class__.__name__, zone, job.name)
        self.stop = False
        self.zone = zone

    def run(self):
        """Run the action."""
        if not self.job.deploy(self.zone):
            logger.error(
                "%s: failed to deploy new machine" % self)
            return
        # Take our new machine out of Deploying mode.
        self.state.update_machine(self.machine, self.state.Active)


class RedeployMachineAction(MachineAction):
    """Redeploy an unreachable machine."""

    def __init__(self, sitter, zone, machine, job):
        """
        Redeploy a machinesitter job to a machine.

        @param sitter The cluster sitter object.
        @param zone The zone the job is being deployed to.
        @param machine The machine to manage the job on.
        @param job The repair job to run.
        """
        super(RedeployMachineAction, self).__init__(sitter, zone, machine)
        self.job = job

    def run(self):
        """Run the action."""
        logger.info("%s: redeploying machine sitter" % self)
        if self.job.deploy(self.zone, self.machine, True):
            self.state.update_machine(self.machine, self.state.Active)
            logger.info(
                "%s: successfully redeployed machine")
        else:
            logger.info(
                "%s: failed to redeploy, decomissioning and reassigning jobs" %
                self)
            self.state.detach_machine(self.machine)
            self.sitter.decomission_machine(self.machine)


class DecomissionMachineAction(MachineAction):
    """Decomission an existing machine."""

    def run(self):
        """Run the action."""
        self.sitter.decomission_machine(self.machine)
