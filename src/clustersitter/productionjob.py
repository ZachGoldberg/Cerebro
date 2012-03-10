import logging
import threading
import time
from datetime import datetime, timedelta

from eventmanager import ClusterEventManager
from jobfiller import JobFiller

logger = logging.getLogger(__name__)


class ProductionJob(object):
    def __init__(self,
                 dns_basename,
                 task_configuration,
                 deployment_layout,
                 deployment_recipe,
                 recipe_options=None,
                 persistent=False,
                 linked_job=None,
                 ):
        """
        Args:
          task_configuration: JSON representing options to pass to a tasksitter
          to launch the job
          deployment_layout: How many CPUS/Memory per shared fate zone,  dict
          deployment_recipe: A python class which knows how to deploy this task
          recipe_options: any options to pass to the recipe
          linked_job: Specify the name of another job.  This job will
          be placed on the same machine.
        """
        # The config to pass to a machinesitter / tasksitter
        self.dns_basename = dns_basename or ""
        self.task_configuration = task_configuration
        self.linked_job = linked_job
        self.linked_job_object = None
        self.name = task_configuration['name']
        self.deployment_recipe = deployment_recipe
        self.recipe_options = recipe_options or {}
        self.sitter = None
        self.currently_spawning = {}
        self.persistent = persistent

        # A mapping of SharedFateZoneName: {'cpu': #CPU, 'mem': MB_Mem_Per_CPU}
        self.deployment_layout = deployment_layout

        self.fillers = {}
        for zone in self.deployment_layout.keys():
            self.fillers[zone] = []
            self.currently_spawning[zone] = 0

        #!MACHINEASSUMPTION!
        # Hack to make num_machines == num_cpu, for now.
        for zone in self.deployment_layout.keys():
            self.deployment_layout[zone]['num_machines'] = \
                self.deployment_layout[zone]['cpu']

    def get_shared_fate_zones(self):
        return self.deployment_layout.keys()

    def get_num_required_machines_in_zone(self, zone, state):
        """
        Return the total number of machines needed in this zone
        """
        #!MACHINEASSUMPTION!
        if not self.linked_job:
            return self.deployment_layout.get(zone, {}).get('num_machines', 0)

        self.find_linked_job(state)

        return self.linked_job_object.deployment_layout.get(
            zone, {}).get('num_machines', 0)

    def get_name(self):
        return self.task_configuration['name']

    def __str__(self):
        return self.name

    def __repr__(self):
        return str(self.to_dict())

    def to_dict(self):
        return {
            'task_configuration': self.task_configuration,
            'deployment_layout': self.deployment_layout,
            'deployment_recipe': self.deployment_recipe,
            'recipe_options': self.recipe_options,
            'persistent': self.persistent,
            'linked_job': self.linked_job,
            }

    def do_update_deployment(self, state, version=None):
        """
        1. Find all machines running this job
        2. Build a JobFiller with all those machines
        3. Run the JobFiller, making sure the version is being
           passed to the deployment recipe
        """
        self.recipe_options['version'] = version

        for zone, machines in state.machines_by_zone.items():
            job_machines = []
            for machine in machines:
                tasks = machine.get_running_tasks()
                names = [task['name'] for task in tasks]
                if self.name in names:
                    job_machines.append(machine)

            if job_machines:
                filler = JobFiller(
                    len(job_machines),
                    self,
                    zone,
                    job_machines,
                    reboot_task=True
                    )

                logger.info(
                    "Starting job filler for code update for %s" % self.name)
                filler.start()
                if not zone in self.fillers:
                    self.fillers[zone] = []

                self.fillers[zone].append(filler)

    def get_zone_overflow(self, state):
        zone_overflow = {}

        for zone in self.get_shared_fate_zones():
            zone_overflow[zone] = 0
            required = self.get_num_required_machines_in_zone(zone, state)
            active = state.job_fill.get(self.name, {}).get(zone, 0)

            if active > required:
                zone_overflow[zone] += (active - required)

        return zone_overflow

    def find_dependent_jobs(self):
        dependent_jobs = []
        for job in self.sitter.state.jobs:
            if job.linked_job == self.name:
                dependent_jobs.append(job)

        return dependent_jobs

    def find_linked_job(self, state):
        if self.linked_job_object:
            return self.linked_job_object

        linked_job = None
        for job in state.jobs:
            if job.name == self.linked_job:
                linked_job = job
                break

        self.linked_job_object = linked_job
        return self.linked_job_object


    def ensure_on_linked_job(self, state, sitter):
        """
        1. Ensure the linked job exists, if not bail out
        2. Ensure that this job is running on each machine
        that the linked job is on.  If not, create a job filler for
        those machines and this job.
        Note: As a linked job we should never create a job filler
        that spawns new machines.  We should always just be populating
        existing machines.
        """
        linked_job = self.find_linked_job(state)

        if not linked_job:
            logger.warn("Couldn't find linked job (%s) for %s!" % (
                    self.linked_job, str(self)))
            # Returning False stops all other jobs this cycle, which
            # we don't want to do.
            return True

        for zone in linked_job.get_shared_fate_zones():
            machines_to_fill = []
            machines = state.job_fill_machines[linked_job.name][zone]

            for machine in machines:
                task_names = [
                    task['name'] for task in machine.get_running_tasks()]

                if not self.name in task_names:
                    machines_to_fill.append(machine)

            current_fillers = self.fillers[zone]
            currently_spawning = 0
            for filler in current_fillers:
                currently_spawning += filler.num_remaining()

            # Also check the linked job for active job fillers
            # we don't want to start a filler here if the linked job
            # is also actively filling, it should be sequential.
            current_fillers = linked_job.fillers[zone]
            for filler in current_fillers:
                currently_spawning += filler.num_remaining()

            if not currently_spawning and len(machines_to_fill) > 0:
                ClusterEventManager.handle(
                    "New JobFiller for Linked Job: %s, %s, %s, %s" % (
                        machines_to_fill, zone, str(self), self.linked_job))

                filler = JobFiller(len(machines_to_fill), self,
                                   zone, machines_to_fill)
                filler.start()
                self.fillers[zone].append(filler)

        return True

    def refill(self, state, sitter):
        self.sitter = sitter

        new_machines = False
        while self.sitter.machines_in_queue():
            new_machines = True
            # We want to ensure any machines recently added to monitoring
            # have had a chance to load their data, incase they are
            # running this job
            logger.info("Waiting for machine monitors to load machine data "
                        "before filling jobs")
            time.sleep(0.5)

        if new_machines:
            # If we had to wait for new machines that means that
            # there are new machines, and we need to recalculate
            # job fill before it is safe to do refill.  The next
            # pass should be OK.
            logger.info("Waiting for next jobfill to be calculated before "
                        "doing a refill")

            return False

        while not self.name in state.job_fill:
            # 1) Assume this job has already been added to state.jobs
            # 2) Want to ensure calculator has run at least once to find out
            #    if this job already exists throughout the cluster
            logger.info("Waiting for calculator thread to kick in before "
                         "filling jobs")
            time.sleep(0.5)

        # Clear out finished fillers after 5 minutes
        for zone, fillers in self.fillers.items():
            for filler in fillers:
                now = datetime.now()
                if (filler.is_done() and
                    now - filler.end_time > timedelta(minutes=5)):
                    logger.info("Removing a filler from %s for %s" % (
                            zone, self.name))
                    self.fillers[zone].remove(filler)

        # If we have a linked job then bypass all the normal logic
        # and just piggyback on those machines
        if self.linked_job:
            return self.ensure_on_linked_job(state, sitter)

        #!MACHINEASSUMPTION!
        # Step 1: Ensure we have enough machines in each SFZ
        # Step 1a: Check for idle machines and reserve as we find them
        for zone in self.get_shared_fate_zones():
            idle_available = state.get_idle_machines_in_zone(zone)
            total_required = self.get_num_required_machines_in_zone(zone, state)
            idle_required = total_required - state.job_fill[self.name][zone]

            current_fillers = self.fillers[zone]
            currently_spawning = 0
            for filler in current_fillers:
                currently_spawning += filler.num_remaining()

            self.currently_spawning[zone] = currently_spawning

            idle_required -= currently_spawning

            # !MACHINEASSUMPTION! Ideally we're counting resources here
            # not machines
            required_new_machine_count = max(
                (idle_required - len(idle_available)), 0)

            do_log = logger.debug
            if idle_required > 0:
                do_log = logger.info

            do_log(
                ("Calculated job requirements for %s in %s: " % (self.name,
                                                                 zone)) +
                "Currently Active: %s " % (state.job_fill[self.name][zone]) +
                "Idle Required: %s, Total New: %s " % (
                    idle_required,
                    required_new_machine_count) +
                "Currently Spawning: %s " % (currently_spawning) +
                "idle-available: %s " % (len(idle_available)) +
                "total_required: %s " % (total_required)
                )

            usable_machines = []
            if required_new_machine_count <= 0:
                # idle_available > idle_required, so use just as many
                # as we need
                usable_machines = idle_available[:idle_required]
            elif required_new_machine_count > 0:
                # Otherwise take all the available idle ones, and
                # we'll make more
                usable_machines.extend(idle_available)

            if idle_required > 0:
                ClusterEventManager.handle(
                    "New JobFiller: %s, %s, %s, %s" % (
                        idle_required, zone, str(self), usable_machines))

                filler = JobFiller(idle_required, self,
                                   zone, usable_machines)
                self.fillers[zone].append(filler)
                filler.start()

        return True
