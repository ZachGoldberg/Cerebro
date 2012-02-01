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
                 recipe_options={},
                 persistent=False,
                 ):
        """
        Args:
          task_configuration -- JSON representing options to pass to a tasksitter
          to launch the job
          deployment_layout -- How many CPUS/Memory per shared fate zone,  dict
          deployment_recipe -- A python class which knows how to deploy this task
          recipe_options -- any options to pass to the recipe
        """
        # The config to pass to a machinesitter / tasksitter
        self.dns_basename = dns_basename or ""
        self.task_configuration = task_configuration
        self.name = task_configuration['name']
        self.deployment_recipe = deployment_recipe
        self.recipe_options = recipe_options
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

    def get_num_required_machines_in_zone(self, zone):
        """
        Return the total number of machines needed in this zone
        """
        #!MACHINEASSUMPTION!
        return self.deployment_layout[zone]['num_machines']

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
            'persistent': self.persistent
            }

    def get_zone_overflow(self, state):
        zone_overflow = {}

        for zone in self.get_shared_fate_zones():
            zone_overflow[zone] = 0
            required = self.get_num_required_machines_in_zone(zone)
            active = state.job_fill.get(self.name, {}).get(zone, 0)

            if active > required:
                zone_overflow[zone] += (active - required)

        return zone_overflow

    def refill(self, state, sitter):
        self.sitter = sitter

        new_machines = False
        while self.sitter.machines_in_queue():
            new_machines = True
            # We want to ensure any machines recently added to monitoring
            # have had a chance to load their data, incase they are
            # running this job
            logger.info("Waiting for machine monitors to load machine data f"
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
                if filler.is_done() and now - filler.end_time > timespan(minutes=5):
                    self.fillers[zone].remove(filler)

        #!MACHINEASSUMPTION!
        # Step 1: Ensure we have enough machines in each SFZ
        # Step 1a: Check for idle machines and reserve as we find them
        for zone in self.get_shared_fate_zones():
            idle_available = state.get_idle_machines_in_zone(zone)
            total_required = self.get_num_required_machines_in_zone(zone)
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
                filler.start_fill()
                self.fillers[zone].append(filler)

        return True
