import json
import logging
import time

logger = logging.getLogger(__name__)


class ClusterState(object):
    def __init__(self, parent):
        # list of tuples (MachineMonitor, ThreadObj)
        self.monitors = []
        self.machines_by_zone = {}
        self.zones = []
        self.provider_by_zone = {}
        self.jobs = []
        self.job_fill = {}
        self.job_fill_machines = {}
        self.providers = {}
        self.job_overflow = {}
        self.unreachable_machines = []
        self.machine_spawn_threads = []
        self.idle_machines = []
        self.sitter = parent
        self.job_file = "%s/jobs.json" % self.sitter.log_location
        self.repair_jobs = []
        self.max_idle_per_zone = -1
        self.loggers = []

    def add_job(self, job):
        logger.info("Add Job: %s" % job.name)
        for zone in job.get_shared_fate_zones():
            if not zone in self.zones:
                logger.warn("Tried to add a job with an unknown SFZ %s" % zone)
                return False

        # Ensure we can find the job's recipe
        if (job.deployment_recipe and
            not self.sitter._get_recipe_class(job.deployment_recipe)):
            logger.warn(
                "Tried to add a job with an invalid recipe class: %s" % (
                    job.deployment_recipe))

            return False

        # Ensure we don't already have a job with this name,
        # if we do, replace it
        found_existing_job = False
        for existing_job in self.jobs:
            if existing_job.name == job.name:
                found_existing_job = True
                # Rather than wholesale replace the object we will
                # simply update the 'configured' fields, this way
                # existing state aka job fillers are not lost
                fields = ['dns_basename',
                          'task_configuration',
                          'deployment_layout',
                          'deployment_recipe',
                          'recipe_options',
                          'persistent',
                          'linked_job']
                for field in fields:
                    setattr(existing_job, field, getattr(job, field))

        if not found_existing_job:
            self.jobs.append(job)

        self.persist_jobs()
        return True

    def remove_job(self, jobname):
        removed = False
        for job in self.jobs:
            if job.name == jobname:
                self.jobs.remove(job)
                removed = True

        self.persist_jobs()
        return removed

    def persist_jobs(self):
        """
        Naively just rewrite the whole "job db"
        """
        jobs_to_write = [j for j in self.jobs if j.persistent]
        f = open(self.job_file, 'w')
        f.write(json.dumps([j.to_dict() for j in jobs_to_write]))
        f.close()

    def get_idle_machines_in_zone(self, zone):
        """
        @ TODO Do some sort of calculation -- if we have too many idle
        machines we should decomission them.  Define a configurable
        threshold somewhere.
        """
        return self.idle_machines[zone]

    def calculate_job_fill(self):
        # If we find out that a job has TOO MANY tasks,
        # then we should decomission some machines or make
        # them idle

        job_fill = {}
        job_fill_machines = {}
        #!MACHINEASSUMPTION! Should be cpu_count not machine_count
        # Fill out a mapping of [job][task] -> machine_count
        logger.debug("Calculating job fill for jobs: %s" % self.jobs)
        for job in self.jobs:
            job_fill[job.name] = {}
            job_fill_machines[job.name] = {}

            for zone in job.get_shared_fate_zones():
                job_fill[job.name][zone] = 0
                job_fill_machines[job.name][zone] = []

        # Actually do the counting
        for zone, machines in self.machines_by_zone.items():
            for machine in machines:
                for task in machine.get_running_tasks():
                    # Don't add tasks from machines to the job_fill
                    # dict unless we already know about the job
                    if not task['name'] in job_fill:
                        continue

                    job_fill[task['name']][zone] += 1
                    job_fill_machines[task['name']][zone].append(machine)

        self.job_fill = job_fill
        self.job_fill_machines = job_fill_machines
        logger.debug("Calculated job fill: %s" % self.job_fill)

    def calculate_job_refill(self):
        logger.debug("Calculating job refill for jobs: %s" % self.jobs)
        # Now see if we need to add any new machines to any jobs
        for job in self.jobs:
            if job.name in self.job_fill:
                if not job.refill(self, self.sitter):
                    # refill returning false means
                    # we need to wait for another calculation run before
                    # we can keep working.
                    break

        logger.debug("Calculated job refill: %s" % self.job_fill)

    def calculate_idle_machines(self):
        idle_machines = {}
        for zone in self.zones:
            idle_machines[zone] = []
            for machine in self.machines_by_zone.get(zone, []):
                tasks = machine.get_running_tasks()

                #!MACHINEASSUMPTION! Here we assume no tasks == idle,
                # not sum(jobs.cpu) < machine.cpu etc.
                idle = bool(not tasks)
                idle = idle and machine.has_loaded_data()
                idle = idle and not machine.is_in_deployment()

                if idle:
                    idle_machines[zone].append(machine)

        # The DICT swap must be atomic, or else another
        # thread could get a bad value during calculation.
        self.idle_machines = idle_machines
        logger.debug("Calculated idle machines: %s" % str(self.idle_machines))

    def calculate_job_overfill(self):
        # TODO we really should just do the calculating here
        # and let the machinedoctor spin down the task
        job_overflow = {}
        for job in self.jobs:
            zone_overflow = job.get_zone_overflow(self)
            job_overflow[job.name] = zone_overflow

        self.job_overflow = job_overflow
        logger.debug("Calculated job overflow: %s" % self.job_overflow)

    def _calculate(self):
            def run_job(job, name):
                # Since all state is accessed and shared there
                # are all sorts of race conditions if a calculator
                # is running and a job is added or removed.
                # If one calculator run crashes because of this
                # thats OK.
                try:
                    job()
                except:
                    import traceback
                    logger.warn("Crash in %s" % name)
                    logger.warn(traceback.format_exc())

            run_job(self.calculate_idle_machines, "Calculate Idle Machines")
            run_job(self.calculate_job_fill, "Calculate Job Fill")
            run_job(self.calculate_job_refill, "Calculate Job ReFill")
            run_job(self.calculate_job_overfill, "Calculate Job OverFill")

    def _calculator(self):
        while True:
            self._calculate()
            time.sleep(self.sitter.stats_poll_interval)
