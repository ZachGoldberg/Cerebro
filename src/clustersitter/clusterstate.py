import json
import logging
import threading
import time

from eventmanager import ClusterEventManager
from jobfiller import JobFiller
from productionjob import ProductionJob

logger = logging.getLogger(__name__)


def call_safely(job, name, *args, **kwargs):
    """
    Call a function safely. Any errors raised by the function are caught
    and logged, allowing program execution to continue. Used primarily to
    wrap state calculations and processors that may fail due to concurency
    restrictions.
    """
    try:
        job(*args, **kwargs)
    except:
        import traceback
        logger.warn("Crash in %s" % name)
        logger.warn(traceback.format_exc())


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
        self.awol_tasks = []
        self.providers = {}
        self.job_overflow = {}
        self.unreachable_machines = []
        self.machine_spawn_threads = []
        self.idle_machines = []
        self.sitter = parent
        self.job_file = "%s/jobs.json" % self.sitter.log_location
        self.repair_jobs = {}
        self.max_idle_per_zone = -1
        self.loggers = []
        self.process_thread = None

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

    def get_machine_awol_tasks(self, machine):
        """
        Get a list of the stopped master tasks on a machine.
        """
        awol = []
        tasks = machine.get_tasks()
        running = {task['name']: task for task in machine.get_running_tasks()}
        for job in self.jobs:
            if (job.name in tasks and job.name not in running and
                    job.linked_job is None):
                awol.append(running[job.name])
        return awol

    def calculate_awol_tasks(self):
        """
        Fill the AWOL tasks list.
        """
        awol_tasks = []
        for zone, machines in self.machines_by_zone.items():
            for machine in machines:
                for task in self.get_awol_tasks(machine):
                    awol_tasks.append(zone, machine, task)

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

    def process_job_refill(self):
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
        """
        Calculate idle machines. Idle machines are classified as not having any
        tasks running on them.
        """
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

    def calculate_unreachable_machines(self):
        """
        Find unreachable machines.
        """
        for machine, monitor in self.unreachable_machines:
            # Check if we've aready launched a redeploy job to
            # this machine, if so skip it
            found = False
            jobs = []
            for zone_jobs in self.repair_jobs.values():
                jobs.extend(zone_jobs)
            for job in jobs:
                for fillers in job.fillers.values():
                    for filler in fillers:
                        for jobmachine in filler.machines:
                            if machine == jobmachine:
                                found = True
                                break
            if found:
                continue

            ClusterEventManager.handle("Attempting to redeploy to %s" %
                                       machine)

            # Build a 'fake' job for the doctor to run
            zone = machine.config.shared_fate_zone
            job = ProductionJob(
                "",
                {'name': 'Machine Doctor Redeployer'},
                {
                    machine.config.shared_fate_zone: {
                        'mem': machine.config.mem,
                        'cpu': machine.config.cpus
                    }
                }, None)

            if zone not in self.repair_jobs:
                self.repair_jobs[zone] = {}
            job.sitter = self.sitter
            self.repair_jobs[zone][job.name] = (job, 'new')

            def post_filler(success):
                if success:
                    logger.info("Successful redeploy of %s!" % machine)
                else:
                    logger.info(
                        "Redeploy failed! Decomissioning %s" % machine)

                    # Decomission time!
                    ClusterEventManager.handle(
                        "Decomissioning %s" % machine)

                    self.sitter.decomission_machine(machine)

                # Can happen if machine is decomissioned by
                # somebody else while we were redeploying.
                if (machine, monitor) in \
                        self.unreachable_machines:
                    self.unreachable_machines.remove(
                        (machine, monitor))

                del self.repair_jobs[zone][job.name]

            filler = JobFiller(1, job, machine.config.shared_fate_zone,
                               raw_machines=[machine],
                               post_callback=post_filler,
                               fail_on_error=True)

            job.fillers[machine.config.shared_fate_zone] = [filler]

            filler.start()

    def process_job_repairs(self):
        """
        Attempt to repair jobs on unreachable machines.
        """
        repair_jobs = dict(self.repair_jobs)
        for zone, zone_jobs in repair_jobs.iteritems():
            for name, job in zone_jobs.iteritems():
                if job[1] == 'new':
                    self.repair_jobs[zone][name] = (job[0], 'running')
                    logger.debug("Repair job: %s/%s" % (zone, name))
                    for filler in job.fillers[zone]:
                        filler.start()

    def calculate_job_overfill(self):
        job_overflow = {}
        for job in self.jobs:
            zone_overflow = job.get_zone_overflow(self)
            job_overflow[job.name] = zone_overflow

        self.job_overflow = job_overflow
        logger.debug("Calculated job overflow: %s" % self.job_overflow)

    def process_job_overflow(self):
        """
        Turn off any overflowed jobs
        """
        for jobname, zone_overflow in self.job_overflow.items():
            for zone, count in zone_overflow.items():
                if count <= 0:
                    continue

                ClusterEventManager.handle(
                    "Detected job overflow -- " +
                    "Job: %s, Zone: %s, Count: %s" % (jobname,
                                                      zone,
                                                      count))

                decomissioned = 0
                for machine in self.machines_by_zone[zone]:
                    if decomissioned == count:
                        break

                    for task in machine.get_running_tasks():
                        if task['name'] == jobname:
                            while jobname in [
                                    t['name'] for t
                                    in machine.get_running_tasks()]:
                                ClusterEventManager.handle(
                                    "Stopping %s on %s" % (
                                        jobname,
                                        str(machine)))

                                machine.datamanager.stop_task(jobname)
                                machine.datamanager.reload()

                                decomissioned += 1
                                break

    def process_idle_limit(self):
        """
        Enforce idle machine limits
        """
        if self.max_idle_per_zone != -1:
            logger.info("Enforcing an idle limit")
            idle_limit = self.max_idle_per_zone
            self.max_idle_per_zone = -1
            for zone, machines in self.idle_machines.items():
                if len(machines) > idle_limit:
                    decomission_targets = [m for m in machines[idle_limit:]]
                    for machine in decomission_targets:
                        self.sitter.decomission_machine(machine)

    def _calculate(self):
        """
        Perform all state calculations.
        """
        call_safely(self.calculate_job_overfill, "Calculate Job OverFill")
        call_safely(self.calculate_awol_tasks, "Calculate AWOL Tasks")
        call_safely(self.calculate_idle_machines, "Calculate Idle Machines")
        call_safely(self.calculate_job_fill, "Calculate Job Fill")
        call_safely(
            self.calculate_unreachable_machines,
            "Calculate Unreachable Machines")

    def _process(self):
        """
        Asynchronously perform processing as a result ofprevious state
        calculations. Will not be performed more than once simultaneously.
        """
        def run():
            call_safely(
                self.process_job_refill,
                "Process Job ReFill State Changes")
            call_safely(self.process_job_repairs, "Process Job Repairs")
            call_safely(self.process_job_overflow, "Process Job Overflow")
        if self.process_thread is None or not self.process_thread.is_alive():
            self.process_thread = threading.thread(
                target=run,
                name="StateProcessor")
            self.process_thread.start()

    def _calculator(self):
        while True:
            self._calculate()
            self._process()
            time.sleep(self.sitter.stats_poll_interval)
