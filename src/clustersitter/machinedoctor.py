import logging
import threading
import time
from datetime import datetime

from eventmanager import ClusterEventManager
from jobfiller import JobFiller
from productionjob import ProductionJob

logger = logging.getLogger(__name__)


class MachineDoctor(object):
    """
    Try and SSH into the machine and see whats up.  If we can't
    get to it and reboot a sitter than decomission it.

    NOTE: We should do some SERIOUS rate limiting here.
    If we just have a 10 minute network hiccup we *should*
    try and replace those machines, but we should continue
    to check for the old ones for *A LONG TIME*
    to see if they come back.  After that formally decomission
    them.  If they do come back after we've moved their jobs around
    then simply remove the jobs from the machine and add them
    to the idle resources pool.
    """
    def __init__(self, sitter):
        self.sitter = sitter
        self.state = self.sitter.state
        self.thread = None

    def start(self):
        self.thread = threading.Thread(target=self._run,
                                           name="MachineDoctor")
        self.thread.start()

    def _run(self):
        while True:
            start_time = datetime.now()
            logger.debug("Begin machine doctor run.  Unreachables: %s"
                         % (self.state.unreachable_machines))
            try:
                self._check_for_unreachable_machines()
                self._turn_off_overflowed_jobs()
                self._enforce_idle_machine_limits()
            except:
                # Der?  Not sure what this could be...
                import traceback
                traceback.print_exc()
                logger.error(traceback.format_exc())

            time_spent = datetime.now() - start_time
            sleep_time = self.sitter.stats_poll_interval - \
                time_spent.seconds
            logger.debug(
                "Finished Machine Doctor run. " +
                "Time_spent: %s, sleep_time: %s" % (
                    time_spent,
                    sleep_time))

            if sleep_time > 0:
                time.sleep(sleep_time)

    def _check_for_unreachable_machines(self):
                """
                Try and fix unreachable machines
                """
                for machine, monitor in self.state.unreachable_machines:
                    # Check if we've aready launched a redeploy job to
                    # this machine, if so skip it
                    found = False
                    for job in self.state.repair_jobs:
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
                    job = ProductionJob(
                        "",
                        {'name': 'Machine Doctor Redeployer'},
                        {
                            machine.config.shared_fate_zone: {
                                'mem': machine.config.mem,
                                'cpu': machine.config.cpus
                                }
                            }, None)

                    job.sitter = self.sitter
                    self.state.repair_jobs.append(job)

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
                                self.state.unreachable_machines:
                            self.state.unreachable_machines.remove(
                                (machine, monitor))

                        self.state.repair_jobs.remove(job)

                    filler = JobFiller(1, job, machine.config.shared_fate_zone,
                                       raw_machines=[machine],
                                       post_callback=post_filler,
                                       fail_on_error=True)

                    job.fillers[machine.config.shared_fate_zone] = [filler]

                    filler.start()

    def _turn_off_overflowed_jobs(self):
        """
        Turn off any overflowed jobs
        """
        did_overflow_reduction = False
        for jobname, zone_overflow in self.state.job_overflow.items():
            for zone, count in zone_overflow.items():
                if count <= 0:
                    continue

                did_overflow_reduction = True

                ClusterEventManager.handle(
                    "Detected job overflow -- " +
                    "Job: %s, Zone: %s, Count: %s" % (jobname,
                                                      zone,
                                                      count))

                decomissioned = 0
                for machine in self.state.machines_by_zone[zone]:
                    if decomissioned == count:
                        break

                    for task in machine.get_running_tasks():
                        if task['name'] == jobname:
                            while jobname in [
                                t['name'] for t in \
                                    machine.get_running_tasks()]:

                                ClusterEventManager.handle(
                                    "Stopping %s on %s" % (
                                        jobname,
                                        str(machine)))

                                machine.datamanager.stop_task(jobname)
                                machine.datamanager.reload()

                                decomissioned += 1
                                break

                # If we turned off any processess for overflow then we
                # should force an overflow recalculation.  This is to
                # avoid the case of the machinedoctor doing another
                # full loop before the calculator thread has run.  If
                # that happens we might do another round of
                # decomissioning when we haven't yet taken into
                # account the decomissioning we already did
                if did_overflow_reduction:
                    self.state._calculate()

    def _enforce_idle_machine_limits(self):
        """
        Enforce idle machine limits
        """
        if self.state.max_idle_per_zone != -1:
            logger.info("Enforcing an idle limit")
            idle_limit = self.state.max_idle_per_zone
            self.state.max_idle_per_zone = -1
            for zone, machines in self.state.idle_machines.items():
                provider = self.state.provider_by_zone[zone]

                if len(machines) > idle_limit:
                    decomission_targets = [
                        m for m in machines[idle_limit:]]
                    for machine in decomission_targets:
                        self.sitter.decomission_machine(machine)
