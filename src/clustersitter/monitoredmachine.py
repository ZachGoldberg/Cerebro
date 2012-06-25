import logging
from sittercommon.machinedata import MachineData

logger = logging.getLogger(__name__)


class HasMachineSitter(object):
    """
    Everything is asynchronous -- always returns a request
    object that can be run later.
    """
    def __init__(self):
        self.hostname = None
        self.datamanager = None
        self.historic_data = []
        self.loaded = False

    def _api_start_task(self, job):
        val = self.datamanager.add_task(job.task_configuration)
        logger.info("Add task %s result: %s" % (job.name, val))
        self.datamanager.reload()
        if job.name in self.datamanager.tasks:
            val = self.datamanager.start_task(
                self.datamanager.tasks[job.name])
            logger.info("Start task %s result: %s" % (job.name, val))
            self.datamanager.reload()
            return val
        else:
            return False

    def _api_stop_task(self, job):
        if job.name in self.datamanager.tasks:
            val = self.datamanager.stop_task(
                self.datamanager.tasks[job.name])
            logger.info("Stop task %s result: %s" % (job.name, val))
            self.datamanager.reload()
            return val
        else:
            return False

    def _api_identify_sitter(self):
        logger.info("Attempting to find a machinesitter at %s" % (
                self.hostname))
        if not self.datamanager:
            self.datamanager = MachineData(self.hostname, 40000)
        else:
            self.datamanager._find_portnum()

        if self.datamanager.url:
            logger.info("Found sitter at %s" % self.datamanager.url)
        else:
            logger.warn("Couldn't find a sitter for %s" % self.hostname)

        return self.datamanager.portnum

    def _api_get_endpoint(self, path):
        return "%s/%s" % (self.datamanager.url,
                                    path)

    def _api_get_stats(self):
        logger.debug("Get stats for %s" % str(self))
        tasks = None
        try:
            tasks = self.datamanager.reload()
        except:
            self.loaded = False
            import traceback
            traceback.print_exc()
            logger.error(traceback.format_exc())

        if tasks != None:
            self.loaded = True
        else:
            self.loaded = False

        logger.debug("Get stats for %s result: %s" % (str(self),
                                                     self.loaded))
        return self.loaded

    def _get_machinename(self):
        if self.datamanager:
            return "%s:%s" % (self.datamanager.hostname,
                              self.datamanager.portnum)
        else:
            return self.hostname


class MonitoredMachine(HasMachineSitter):
    """
    An interface for a single
    machine to monitor.  Some functions
    Should be implemented per cloud provider.
    Note: It it assumed that the MachineMonitor
    keeps all MonitoredMachines up to date, and that
    with the exception of functions explicitly about
    downloading data, all calls are accessing LOCAL
    CACHED data and NOT making network calls.
    """
    def __init__(self, config, machine_number=0,
                  *args, **kwargs):
        super(MonitoredMachine, self).__init__(*args, **kwargs)
        self.config = config
        self.running_tasks = []
        self.machine_number = machine_number
        self.hostname = self.config.hostname
        self.detected_sitter_failures = 0

        # Used for deployment purposes by a JobFiller
        self.state = None

    def is_in_deployment(self):
        if not self.state:
            return False

        if self.state.get_state() == 6:
            return False

        return True

    def get_tasks(self):
        return self.datamanager.tasks

    def get_running_tasks(self):
        """
        Return cached data about running task status
        """
        if not self.datamanager:
            return []

        return [task for task in self.datamanager.tasks.values() if \
                    task["running"]]

    def start_task(self, job):
        if self.is_initialized():
            logger.info("Starting a task %s on %s" % (job.name, str(self)))
            self._api_start_task(job)

    def stop_task(self, job):
        if self.is_initialized():
            logger.info("Stopping a task %s on %s" % (job.name, str(self)))
            self._api_stop_task(job)

    def initialize(self):
        return self._api_identify_sitter()

    def is_initialized(self):
        if not self.datamanager:
            return False

        return self.datamanager.url != ""

    def has_loaded_data(self):
        return self.is_initialized() and self.loaded

    def __str__(self):
        return self._get_machinename()

    def __repr__(self):
        return str(self)
