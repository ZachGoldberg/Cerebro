import logging
from sittercommon.machinedata import MachineData


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

    def _api_start_task(self, name):
        pass

    def _api_identify_sitter(self):
        logging.info("Attempting to find a machinesitter at %s" % (
                self.hostname))
        if not self.datamanager:
            self.datamanager = MachineData(self.hostname, 40000)
        else:
            self.datamanager._find_portnum()

        if self.datamanager.url:
            logging.info("Found sitter at %s" % self.datamanager.url)
        else:
            logging.warn("Couldn't find a sitter for %s" % self.hostname)

        return self.datamanager.portnum

    def _api_run_request(self, request):
        """
        Explicitly run the async object
        """
        #result = async.map(request)

    def _api_get_endpoint(self, path):
        return "%s/%s" % (self.datamanager.url,
                                    path)

    def _api_get_stats(self):
        logging.info("Get stats for %s" % str(self))
        tasks = None
        try:
            tasks = self.datamanager.reload()
        except:
            self.loaded = False
            import traceback
            traceback.print_exc()

        if tasks != None:
            self.loaded = True
        else:
            self.loaded = False

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

    def get_running_tasks(self):
        """
        Return cached data about running task status
        """
        if not self.datamanager:
            return []

        return [task for task in self.datamanager.tasks.values() if \
                    task["running"] == "True"]

    def start_task(self, job):
        # If the machine is up and initalized, make the API call
        # Otherwise, spawn a thread to wait for the machine to be up
        # and then make the call
        if self.is_initialized():
            self._api_run_request(self._api_start_task(job.get_name()))

    def begin_initialization(self):
        # Start an async request to find the
        # machinesitter port number
        # and load basic configuration
        pass

    def is_initialized(self):
        if not self.datamanager:
            return False

        return self.datamanager.url != ""

    def has_loaded_data(self):
        return self.is_initialized() and self.loaded

    def __str__(self):
        return self._get_machinename()
