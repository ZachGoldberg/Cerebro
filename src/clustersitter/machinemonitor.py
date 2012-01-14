import logging
import time
from datetime import datetime


class MachineMonitor:
    def __init__(self, parent, number, monitored_machines=[]):
        self.clustersitter = parent
        self.number = number
        self.monitored_machines = monitored_machines
        self.add_queue = []
        logging.info("Initialized a machine monitor for %s" % (
                str(self.monitored_machines)))

    def add_machines(self, monitored_machines):
        self.add_queue.extend(monitored_machines)
        logging.info("Queued %s for inclusion in next stats run" % (
                [str(a) for a in monitored_machines]))

    def initialize_machines(self, monitored_machines):
        for m in monitored_machines:
            m._api_identify_sitter()

    def start(self):
        self.initialize_machines(self.monitored_machines)

        while True:
            start_time = datetime.now()
            logging.info("Processing add queue")
            while len(self.add_queue) > 0:
                machine = self.add_queue.pop()
                self.initialize_machines([machine])
                self.monitored_machines.append(machine)

            logging.info("Finished processing add queue")
            logging.info("Beggining machine monitoring poll for %s" % (
                    [str(a) for a in self.monitored_machines]))

            for machine in self.monitored_machines:
                if machine.is_initialized():
                    machine._api_get_stats()
                else:
                    self.initialize_machines([machine])

            time_spent = datetime.now() - start_time
            sleep_time = self.clustersitter.stats_poll_interval - \
                time_spent.seconds
            logging.info(
                "Finished poll run for %s.  Time_spent: %s, sleep_time: %s" % (
                    [str(a) for a in self.monitored_machines],
                    time_spent,
                    sleep_time))
            if sleep_time > 0:
                time.sleep(sleep_time)
