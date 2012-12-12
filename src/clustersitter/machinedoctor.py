import logging
import threading
import time
from datetime import datetime

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
            logger.debug("Begin machine doctor run.")
            try:
                pass
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
