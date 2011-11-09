"""
A class which knows how to pull information out of a process harness
and process.  Exposes a simple interface to get all this information.
"""


class StatsCollector(object):
    def __init__(self, harness):
        self.process_harness = harness

    def start(self):
        pass
