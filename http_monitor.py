"""
Reads data from a stats collector and exposes it via HTTP
"""


class HTTPMonitor(object):
    def __init__(self, stats, port):
        self.port = port
        self.stats = stats

    def start(self):
        pass
