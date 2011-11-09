"""
Reads data from a stats collector and exposes it via HTTP
"""
import simplejson
import threading
import urlparse
import SocketServer
import BaseHTTPServer


class HTTPMonitorHandler(BaseHTTPServer.BaseHTTPRequestHandler):
    def __init__(self, monitor, *args, **kwargs):
        self.monitor = monitor
        BaseHTTPServer.BaseHTTPRequestHandler.__init__(self, *args, **kwargs)

    def do_GET(self):
        urldata = urlparse.urlparse(self.path)
        args = urlparse.parse_qs(urldata.query)

        self.wfile.write(self.monitor.get_stats())
        return


class HTTPMonitor(object):
    def __init__(self, stats, port):
        self.port = int(port)
        self.stats = stats
        self.httpd = None
        self.stopped = False

    def get_stats(self):
        return simplejson.dumps({'child_pid': 10})

    def start(self):
        if self.stopped:
            return

        self.run_thread = threading.Thread(target=self._start_server)
        self.run_thread.start()

    def stop(self):
        if self.httpd:
            self.httpd.shutdown()

        self.stopped = True

    def _start_server(self):
        handler = lambda x, y, z: HTTPMonitorHandler(self, x, y, z)
        self.httpd = SocketServer.TCPServer(('', self.port), handler)
        self.httpd.serve_forever(poll_interval=0.1)
