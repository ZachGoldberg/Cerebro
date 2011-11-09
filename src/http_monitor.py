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
        array_args = urlparse.parse_qs(urldata.query)
        args = dict([(k, v[0]) for k, v in array_args.items()])

        stats = self.monitor.get_stats()

        output = ""
        if "format" in args and args["format"] != "flat":
            if args['format'] == "json":
                output = simplejson.dumps(stats)
            else:
                output = "Invalid Format"
        else:
            for key, value in stats.items():
                output += "%s=%s\n" % (key, value)

        self.wfile.write(output)
        return


class HTTPMonitor(object):
    def __init__(self, stats, port):
        self.port = int(port)
        self.stats = stats
        self.httpd = None
        self.stopped = False
        self.run_thread = None

    def get_stats(self):
        """
        Return metadata and running stats for the process
        """
        metadata = self.stats.get_metadata()
        return metadata

    def start(self):
        """
        Begin serving HTTP requests with stats data
        """
        if self.stopped:
            return

        self.run_thread = threading.Thread(target=self._start_server)
        self.run_thread.start()

    def stop(self):
        """
        Stop the HTTP server
        """
        if self.httpd:
            self.httpd.shutdown()

        self.stopped = True

    def _start_server(self):
        """
        Internal method to start the server.
        """
        handler = lambda x, y, z: HTTPMonitorHandler(self, x, y, z)
        self.httpd = SocketServer.TCPServer(('', self.port), handler)
        self.httpd.serve_forever(poll_interval=0.1)
