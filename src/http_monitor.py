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

        self.handlers = {
            "/stats": self._get_stats,
            "/logs": self._get_logs,
            }

        BaseHTTPServer.BaseHTTPRequestHandler.__init__(self, *args, **kwargs)

    def _usage(self, args):
        output = """
<html>
<body>
Invalid Path Requests.  Options:<br>
<ul>
"""

        for handler in self.handlers.keys():
            output += "<li><a href='%s'>%s</a></li>" % (handler, handler)
        output += "</ul></body></html>"
        return output

    def _get_logs(self, args):
        pass

    def _get_stats(self, args):
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

        return output

    def do_GET(self):
        urldata = urlparse.urlparse(self.path)
        array_args = urlparse.parse_qs(urldata.query)
        args = dict([(k, v[0]) for k, v in array_args.items()])

        if not urldata.path in self.handlers:
            self.wfile.write(self._usage(args))
            return

        self.wfile.write(self.handlers[urldata.path](args))
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
