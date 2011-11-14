"""
Reads data from a stats collector and exposes it via HTTP
"""
import os
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
            "/logfile": self._get_logfile,
            }

        BaseHTTPServer.BaseHTTPRequestHandler.__init__(self, *args, **kwargs)

    def _usage(self, _):
        output = """Invalid Path Requests.  Options:<br><ul>"""

        for handler in self.handlers.keys():
            output += "<li><a href='%s'>%s</a></li>" % (handler, handler)
        output += "</ul></body></html>"
        return output

    def _get_logfile(self, args):
        # @TODO This is a bit of a security problem...
        try:
            return open(args['name']).read()
        except IOError:
            return "File not found"

    def _get_logs(self, args):
        logfiles = self.monitor.get_logs()
        for k, v in logfiles.items():
            if 'nohtml' in args:
                logfiles[k] = v
            else:
                size = 0
                try:
                    size = float(os.stat(v).st_size) / 1024
                except:
                    pass

                logfiles[k] = "<a href='/logfile?name=%s'>%s (%s kB)</a>" % (
                    v,
                    v,
                    size)

        return self._format_dict(logfiles, args)

    def _get_stats(self, args):
        stats = self.monitor.get_stats()
        return self._format_dict(stats, args)

    def _format_dict(self, data, args):
        """
        Convert a dictionary of data into an appropriate
        output format based on the query string args
        """

        output = ""
        if "format" in args and args["format"] != "flat":
            if args['format'] == "json":
                output = simplejson.dumps(data)
            else:
                output = "Invalid Format"
        else:
            seperator = "<br>"
            if "nohtml" in args:
                seperator = "\n"

            for key, value in data.items():
                output += "%s=%s%s" % (key, value, seperator)

        return output

    def do_GET(self):
        urldata = urlparse.urlparse(self.path)
        array_args = urlparse.parse_qs(urldata.query)
        args = dict([(k, v[0]) for k, v in array_args.items()])

        if not "nohtml" in args:
            self.wfile.write("<html><body>")

        if not urldata.path in self.handlers:
            self.wfile.write(self._usage(args))
            return

        self.wfile.write(self.handlers[urldata.path](args))

        if not "nohtml" in args:
            self.wfile.write("</body></html>")

        return


class HTTPMonitor(object):
    def __init__(self, stats, harness, port):
        self.port = int(port)
        self.stats = stats
        self.harness = harness
        self.logmanager = harness.logmanager
        self.httpd = None
        self.stopped = False
        self.run_thread = None

    def get_logs(self):
        """
        Pull a list of logfiles for all of the tasks
        that this tasksitter has created.
        """
        return self.logmanager.get_logfile_names()

    def get_stats(self):
        """
        Return metadata and running stats for the process
        """
        data = self.stats.get_metadata()
        data.update(self.stats.get_live_data())
        return data

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
