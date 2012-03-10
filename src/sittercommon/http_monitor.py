"""
Reads data from a stats collector and exposes it via HTTP
"""
import cgi
import os
import simplejson
import threading
import tenjin
import urlparse
import sys
import SocketServer
import BaseHTTPServer
from pkg_resources import resource_filename

# A weird requirement from tenjin to have this
# The world explodes if we don't have it
from tenjin.helpers import *


def head(filename, num_lines):
    return file_process("head", filename, num_lines)


def tail(filename, num_lines):
    return file_process("tail", filename, num_lines)


def file_process(cmd, filename, num_lines):
    """
    Implementing tail in python is like 50 lines of code,
    and is pretty unreadable.  Just cop out and use something
    we know works.

    """
    stdin, stdout = os.popen2("%s -n %s %s" % (cmd, num_lines, filename))
    stdin.close()
    data = stdout.read()
    stdout.close()
    return data


class HTTPMonitorHandler(BaseHTTPServer.BaseHTTPRequestHandler):

    def __init__(self, monitor, new_handlers, *args, **kwargs):
        self.monitor = monitor

        self.handlers = {
            "/stats": self._get_stats,
            "/logs": self._get_logs,
            "/logfile": self._get_logfile,
            }

        self.handlers.update(new_handlers)

        paths = [
            resource_filename(__name__, 'templates'),
            'templates',
            '/usr/local/cerebro/templates',
            '/opt/tasksitter/templates/',
            os.path.join(
                os.getenv('HOME'),
                'workspace',
                'tasksitter',
                'templates')]

        if hasattr(monitor.harness, "launch_location"):
            paths.append(
                os.path.join(monitor.harness.launch_location, "templates"))

        tenjin.Engine.cache = tenjin.MemoryCacheStorage()
        self.engine = tenjin.Engine(path=paths)

        BaseHTTPServer.BaseHTTPRequestHandler.__init__(self, *args, **kwargs)

    def _usage(self, _):
        output = """Invalid Path Requests.  Options:<br><ul>"""

        return self.engine.render('index.html', {'handlers':
                                                     self.handlers,
                                                 'name': sys.argv[0]})

        for handler in self.handlers.keys():
            output += "<li><a href='%s'>%s</a></li>" % (handler, handler)
        output += "</ul></body></html>"
        return output

    def add_handler(self, path, callback):
        self.handlers[path] = callback

    def _get_logfile(self, args):
        # @TODO This is a bit of a security problem...
        filename = args.get('name')
        if not filename:
            if not 'logname' in args:
                return "No filename specified"

            logfiles = self.monitor.get_logs()
            filename = logfiles[args['logname']]

        filehandle = None
        try:
            filehandle = open(filename)
        except IOError:
            return "File not found"

        if not args.get('head') and not args.get('tail'):
            return filehandle.read()

        if args.get('tail'):
            return tail(filename, int(args['tail']))
        else:
            return head(filename, int(args['head']))

    def _get_logs(self, args):
        logfiles = self.monitor.get_logs()
        for k, v in logfiles.items():
            size = 0
            try:
                size = float(os.stat(v).st_size) / 1024
            except:
                pass

            logfiles[k] = {'url': "/logfile?name=%s" % v,
                           'location': v,
                           'size': size}

        if 'nohtml' in args:
            return self._format_dict(logfiles, args)
        else:
            return self.engine.render('logs.html', {'data': logfiles})

    def _get_stats(self, args):
        stats = self.monitor.get_stats()
        if "nohtml" in args:
            return self._format_dict(stats, args)
        else:
            return self.engine.render('stats.html', {'data': stats})

    def _format_dict(self, data, args):
        """
        Convert a dictionary of data into an appropriate
        output format based on the query string args
        """
        def todict(obj):
            if isinstance(obj, dict):
                return obj

            try:
                return obj.__dict__
            except:
                return str(obj)

        output = ""
        if "format" in args and args["format"] != "flat":
            if args['format'] == "json":
                output = simplejson.dumps(data,
                                          skipkeys=True,
                                          default=todict)
            else:
                output = "Invalid Format"
        else:
            seperator = "\n"
            for key, value in data.items():
                output += "%s=%s%s" % (key, value, seperator)

        return output

    def do_GET(self):
        urldata = urlparse.urlparse(self.path)
        array_args = urlparse.parse_qs(urldata.query)
        args = dict([(k, v[0]) for k, v in array_args.items()])

        if not urldata.path in self.handlers:
            self.wfile.write(self._usage(args))
            return

        args['engine'] = self.engine

        try:
            output = self.handlers[urldata.path](args)
            if not output:
                output = "No Data"

            if isinstance(output, dict):
                self.wfile.write(self._format_dict(output, args))
            else:
                self.wfile.write(output)
        except:
            import traceback
            self.wfile.write(traceback.format_exc())

        return

    def do_POST(self):
        urldata = urlparse.urlparse(self.path)
        ctype, pdict = cgi.parse_header(self.headers.getheader('content-type'))
        if ctype == 'application/x-www-form-urlencoded':
            length = int(self.headers.getheader('content-length'))
            try:
                args = cgi.parse_qs(self.rfile.read(length),
                                    keep_blank_values=1)
                args = simplejson.loads(args['data'][0])
            except:
                import traceback
                traceback.print_exc()
                self.wfile.write("Error decoding POST data")
                return traceback.format_exc()
        else:
            args = {}

        if not urldata.path in self.handlers:
            self.wfile.write(self._usage(args))
            return

        args['engine'] = self.engine

        self.wfile.write(self.handlers[urldata.path](args))

        return


class ThreadedTCPServer(SocketServer.ThreadingMixIn, SocketServer.TCPServer):
    pass


class HTTPMonitor(object):
    def __init__(self, stats, harness, port):
        self.port = int(port)
        self.stats = stats
        self.harness = harness
        self.logmanager = harness.logmanager
        self.httpd = None
        self.stopped = False
        self.run_thread = None
        self.new_handlers = {}

    def add_handler(self, path, callback):
        self.new_handlers[path] = callback

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

        self.run_thread = threading.Thread(target=self._start_server,
                                           name="HTTPServer")
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
        handler = lambda x, y, z: HTTPMonitorHandler(
            self,
            self.new_handlers, x, y, z)
        self.httpd = ThreadedTCPServer(('', self.port), handler)
        self.httpd.timeout = 1
        self.httpd.serve_forever(poll_interval=0.1)
