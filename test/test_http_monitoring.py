import json
import random
import simplejson
import time
import unittest
import urllib2

import tasksitter.main as main


class HTTPMonitoringTests(unittest.TestCase):

    def start_http_server(self, args):
        self.port = 1024 + int(10000 * random.random())
        print "Port %s Chosen" % self.port
        args.append("--http-monitoring")
        args.append("--http-monitoring-port=%s" % self.port)
        self.stats, self.httpd, self.harness = main.main(
            args, wait_for_child=False)

        # stop execution of this thread to give the HTTP thread time to
        # boot up
        time.sleep(.1)

    def stop_http_server(self):
        self.httpd.stop()
        self.harness.terminate_child()

    def make_call(self, url):
        data = None
        try:
            data = urllib2.urlopen(url).read()
        except:
            import traceback
            traceback.print_exc()
            self.fail()

        return data

    def run_check(self, args, path, output_format='json',
                  stop_server=True):
        self.start_http_server(args)
        data = self.make_call('http://localhost:%s/%s?format=%s&nohtml=1' % (
                self.port,
                path,
                output_format))

        if stop_server:
            self.stop_http_server()

        if not data:
            return {}

        if output_format == 'json':
            return simplejson.loads(data)
        else:
            rows = data.split("\n")
            return_data = {}
            for row in rows:
                if not row:
                    continue

                k, v = row.split('=', 1)
                return_data[k] = v

            return return_data

    def test_basic_monitoring_flat(self):
        data = self.run_check(['--cpu=.2',
                               '--command', 'sleep .2; ./spin.sh'],
                              path="stats",
                              output_format="flat")

        self.assertTrue("child_pid" in data)
        self.assertTrue(data["process_start_time"] is not None)
        self.assertTrue(data["task_start_time"] is not None)
        self.assertEquals(data["max_restarts"], '-1')
        self.assertEquals(data["num_task_starts"], '1')
        self.assertTrue("spin.sh" in data["command"])
        self.assertTrue("sleep" in data["command"])
        self.assertTrue("violated_CPU Constraint (0.2)" in data)

    def test_basic_monitoring_json(self):
        data = self.run_check(['--cpu=.2',
                               '--command', 'sleep .2; ./spin.sh'],
                              path="stats")

        self.assertTrue("child_pid" in data)
        self.assertTrue(data["process_start_time"] is not None)
        self.assertTrue(data["task_start_time"] is not None)
        self.assertEquals(data["max_restarts"], -1)
        self.assertEquals(data["num_task_starts"], 1)
        self.assertTrue("spin.sh" in data["command"])
        self.assertTrue("sleep" in data["command"])
        self.assertTrue("violated_CPU Constraint (0.2)" in data)

        self.assertTrue("file_version" in data)
        self.assertTrue("launch_location" in data)
        self.assertTrue("dir_version" in data)
        print data

    def test_logs_list_json(self):
        data = self.run_check(['--cpu=.2', "--ensure-alive",
                               '--command', 'id',
                               '--stdout-location', '/tmp/',
                               '--stderr-location', '/tmp/'],
                              path="logs",
                              output_format='json')

        self.assertTrue("stdout.0" in data)
        self.assertTrue("stderr.0" in data)
        self.assertTrue(len(data["stdout.0"]) > 0)
        self.assertTrue(len(data["stderr.0"]) > 0)

    def test_logs_list_flat(self):
        data = self.run_check(['--cpu=.2', "--ensure-alive",
                               '--command', 'id',
                               '--stdout-location', '/tmp/',
                               '--stderr-location', '/tmp/'],
                              path="logs",
                              output_format="flat")

        self.assertTrue("stdout.0" in data)
        self.assertTrue("stderr.0" in data)
        self.assertTrue(len(data["stdout.0"]) > 0)
        self.assertTrue(len(data["stderr.0"]) > 0)

    def test_download_log_file(self):
        data = self.run_check(['--cpu=.2', "--ensure-alive",
                               '--command', 'echo -n "hello"; sleep 10',
                               '--stdout-location', '/tmp/'],
                              path="logs",
                              output_format="json",
                              stop_server=False)

        filename = data["stdout.0"]['url']

        log_data = self.make_call(
            'http://localhost:%s%s&nohtml=1' % (
                self.port, filename))
        self.stop_http_server()

        self.assertEquals(log_data, "hello")

    def test_download_log_file_by_name(self):
        data = self.run_check(['--cpu=.2', "--ensure-alive",
                               '--command', 'echo -n "hello"; sleep 10',
                               '--stdout-location', '/tmp/'],
                              path="logs",
                              output_format="json",
                              stop_server=False)

        print data

        filename = data["stdout.0"]['url']

        log_data = self.make_call(
            'http://localhost:%s/logfile?logname=stdout.0&nohtml=1' % (
                self.port))
        self.stop_http_server()

        self.assertEquals(log_data, "hello")
