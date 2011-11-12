import main
import os
import random
import simplejson
import time
import unittest
import urllib2


class HTTPMonitoringTests(unittest.TestCase):
    def run_check(self, args, output_format='json'):
        port = 1024 + int(10000 * random.random())
        print "Port %s Chosen" % port
        args.append("--http-monitoring")
        args.append("--http-monitoring-port=%s" % port)
        stats, httpd, harness = main.main(args, wait_for_child=False)
        data = None

        # stop execution of this thread to give the HTTP thread time to
        # boot up
        time.sleep(.1)
        try:
            data = urllib2.urlopen(
                'http://localhost:%s/stats?format=%s' % (port,
                                                         output_format)).read()
        except:
            pass

        httpd.stop()
        harness.terminate_child()

        print data
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

                k, v = row.split('=')
                return_data[k] = v

            return return_data

    def test_basic_monitoring_flat(self):
        data = self.run_check(['--cpu=.2',
                               '--command', 'sleep .2; ./test/spin.sh'],
                              output_format="flat")
        print data

        self.assertTrue("child_pid" in data)
        self.assertTrue(data["process_start_time"] is not None)
        self.assertTrue(data["task_start_time"] is not None)
        self.assertEquals(data["max_restarts"], '-1')
        self.assertEquals(data["num_task_starts"], '1')
        self.assertTrue("spin.sh" in data["command"])
        self.assertTrue("sleep" in data["command"])

    def test_basic_monitoring_json(self):
        data = self.run_check(['--cpu=.2',
                               '--command', 'sleep .2; ./test/spin.sh'])
        print data

        self.assertTrue("child_pid" in data)
        self.assertTrue(data["process_start_time"] is not None)
        self.assertTrue(data["task_start_time"] is not None)
        self.assertEquals(data["max_restarts"], -1)
        self.assertEquals(data["num_task_starts"], 1)
        self.assertTrue("spin.sh" in data["command"])
        self.assertTrue("sleep" in data["command"])
