import main
import os
import random
import simplejson
import time
import unittest
import urllib2


class HTTPMonitoringTests(unittest.TestCase):
    def run_check(self, args):
        port = 1024 + int(10000 * random.random())
        print "Port %s Chosen" % port
        args.append("--http_monitoring")
        args.append("--http_monitoring_port=%s" % port)
        stats, httpd, harness = main.main(args, wait_for_child=False)
        data = None

        # stop execution of this thread to give the HTTP thread time to
        # boot up
        time.sleep(.1)
        try:
            data = urllib2.urlopen('http://localhost:%s/stats' % port).read()
        except:
            pass

        httpd.stop()
        harness.TerminateChild()

        return simplejson.loads(data)

    def test_basic_monitoring(self):
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
