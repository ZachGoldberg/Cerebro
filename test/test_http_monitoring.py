import main
import os
import simplejson
import unittest
import urllib2

class HTTPMonitoringTests(unittest.TestCase):
    def run_check(self, args):
        args.append("--http-monitoring")
        args.append("--http-monitoring-port=1234")
        main.main(args, wait_for_child=False)

        data = urllib2.urlopen('http://localhost:1234/stats').read()

        return simplejson.loads(data)

    def test_basic_monitoring(self):
        data = self.run_check(['--cpu=.2',
                   '--command', 'sleep .2; ./test/spin.sh'])
        print data

        self.assertTrue("child_pid" in data)
