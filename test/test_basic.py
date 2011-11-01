import main
import unittest


class BasicTests(unittest.TestCase):

    def run_check(self, args, retval=0):
        try:
            main.main(args)
        except SystemExit, e:
            self.assertEqual(e.code, retval)

    def test_quick_command(self):
        self.run_check(["--command", "ls >/dev/null"])

    def test_slow_command(self):
        self.run_check(["--command", "ls >/dev/null; sleep .11"])

    def test_cpu_constraint(self):
        self.run_check(["--cpu=.1", "--command", "./test/spin.sh"], 9)
