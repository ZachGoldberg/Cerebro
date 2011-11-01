import main
import unittest


class BasicTests(unittest.TestCase):

    def run_check(self, args, retval=0):
        try:
            main.main(args)
        except SystemExit, e:
            self.assertEqual(e.code, retval)

    def test_quick_command(self):
        self.run_check(["--command", "ls"])

    def test_slow_Command(self):
        self.run_check(["--command", "ls ; sleep 2"])
