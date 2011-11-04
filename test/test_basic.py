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

    def test_quick_command_cpu_constraint(self):
        self.run_check(["--cpu=.1", "--command", "ls >/dev/null"])

    def test_quick_command_mem_constraint(self):
        self.run_check(["--mem=10", "--command", "ls >/dev/null"])

    def test_quick_command_cpu_mem_constraint(self):
        self.run_check(["--cpu=.1", "--mem=10", "--command", "ls >/dev/null"])

    def test_slow_command(self):
        self.run_check(["--command", "ls >/dev/null; sleep .11"])

    def test_slow_command_cpu_constraint(self):
        self.run_check(["--cpu=.1", "--command", "ls >/dev/null; sleep .11"])

    def test_slow_command_mem_constraint(self):
        self.run_check(["--mem=10", "--command", "ls >/dev/null; sleep .11"])

    def test_slow_command_cpu_mem_constraint(self):
        self.run_check(["--cpu=.1", "--mem=10",
                        "--command", "ls >/dev/null; sleep .11"])

    def test_cpu_constraint(self):
        self.run_check(["--cpu=.1", "--command", "./test/spin.sh"], 9)

    def test_cpu_constraint_redirect(self):
        for i in range(1, 5):
            self.run_check(["--cpu=.1", "--command",
                            "bash -c './test/spin.sh 2>/dev/null'"],
                           9)

    def test_mem_constraint(self):
        self.run_check(["--mem=10", "--command", "./test/mem.sh"],
                       9)

    def test_mem_constraint(self):
        """
        Test process group memory checking

        There is a case where bash forks children strangely when there is
        redirection.  We want to ensure we catch ALL children and account for
        ALL their memory usage
        """
        for i in range(1, 5):
            self.run_check(["--mem=3",
                            "--command", "./test/mem.sh 2>/dev/null"], 9)
