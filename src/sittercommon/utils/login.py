import os
import sys

from clustersitter import MonitoredMachine, ProductionJob
from sittercommon.api import ClusterState


def get_help_string():
    return "Login to a cerebro managed machine"


def get_command():
    return "login"


def get_parser(parser):
    parser.add_argument("--remotecommand",
                        dest="remote_command",
                        help='Command to run on machine or a @file to run')

    parser.add_argument(dest="name",
                        nargs='*',
                        help='Job or machine to log in to')

    return parser


def get_options(state, name):
    if not name:
        # Show all jobs, then show machines
        return state.jobs + state.machines
    else:
        name = name.lower()
        algos = [
            lambda x, y: x == y,
            lambda x, y: x.startswith(y),
            lambda x, y: x in y]

        for algo in algos:
            for jobname in state.get_job_names():
                if algo(jobname.lower(), name):
                    sys.stderr.write("\nUsing **%s** as a match for %s\n\n" % (
                        jobname, name))
                    return state.get_machines_for_job(state.get_job(jobname))

    return []


def login(state, machine, command=None):
    key = state.provider_config.get_key_for_zone(
        machine.config.shared_fate_zone)

    key_loc = state.find_key(key)
    login_user = state.login_user
    if not key_loc:
        sys.stderr.write("Couldn't find key to login to %s" % machine)
        sys.exit(1)

    sys.stderr.write("opening shell to %s...\n" % machine)
    options = "-o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no"
    remote_cmd = ''
    if command:
        remote_cmd = "'%s'" % command

    cmd = "ssh %s -i '%s' %s@%s %s" % (
        options, key_loc,
        login_user,
        machine.hostname,
        remote_cmd)

    # Try 3 times to login
    for i in xrange(3):
        sys.stderr.write("%s\n" % cmd)
        ret = os.system(cmd)
        if ret == 0:
            return
        else:
            sys.stderr.write("Login failed, trying again (%s/3)...\n" % (
                i + 1))


def run_command(clustersitter_url=None,
                name=None,
                remote_command=None):
    if remote_command and remote_command.startswith('@'):
        remote_command = open(remote_command[1:]).read()

    state = ClusterState(clustersitter_url)
    state.reload()
    if isinstance(name, list):
        name = ' '.join(name)

    def menu(options):
        if not options:
            sys.stderr.write("No matches for %s found, trying all\n" % name)
            options = get_options(state, None)

        if len(options) == 1 and isinstance(options[0], MonitoredMachine):
            sys.stderr.write("Only one machine found, logging in...\n")
            return login(state, options[0], remote_command)

        selected = None
        while not selected:
            for index, option in enumerate(options):
                print "%s. %s" % (index, option)

            result = raw_input("Chose a machine or job to log into: ")
            try:
                selected = options[int(result)]
            except:
                continue

        if isinstance(selected, ProductionJob):
            return menu(get_options(state, selected.name))

        if isinstance(selected, MonitoredMachine):
            return login(state, selected, remote_command)

    options = get_options(state, name)
    menu(options)
