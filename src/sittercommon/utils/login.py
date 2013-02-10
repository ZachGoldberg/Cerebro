import os
import sys

from sittercommon.api import ClusterState


def get_help_string():
    return "Login to a cerebro managed machine"


def get_command():
    return "login"


def get_parser(parser):
    parser.add_argument(dest="name",
                        nargs='*',
                        help='Job or machine to log in to')

    return parser


def get_options(state, name):
    if not name:
        # Show all jobs, then show machines
        return state.jobs
    else:
        name = name.lower()
        if name in state.get_job_names():
            return state.get_machines_for_job(state.get_job(name))
        else:
            # Look for a partial match
            pass


def login(state, machine):
    key = state.provider_config.get_key_for_zone(
        machine.config.shared_fate_zone)

    key_loc = state.find_key(key)
    login_user = state.login_user
    if not key_loc:
        sys.stderr.write("Couldn't find key to login to %s" % machine)
        sys.exit(1)

    sys.stderr.write("opening shell to %s...\n" % machine)
    options = "-o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no"
    os.system("ssh %s -i '%s' %s@%s" % (
        options, key_loc,
        login_user,
        machine.hostname))


def run_command(clustersitter_url=None,
                name=None):
    state = ClusterState(clustersitter_url)
    if isinstance(name, list):
        name = ' '.join(name)

    options = get_options(state, name)
    if len(options) == 1:
        return login(state, options[0])

    for index, option in enumerate(options):
        print "%s. %s" % (index, option)

    result = raw_input("Chose a machine or job to log into:")
    print result
