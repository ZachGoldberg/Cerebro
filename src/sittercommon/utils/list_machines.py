from sittercommon.api import ClusterState


def get_help_string():
    return "List machines in clustersitter"


def get_command():
    return "listmachines"


def get_parser(parser):
    return parser


def run_command(clustersitter_url=None):

    state = ClusterState(clustersitter_url)
    for machine in state.machines:
        print "%s (%s)" % (machine.hostname, machine.config.ip)
        for taskname, taskdata in machine.tasks.iteritems():
            running = "Not Running"
            if taskdata['running']:
                running = "Running    "
            print "  - %s %s (%s)" % (
                running, taskname, taskdata['command'],)

        print ""
