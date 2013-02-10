from sittercommon.api import ClusterState


def get_help_string():
    return "List machines in clustersitter"


def get_command():
    return "listmachines"


def get_parser(parser):
    return parser


def run_command(clustersitter_url=None):

    state = ClusterState(clustersitter_url)
    print '\n'.join([m.config.ip for m in state.machines])
