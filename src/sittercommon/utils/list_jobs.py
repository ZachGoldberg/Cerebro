from sittercommon.api import ClusterState


def get_help_string():
    return "List jobs active in clustersitter"


def get_command():
    return "listjobs"


def get_parser(parser):
    return parser


def run_command(clustersitter_url=None,
                job_name=None,
                version=None):

    state = ClusterState(clustersitter_url)
    print '\n'.join([j.name for j in state.jobs])
