from sittercommon.api import ClusterState


def get_help_string():
    return "List jobs active in clustersitter"


def get_command():
    return "listjobs"


def get_parser(parser):
    return parser


def run_command(clustersitter_url=None):
    state = ClusterState(clustersitter_url)
    for job in state.jobs:
        print "%s - %s instances" % (
            job.name,
            sum([len(v) for v in job.fill_machines.values()]))
