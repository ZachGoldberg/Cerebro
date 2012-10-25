import json
import requests


def get_help_string():
    return "Update clustersitter job config"


def get_command():
    return "update_job_cfg"


def get_parser(parser):
    parser.add_argument("--job-config", dest="job_config",
                        required=True, nargs='+',
                        help='JSON file of Job configuration')

    return parser


def add_config_from_file(clustersitter_url, filename):
    try:
        jobs = json.load(open(filename))
    except:
        print "Error opening config file %s" % filename
        import traceback
        traceback.print_exc()
        return

    print "%s/add_job" % clustersitter_url
    for job in jobs:
        data = {'data': json.dumps(job)}
        resp = requests.post("%s/add_job" % clustersitter_url, data=data)
        print "Response for %s: %s" % (job['task_configuration']['name'], resp.content)


def run_command(clustersitter_url=None,
                job_config=None):
    if isinstance(job_config, basestring):
        add_config_from_file(clustersitter_url, job_config)
    else:
        for filename in job_config:
            add_config_from_file(clustersitter_url, filename)
