#!/usr/bin/python -u
"""
This is the main file of the clustersitter application.

It's job is to manage an entire fleet of machines, each running
machine sitters and task sitters.  It reaches out across
the cluster and monitors everything, and exposes several semantic
interfaces (all via HTTP).  There are NAGIOS type monitoring
endpoints as well as an API for interactions via a command line tool.
"""
import logging
import os
import sys
import time

from clustersitter import ClusterSitter, ProductionJob
from clustersitter import MachineConfig
from sittercommon import arg_parser


def parse_args(args):
    parser = arg_parser.ArgumentParser(
        description="Start the cluster sitter daemon")

    parser.add_argument("--daemon", dest="daemon",
                        default=False,
                        action="store_true",
                        help='Daemonize and split from launching shell')

    parser.add_argument("--aws-access-key", dest="aws_access_key")

    parser.add_argument("--aws-secret-key", dest="aws_secret_key")

    parser.add_argument("--key-files", dest="keyfiles",
                        required=True,
                        help="A comma separated list of SSH key files to " \
                        "use to access machines in the cluster")

    parser.add_argument("--login-user", dest="username",
                        default="ubuntu", help="User to login as")

    return parser.parse_args(args=args)


def daemonize():
    pid = os.fork()
    if pid > 0:
        sys.exit(0)

    os.setsid()
    os.umask(0)
    os.chdir("/")

    pid = os.fork()
    if pid > 0:
        sys.exit(0)

    sys.stdout.flush()
    sys.stderr.flush()
    sys.stdin.close()

    print "Sitter PID: %s" % os.getpid()


def main(sys_args=None):

    if not sys_args:
        sys_args = sys.argv[1:]

    args = parse_args(sys_args)

    if not os.getenv('AWS_ACCESS_KEY_ID'):
        os.putenv('AWS_ACCESS_KEY_ID', args.aws_access_key)
        os.putenv('AWS_SECRET_ACCESS_KEY', args.aws_secret_key)

    if args.daemon:
        daemonize()

    logging.getLogger().setLevel(logging.INFO)
    logging.basicConfig(
        format='%(asctime)s %(name)s:%(levelname)s %(message)s')
    logging.getLogger().handlers[0].setLevel(logging.ERROR)

    keys = args.keyfiles.split(',')

    provider_config = {
        'aws': {
            'us-east-1a': {
                '32b_image_id': 'ami-c59259ac',
                '64b_image_id': 'ami-eb915a82',
                'key_name': 'WiFastAWS',
                'security_groups': ['clustersitter'],
                }
            }
        }

    provider_config['aws']['us-east-1b'] = provider_config['aws']['us-east-1a']
    provider_config['aws']['us-east-1c'] = provider_config['aws']['us-east-1a']
    provider_config['aws']['us-east-1d'] = provider_config['aws']['us-east-1a']

    sitter = ClusterSitter(daemon=args.daemon,
                           provider_config=provider_config,
                           keys=keys, user=args.username,
                           log_location="/mnt/data/clustersitter/")
    sitter.start()

    localhost = MachineConfig("localhost",
                              "zg-workstation",
                              6, 16000)

    sitter.add_machines([localhost])

    time.sleep(1)

    job = ProductionJob(
        task_configuration={
            "allow_exit": False,
            "name": "Simple Web Server",
            "command": "/usr/bin/python -m SimpleHTTPServer",
            "auto_start": True,
            "ensure_alive": True,
            "max_restarts": -1,
            "restart": True,
            "uid": 0
        },
        deployment_layout={'aws-us-east-1a': {'cpu': 1, 'mem': 50}},
        deployment_recipe=None,
        )

    #sitter.add_job(job)

    # wait forever
    os.system("tail -f %s" % (sitter.logfiles[0]))
    while True:
        try:
            for index, name in enumerate(sitter.logfiles):
                print "%s. %s" % (index, name)
            num = raw_input("Chose a log to view: ")
            try:
                num = int(num)
                os.system("tail -f %s" % (sitter.logfiles[num]))
            except:
                pass
        except KeyboardInterrupt:
            print "Caught Control-C, exiting"
            os._exit(0)
