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


def main(sys_args=None):

    if not sys_args:
        sys_args = sys.argv[1:]

    args = parse_args(sys_args)

    if not os.getenv('AWS_ACCESS_KEY_ID'):
        if args.aws_access_key:
            os.putenv('AWS_ACCESS_KEY_ID', args.aws_access_key)
        if args.aws_secret_key:
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
                '32b_image_id': 'ami-8b78afe2',
                '64b_image_id': 'ami-eb915a82',
                'key_name': 'WiFastAWS',
                'security_groups': ['clustersitter'],
                },
            'us-west-2a': {
                '32b_image_id': 'ami-da41ccea',
                '64b_image_id': 'ami-ce4bc6fe',
                'key_name': 'WiFastAWSus-west-2',
                'security_groups': ['clustersitter'],
                },
            'us-west-1a': {
                '32b_image_id': 'ami-7dd48a38',
                '64b_image_id': 'ami-15d48a50',
                'key_name': 'WiFastAWSus-west-1',
                'security_groups': ['clustersitter'],
                }
            },
        }

    dns_provider_config = {
        'class': 'dreamhost:DreamhostDNS',
        'username': 'zgold550@gmail.com',
        'api_key': '5Y8PAWC6KXSLWUGD',
        }

    for az in ['b', 'c', 'd']:
        provider_config['aws']['us-east-1%s' % az] = \
            provider_config['aws']['us-east-1a']

        provider_config['aws']['us-west-2%s' % az] = \
            provider_config['aws']['us-west-2a']

        provider_config['aws']['us-west-1%s' % az] = \
            provider_config['aws']['us-west-1a']

    sitter = ClusterSitter(daemon=args.daemon,
                           provider_config=provider_config,
                           dns_provider_config=dns_provider_config,
                           keys=keys, user=args.username,
                           log_location="/mnt/data/clustersitter")
    sitter.start()

    localhost = MachineConfig("localhost",
                              "zg-workstation",
                              6, 16000)

    sitter.add_machines([localhost])

    # wait forever
    os.system("tail -f -n 100 %s" % (sitter.logfiles[0]))
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
