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

from clustersitter import ClusterSitter
from machineconfig import MachineConfig
from sittercommon import arg_parser


def parse_args(args):
    parser = arg_parser.ArgumentParser(
        description="Start the cluster sitter daemon")

    parser.add_argument("--daemon", dest="daemon",
                        default=False,
                        action="store_true",
                        help='Daemonize and split from launching shell')

    parser.add_argument("--login-user", dest="username",
                        default="ubuntu", help="User to login as")

    return parser.parse_args(args=args)


def daemonize():
    pid = os.fork()
    if pid > 0:
        sys.exit(0)

    os.setsid()
    os.chdir("/")

    pid = os.fork()
    if pid > 0:
        sys.exit(0)

    os.umask(0)
    sys.stdout.flush()
    sys.stderr.flush()
    sys.stdin.close()

    print "Sitter PID: %s" % os.getpid()


def main(sys_args=None):

    if not sys_args:
        sys_args = sys.argv[1:]

    args = parse_args(sys_args)

    settings_module = os.getenv('CLUSTERSITTER_SETTINGS',
                                'clustersitter.settings')
    try:
        __import__(settings_module)
        settings = sys.modules[settings_module]
    except:
        print "Couldn't find settings module %s" % settings_module
        print "Please set the CLUSTERSITTER_SETTINGS environmental " + \
            "variable an appropriate clustersitter settings module"
        sys.exit(1)

    launch_location = os.getcwd()

    if args.daemon:
        daemonize()

    logging.getLogger().setLevel(logging.INFO)
    logging.basicConfig(
        format='%(asctime)s %(name)s:%(levelname)s %(message)s')
    logging.getLogger().handlers[0].setLevel(logging.ERROR)

    sitter = ClusterSitter(daemon=args.daemon,
                           provider_config=settings.provider_config,
                           dns_provider_config=settings.dns_provider_config,
                           keys=settings.keys, user=settings.login_user,
                           log_location=settings.log_location,
                           launch_location=launch_location)
    sitter.start()

    if False:
        # For testing.
        logging.getLogger().setLevel(logging.ERROR)
        localhost = MachineConfig("localhost", "localhost",
                                  1, 1)

        sitter.state.zones.append("localhost")
        sitter.add_machines([localhost], False)

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
