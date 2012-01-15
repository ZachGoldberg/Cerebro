import boto
import logging
from boto import ec2

from clustersitter.machineconfig import MachineConfig
from machineprovider import MachineProvider


class AmazonEC2(MachineProvider):
    instance_types = {
        'm1.small': (1, 1700, 160, 32),
        'm1.large': (2, 7500, 850, 64),
        'm1.xlarge': (4, 15000, 1690, 64),
        't1.micro': (1, 613, 0, 64),
        'm2.xlarge': (2, 17100, 420, 64),
        'm2.2xlarge': (4, 34200, 850, 64),
        'm2.4xlarge': (8, 68400, 1690, 64),
        'c1.medium': (2, 1700, 350, 32),
        'c1.xlarge': (8, 7000, 1690, 64)
        }

    def __init__(self):
        self.zones = []
        self.regions = []
        self.connections = {}
        self.machines = {}
        try:
            logging.info("Download EC2 Region List")
            self.regions = ec2.regions()
        except boto.exception.NoAuthHandlerFound:
            logging.warn("Couldn't connect to EC2.  Auth Error.  " +
                         "Ensure AWS crednetials are set in the env " +
                         "(AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY")

    def usable(self):
        return bool(self.regions)

    @classmethod
    def config_from_instance(cls, instance):
        perf = cls.instance_types[instance.instance_type]
        return MachineConfig(instance.public_dns_name,
                             "aws-%s" % instance.placement,
                             cpus=perf[0],
                             mem=perf[1],
                             disk=perf[2],
                             bits=perf[3],
                             )

    def get_machine_list(self):
        self._initialize_connections()
        machine_list = []
        for conn in self.connections.values():
            reservations = conn.get_all_instances()
            if reservations:
                for reservation in reservations:
                    for instance in reservation.instances:
                        sf_zone = "aws-%s" % instance.placement
                        if sf_zone not in self.machines:
                            self.machines[sf_zone] = []
                        self.machines[sf_zone].append(instance)
                        machine_list.append(
                            AmazonEC2.config_from_instance(instance))

        return machine_list

    def _initialize_connections(self):
        for region in self.regions:
            if not region.name in self.connections:
                logging.info("Connecting to %s" % region.name)
                self.connections[region.name] = ec2.connect_to_region(
                    region.name)

    def get_all_shared_fate_zones(self):
        if self.zones:
            return self.zones

        #self.zones = [u'eu-west-1a', u'eu-west-1b', u'eu-west-1c', u'sa-east-1a', u'sa-east-1b', u'us-east-1a', u'us-east-1b', u'us-east-1c', u'us-east-1d', u'ap-northeast-1a', u'ap-northeast-1b', u'us-west-2a', u'us-west-2b', u'us-west-1b', u'us-west-1c', u'ap-southeast-1a', u'ap-southeast-1b']

        self._initialize_connections()
        for conn in self.connections.values():
            zones = conn.get_all_zones()
            for zone in zones:
                self.zones.append(zone.name)

        return ["aws-%s" % s for s in self.zones]
