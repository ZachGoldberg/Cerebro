import boto
import logging
import os
import socket
import time
from boto import ec2

from clustersitter.machineconfig import MachineConfig
from clustersitter.eventmanager import ClusterEventManager
from machineprovider import MachineProvider

logger = logging.getLogger(__name__)


class AmazonEC2(MachineProvider):
    instance_types = {
        # Sorted by price
        't1.micro': (1, 613, 0, 64),
        'm1.small': (1, 1700, 160, 64),
        'm1.medium': (1, 4000, 400, 64),
        'c1.medium': (2, 1701, 350, 64),
        'm1.large': (2, 7500, 850, 64),
        'm1.xlarge': (4, 15000, 1690, 64),
        'c1.xlarge': (8, 7000, 1690, 64),
        'm2.xlarge': (2, 17100, 420, 64),
        'm2.2xlarge': (4, 34200, 850, 64),
        'm2.4xlarge': (8, 68400, 1690, 64),
        }

    def __init__(self, config):
        self.config = config
        self.zones = []
        self.connection_by_zone = {}
        self.regions = []
        self.connections = {}
        self.machines = {}

        # TODO Ensure security group has port 22 permissions
        try:
            logger.info("Download EC2 Region List")
            self.regions = ec2.regions()
        except boto.exception.NoAuthHandlerFound:
            logger.error("Couldn't connect to EC2.  Auth Error.  " +
                         "Ensure AWS crednetials are set in the env " +
                         "(AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY")

            logger.error("Got: %s, %s" % (os.getenv('AWS_ACCESS_KEY_ID'),
                                          os.getenv('AWS_SECRET_ACCESS_KEY')))

        except:
            import traceback
            logger.error("Couldn't connect to EC2.")
            logger.error(traceback.format_exc())

    def usable(self):
        return bool(self.regions)

    def decomission(self, machine):
        """
        machine -- A Monitored Machine
        """
        aws_placement = machine.config.shared_fate_zone.replace(
            'aws-', '')
        conn = self.connection_by_zone[aws_placement]
        instance = machine.config.data
        try:
            conn.terminate_instances([instance.id])
        except:
            import traceback
            logger.warn(traceback.format_exc())
            return False

        return True

    @classmethod
    def config_from_instance(cls, instance):
        perf = cls.instance_types[instance.instance_type]
        ip = socket.gethostbyname(instance.public_dns_name)
        return MachineConfig(instance.public_dns_name,
                             "aws-%s" % instance.placement,
                             cpus=perf[0],
                             mem=perf[1],
                             disk=perf[2],
                             bits=perf[3],
                             data=instance,
                             ip=ip
                             )

    def _get_image_by_type(self, zone, instance_type):
        if self.instance_types[instance_type][3] == 32:
            return self.config[zone]['32b_image_id']
        else:
            return self.config[zone]['64b_image_id']

    def fill_request(self, zone, cpus, mem_per_job=None):
        """
        Ideally what we do now is figure out
        what instance_type combination best fills
        this request.  For simplicity for now
        we'll just spin up a new 64b instance per CPU
        with the instance type that most closely matches
        mem_per_job
        """
        # TODO if this fails decomission the machines and return false
        aws_placement = zone.replace('aws-', '')
        conn = self.connection_by_zone[aws_placement]

        # TODO detect this better
        closest_type = 'm1.large'
        closest_amount = 1000000
        if mem_per_job:
            for itype, perf in self.instance_types.items():
                if perf[3] != 64:
                    continue

                mem_distance = abs(mem_per_job - perf[1])
                if mem_distance < closest_amount:
                    closest_amount = mem_distance
                    closest_type = itype

        instance_type = closest_type
        print instance_type
        ClusterEventManager.handle(
            "Spinning up %s amazon instances of type %s..." % (cpus,
                                                               instance_type))
        reservation = None
        try:
            reservation = conn.run_instances(
                image_id=self._get_image_by_type(aws_placement, instance_type),
                key_name=self.config[aws_placement]['key_name'],
                security_groups=self.config[aws_placement]['security_groups'],
                min_count=cpus,
                max_count=cpus,
                instance_type=instance_type,
                placement=aws_placement,
                monitoring_enabled=False
                )
        except:
            import traceback
            logger.error(traceback.format_exc())
            return False

        instance_ids = [i.id for i in reservation.instances]
        logger.info("Reservation made for %s instances of type %s" % (
                cpus, instance_type))
        logger.info("Ids: %s" % instance_ids)
        done = False
        instances = []
        failure_count = 0
        while not done:
            # Add logging here
            # Don't wait for all of them, incase some don't come up
            available = 0
            reservation = None
            try:
                reservation = conn.get_all_instances(instance_ids)[0]
            except:
                failure_count += 1
                if failure_count > 5:
                    logger.error(traceback.format_exc())
                    return False
                continue
            instances = reservation.instances
            all_found = True
            for instance in instances:
                if not (instance.public_dns_name and
                        instance.state == 'running'):
                    all_found = False
                else:
                    available += 1

            logger.info("%s of %s instances are ready" % (available,
                                                           len(instances)))
            done = all_found
            if not done:
                time.sleep(1)

        logger.info("All instances up, returning from AWS deploy routine")

        return [AmazonEC2.config_from_instance(i) for i in instances]

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

                        if (instance.public_dns_name and
                            instance.state == 'running'):
                            self.machines[sf_zone].append(instance)
                            machine_list.append(
                                AmazonEC2.config_from_instance(instance))

        return machine_list

    def _initialize_connections(self):
        for region in self.regions:
            if not region.name in self.connections:
                logger.info("Connecting to %s" % region.name)
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
                self.connection_by_zone[zone.name] = conn
                self.zones.append(zone.name)

        return ["aws-%s" % s for s in self.zones]
