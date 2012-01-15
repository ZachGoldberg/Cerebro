import boto
import logging
from boto import ec2

from machineprovider import MachineProvider


class AmazonEC2(MachineProvider):
    def __init__(self):
        self.zones = []
        self.regions = []

        try:
            logging.info("Download EC2 Region List")
            self.regions = ec2.regions()
        except boto.exception.NoAuthHandlerFound:
            logging.warn("Couldn't connect to EC2.  Auth Error.  " +
                         "Ensure AWS crednetials are set in the env " +
                         "(AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY")

    def usable(self):
        return bool(self.regions)

    def get_all_shared_fate_zones(self):
        if self.zones:
            return self.zones

        self.zones = [u'eu-west-1a', u'eu-west-1b', u'eu-west-1c', u'sa-east-1a', u'sa-east-1b', u'us-east-1a', u'us-east-1b', u'us-east-1c', u'us-east-1d', u'ap-northeast-1a', u'ap-northeast-1b', u'us-west-2a', u'us-west-2b', u'us-west-1b', u'us-west-1c', u'ap-southeast-1a', u'ap-southeast-1b']

        return self.zones

        for region in self.regions:
            logging.info("Connecting to %s" % region.name)
            conn = ec2.connect_to_region(region.name)
            zones = conn.get_all_zones()
            for zone in zones:
                self.zones.append(zone.name)

        return self.zones
