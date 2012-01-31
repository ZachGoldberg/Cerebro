import logging
from dynect_client import DynectDNSClient

from dnsprovider import DNSProvider

logger = logging.getLogger(__name__)


class Dynect(DNSProvider):
    def __init__(self, options):
        self.customername = options['customername']
        self.username = options['username']
        self.password = options['password']
        self.default_domain = options['default_domain']

        self.client = DynectDNSClient(self.customername,
                                      self.username,
                                      self.password,
                                      self.default_domain)

    def add_record(self, data, hostName, type="A", TTL=3600, domainName=None):
        return self.client.addRecord(data=data,
                                     type=type,
                                     hostName=hostName,
                                     TTL=TTL,
                                     domainName=domainName)

    def remove_record(self, data, hostName, type="A", domainName=None):
        return self.client.deleteRecord(data=data,
                                        type=type,
                                        hostName=hostName,
                                        domainName=domainName)

    def get_records(self, hostName=None, type="A", domainName=None):
        logger.debug("Get Records for %s" % hostName)
        records = self.client.getRecords(hostName=hostName,
                                         type=type,
                                         domainName=domainName)

        if not hostName:
            return records

        records = [r for r in records if hostName in r['record']]

        if type == "*":
            return records

        return [r for r in records if r['type'] == type]
