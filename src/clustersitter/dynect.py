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

    def find_domainname(self, hostName, default):
        # NOTE!  This won't work for domains like .co.uk
        if not hostName:
            return default

        domain_parts = hostName.split('.')
        return ".".join(domain_parts[-2:])

    def add_record(self, data, hostName, type="A", TTL=300, domainName=None):
        domainName = self.find_domainname(hostName, domainName)

        try:
            ret = self.client.addRecord(data=data,
                                        type=type,
                                        hostName=hostName,
                                        TTL=TTL,
                                        domainName=domainName)
        except:
            import traceback
            logger.error(traceback.format_exc())
            logger.error(self.client.get_errors())
            return False

        if not ret:
            logger.warn("Couldn't add %s to %s: %s" % (
                    data,
                    hostName,
                    self.client.get_errors()))

        return ret

    def remove_record(self, data, hostName, type="A", domainName=None):
        domainName = self.find_domainname(hostName, domainName)

        try:
            ret = self.client.deleteRecord(data=data,
                                           type=type,
                                           hostName=hostName,
                                           domainName=domainName)
        except:
            import traceback
            logger.error(traceback.format_exc())
            logger.error(self.client.get_errors())
            return False

        if not ret:
            logger.warn("Couldn't delete %s from %s: %s" % (
                    data,
                    hostName,
                    self.client.get_errors()))

        return ret

    def get_records(self, hostName=None, type="A", domainName=None):
        domainName = self.find_domainname(hostName, domainName)

        logger.debug("Get Records for %s" % hostName)
        try:
            records = self.client.getRecords(hostName=hostName,
                                             type=type,
                                             domainName=domainName)
        except:
            import traceback
            logger.error(traceback.format_exc())
            logger.error(self.client.get_errors())
            return []

        if not records:
            return []

        if not hostName:
            return records

        def get_record(record):
            if r.get('record', '') == None:
                return ''

            return r.get('record', '')

        records = [r for r in records if hostName in get_record(r['record'])]

        if type == "*":
            return records

        return [r for r in records if r['type'] == type]
