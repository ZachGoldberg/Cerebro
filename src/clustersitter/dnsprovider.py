class DNSProvider(object):
    def add_record(self, data, hostName, type="A", TTL=3600, domainName=None):
        pass

    def remove_record(self, data, hostName, type="A", domainName=None):
        pass

    def get_records(self, hostName, type="A", domainName=None):
        pass
