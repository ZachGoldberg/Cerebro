from dreampylib import DreampyLib

from dnsprovider import DNSProvider


class DreamhostDNS(DNSProvider):
    def __init__(self, options):
        self.username = options['username']
        self.api_key = options['api_key']
        self.connection = None
        self._connect()

    def _connect(self):
        if self.connection and self.connection.IsConnected():
            return True

        self.connection = DreampyLib(self.username,
                                                self.api_key)

        return self.connection.IsConnected()

    def valid_response(self, response):
        try:
            return response and not (
                response[0] == False or response[1] == 'error')
        except:
            print response
            return False

    def add_record(self, data, hostName, type="A", TTL=3600, domainName=None):
        self._connect()

        # Dreamhost doesn't allow us to use TTLs
        return self.connection.dns.add_record(value=data,
                                              type=type,
                                              record=hostName)

    def remove_record(self, data, hostName, type="A", domainName=None):
        self._connect()

        return self.connection.dns.remove_record(record=hostName,
                                                 value=data,
                                                 type=type)

    def get_records(self, hostName=None, type="A", domainName=None):
        self._connect()

        records = self.connection.dns.list_records()

        if not hostName:
            return records

        records = [r for r in records if hostName in r['record']]

        if type == "*":
            return records

        return [r for r in records if r['type'] == type]
