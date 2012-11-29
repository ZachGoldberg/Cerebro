"""
Determine external address using a few different services.
"""

import re
import socket
import time
import unittest
import urllib2


__all__ = ['ExternalAddress', 'SimpleAddressLookup']


class ExternalAddress(object):
    """
    Class used to look up public internet address. This is also the base class
    for specific implementations.
    """

    implementations = []

    def __init__(self, reverse_lookup, timeout):
        """
        Initialize the object.

        @param reverse_lookup Whether or not to resolve IP addresses to
            hostnames.
        @param timeout The timeout on API HTTP requests.
        """
        self.timeout = timeout
        self.reverse_lookup = reverse_lookup

    @classmethod
    def add_implementation(cls, implementation):
        """
        Add an implementation to the lookup.

        @param implementation A class implementing the lookup routine.
        """
        cls.implementations.append(implementation)

    @classmethod
    def lookup(cls, reverse_lookup=False, timeout=3):
        """
        Perform the external address lookup.

        @param reverse_lookup Whether or not to resolve IP addresses to
            hostnames.
        @param timeout The timeout on API HTTP requests.
        @return The external address.
        """
        address = None
        for impl in cls.implementations:
            obj = impl(reverse_lookup, timeout)
            address = obj.lookup_external_address()
            if address is not None:
                break
        return address

    @classmethod
    def benchmark(cls, iterations=10):
        """
        Benchmark the available implementations and reoder them according to
        speed. The Hostname implementation will always be last.

        @param iteration The number of times to execute each implementation
            during the benchmark.
        """
        has_hostname = False
        impls = []
        for impl in cls.implementations:
            if impl == Hostname:
                has_hostname = True
            else:
                now = time.time()
                obj = impl(False, 5)
                for i in range(iterations):
                    obj.lookup_external_address()
                impls.append((time.time() - now, impl))

        def cmp(a, b):
            return 1 if (a[0] - b[0]) > 0 else -1
        cls.implementations = [i[1] for i in sorted(impls, cmp)]
        if has_hostname:
            cls.implementations.append(Hostname)

    def read_uri(self, uri):
        """
        Get the contents of the page at the given URI. Return None on failure
        or timeout.

        @param uri The URI to retrieve the contents of.
        @return A string containing the contents of the requested page or None
            on failure or timeout.
        """
        try:
            return urllib2.urlopen(uri, timeout=self.timeout).read()
        except:
            return None

    def lookup_reverse_dns(self, ipaddr):
        """
        Do a reverse DNS lookup on an IP address. This does not obey the
        timeout value. If the reverse lookup fails then the value of ipaddr
        will be returned.

        @param ipaddr An IP address to do a reverse lookup on.
        @return The hostname of the IP or the original IP if that fails.
        """
        try:
            address = socket.gethostbyaddr(ipaddr)
            ipaddr = address[0]
        except:
            pass
        return ipaddr


class Hostname(ExternalAddress):
    """
    Return the hostname of the machine in the hopes that it's correct. This is
    used as a fallback method.
    """

    def lookup_external_address(self):
        return socket.getfqdn()


class SimpleAddressLookup(ExternalAddress):
    """
    Class implementing simple HTTP lookup with or without a regular expression.
    Extend this class and override the uri and regex attributes for a custom
    lookup implementation.

    Attributes:
        uri -- The URI of a page containing an the external IP.
        regex -- If not none should be a regex where group 1 contains the
            matched IP.
    """

    uri = None
    regex = None

    def lookup_external_address(self):
        address = self.read_uri(self.uri)
        if address is None:
            return None
        if self.regex is None:
            address = address.strip()
        else:
            m = re.search(self.regex, address, re.IGNORECASE)
            if m is None:
                return None
            address = m.group(1).strip()
        if self.reverse_lookup:
            address = self.lookup_reverse_dns(address)
        return address


class DynDNS(SimpleAddressLookup):
    """
    Do an address lookup using checkip.dyndns.org.
    """
    uri = 'http://checkip.dyndns.org/'
    regex = r'Address:([^<]*)<'


class ICanHazIP(SimpleAddressLookup):
    """
    Do an address lookup using icanhazip.com.
    """
    uri = 'http://icanhazip.com/'


class IfconfigDotMe(SimpleAddressLookup):
    """
    Do an address lookup using ifconfig.me.
    """
    uri = 'http://ifconfig.me/ip'


ExternalAddress.add_implementation(ICanHazIP)
ExternalAddress.add_implementation(DynDNS)
ExternalAddress.add_implementation(IfconfigDotMe)
ExternalAddress.add_implementation(Hostname)


class TestExternalAddress(unittest.TestCase):
    """
    Run tests against external address functionality.
    """

    def test_implementations(self):
        """
        Test lookup implementations. They should all work within five seconds
        and return the same external IP.
        """
        address = None
        for impl in ExternalAddress.implementations:
            if impl == Hostname:
                continue
            obj = impl(False, 5)
            if address is None:
                address = obj.lookup_external_address()
            else:
                lookup = obj.lookup_external_address()
                self.assertEqual(
                    address, lookup,
                    "invalid address returned by %s ('%s' != '%s')" %
                    (impl.__name__, address, lookup))

    def test_benchmark(self):
        """
        Verify that the implemenatations have been added in order of response
        time.
        """
        impls = list(ExternalAddress.implementations)
        ExternalAddress.benchmark()
        self.assertEqual(
            ExternalAddress.implementations, impls,
            "implemenations not in order of response time")

    def test_hostname(self):
        """
        Test the fallback Hostname implementation.
        """
        fqdn = socket.getfqdn()
        address = Hostname(False, 5).lookup_external_address()
        self.assertEqual(
            fqdn, address,
            "fallback Hostname implementation invalid ('%s' != '%s')" %
            (fqdn, address))

    def test_reverse_lookup(self):
        """
        Test reverse DNS lookup.
        """
        ipaddr = '8.8.8.8'
        expect = 'google-public-dns-a.google.com'
        address = ExternalAddress(False, 5).lookup_reverse_dns(ipaddr)
        self.assertEqual(
            expect, address,
            "invalid reverse lookup for ip %s ('%s' != '%s')" %
            (ipaddr, expect, address))


if __name__ == '__main__':
    ExternalAddress.lookup()
