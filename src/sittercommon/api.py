import os
import requests
import simplejson
import sys
import zlib

from clustersitter.monitoredmachine import MonitoredMachine
from clustersitter.productionjob import ProductionJob


class ProviderConfig(object):
    def __init__(self):
        self.providers = {}

    def serialize(self):
        return self.providers

    @classmethod
    def deserialize(cls, data):
        obj = cls()
        obj.providers = data
        return obj

    def get_key_for_zone(self, shared_fate_zone):
        provider, zone = shared_fate_zone.split('-', 1)
        return self.providers.get(provider, {}).get(zone, {}).get('key_name')


class ClusterState(object):
    def __init__(self, url):
        self.url = url
        self.machines = []
        self.jobs = {}
        self.zones = {}
        self.provider_config = {}
        self.login_user = None
        self.raw = None

    def reload(self):
        url = "%s/overview?nohtml=1&format=json&compress=1" % self.url
        # 3 attempts, since sometimes downloading json is a bit flaky
        data = None
        for _ in xrange(3):
            response = requests.get(url)
            if response.status_code != 200:
                continue

            try:
                raw_data = zlib.decompress(response.content)
                data = simplejson.loads(raw_data)
                break
            except:
                continue

        if not data:
            sys.stderr.write("Couldn't load data!")
            sys.exit(1)

        self.raw = data
        self.jobs = [ProductionJob.deserialize(j) for j in data['jobs']]
        self.machines = [MonitoredMachine.deserialize(
            m) for m in data['machines']]
        self.provider_config = ProviderConfig.deserialize(
            data['provider_config'])

        self.keys = self.load_keys(data['keys'])
        self.login_user = data.get("login_user", data.get("username"))

    def find_key(self, key):
        return self.keys.get(key)

    def load_keys(self, keys):
        data = {}
        for key in keys:
            name = os.path.basename(key).replace('.pem', '')
            data[name] = key

        return data

    def get_job_names(self):
        return [j.name.lower() for j in self.jobs]

    def get_machines_for_job(self, job):
        machines = []
        for machine_list in job.fill_machines.values():
            for machine in machine_list:
                host, ip = machine.split(':')
                machines.append(self.get_machine(host))

        return machines

    def get_machine(self, hostname):
        for machine in self.machines:
            if hostname == machine.hostname:
                return machine

    def get_job(self, name):
        name = name.lower()
        for job in self.jobs:
            if job.name.lower() == name:
                return job

        return None
