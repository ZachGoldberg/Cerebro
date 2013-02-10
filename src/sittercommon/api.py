import requests
import simplejson
import sys

from clustersitter.productionjob import ProductionJob


class ClusterState(object):
    def __init__(self, url):
        self.url = url
        self.machines = []
        self.jobs = {}
        self.zones = {}
        self.reload()

    def reload(self):
        url = "%s/overview?nohtml=1&format=json" % self.url
        response = requests.get(url)
        if response.status_code != 200:
            print response
            sys.exit(1)

        data = simplejson.loads(response.content)
        self.jobs = [ProductionJob.deserialize(j) for j in data['jobs']]
        self.machines = data['machines']

        print self.machines[0].keys()
