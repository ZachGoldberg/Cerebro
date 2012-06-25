import logging
import re
import requests
import simplejson
import socket
import time
import urllib

logger = logging.getLogger(__name__)


class MachineData(object):
    def __init__(self, hostname, starting_port):
        self.hostname = hostname
        self.portnum = None
        self.starting_port = starting_port
        self.url = ""
        self._find_portnum()
        self.tasks = {}
        self.metadata = {}
        logger.info("New Machinedata: %s:%s" % (self.hostname,
                                                self.portnum))

    def _find_portnum(self):
        found = False
        port = self.starting_port
        while not found:
            try:
                logger.info("Attempting to connect to %s" % (
                        "%s:%s" % (self.hostname, port)))

                # Verify we can actually make an http request
                req = requests.get("http://%s:%s" % (self.hostname,
                                                     port),
                                   timeout=2)
                if not req.content:
                    raise Exception("No content recieved!")

                found = True
                self.portnum = port
                logger.info("Successfully connected to %s:%s" % (
                        self.hostname, self.portnum))
            except:
                import traceback
                logger.warn("Failed to connect to %s:%s" % (
                        self.hostname, port))
                logger.warn(traceback.format_exc().splitlines()[-1])
                port += 1
                if port > self.starting_port + 15:
                    self.url = ""
                    self.portnum = None
                    return

        self.url = "http://%s:%s" % (self.hostname,
                                     self.portnum)
        return self.url

    def _make_request(self, function, path, host=None):
        val = None
        hostname = host
        if not hostname:
            hostname = self.url

        try:
            val = function("%s/%s" % (hostname, path),
                           timeout=5)
        except:
            self._find_portnum()
            hostname = host
            if not hostname:
                hostname = self.url
            try:
                val = function("%s/%s" % (hostname, path),
                               timeout=5)
            except:
                logger.warn("Couldn't execute %s/%s!" % (
                        hostname, path))
                import traceback
                logger.error(traceback.format_exc())
                return None

        return val

    def load_generic_page(self, host, page):
        response = self._make_request(requests.get,
                                      path="%s?nohtml=1&format=json" % page,
                                      host=host)
        if not response:
            return {}

        data = simplejson.loads(response.content)
        return data

    def reload(self):
        response = self._make_request(requests.get,
                                      path="stats?nohtml=1&format=json")
        if not response:
            return None

        data = simplejson.loads(response.content)

        task_data = {}
        new_tasks = {}
        for key, value in data.iteritems():
            try:
                task_name, metric = key.split('-')
            except:
                self.metadata[key] = value
                continue

            if not task_name in task_data:
                task_data[task_name] = {}
            task_data[task_name][metric] = value

        for task_name in task_data.keys():
            task_dict = task_data[task_name]
            new_tasks[task_dict['name']] = task_dict
            if task_dict['running']:
                updated = False
                tries = 0
                while not updated and tries < 10:
                    tries += 1
                    try:
                        new_tasks = self.update_task_data(new_tasks,
                                                         task_dict['name'])
                        updated = True
                    except:
                        # Http server might not be up yet
                        time.sleep(0.01)
                        logger.warn("Couldn't update task")

        self.tasks = new_tasks
        return self.tasks

    def update_task_data(self, tasks, task_name):
        stats_page = self.strip_html(tasks[task_name]['monitoring'])
        tasks[task_name].update(
            self.load_generic_page(
                stats_page,
                'stats'))
        tasks[task_name]['stats_page'] = "%s/stats" % stats_page
        tasks[task_name]['logs_page'] = "%s/logs" % stats_page
        return tasks

    def add_task(self, config):
        params = '&'.join(
            "%s=%s" % (
                k, urllib.quote_plus(str(v))) for k, v in config.items())
        val = self._make_request(requests.get,
                                 path="add_task?%s" % params)
        if val:
            return val.content
        else:
            return None

    def start_task(self, task):
        if isinstance(task, str):
            task = self.tasks[task]

        tid = urllib.quote(task['name'])
        val = self._make_request(
            requests.get,
            path="start_task?task_name=%s" % tid)

        if val:
            return val.content

        return None

    def stop_task(self, task):
        if isinstance(task, str):
            task = self.tasks[task]

        tid = urllib.quote(task['name'])
        val = self._make_request(
            requests.get,
            path="stop_task?task_name=%s" % tid)

        if val:
            return val.content

        return None

    def strip_html(self, val):
        return re.sub('<[^<]+?>', '', val)

    def get_sitter_logs(self):
        return self.load_generic_page(self.url, "logs")

    def get_task_logs(self, task):
        sitter_logs = self.get_sitter_logs()
        logs = {}
        for logname, logfile in sitter_logs.items():
            if task['name'] in logname:
                logs[logname] = logfile
        try:
            url = self.strip_html(task['monitoring'])
            logs.update(self.load_generic_page(url, 'logs'))
        except:
            pass

        return logs

    def get_logfile(self, task, stderr=False):
        url = self.strip_html(task['monitoring'])
        data = self.load_generic_page(url,
                                      'stats')
        logs = self.load_generic_page(url,
                                      'logs')

        tasknum = str(int(data['num_task_starts']) - 1)
        handle = "stdout.%s" % tasknum
        if stderr:
            handle = "stderr.%s" % tasknum

        return logs[handle]

if __name__ == '__main__':
    import subprocess
    d = MachineData("localhost", 40000)
    d.reload()
    print d.tasks
    task = d.tasks['REX (Remote Extractor)']
    print d.get_task_logs(task)
