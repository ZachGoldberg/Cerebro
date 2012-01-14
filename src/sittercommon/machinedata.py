import logging
import re
import requests
import socket
import time
import urllib


class MachineData(object):
    def __init__(self, hostname, starting_port):
        self.hostname = hostname
        self.portnum = None
        self.starting_port = starting_port
        self.url = ""
        self._find_portnum()
        self.tasks = {}
        self.metadata = {}

    def _find_portnum(self):
        found = False
        port = self.starting_port
        while not found:
            try:
                logging.info("Attempting to connect to %s" % (
                        "%s:%s" % (self.hostname, port)))
                sock = socket.socket(socket.AF_INET)
                sock.connect((self.hostname, port))
                sock.close()
                found = True
                self.portnum = port
                logging.info("Successfully connected")
            except:
                port += 1
                if port > self.starting_port + 1000:
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
            val = function("%s/%s" % (hostname, path))
        except:
            self._find_portnum()
            hostname = host
            if not hostname:
                hostname = self.url
            try:
                val = function("%s/%s" % (hostname, path))
            except:
                logging.warn("Couldn't execute %s/%s!" % (
                        hostname, path))
                return None

        return val

    def load_generic_page(self, host, page):
        response = self._make_request(requests.get,
                                      path="%s?nohtml=1" % page,
                                      host=host)
        if not response:
            return {}

        lines = response.content.split('\n')
        data = {}
        for item in lines:
            try:
                key, value = item.split('=', 1)
            except:
                continue

            data[key] = value

        return data

    def reload(self):
        response = self._make_request(requests.get,
                                      path="stats?nohtml=1")
        if not response:
            return

        data = response.content.split('\n')

        task_data = {}
        self.tasks = {}

        for item in data:
            try:
                key, value = item.split('=', 1)
            except:
                continue
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
            self.tasks[task_dict['name']] = task_dict
            if task_dict['running'] == "True":
                updated = False
                tries = 0
                while not updated and tries < 10:
                    tries += 1
                    try:
                        self.update_task_data(task_dict['name'])
                        updated = True
                    except:
                        # Http server might not be up yet
                        time.sleep(0.01)

        return self.tasks

    def update_task_data(self, task_name):
        self.tasks[task_name].update(
            self.load_generic_page(
                self.strip_html(self.tasks[task_name]['monitoring']),
                'stats'))

    def start_task(self, task):
        tid = urllib.quote(task['name'])
        self._make_request(requests.get,
                           path="start_task?task_name=%s" % tid)

    def stop_task(self, task):
        tid = urllib.quote(task['name'])
        url = "%s/stop_task?task_name=%s" % (self.url, tid)
        print self._make_request(
            requests.get,
            path="stop_task?task_name=%s" % tid).content

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
