import re
import requests
import urllib


class MachineData(object):
    def __init__(self, url):
        self.url = url
        self.tasks = {}
        self.metadata = {}

    def reload(self):
        response = requests.get("%s/stats?nohtml=1" % self.url)
        data = response.content.split('\n')
        task_data = {}
        self.tasks = {}

        for item in data:
            try:
                key, value = item.split('=', 1)
            except:
                continue
            try:
                task_id, metric = key.split('-')
            except:
                self.metadata[key] = value
                continue

            if not task_id in task_data:
                task_data[task_id] = {}
            task_data[task_id][metric] = value

        for task_id in task_data.keys():
            task_dict = task_data[task_id]
            task_dict['id'] = task_id
            self.tasks[task_dict['name']] = task_dict

        return self.tasks

    def load_generic_page(self, host, page):
        response = requests.get("%s/%s?nohtml=1" % (host, page))
        lines = response.content.split('\n')
        data = {}
        for item in lines:
            try:
                key, value = item.split('=', 1)
            except:
                continue

            data[key] = value

        return data

    def start_task(self, task):
        tid = urllib.quote(task['id'])
        url = "%s/start_task?task_id=%s" % (self.url, tid)
        requests.get(url)

    def stop_task(self, task):
        tid = urllib.quote(task['id'])
        url = "%s/stop_task?task_id=%s" % (self.url, tid)
        print requests.get(url).content

    def strip_html(self, val):
        return re.sub('<[^<]+?>', '', val)

    def get_sitter_logs(self):
        return self.load_generic_page(self.url, "logs")

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
    d = MachineData("http://localhost:40000")
    d.reload()
    print d.stop_task(d.tasks['REX (Remote Extractor)'])
