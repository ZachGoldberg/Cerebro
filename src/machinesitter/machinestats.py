from tasksitter.stats_collector import StatsCollector


class MachineStats(StatsCollector):

    def get_live_data(self):
        data = {}
        for task_id, task in self.harness.tasks.items():
            running = bool(task.is_running())
            data["%s-running" % task.id] = running
            if not running:
                data["%s-start" % task.id] = \
                    "<a href='http://%s:%s/start_task?task_id=%s'>start</a>" % (
                    self.hostname,
                    self.harness.http_monitor.port,
                    task.id)
            else:
                data["%s-stop" % task.id] = \
                    "<a href='http://%s:%s/stop_task?task_id=%s'>stop</a>" % (
                    self.hostname,
                    self.harness.http_monitor.port,
                    task.id)
                location = "http://%s:%s" % (
                    self.hostname,
                    task.http_monitoring_port)
                data["%s-monitoring" % task.id] = "<a href='%s'>%s</a>" % (location,
                                                                           location)

        return data

    def get_metadata(self):
        data = {}
        data['log_location'] = self.harness.log_location
        data['starting_port'] = self.harness.starting_port
        data['task_definition_file'] = self.harness.task_definition_file
        for task_id, task in self.harness.tasks.items():
            data["%s-name" % task.id] = task.name
            data["%s-command" % task.id] = task.command

        return data
