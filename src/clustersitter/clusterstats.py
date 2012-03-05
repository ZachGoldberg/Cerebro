import os
import re
import threading

from tenjin.helpers import *

from eventmanager import ClusterEventManager
from tasksitter.stats_collector import StatsCollector


def strip_html(text):
    return re.sub('<[^<]+?>', '', text)


class ClusterStats(StatsCollector):

    def overview(self, args):
        engine = args['engine']
        data = self.get_metadata()
        data.update(self.get_live_data())
        if "nohtml" not in args:
            return engine.render('cluster_overview.html', {'data': data,
                                                           'pagewidth': 1300})
        else:
            return data

    def get_live_data(self):
        data = {}

        data['events'] = ClusterEventManager.get_events()

        state = self.harness.state

        data['providers'] = state.providers.keys()
        data['machines_by_zone'] = str(state.machines_by_zone)
        data['job_fill'] = str(state.job_fill)
        data['idle_machines'] = str(state.idle_machines)
        data['unreachable_machines'] = [
            str(m) for m in state.unreachable_machines]

        monitors = []
        machines = []

        def serialize_machine(machine):
            machine_data = {}
            machine_data['repr'] = repr(machine)
            machine_data['zone'] = machine.config.shared_fate_zone
            machine_data['bits'] = machine.config.bits
            machine_data['cpus'] = machine.config.cpus
            machine_data['mem'] = machine.config.mem
            machine_data['url'] = machine.datamanager.url
            machine_data['disk'] = machine.config.disk
            machine_data['dns_name'] = machine.config.dns_name
            machine_data['hostname'] = machine.hostname
            machine_data['tasks'] = machine.get_tasks()
            for task in machine_data['tasks'].values():
                # task is running
                if 'monitoring' in task:
                    filenum = int(task.get('num_task_starts', 1))

                    # Counting starts at 0
                    filenum -= 1

                    files = ["stdout", "stderr"]
                    for filename in files:
                        link = strip_html(
                            "%s/logfile?logname=%s.%s&tail=100" % (
                                task['monitoring'],
                                filename,
                                filenum))

                        task[filename] = "<a href='%s'>%s</a>" % (
                            link, filename)

            machine_data['running_tasks'] = machine.get_running_tasks()
            machine_data['is_in_deployment'] = machine.is_in_deployment()
            machine_data['number'] = machine.machine_number
            machine_data['initialized'] = machine.is_initialized()
            machine_data['has_loaded_data'] = machine.has_loaded_data()
            machine_data['pull_failures'] = pull_failures.get(machine, 0)
            machine_data['idle'] = machine in state.idle_machines.get(
                machine_data['zone'], [])

            return machine_data

        for monitor, thread in state.monitors:
            monitor_data = {}
            monitor_data['monitored_machines'] = [
                repr(m) for m in monitor.monitored_machines]
            monitor_data['add_queue'] = [repr(m) for m in monitor.add_queue]
            monitor_data['pull_failures'] = dict([
                (str(k), v) for k, v in monitor.pull_failures.iteritems()])
            monitor_data['failure_threshold'] = monitor.failure_threshold
            monitor_data['number'] = monitor.number
            monitors.append(monitor_data)

            pull_failures = dict(monitor.pull_failures)

            for machine in monitor.monitored_machines:
                machine_data = serialize_machine(machine)
                machines.append(machine_data)

        data['machines'] = machines
        data['monitors'] = monitors

        jobs = []
        check_jobs = state.jobs + state.repair_jobs
        for job in check_jobs:
            job_data = {}
            job_data['name'] = job.name
            job_data['task_configuration'] = job.task_configuration
            job_data['deployment_layout'] = job.deployment_layout
            job_data['deployment_recipe'] = job.deployment_recipe
            job_data['recipe_options'] = job.recipe_options
            job_data['linked_job'] = job.linked_job
            fillers = []
            for filler_list in job.fillers.values():
                for filler in filler_list:
                    filler_data = {}
                    filler_data['zone'] = filler.zone
                    filler_data['num_cores'] = filler.num_cores
                    filler_data['machine_states'] = [
                        (m.hostname, str(m.state)) for m in filler.machines]
                    filler_data['state'] = str(filler.state)
                    fillers.append(filler_data)

            job_data['fillers'] = fillers
            job_data['fill'] = state.job_fill.get(job.name, {})

            fill_machines = state.job_fill_machines.get(job.name, {})
            for zone in fill_machines.keys():
                fill_machines[zone] = [str(m) for m in fill_machines[zone]]

            job_data['fill_machines'] = fill_machines

            job_data['spawning'] = job.currently_spawning
            jobs.append(job_data)

        data['jobs'] = jobs

        load = os.getloadavg()
        data['load_one_min'] = load[0]
        data['load_five_min'] = load[1]
        data['load_fifteen_min'] = load[2]

        py_threads = threading.enumerate()
        alive_thread_names = [t.getName() for t in py_threads]
        threads = {}

        std_threads = ['MachineDoctor', 'MainThread',
                       'Calculator', "HTTPServer"]
        for i in range(self.harness.worker_thread_count):
            std_threads.append("Monitoring-%s" % i)

        for name in std_threads:
            if name in alive_thread_names:
                threads[name] = True
                alive_thread_names.remove(name)
            else:
                threads[name] = False

        other_threads = {}
        for name in alive_thread_names:
            other_threads[name] = True

        data['std_threads'] = std_threads
        data['threads'] = threads
        data['other_threads'] = other_threads
        return data

    def get_metadata(self):
        data = {}
        data['clustersitter_pid'] = os.getpid()
        data['log_location'] = self.harness.log_location
        data['provider_config'] = self.harness.provider_config
        data['dns_provider_config'] = self.harness.dns_provider_config
        data['username'] = self.harness.user
        data['keys'] = self.harness.keys
        data['launch_time'] = str(self.harness.launch_time)
        data['launch_location'] = self.harness.launch_location
        data['start_state'] = self.harness.start_state
        data['logfiles'] = self.harness.logmanager.get_logfile_names()

        return data
