import curses
import logging
import requests
import os
import sys

from datetime import datetime
from clustersitter import MonitoredMachine, ProductionJob
from machineconsole.main import (
    MachineManagementScreen
)
from machineconsole.menu import MenuChanger, MenuOption, Table
from sittercommon import arg_parser
from sittercommon.api import ClusterState
from sittercommon.machinedata import MachineData
from sittercommon.utils import load_defaults, write_defaults, strip_html


class ClusterManagementScreen(MachineManagementScreen):
    def __init__(self, cluster_data, *args, **kwargs):
        super(MachineManagementScreen, self).__init__(*args, **kwargs)
        self.cluster_data = cluster_data
        self.machine_data = None

    def header(self):
        self.add_line("#" * self.scr.getmaxyx()[1])
        self.add_line(
            "ClusterSitter at %s - Curses UI %s" % (
                self.cluster_data.url, datetime.now()))
        self.add_line("#" * self.scr.getmaxyx()[1])

    def reload_data(self):
        if self.machine_data:
            self.machine_data.reload()
        else:
            self.cluster_data.reload()

    def basic_tasks(self):
        hotkey = 1
        table = None
        if self.current_loc == "mainmenu":
            self.machine_data = None
            self.reload_data()
            table = Table(self.scr.getmaxyx()[1],
                          ["Running Job Name", "Active Instances",
                           "Command"])
            for job in self.cluster_data.jobs:
                table.add_row(
                    [job.name,
                     sum([len(v) for v in job.fill_machines.values()]),
                     job.task_configuration['command']])

                option = MenuOption(
                    job.name,
                    action=MenuChanger(self.change_menu, "show_job",
                                       (job.name, job)),
                    hotkey=str(hotkey),
                    hidden=True)
                hotkey += 1
                self.factory.add_default_option(option)
        else:
            if len(self.aux) == 2 and isinstance(self.aux[1], ProductionJob):
                table = Table(self.scr.getmaxyx()[1],
                              ["Machine Name", "Machine IP",
                               "Machine Config"])
                job = self.aux[1]
                for machine in self.cluster_data.get_machines_for_job(job):
                    table.add_row(
                        [machine.hostname, machine.config.ip, machine.config])

                    option = MenuOption(
                        machine.hostname,
                        action=MenuChanger(self.change_menu, "show_machine",
                                           (machine.hostname, machine)),
                        hotkey=str(hotkey),
                        hidden=True)
                    hotkey += 1
                    self.factory.add_default_option(option)

            elif len(self.aux) == 2 and isinstance(
                    self.aux[1], MonitoredMachine):
                self.setup_machine()

        if self.machine_data:
            super(ClusterManagementScreen, self).basic_tasks()

        if table:
            table.render(self)

    def mainmenu(self):
        menu = self.factory.new_menu("Main Menu")
        menu.add_option_vals("Refresh Window", action=dir, hotkey="*")
        menu.add_option_vals("Add a new Job",
                             action=lambda: self.change_menu('addtask'))

        menu.render()

    def show_job(self):
        name, job = self.aux

        menu = self.factory.new_menu(
            "%s (%s)" % (
                name,
                job.task_configuration['command'],
            ))

        menu.add_option_vals(
            "Main Menu",
            action=lambda: self.change_menu(
                'mainmenu'), hotkey="*")

        menu.add_option_vals("Pause Job",
                             action=lambda: None)

        menu.add_option_vals("Add New Instance",
                             action=lambda: None)

        menu.add_option_vals("Remove an Instance",
                             action=lambda: None)

        menu.render()

    def setup_machine(self):
        hostname, machine = self.aux
        self.machine_data = MachineData(hostname, 40000)

    def show_machine(self):
        super(ClusterManagementScreen, self).mainmenu()

    def show_log(self, task):
        curses.endwin()
        os.system("clear")
        stdout = strip_html("%s/logfile?logname=%s.%s&tail=100" % (
            task['monitoring'],
            'stdout',
            task['num_task_starts'] - 1))
        print requests.get(stdout).content
        raw_input()


def main(sys_args=None):
    if not sys_args:
        sys_args = sys.argv[1:]

    default_options = load_defaults()
    parser = arg_parser.ArgumentParser(
        description="Cerebro Command Line Interaction Tool")

    parser.add_argument(
        "--clustersitterurl", dest="clustersitter_url",
        help="URL to the root of the clustersitter",
        default=default_options.get('clustersitter_url'))

    args = parser.parse_args(sys_args)
    default_options['clustersitter_url'] = args.clustersitter_url
    write_defaults(default_options)

    # Silence misc. logging.getLogger() 'can't find handler' warnings
    logging.basicConfig()
    logging.getLogger().setLevel(logging.CRITICAL)

    cluster_data = ClusterState(default_options['clustersitter_url'])

    screen = ClusterManagementScreen(cluster_data)

    try:
        if not cluster_data.url:
            print "Couldn't find a running cluster sitter!"
            return

        screen.run()
    except:
        curses.endwin()
        import traceback
        traceback.print_exc()

if __name__ == '__main__':
    main()
