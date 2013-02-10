import curses
import sys

from datetime import datetime
from machineconsole.main import (
    show_machinesitter_logs, show_task_logs, show_logs,
    start_task, stop_task, restart_task,
    ManagementScreen
)
from sittercommon import arg_parser
from sittercommon.api import ClusterState
from sittercommon.machinedata import MachineData
from sittercommon.utils import load_defaults, write_defaults

CLUSTER_DATA = None
MACHINE_DATA = None


class ClusterManagementScreen(ManagementScreen):
    def header(self):
        self.add_line("#" * self.scr.getmaxyx()[1])
        self.add_line(
            "ClusterSitter at %s - Curses UI %s" % (
                CLUSTER_DATA.url, datetime.now()))
        self.add_line("#" * self.scr.getmaxyx()[1])

    def reload_data(self):
        global CLUSTER_DATA
        CLUSTER_DATA.reload()

    def basic_tasks(self):
        pass

    def mainmenu(self):
        menu = self.factory.new_menu("Main Menu")
        menu.add_option_vals("Refresh Window", action=dir, hotkey="*")
        menu.add_option_vals("Add a new task",
                             action=lambda: SCREEN.change_menu('addtask'))

        menu.add_option_vals(
            "Show task definitions",
            action=lambda: SCREEN.change_menu(
                'show_task_definitions'))

        menu.add_option_vals(
            "Show machine sitter logs",
            action=lambda: SCREEN.change_menu(
                'show_machinesitter_logs'))

        menu.render()


SCREEN = ClusterManagementScreen()


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

    global CLUSTER_DATA
    if not CLUSTER_DATA:
        CLUSTER_DATA = ClusterState(default_options['clustersitter_url'])

    try:
        if not CLUSTER_DATA.url:
            print "Couldn't find a running cluster sitter!"
            return

        SCREEN.run()
    except:
        curses.endwin()
        import traceback
        traceback.print_exc()

if __name__ == '__main__':
    main()
