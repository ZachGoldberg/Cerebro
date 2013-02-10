import curses
import os
import subprocess
import sys

from datetime import datetime
from menu import MenuFactory, MenuOption, MenuChanger, Table
from sittercommon.machinedata import MachineData
from sittercommon.utils import strip_html

MACHINE_DATA = None
SCREEN = None


class ManagementScreen(object):

    def __init__(self):
        self.factory = None
        self.scr = None
        self.ypos = 0
        self.current_loc = "mainmenu"
        self.aux = None

    def add_line(self, msg):
        self.scr.addstr(self.ypos, 0, msg)
        self.ypos += 1

    def remove_line(self):
        self.scr.clrtoeol()
        y, x = self.scr.getyx()
        self.scr.move(y - 1, x)
        self.ypos -= 1

    def refresh(self):
        self.scr.clear()
        self.scr.refresh()
        self.ypos = 0

    def change_menu(self, newmenu, aux=None):
        self.current_loc = newmenu
        if aux:
            self.aux = aux

    def header(self):
        pass

    def basic_tasks(self):
        pass

    def mainmenu(self):
        pass

    def reload_data(self):
        pass

    def run(self):
        self.scr = curses.initscr()
        self.refresh()

        while True:
            self.refresh()
            self.header()
            self.factory = MenuFactory(self.scr,
                                       self.add_line,
                                       self.remove_line)

            self.add_line("-" * self.scr.getmaxyx()[1])
            self.basic_tasks()
            self.add_line("-" * self.scr.getmaxyx()[1])
            if hasattr(self, self.current_loc):
                getattr(self, self.current_loc)()
            else:
                globals()[self.current_loc]()


class MachineManagementScreen(ManagementScreen):
    def __init__(self, machine_data, *args, **kwargs):
        super(MachineManagementScreen, self).__init__(*args, **kwargs)
        self.machine_data = machine_data

    def header(self):
        self.add_line("#" * self.scr.getmaxyx()[1])
        self.add_line(
            "MachineSitter at %s - Curses UI %s" % (
                self.machine_data.url, datetime.now()))
        self.add_line("#" * self.scr.getmaxyx()[1])

    def reload_data(self):
        self.machine_data.reload()

    def mainmenu(self):
        menu = self.factory.new_menu("Main Menu")
        menu.add_option_vals("Main Menu", hotkey="*",
                             action=lambda: self.change_menu('mainmenu'))
        menu.add_option_vals("Add a new task",
                             action=lambda: self.change_menu('addtask'))

        menu.add_option_vals(
            "Show task definitions",
            action=lambda: self.change_menu(
                'show_task_definitions'))

        menu.add_option_vals(
            "Show machine sitter logs",
            action=lambda: self.change_menu(
                'show_machinesitter_logs'))

        menu.add_option_vals("Stop machine sitter",
                             action=stop_sitter)

        menu.render()

    def basic_tasks(self):
        self.reload_data()
        running = []
        not_running = []
        not_running_lines = []
        for name, task in self.machine_data.tasks.items():
            if task['running']:
                running.append((name, task))
            else:
                not_running.append((name, task))

        hotkey = 1

        table = Table(self.scr.getmaxyx()[1],
                      ["Running Task Name", "Restarts",
                       "Runtime", "Stdout", "Stderr"])

        for name, task in running:
            runtime = '?'
            stdout_kb = -1.0
            stderr_kb = -1.0
            if task.get('process_start_time'):
                runtime = str(
                    datetime.now() - datetime.strptime(
                        task['process_start_time'],
                        '%Y-%m-%d %H:%M:%S.%f')
                )

                # strip useconds
                runtime = runtime[:runtime.find('.')]
                try:
                    stdout = self.machine_data.get_logfile(task)['location']
                    stderr = self.machine_data.get_logfile(
                        task, True)['location']

                    stdout_kb = os.stat(stdout).st_size / 1024
                    stderr_kb = os.stat(stderr).st_size / 1024
                except:
                   pass

            table.add_row([task['name'],
                           task.get('num_task_starts', '?'),
                           runtime,
                           "%.0f kB" % stdout_kb,
                           "%.0f kB" % stderr_kb])

            option = MenuOption(
                task['name'],
                action=MenuChanger(self.change_menu, "show_task",
                                   (name, task)),
                hotkey=str(hotkey),
                hidden=True)

            self.factory.add_default_option(option)
            hotkey += 1

        for name, task in not_running:
            line = task['name']
            not_running_lines.append(line)
            option = MenuOption(
                task['name'],
                action=MenuChanger(self.change_menu, "show_task",
                                   (name, task)),
                hotkey=str(hotkey),
                hidden=True)

            self.factory.add_default_option(option)
            hotkey += 1

        table.render(self)

        self.add_line("Stopped Tasks:")
        for num, l in enumerate(not_running_lines):
            self.add_line("%s. %s" % ((len(running) + num + 1), l))

    def show_task(self):
        name, task = self.aux
        self.reload_data()
        task = self.machine_data.tasks[name]

        menu = self.factory.new_menu(
            "%s (%s) (%s)" % (
                name,
                task['command'],
                strip_html(task.get('monitoring', 'Not Running'))
            ))

        menu.add_option_vals(
            "Main Menu",
            action=lambda: self.change_menu(
                'mainmenu'), hotkey="*")

        if not task['running']:
            menu.add_option_vals("Start Task",
                                 action=lambda: self.start_task(task))
            menu.add_option_vals("Remove Task",
                                 action=lambda: self.remove_task(task))
        else:
            menu.add_option_vals("Stop Task",
                                 action=lambda: self.stop_task(task))
            menu.add_option_vals("Restart Task",
                                 action=lambda: self.restart_task(task))

            menu.add_option_vals("Show stdout/stderr",
                                 action=lambda: self.show_log(task))

        menu.add_option_vals(
            "Show historic task log files",
            action=lambda: SCREEN.change_menu('show_task_logs', task))

        menu.render()

    def show_log(self, task):
        tail_file([
            self.machine_data.get_logfile(task)['location'],
            self.machine_data.get_logfile(task, True)['location']])

    def show_machinesitter_logs(self):
        logs = self.machine_data.get_sitter_logs()
        self.show_logs(logs, "Machine Sitter Logs")

    def show_task_logs(self):
        task = self.aux
        logs = self.machine_data.get_task_logs(task)
        self.show_logs(logs, "%s Logs" % task['name'])

    def show_logs(self, logs, title):
        menu = self.factory.new_menu(title)
        menu.add_option_vals(
            "Main Menu",
            action=lambda: self.change_menu('mainmenu'),
            hotkey="*")

        lognames = logs.keys()
        lognames.sort()

        for logname in lognames:
            logfile = logs[logname]['location']
            menu.add_option_vals("%s (%s)" % (logname, logfile),
                                 action=MenuChanger(tail_file, [logfile]))

        menu.render()

    def start_task(self, task):
        self.machine_data.start_task(task)

    def stop_task(self, task):
        self.machine_data.stop_task(task)

    def restart_task(self, task):
        self.machine_data.restart_task(task)

    def remove_task(self, task):
        self.machine_data.remove_task(task)
        self.current_loc = "mainmenu"


def stop_sitter():
    os.kill(int(MACHINE_DATA.metadata['machinesitter_pid']), 15)
    sys.exit(0)


def tail_file(filenames):
    curses.endwin()
    os.system("clear")
    subprocess.call(["echo", ' '.join(filenames)])
    try:
        args = ["tail", "-n", "100", "-f"]
        args.extend(filenames)
        subprocess.call(args)
    except:
        pass


def main():
    global MACHINE_DATA, SCREEN
    if not MACHINE_DATA:
        MACHINE_DATA = MachineData("localhost", 40000)

    SCREEN = MachineManagementScreen(MACHINE_DATA)

    try:

        if not MACHINE_DATA.url:
            print "Couldn't find a running machine sitter!"
            return

        SCREEN.run()
    except:
        curses.endwin()
        import traceback
        traceback.print_exc()

if __name__ == '__main__':
    main()
