import curses
import os
import socket
import subprocess
import sys

from datetime import datetime
from menu import MenuFactory, MenuOption, MenuChanger, Table
from sittercommon.machinedata import MachineData

MACHINE_DATA = None
MENUFACTORY = None
SCR = None
YPOS = 0
CURRENT_LOC = "mainmenu"
AUX = None


def add_line(msg):
    global YPOS
    SCR.addstr(YPOS, 0, msg)
    YPOS += 1


def remove_line():
    global YPOS
    SCR.clrtoeol()
    y, x = SCR.getyx()
    SCR.move(y - 1, x)
    YPOS -= 1


def refresh():
    global YPOS
    SCR.clear()
    SCR.refresh()
    YPOS = 0


def header():
    add_line("#" * SCR.getmaxyx()[1])
    add_line(
        "MachineSitter at %s - Curses UI %s" % (
            MACHINE_DATA.url, datetime.now()))
    add_line("#" * SCR.getmaxyx()[1])


def change_menu(newmenu, aux=None):
    global CURRENT_LOC, AUX
    CURRENT_LOC = newmenu
    AUX = aux


def stop_sitter():
    os.kill(int(MACHINE_DATA.metadata['machinesitter_pid']), 15)
    sys.exit(0)


def mainmenu():
    menu = MENUFACTORY.new_menu("Main Menu")
    menu.add_option_vals("Refresh Window", action=dir, hotkey="*")
    menu.add_option_vals("Add a new task",
                    action=lambda: change_menu('addtask'))

    menu.add_option_vals("Show task definitions",
                    action=lambda: change_menu('show_task_definitions'))

    menu.add_option_vals("Show machine sitter logs",
                    action=lambda: change_menu('show_machinesitter_logs'))

    menu.add_option_vals("Stop machine sitter",
                         action=stop_sitter)

    menu.render()


def show_machinesitter_logs():
    logs = MACHINE_DATA.get_sitter_logs()
    show_logs(logs, "Machine Sitter Logs")


def show_task_logs():
    task = AUX
    logs = MACHINE_DATA.get_task_logs(task)
    show_logs(logs, "%s Logs" % task['name'])


def show_logs(logs, title):
    menu = MENUFACTORY.new_menu(title)
    menu.add_option_vals("Main Menu",
                    action=lambda: change_menu('mainmenu'), hotkey="*")

    lognames = logs.keys()
    lognames.sort()

    for logname in lognames:
        logfile = logs[logname]['location']
        menu.add_option_vals("%s (%s)" % (logname, logfile),
                         action=MenuChanger(tail_file, [logfile]))

    menu.render()


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


def show_log(task):
    tail_file([
            MACHINE_DATA.get_logfile(task)['location'],
            MACHINE_DATA.get_logfile(task, True)['location']])


def start_task(task):
    MACHINE_DATA.start_task(task)


def stop_task(task):
    MACHINE_DATA.stop_task(task)


def show_task():
    name, task = AUX
    reload_data()
    task = MACHINE_DATA.tasks[name]

    menu = MENUFACTORY.new_menu("%s (%s) (%s)" % (
            name,
            task['command'],
            MACHINE_DATA.strip_html(task.get('monitoring', 'Not Running'))
            ))

    menu.add_option_vals("Main Menu",
                    action=lambda: change_menu('mainmenu'), hotkey="*")

    if task['running'] == "False":
        menu.add_option_vals("Start Task",
                             action=lambda: start_task(task))
    else:
        menu.add_option_vals("Stop Task",
                             action=lambda: stop_task(task))

        menu.add_option_vals("Show stdout/stderr",
                         action=lambda: show_log(task))

    menu.add_option_vals("Show historic task log files",
                         action=lambda: change_menu('show_task_logs', task))

    menu.render()


def basic_tasks():
    global MENUFACTORY
    add_line("-" * SCR.getmaxyx()[1])

    reload_data()

    running = []
    not_running = []
    not_running_lines = []
    MENUFACTORY = MenuFactory(SCR, add_line, remove_line)

    for name, task in MACHINE_DATA.tasks.items():
        if task['running']:
            running.append((name, task))
        else:
            not_running.append((name, task))

    hotkey = 1

    table = Table(SCR.getmaxyx()[1],
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
                stdout = MACHINE_DATA.get_logfile(task)['location']
                stderr = MACHINE_DATA.get_logfile(task, True)['location']

                stdout_kb = os.stat(stdout).st_size / 1024
                stderr_kb = os.stat(stderr).st_size / 1024
            except:
                import traceback
                traceback.print_exc()
                pass

        table.add_row([task['name'],
                       task.get('num_task_starts', '?'),
                       runtime,
                       "%.0f kB" % stdout_kb,
                       "%.0f kB" % stderr_kb])

        option = MenuOption(
                task['name'],
                action=MenuChanger(change_menu, "show_task",
                                   (name, task)),
                hotkey=str(hotkey),
                hidden=True)

        MENUFACTORY.add_default_option(option)
        hotkey += 1

    for name, task in not_running:
        line = task['name']
        not_running_lines.append(line)
        option = MenuOption(
                task['name'],
                action=MenuChanger(change_menu, "show_task",
                                   (name, task)),
                hotkey=str(hotkey),
                hidden=True)

        MENUFACTORY.add_default_option(option)
        hotkey += 1

    show_table = str(table).split('\n')
    add_line("   %s" % show_table[0])

    for num, l in enumerate(show_table[1:]):
        add_line("%s. %s" % ((num + 1), l))

    add_line("Stopped Tasks:")
    for num, l in enumerate(not_running_lines):
        add_line("%s. %s" % ((len(running) + num + 1), l))

    add_line("-" * SCR.getmaxyx()[1])


def reload_data():
    global MACHINE_DATA
    MACHINE_DATA.reload()


def run():
    global SCR, MENUFACTORY
    SCR = curses.initscr()

    refresh()
    reload_data()

    while True:
        refresh()
        header()
        basic_tasks()
        globals()[CURRENT_LOC]()


def main():
    global MACHINE_DATA
    if not MACHINE_DATA:
        MACHINE_DATA = MachineData("localhost", 40000)

    try:

        if not MACHINE_DATA.url:
            print "Couldn't find a running machine sitter!"
            return

        run()
    except:
        curses.endwin()
        import traceback
        traceback.print_exc()

if __name__ == '__main__':
    main()
