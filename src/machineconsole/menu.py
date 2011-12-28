class Table(object):
    def __init__(self, max_width, headers):
        self.headers = headers
        self.rows = []
        self.column_widths = []
        self.max_width = max_width

    def add_row(self, items):
        if len(items) != len(self.headers):
            raise Exception("Invalid number of items in this row!")
        self.rows.append(items)

    def render_cell(self, columnnum, column):
        col_len = len(str(column))
        padding = int((self.column_widths[columnnum] - col_len))
        return "%s%s" % (column, " " * padding)

    def __str__(self):
        self.column_widths = [0] * len(self.headers)
        self.rows = [self.headers] + self.rows
        for column in range(len(self.headers)):
            for row in self.rows:
                if len(str(row[column])) > self.column_widths[column]:
                    self.column_widths[column] = len(str(row[column]))

        spacer = '|  '
        # Don't forget to account for the bars!
        total_width = sum(self.column_widths) + (
            len(spacer) * len(self.column_widths))

        if total_width < self.max_width:
            for i in range(len(self.column_widths)):
                self.column_widths[i] += (
                    (self.max_width - total_width) / len(self.column_widths))

        lines = []
        for row in self.rows:
            line = spacer.join(
                [self.render_cell(i, cell) for i, cell in enumerate(row)])
            lines.append(line)
        return '\n'.join(lines)


class MenuOption(object):
    def __init__(self, value, action, hotkey=None, hidden=False):
        self.value = value
        self.action = action
        self.hotkey = hotkey
        self.hidden = hidden

    def __str__(self):
        return "%s. %s" % (self.hotkey, self.value)


class MenuChanger(object):
    def __init__(self, changer, *args):
        self.changer = changer
        self.args = args

    def __call__(self):
        self.changer(*self.args)


class Menu(object):
    def __init__(self, title, screen, writer, remover):
        self.title = title
        self.options = []
        self.page = 0
        self.items_per_page = screen.getmaxyx()[0] - 11
        self.screen = screen
        self.writer = writer
        self.remover = remover
        self.written_lines = 0

    def add_option(self, option):
        self.options.append(option)

    def add_option_vals(self, value, action, hotkey=None, hidden=False):
        self.options.append(MenuOption(value, action, hotkey, hidden))

    def next_page(self):
        self.page += 1
        self.clear()
        self.render()

    def prev_page(self):
        self.page -= 1
        self.clear()
        self.render()

    def clear(self):
        for _ in range(self.written_lines):
            self.remover()

        self.written_lines = 0

    def write(self, msg):
        self.writer(msg)
        self.written_lines += 1

    def render(self):
        hotkeys = ['q', 'w', 'e', 'r', 't', 'y', 'a', 's', 'd', 'f',
                   'g', 'z', 'x', 'c', 'v', 'b', 'y', 'u', 'i', 'o',
                   'h', 'j', 'k', 'l']
        hotkeys *= 30
        hotkeys.reverse()
        option_list = [o for o in self.options]

        # Count how many are hidden so we know how many
        # we actually can display
        hidden_count = 0
        for option in option_list:
            if option.hidden:
                hidden_count += 1

            if not option.hotkey:
                option.hotkey = hotkeys.pop()
        shown_options = (len(self.options) - hidden_count)
        max_per_page = (self.items_per_page - hidden_count) or 1
        pages = int(shown_options / max_per_page) + 1

        # Add a page count to the title of necessary
        if pages == 1:
            self.write(" " * 20 + self.title)
        else:
            self.write(" " * 20 + self.title + " (%s/%s)" % (
                    self.page + 1, pages))

        index = 0
        for option in option_list:
            if not option.hidden:
                if (index >= (self.page * max_per_page) and
                    index < ((self.page + 1) * max_per_page)):

                    self.write(str(option))

                index += 1

        # Add the next/prev buttons
        if shown_options > max_per_page:
            if self.page != (pages - 1):
                opt = MenuOption('Next Page',
                                 self.next_page, 'n')
                option_list.append(opt)
                self.write(str(opt))

            if self.page != 0:
                opt = MenuOption('Prev. Page', self.prev_page, 'p')
                option_list.append(opt)
                self.write(str(opt))

        self.write("Your Choice: ")
        self.screen.refresh()
        char = self.screen.getstr()

        if not char:
            return

        for option in option_list:
            if str(char) == str(option.hotkey):
                return option.action()

        # We didn't match any of the main options, now try a wildcard
        for option in self.options:
            if option.hotkey == '*':
                return option.action()

    def __str__(self):
        return '\n'.join([str(option) for option in self.options])


class MenuFactory(object):
    def __init__(self, screen, writer, remover):
        self.default_options = []
        self.screen = screen
        self.writer = writer
        self.remover = remover

    def add_default_option(self, option):
        self.default_options.append(option)

    def new_menu(self, title):
        menu = Menu(title, self.screen,
                    self.writer,
                    self.remover)
        for opt in self.default_options:
            menu.add_option(opt)

        return menu
