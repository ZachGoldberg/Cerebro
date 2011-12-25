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
        self.items_per_page = 10
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
        self.write(" " * 20 + self.title)
        hotkeys = ['q', 'w', 'e', 'r', 't', 'y', 'a', 's', 'd', 'f',
                   'g', 'z', 'x', 'c', 'v', 'b', 'y', 'u', 'i', 'o',
                   'h', 'j', 'k', 'l']
        hotkeys.reverse()
        option_list = [o for o in self.options]
        for index, option in enumerate(option_list):
            if (index > (self.page * self.items_per_page) and
                index < ((self.page + 1) * self.items_per_page)):

                if not option.hotkey:
                    option.hotkey = hotkeys.pop()

                if not option.hidden:
                    self.write(str(option))

        if len(self.options) > self.items_per_page:
            pages = int(len(self.options) / self.items_per_page) + 1
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
