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
    def __init__(self, title):
        self.title = title
        self.options = []

    def add_option(self, option):
        self.options.append(option)

    def add_option_vals(self, value, action, hotkey=None, hidden=False):
        self.options.append(MenuOption(value, action, hotkey, hidden))

    def render(self, screen, writer):
        writer(" " * 20 + self.title)
        hotkeys = ['q', 'w', 'e', 'r', 't', 'y', 'a', 's', 'd', 'f',
                   'g', 'z', 'x', 'c', 'v', 'b', 'y', 'u', 'i', 'o',
                   'p', 'h', 'j', 'k', 'l', 'n']
        hotkeys.reverse()
        for option in self.options[:20]:
            if not option.hotkey:
                option.hotkey = hotkeys.pop()
            if not option.hidden:
                writer(str(option))

        writer("Your Choice: ")
        screen.refresh()
        char = screen.getstr()

        if not char:
            return

        for option in self.options:
            if str(char) == str(option.hotkey):
                return option.action()

        # We didn't match any of the main options, now try a wildcard
        for option in self.options:
            if option.hotkey == '*':
                return option.action()

    def __str__(self):
        return '\n'.join([str(option) for option in self.options])


class MenuFactory(object):
    def __init__(self):
        self.default_options = []

    def add_default_option(self, option):
        self.default_options.append(option)

    def new_menu(self, title):
        menu = Menu(title)
        for opt in self.default_options:
            menu.add_option(opt)

        return menu
