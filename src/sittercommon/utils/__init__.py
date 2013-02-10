import re


def strip_html(text):
    return re.sub('<[^<]+?>', '', text)
