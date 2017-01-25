import cookielib
import os
import pickle
import mechanize

class Opener:
    def __init__(self, name):
        self.opener = mechanize.Browser()
        self.opener.set_handle_robots(False)

    def set_cookie(self, name, value):
        self.opener.set_cookie(str(name) + '=' + str(value))

    def save_cookies(self):
        return

    def open(self, *args):
        url = args[0]
        if len(args) == 2:
            data = args[1]
            return self.opener.open(url, data)
        return self.opener.open(url)
