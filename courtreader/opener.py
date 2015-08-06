import cookielib
import os
import pickle
import urllib2

class Opener:
    user_agent = u"Mozilla/5.0 (Macintosh; U; Intel Mac OS X 10.6; " + \
        u"en-US; rv:1.9.2.11) Gecko/20101012 Firefox/3.6.11"

    def __init__(self, name):
        self.name = name
        # Create page opener that stores cookie
        self.cookieJar = cookielib.CookieJar()
        cookie_processor = urllib2.HTTPCookieProcessor(self.cookieJar)
        self.opener = urllib2.build_opener(cookie_processor)
        self.opener.addheaders = [('User-Agent', Opener.user_agent)]

        # Try to load cookies
        if os.path.isfile(self.name + '.cookie'):
            with open(self.name + '.cookie', 'r') as f:
                for cookie in pickle.loads(f.read()):
                    self.cookieJar.set_cookie(cookie)

    def set_cookie(self, name, value):
        for cookie in self.cookieJar:
            if cookie.name == name:
                cookie.value = value

    def save_cookies(self):
        with open(self.name + '.cookie', 'w') as f:
            f.write(pickle.dumps(list(self.cookieJar)))

    def open(self, *args):
        url = args[0]
        if len(args) == 2:
            data = args[1]
            return self.opener.open(url, data)
        return self.opener.open(url)
