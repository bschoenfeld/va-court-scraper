from __future__ import absolute_import
import mechanize

class NoHistory(object):
    def add(self, *a, **k): pass
    def clear(self): pass

class Opener:
    def __init__(self, name):
        self.opener = mechanize.Browser(history=NoHistory())
        self.opener.set_handle_robots(False)
        self.opener.set_handle_redirect(True)
        self.opener.addheaders = [
            ('User-Agent', 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36'),
            ('Accept', 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8'),
            ('Accept-Language', 'en-US,en;q=0.9'),
            ('Sec-Ch-Ua', '"Chromium";v="122", "Not(A:Brand";v="24", "Google Chrome";v="122"'),
            ('Sec-Ch-Ua-Mobile', '?0'),
            ('Sec-Ch-Ua-Platform', '"Windows"'),
            ('Sec-Fetch-Dest', 'document'),
            ('Sec-Fetch-Mode', 'navigate'),
            ('Sec-Fetch-Site', 'none'),
            ('Sec-Fetch-User', '?1'),
            ('Upgrade-Insecure-Requests', '1')
        ]

    def set_cookie(self, name, value):
        self.opener.set_cookie(str(name) + '=' + str(value))

    def save_cookies(self):
        return

    def can_prompt(self):
        """True only when running interactively (a real terminal), so unattended
        collectors never block waiting on a browser/keyboard."""
        import sys
        try:
            return bool(sys.stdin) and sys.stdin.isatty()
        except Exception:
            return False

    def solve_with_browser(self, url):
        """Open a real browser at ``url`` so the user can accept terms / solve a
        CAPTCHA, then copy the resulting cookies into this opener."""
        from selenium import webdriver
        from six.moves import input
        try:
            driver = webdriver.Chrome()
        except Exception as e:
            print('Could not open a browser (%s). Make sure Chrome and a matching '
                  'chromedriver are available.' % e)
            raise
        try:
            driver.get(url)
            input('A browser has opened. Accept any terms / solve the CAPTCHA there, '
                  'then press Enter here to continue...')
            for cookie in driver.get_cookies():
                try:
                    self.set_cookie(cookie['name'], cookie['value'])
                except Exception:
                    pass
        finally:
            try:
                driver.quit()
            except Exception:
                pass

    def open(self, *args):
        import time
        import socket
        from six.moves.urllib.error import URLError

        url = args[0]
        data = args[1] if len(args) == 2 else None

        max_attempts = 4
        for attempt in range(max_attempts):
            try:
                if data:
                    page = self.opener.open(url, data, timeout=120)
                else:
                    page = self.opener.open(url, timeout=120)

                content = page.read()
                page.close()

                class DummyPage:
                    def __init__(self, c):
                        self.c = c
                    def read(self):
                        return self.c

                return DummyPage(content)

            except Exception as e:
                # Retry transient network errors (dropped/reset connections,
                # timeouts) instead of letting one bubble up and cost the whole
                # task a 10-minute back-off. Only give up after max_attempts.
                msg = str(e).lower()
                # HTTP status errors (403, 404, 5xx) won't be fixed by retrying.
                code = getattr(e, 'code', None)
                if (isinstance(code, int) and code >= 400) or 'http error 4' in msg or 'http error 5' in msg:
                    raise
                transient = (
                    isinstance(e, (socket.timeout, socket.error, URLError, ConnectionError))
                    or 'timeout' in msg
                    or 'read operation' in msg
                    or 'forcibly closed' in msg
                    or 'connection reset' in msg
                    or '10054' in msg
                )
                if transient and attempt < max_attempts - 1:
                    wait = 5 * (attempt + 1)
                    print('Network error in opener (%s). Retry %d/%d in %ds...' % (
                        e, attempt + 1, max_attempts - 1, wait))
                    time.sleep(wait)
                    continue
                raise
