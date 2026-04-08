from __future__ import absolute_import
import mechanize

class NoHistory(object):
    def add(self, *a, **k): pass
    def clear(self): pass

class Opener:
    def __init__(self, name):
        self.opener = mechanize.Browser(history=NoHistory())
        self.opener.set_handle_robots(False)
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

    def open(self, *args):
        import time
        import socket
        import logging
        log = logging.getLogger('logentries')
        
        url = args[0]
        data = args[1] if len(args) == 2 else None
        
        for attempt in range(5):
            try:
                if data:
                    page = self.opener.open(url, data)
                else:
                    page = self.opener.open(url)
                
                content = page.read()
                
                class DummyPage:
                    def __init__(self, c):
                        self.c = c
                    def read(self):
                        return self.c
                
                return DummyPage(content)
            
            except Exception as e:
                # Catch timeout errors to prevent losing the session
                if isinstance(e, socket.timeout) or "timeout" in str(e).lower() or "read operation" in str(e).lower():
                    log.warning('Network timeout in opener. Retrying... (Attempt %d of 5)', attempt + 1)
                    print('WARNING: Network timeout in opener. Retrying in 10 seconds...')
                    time.sleep(10)
                    if attempt == 4:
                        raise
                else:
                    raise
