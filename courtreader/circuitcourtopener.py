import urllib
from bs4 import BeautifulSoup
from opener import Opener

class CircuitCourtOpener:
    url_root = 'http://ewsocis1.courts.state.va.us/CJISWeb/'

    def __init__(self):
        self.opener = Opener()

    def url(self, url):
        return CircuitCourtOpener.url_root + url;

    def open_welcome_page(self):
        url = self.url('circuit.jsp')
        page = self.opener.open(url)
        return BeautifulSoup(page.read(), 'html.parser')
