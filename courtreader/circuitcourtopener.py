from bs4 import BeautifulSoup

class CircuitCourtOpener:
    url_root = 'http://ewsocis1.courts.state.va.us/CJISWeb/'

    def url(self, url):
        return CircuitCourtOpener.url_root + url;
