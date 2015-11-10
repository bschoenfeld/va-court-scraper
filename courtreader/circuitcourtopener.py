import urllib
from bs4 import BeautifulSoup
from opener import Opener

class CircuitCourtOpener:
    url_root = 'http://ewsocis1.courts.state.va.us/CJISWeb/'

    def __init__(self):
        self.opener = Opener('circuit')

    def url(self, url):
        return CircuitCourtOpener.url_root + url

    def open_welcome_page(self):
        url = self.url('circuit.jsp')
        page = self.opener.open(url)
        return BeautifulSoup(page.read(), 'html.parser')

    def log_off(self):
        data = urllib.urlencode({'searchType': ''})
        url = self.url('Logoff.do')
        self.opener.open(url, data)

    def change_court(self, code, court):
        data = urllib.urlencode({
            'courtId': code,
            'courtType': 'C',
            'caseType': 'ALL',
            'testdos': False,
            'sessionCreate': 'NEW',
            'whichsystem': court
        })
        url = self.url('MainMenu.do')
        self.opener.open(url, data)

    def do_case_number_search(self, code, case_number):
        data = {
            'courtId':code,
            'caseNo':case_number,
            'categorySelected':'R'
        }
        data = urllib.urlencode(data)
        url = self.url('CaseDetail.do')
        page = self.opener.open(url, data)
        return BeautifulSoup(page.read(), 'html.parser')

    def do_name_search(self, code, name, category):
        data = {
            'category': category,
            'lastName': name,
            'courtId': code,
            'submitValue': 'N'
        }
        data = urllib.urlencode(data)
        url = self.url('Search.do')
        page = self.opener.open(url, data)
        return BeautifulSoup(page.read(), 'html.parser')

    def continue_name_search(self, code, category):
        data = {
            'courtId': code,
            'pagelink': 'Next',
            'lastCaseProcessed': '',
            'firstCaseProcessed': '',
            'lastNameProcessed': '',
            'firstNameProcessed': '',
            'category': category,
            'firstCaseSerialNumber': 0,
            'lastCaseSerialNumber': 0,
            'searchType': '',
            'emptyList': ''
        }
        data = urllib.urlencode(data)
        url = self.url('Search.do')
        page = self.opener.open(url, data)
        return BeautifulSoup(page.read(), 'html.parser')
