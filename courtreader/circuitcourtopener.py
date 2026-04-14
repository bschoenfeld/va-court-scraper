from __future__ import absolute_import
import six.moves.urllib.request, six.moves.urllib.parse, six.moves.urllib.error
from bs4 import BeautifulSoup
import time
from .opener import Opener

class CircuitCourtOpener:
    url_root = 'https://eapps.courts.state.va.us/CJISWeb/'

    def __init__(self):
        self.opener = Opener('circuit')

    def make_request(self, url, data=None):
        while True:
            if data:
                page = self.opener.open(url, data)
            else:
                page = self.opener.open(url)
            
            content = page.read()
            if b'You have exceeded the rate limit' in content:
                print('WARNING: Rate limit exceeded. Waiting 10 seconds and retrying...')
                time.sleep(10)
                continue
            
            return content

    def url(self, url):
        return CircuitCourtOpener.url_root + url

    def open_welcome_page(self):
        url = self.url('circuit.jsp')
        self.make_request('https://google.com')
        content = self.make_request(url)
        return BeautifulSoup(content, 'html.parser')

    def log_off(self):
        data = six.moves.urllib.parse.urlencode({'searchType': ''})
        url = self.url('Logoff.do')
        self.make_request(url, data)

    def change_court(self, code, court):
        data = six.moves.urllib.parse.urlencode({
            'courtId': code,
            'courtType': 'C',
            'caseType': 'ALL',
            'testdos': False,
            'sessionCreate': 'NEW',
            'whichsystem': court
        })
        url = self.url('MainMenu.do')
        self.make_request(url, data)

    def do_case_number_search(self, code, case_number, category):
        data = {
            'submitValue': '',
            'courtId':code,
            'caseNo':case_number,
            'categorySelected':category
        }
        data = six.moves.urllib.parse.urlencode(data)
        url = self.url('CaseDetail.do')
        content = self.make_request(url, data)
        return BeautifulSoup(content, 'html.parser')

    def do_case_number_pleadings_search(self, code, case_number, category):
        data = {
            'submitValue':'P',
            'courtId':code,
            'categorySelected':category,
            'caseStatus':'A',
            'caseNo':case_number
        }
        data = six.moves.urllib.parse.urlencode(data)
        url = self.url('CaseDetail.do')
        content = self.make_request(url, data)
        return BeautifulSoup(content, 'html.parser')

    def do_case_number_services_search(self, code, case_number, category):
        data = {
            'submitValue':'S',
            'courtId':code,
            'categorySelected':category,
            'caseStatus':'A',
            'caseNo':case_number
        }
        data = six.moves.urllib.parse.urlencode(data)
        url = self.url('CaseDetail.do')
        content = self.make_request(url, data)
        return BeautifulSoup(content, 'html.parser')

    def return_to_main_menu(self, code):
        data = {
            'courtId':code
        }
        data = six.moves.urllib.parse.urlencode(data)
        url = self.url('MainMenu.do')
        self.make_request(url, data)
        return

    def do_name_search(self, code, name, category):
        data = {
            'category': category,
            'lastName': name,
            'courtId': code,
            'submitValue': 'N'
        }
        data = six.moves.urllib.parse.urlencode(data)
        url = self.url('Search.do')
        content = self.make_request(url, data)
        return BeautifulSoup(content, 'html.parser')

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
        data = six.moves.urllib.parse.urlencode(data)
        url = self.url('Search.do')
        content = self.make_request(url, data)
        return BeautifulSoup(content, 'html.parser')

    def do_date_search(self, code, date, category):
        data = {
            'hearSelect':'',
            'selectDate':date,
            'categorySelected':category,
            'hearDateSelected':date,
            'submitValue':'',
            'courtId':code
        }
        data = six.moves.urllib.parse.urlencode(data)
        url = self.url('hearSearch.do')
        content = self.make_request(url, data)
        return BeautifulSoup(content, 'html.parser')

    def continue_date_search(self, code, category):
        data = {
            'courtId': code,
            'pagelink': 'Next',
            'lastCaseProcessed': '',
            'firstCaseProcessed': '',
            'category': category,
            'firstCaseSerialNumber': 0,
            'lastCaseSerialNumber': 0,
            'searchType': '',
            'emptyList': ''
        }
        data = six.moves.urllib.parse.urlencode(data)
        url = self.url('hearSearch.do')
        content = self.make_request(url, data)
        return BeautifulSoup(content, 'html.parser')
