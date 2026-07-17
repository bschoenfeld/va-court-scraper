from __future__ import absolute_import
import six.moves.urllib.request, six.moves.urllib.parse, six.moves.urllib.error
from bs4 import BeautifulSoup
import time
import requests
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
        session = requests.Session()
        session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.9',
            'Upgrade-Insecure-Requests': '1',
        })
        # Visit the CJISWeb landing page first to establish a session, as a
        # browser would, before requesting the circuit search page.
        try:
            session.get(self.url(''), timeout=60)
        except Exception:
            pass
        try:
            session.get(url, timeout=60)
        except Exception:
            pass

        try:
            self.make_request(url)
        except Exception:
            pass
        for name, value in session.cookies.items():
            try:
                self.opener.set_cookie(name, value)
            except Exception:
                pass

        try:
            content = self.make_request(url)
        except Exception as err:
            # Blocked (e.g. 403). If we can prompt, open a browser so the user
            # can accept terms / solve a CAPTCHA, then retry with those cookies.
            if not self.opener.can_prompt():
                raise
            print('Circuit court refused the request (%s). Opening a browser...' % err)
            self.opener.solve_with_browser(url)
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
