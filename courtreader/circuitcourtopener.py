from __future__ import absolute_import
import logging
from bs4 import BeautifulSoup
from .browser import get_playwright

log = logging.getLogger('logentries')

class CircuitCourtOpener:
    url_root = 'https://eapps.courts.state.va.us/CJISWeb/'

    def __init__(self):
        self.driver_open = False
        self.playwright = None
        self.browser = None
        self.context = None
        self.driver = None
        self.open_driver()

    def open_driver(self):
        if self.driver_open:
            return
        self.playwright = get_playwright()
        self.browser = self.playwright.chromium.launch(headless=False)
        self.context = self.browser.new_context(
            user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
                       '(KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36'
        )
        self.driver = self.context.new_page()
        try:
            from playwright_stealth import stealth_sync
            stealth_sync(self.driver)
        except ImportError:
            log.warning("playwright-stealth not installed.")
        self.driver_open = True

    def url(self, url):
        return CircuitCourtOpener.url_root + url

    def open_welcome_page(self):
        url = self.url('circuit.jsp')
        resp = self.context.request.get(url)
        return BeautifulSoup(resp.text(), 'html.parser')

    def log_off(self):
        data = {'searchType': ''}
        url = self.url('Logoff.do')
        try:
            self.context.request.post(url, form=data)
        except:
            pass
        if self.browser:
            self.browser.close()
            self.browser = None
        if self.playwright:
            self.playwright.stop()
            self.playwright = None
        self.driver_open = False

    def change_court(self, code, court):
        data = {
            'courtId': code,
            'courtType': 'C',
            'caseType': 'ALL',
            'testdos': False,
            'sessionCreate': 'NEW',
            'whichsystem': court
        }
        url = self.url('MainMenu.do')
        self.context.request.post(url, form=data)

    def do_case_number_search(self, code, case_number, category):
        data = {
            'submitValue': '',
            'courtId':code,
            'caseNo':case_number,
            'categorySelected':category
        }
        url = self.url('CaseDetail.do')
        resp = self.context.request.post(url, form=data)
        return BeautifulSoup(resp.text(), 'html.parser')

    def do_case_number_pleadings_search(self, code, case_number, category):
        data = {
            'submitValue':'P',
            'courtId':code,
            'categorySelected':category,
            'caseStatus':'A',
            'caseNo':case_number
        }
        url = self.url('CaseDetail.do')
        resp = self.context.request.post(url, form=data)
        return BeautifulSoup(resp.text(), 'html.parser')

    def do_case_number_services_search(self, code, case_number, category):
        data = {
            'submitValue':'S',
            'courtId':code,
            'categorySelected':category,
            'caseStatus':'A',
            'caseNo':case_number
        }
        url = self.url('CaseDetail.do')
        resp = self.context.request.post(url, form=data)
        return BeautifulSoup(resp.text(), 'html.parser')

    def return_to_main_menu(self, code):
        data = {
            'courtId':code
        }
        url = self.url('MainMenu.do')
        self.context.request.post(url, form=data)
        return

    def do_name_search(self, code, name, category):
        data = {
            'category': category,
            'lastName': name,
            'courtId': code,
            'submitValue': 'N'
        }
        url = self.url('Search.do')
        resp = self.context.request.post(url, form=data)
        return BeautifulSoup(resp.text(), 'html.parser')

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
        url = self.url('Search.do')
        resp = self.context.request.post(url, form=data)
        return BeautifulSoup(resp.text(), 'html.parser')

    def do_date_search(self, code, date, category):
        data = {
            'hearSelect':'',
            'selectDate':date,
            'categorySelected':category,
            'hearDateSelected':date,
            'submitValue':'',
            'courtId':code
        }
        url = self.url('hearSearch.do')
        resp = self.context.request.post(url, form=data)
        return BeautifulSoup(resp.text(), 'html.parser')

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
        url = self.url('hearSearch.do')
        resp = self.context.request.post(url, form=data)
        return BeautifulSoup(resp.text(), 'html.parser')
