from __future__ import absolute_import
from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright

class CircuitCourtOpener:
    url_root = 'https://eapps.courts.state.va.us/CJISWeb/'

    def __init__(self):
        self.playwright = sync_playwright().start()
        self.browser = self.playwright.chromium.launch()
        self.context = self.browser.new_context()
        self.driver = self.context.new_page()

    def url(self, url):
        return CircuitCourtOpener.url_root + url

    def open_welcome_page(self):
        url = self.url('circuit.jsp')
        resp = self.context.request.get(url)
        return BeautifulSoup(resp.text(), 'html.parser')

    def log_off(self):
        data = {'searchType': ''}
        url = self.url('Logoff.do')
        self.context.request.post(url, form=data)
        if self.browser:
            self.browser.close()
        if self.playwright:
            self.playwright.stop()

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
