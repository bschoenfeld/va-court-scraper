from __future__ import absolute_import
import logging
import os
import time
import six.moves.urllib.request, six.moves.urllib.parse, six.moves.urllib.error
from bs4 import BeautifulSoup
from .opener import Opener
from selenium import webdriver
from six.moves import input

log = logging.getLogger('logentries')

class DistrictCourtOpener:
    url_root = 'https://eapps.courts.state.va.us/gdcourts/'

    def __init__(self):
        self.opener = Opener('district')
        self.use_driver = True

    def make_request(self, url, data=None):
        while True:
            if data:
                page = self.opener.open(url, data)
            else:
                page = self.opener.open(url)
            
            content = page.read()
            if b'You have exceeded the rate limit' in content:
                time.sleep(10)
                continue
            
            return content

    def url(self, url):
        return DistrictCourtOpener.url_root + url

    def log_off(self):
        return None

    def open_driver(self):
        self.driver = webdriver.Chrome('./chromedriver')
        self.driver.implicitly_wait(3)
        self.driver_open = True

    def open_welcome_page(self):
        url = self.url('caseSearch.do?welcomePage=welcomePage')
        page_content = self.make_request('https://google.com')
        page_content = self.make_request(url)
        # See if we need to solve a captcha
        if b'By clicking Accept' in page_content:
            self.solve_captcha(url)
            page_content = self.make_request(url)
        if b'By clicking Accept' in page_content:
            raise RuntimeError('CAPTCHA failed')
        return BeautifulSoup(page_content, 'html.parser')

    def solve_captcha(self, url):
        cookie = input('JESSSIONID')
        self.opener.set_cookie('JSESSIONID', cookie)
        self.opener.save_cookies()

    def change_court(self, name, code):
        data = six.moves.urllib.parse.urlencode({
            'selectedCourtsName': name,
            'selectedCourtsFipCode': code,
            'sessionCourtsFipCode': ''
        })
        url = self.url('changeCourt.do')
        self.make_request(url, data)

    def open_hearing_date_search(self, code, search_division):
        url = self.url('caseSearch.do')
        url += '?fromSidebar=true&searchLanding=searchLanding'
        url += '&searchType=hearingDate&searchDivision=' + search_division
        url += '&searchFipsCode=' + code
        url += '&curentFipsCode=' + code
        self.make_request(url)

    def do_hearing_date_search(self, code, date, first_page):
        data = {
            'formAction':'',
            'curentFipsCode':code,
            'searchTerm':date,
            'searchHearingTime':'',
            'searchCourtroom':'',
            'lastName':'',
            'firstName':'',
            'middleName':'',
            'suffix':'',
            'searchHearingType':'',
            'searchUnitNumber':'',
            'searchFipsCode':code
        }
        if first_page:
            data['caseSearch'] = 'Search'
        else:
            data['caseInfoScrollForward'] = 'Next'
            data['unCheckedCases'] = ''
        data = six.moves.urllib.parse.urlencode(data)
        url = self.url('caseSearch.do')
        page_content = self.make_request(url, data)
        content = ''
        for line in page_content.splitlines(True):
            if b'<a href="caseSearch.do?formAction=caseDetails' in line:
                line = line.replace(b'/>', b'>')
            content += line.decode('utf-8', errors='ignore')
        soup = BeautifulSoup(content, 'html.parser')
        return soup

    def open_case_number_search(self, code, search_division):
        url = self.url('criminalCivilCaseSearch.do')
        url += '?fromSidebar=true&formAction=searchLanding&searchDivision=' + search_division
        url += '&searchFipsCode=' + code
        url += '&curentFipsCode=' + code
        self.make_request(url)

    def do_case_number_search(self, code, case_number, search_division):
        data = {
            'formAction':'submitCase',
            'searchFipsCode':code,
            'searchDivision':search_division,
            'searchType':'caseNumber',
            'displayCaseNumber':case_number,
            'localFipsCode':code,
            'clientSearchCounter':0
        }
        data = six.moves.urllib.parse.urlencode(data)
        url = self.url('criminalCivilCaseSearch.do')
        self.make_request(url, data)
        # the post returns 302, then we have to do a GET... strange

        url = self.url('criminalDetail.do' if search_division == 'T' else 'civilDetail.do')
        content = self.make_request(url)
        soup = BeautifulSoup(content, 'html.parser')
        return soup

    def open_case_details(self, details_url):
        url = self.url(details_url)
        content = self.make_request(url)
        return BeautifulSoup(content, 'html.parser')

    def open_name_search(self, code, search_division):
        url = self.url('nameSearch.do')
        url += '?fromSidebar=true&formAction=searchLanding&searchDivision=' + search_division
        url += '&searchFipsCode=' + code
        url += '&curentFipsCode=' + code
        if self.use_driver:
            while True:
                self.driver.get(url)
                if 'You have exceeded the rate limit' in self.driver.page_source:
                    time.sleep(10)
                    continue
                break
        else:
            self.make_request(url)

    def do_name_search_with_driver(self, code, name, count, prev_cases):
        while True:
            if prev_cases:
                xpath = "//input[@value='Next'][@type='submit']"
                self.driver.find_element_by_xpath(xpath).click()
            else:
                name_input = self.driver.find_element_by_name('localnamesearchlastName')
                name_input.clear()
                name_input.send_keys(name)
                xpath = "//input[@value='Search'][@type='submit']"
                self.driver.find_element_by_xpath(xpath).click()
            time.sleep(1)
            source = self.driver.page_source
            if 'You have exceeded the rate limit' in source:
                time.sleep(10)
                self.driver.back()
                time.sleep(1)
                continue
            soup = BeautifulSoup(source, 'html.parser')
            return soup

    def do_name_search(self, code, search_division, name, count, prev_cases=None):
        if self.use_driver:
            return self.do_name_search_with_driver(code, name, count, prev_cases)
        data = {
            'formAction':'newSearch',
            'displayCaseNumber':'',
            'formBean':'',
            'localFipsCode':code,
            'caseActive':'',
            'localLastName':'',
            'forward':'',
            'back':'',
            'localnamesearchlastName':name,
            'lastName':name,
            'localnamesearchfirstName':'',
            'firstName':'',
            'localnamesearchmiddleName':'',
            'middleName':'',
            'localnamesearchsuffix':'',
            'suffix':'',
            'localnamesearchsearchCategory':'A',
            'searchCategory':'A',
            'searchFipsCode':code,
            'searchDivision':search_division,
            'searchType':'name',
            'firstRowName':'',
            'firstRowCaseNumber':'',
            'lastRowName':'',
            'lastRowCaseNumber':'',
            'clientSearchCounter':count
        }
        if prev_cases:
            data['formAction'] = 'next'
            data['unCheckedCases'] = ''
            data['firstRowName'] = prev_cases[0]['defendant']
            data['firstRowCaseNumber'] = prev_cases[0]['case_number']
            data['lastRowName'] = prev_cases[-1]['defendant']
            data['lastRowCaseNumber'] = prev_cases[-1]['case_number']
        data = six.moves.urllib.parse.urlencode(data)
        url = self.url('nameSearch.do')
        content = self.make_request(url, data)
        soup = BeautifulSoup(content, 'html.parser')
        return soup
