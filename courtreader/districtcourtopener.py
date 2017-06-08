import deathbycaptcha
import logging
import os
import time
import urllib
from bs4 import BeautifulSoup
from opener import Opener
from selenium import webdriver

log = logging.getLogger('logentries')

class DistrictCourtOpener:
    url_root = 'https://eapps.courts.state.va.us/gdcourts/'

    def __init__(self):
        self.opener = Opener('district')
        self.use_driver = True

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
        page = self.opener.open('https://google.com')
        page_content = page.read()
        page = self.opener.open(url)
        page_content = page.read()
        # See if we need to solve a captcha
        if 'By clicking Accept' in page_content:
            self.solve_captcha(url)
            page = self.opener.open(url)
            page_content = page.read()
        if 'By clicking Accept' in page_content:
            raise RuntimeError('CAPTCHA failed')
        return BeautifulSoup(page_content, 'html.parser')

    def solve_captcha(self, url):
        self.open_driver()
        self.driver.get(url)
        time.sleep(1)
        current_url = self.driver.current_url
        print current_url

        while current_url == self.driver.current_url:
            time.sleep(1)

        '''
        log.info('Solving CAPTCHA')
        captcha_solver = deathbycaptcha.SocketClient(os.environ['DBC_USER'], \
                                                     os.environ['DBC_PASSWORD'])
        self.driver.get(url)
        captcha = self.driver.find_element_by_id('recaptcha_challenge_image')
        image_src = captcha.get_attribute('src')
        image_filename = str(os.getpid()) + '_captcha.png'
        urllib.urlretrieve(image_src, image_filename)
        try:
            captcha_solution = captcha_solver.decode(image_filename, 60)
            #captcha_solution = {'captcha': 'manual', 'text': raw_input('Enter CAPTCHA:')}
            if captcha_solution:
                log.info('CAPTCHA SOLVED')
                print "CAPTCHA %s solved: %s" % (captcha_solution["captcha"],
                                                 captcha_solution["text"])
                self.driver.find_element_by_name('recaptcha_response_field') \
                      .send_keys(captcha_solution["text"])
                os.remove(image_filename)
        except deathbycaptcha.AccessDeniedException:
            log.error('deathbycaptcha access denied')
            print 'deathbycaptcha access denied'
        time.sleep(1)
        self.driver.find_element_by_name('captchaVerificationForm') \
              .submit()
        '''
        cookie = self.driver.get_cookie('JSESSIONID')['value']
        self.opener.set_cookie('JSESSIONID', cookie)
        self.opener.save_cookies()
        self.driver.quit()

    def change_court(self, name, code):
        data = urllib.urlencode({
            'selectedCourtsName': name,
            'selectedCourtsFipCode': code,
            'sessionCourtsFipCode': ''
        })
        url = self.url('changeCourt.do')
        self.opener.open(url, data)

    def open_hearing_date_search(self, code, search_division):
        url = self.url('caseSearch.do')
        url += '?fromSidebar=true&searchLanding=searchLanding'
        url += '&searchType=hearingDate&searchDivision=' + search_division
        url += '&searchFipsCode=' + code
        url += '&curentFipsCode=' + code
        self.opener.open(url)

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
        data = urllib.urlencode(data)
        url = self.url('caseSearch.do')
        page = self.opener.open(url, data)
        content = ''
        for line in page:
            if '<a href="caseSearch.do?formAction=caseDetails' in line:
                line = line.replace('/>', '>')
            content += line
        soup = BeautifulSoup(content, 'html.parser')
        return soup

    def open_case_number_search(self, code, search_division):
        url = self.url('criminalCivilCaseSearch.do')
        url += '?fromSidebar=true&formAction=searchLanding&searchDivision=' + search_division
        url += '&searchFipsCode=' + code
        url += '&curentFipsCode=' + code
        self.opener.open(url)

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
        data = urllib.urlencode(data)
        url = self.url('criminalCivilCaseSearch.do')
        self.opener.open(url, data)
        # the post returns 302, then we have to do a GET... strange

        url = self.url('criminalDetail.do')
        content = self.opener.open(url)
        soup = BeautifulSoup(content, 'html.parser')
        return soup

    def open_case_details(self, details_url):
        url = self.url(details_url)
        page = self.opener.open(url)
        return BeautifulSoup(page.read(), 'html.parser')

    def open_name_search(self, code, search_division):
        url = self.url('nameSearch.do')
        url += '?fromSidebar=true&formAction=searchLanding&searchDivision=' + search_division
        url += '&searchFipsCode=' + code
        url += '&curentFipsCode=' + code
        if self.use_driver:
            self.driver.get(url)
        else:
            self.opener.open(url)

    def do_name_search_with_driver(self, code, name, count, prev_cases):
        if prev_cases:
            xpath = "//input[@value='Next'][@type='submit']"
            self.driver.find_element_by_xpath(xpath).click()
        else:
            self.driver.find_element_by_name('localnamesearchlastName') \
                .send_keys(name)
            xpath = "//input[@value='Search'][@type='submit']"
            self.driver.find_element_by_xpath(xpath).click()
        time.sleep(1)
        source = self.driver.page_source
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
        data = urllib.urlencode(data)
        url = self.url('nameSearch.do')
        content = self.opener.open(url, data)
        soup = BeautifulSoup(content, 'html.parser')
        return soup
