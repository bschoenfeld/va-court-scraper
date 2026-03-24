from __future__ import absolute_import
import logging
import time
from bs4 import BeautifulSoup
from .browser import get_playwright

log = logging.getLogger('logentries')

class DistrictCourtOpener:
    url_root = 'https://eapps.courts.state.va.us/gdcourts/'

    def __init__(self):
        import uuid
        self.instance_id = str(uuid.uuid4())
        self.state_file = f'.district_court_state_{self.instance_id}.json'
        self.use_driver = True
        self.browser = None
        self.playwright_mgr = None
        self.playwright = None
        self.context = None
        self.driver = None
        self.driver_open = False
        self.open_driver()

    def url(self, url):
        return DistrictCourtOpener.url_root + url

    def log_off(self, preserve_session=False):
        if preserve_session and self.context:
            try:
                self.context.storage_state(path=self.state_file)
            except:
                pass
        if self.browser:
            self.browser.close()
            self.browser = None
        if self.playwright:
            self.playwright.stop()
            self.playwright = None
        if self.playwright_mgr:
            self.playwright_mgr.stop()
            self.playwright_mgr = None
        self.driver_open = False
        
        if not preserve_session:
            import os
            try:
                if os.path.exists(self.state_file):
                    os.remove(self.state_file)
            except:
                pass
        return None

    def open_driver(self):
        if self.driver_open:
            return
        self.playwright_mgr, self.playwright = get_playwright()
        # Launch headful to avoid bot mitigation blocks
        self.browser = self.playwright.chromium.launch(
            headless=False,
            args=[
                '--disable-logging',
                '--disable-gpu',
                '--log-level=3',
                '--silent'
            ]
        )
        
        kwargs = {
            'user_agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36'
        }
        
        import os
        if os.path.exists(self.state_file):
            kwargs['storage_state'] = self.state_file
            
        self.context = self.browser.new_context(**kwargs)
        self.driver = self.context.new_page()
        try:
            from playwright_stealth import stealth_sync
            stealth_sync(self.driver)
        except ImportError:
            log.warning("playwright-stealth not installed. Captcha might be triggered easily.")
        self.driver_open = True

    def _request_get(self, url, **kwargs):
        while True:
            resp = self.context.request.get(url, **kwargs)
            if b"You have exceeded the rate limit" in resp.body():
                log.info("Rate limit exceeded. Waiting 10 seconds before retrying...")
                time.sleep(10)
                continue
            return resp

    def _request_post(self, url, **kwargs):
        while True:
            resp = self.context.request.post(url, **kwargs)
            if b"You have exceeded the rate limit" in resp.body():
                log.info("Rate limit exceeded. Waiting 10 seconds before retrying...")
                time.sleep(10)
                continue
            return resp

    def _driver_goto(self, url, **kwargs):
        while True:
            self.driver.goto(url, **kwargs)
            if "You have exceeded the rate limit" in self.driver.content():
                log.info("Rate limit exceeded. Waiting 10 seconds before retrying...")
                time.sleep(10)
                continue
            break

    def open_welcome_page(self):
        url = self.url('caseSearch.do?welcomePage=welcomePage')
        self._driver_goto(url)
        content = self.driver.content()
        # See if we need to solve a captcha
        if 'By clicking Accept' in content:
            log.info('Captcha detected. Please solve the captcha in the open browser window.')
            # Wait until the page doesn't have 'By clicking Accept' anymore
            self.driver.wait_for_function('() => !document.body.innerText.includes("By clicking Accept")', timeout=300000)
            log.info('Captcha solved successfully.')
        return BeautifulSoup(self.driver.content(), 'html.parser')

    def solve_captcha(self, url):
        # Captcha is now solved interactively inside Playwright (open_welcome_page)
        pass

    def change_court(self, name, code):
        data = {
            'selectedCourtsName': name,
            'selectedCourtsFipCode': code,
            'sessionCourtsFipCode': ''
        }
        url = self.url('changeCourt.do')
        self._request_post(url, form=data)

    def open_hearing_date_search(self, code, search_division):
        url = self.url('caseSearch.do')
        url += '?fromSidebar=true&searchLanding=searchLanding'
        url += '&searchType=hearingDate&searchDivision=' + search_division
        url += '&searchFipsCode=' + code
        url += '&curentFipsCode=' + code
        self._request_get(url)

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
        
        url = self.url('caseSearch.do')
        resp = self._request_post(url, form=data)
        content = ''
        
        for line in resp.text().splitlines():
            if '<a href="caseSearch.do?formAction=caseDetails' in line:
                line = line.replace('/>', '>')
            content += line + '\n'
            
        return BeautifulSoup(content, 'html.parser')

    def open_case_number_search(self, code, search_division):
        url = self.url('criminalCivilCaseSearch.do')
        url += '?fromSidebar=true&formAction=searchLanding&searchDivision=' + search_division
        url += '&searchFipsCode=' + code
        url += '&curentFipsCode=' + code
        self._request_get(url)

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
        url = self.url('criminalCivilCaseSearch.do')
        self._request_post(url, form=data)

        url_postfix = 'criminalDetail.do' if search_division == 'T' else 'civilDetail.do'
        url2 = self.url(url_postfix)
        resp = self._request_get(url2)
        return BeautifulSoup(resp.text(), 'html.parser')

    def open_case_details(self, details_url):
        url = self.url(details_url)
        resp = self._request_get(url)
        return BeautifulSoup(resp.text(), 'html.parser')

    def open_name_search(self, code, search_division):
        url = self.url('nameSearch.do')
        url += '?fromSidebar=true&formAction=searchLanding&searchDivision=' + search_division
        url += '&searchFipsCode=' + code
        url += '&curentFipsCode=' + code
        self._driver_goto(url)

    def do_name_search_with_driver(self, code, name, count, prev_cases):
        while True:
            if prev_cases:
                self.driver.locator("//input[@value='Next'][@type='submit']").click()
            else:
                self.driver.locator("input[name='localnamesearchlastName']").fill(name)
                self.driver.locator("//input[@value='Search'][@type='submit']").click()
            
            self.driver.wait_for_timeout(1000)
            content = self.driver.content()
            
            if "You have exceeded the rate limit" in content:
                log.info("Rate limit exceeded. Waiting 10 seconds before retrying...")
                time.sleep(10)
                self.driver.go_back()
                self.driver.wait_for_timeout(1000)
                continue
                
            return BeautifulSoup(content, 'html.parser')

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
        
        url = self.url('nameSearch.do')
        resp = self._request_post(url, form=data)
        return BeautifulSoup(resp.text(), 'html.parser')
