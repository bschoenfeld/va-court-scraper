import captcha
import urllib
from bs4 import BeautifulSoup
from opener import Opener

class DistrictCourtOpener:
    url_root = 'https://eapps.courts.state.va.us/gdcourts/'
    
    def __init__(self):
        self.opener = Opener()
    
    def url(self, url):
        return DistrictCourtOpener.url_root + url;
    
    def open_welcome_page(self):
        url = self.url('caseSearch.do?welcomePage=welcomePage')
        page = self.opener.open(url)
        page_content = page.read()
        soup = BeautifulSoup(page_content)
        
        # See if we need to solve a captcha
        if 'By clicking Accept' in page_content:
            captcha_url = self.url('captchaVerification.do')
            captcha.solve(self.opener, captcha_url)
            
            page = self.opener.open(url)
            soup = BeautifulSoup(page.read())
        return soup
    
    def change_court(self, name, code):
        data = urllib.urlencode({
            'selectedCourtsName': name,
            'selectedCourtsFipCode': code,
            'sessionCourtsFipCode': ''
        })
        url = self.url('changeCourt.do')
        self.opener.open(url, data)
    
    def open_hearing_date_search(self, code):
        url = self.url('caseSearch.do')
        url += '?fromSidebar=true&searchLanding=searchLanding'
        url += '&searchType=hearingDate&searchDivision=T&'
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
        soup = BeautifulSoup(content)
        return soup
    
    def open_case_details(self, case):
        url = self.url(case['details_url'])
        page = self.opener.open(url)
        return BeautifulSoup(page.read())
