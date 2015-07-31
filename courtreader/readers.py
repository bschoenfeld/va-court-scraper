import districtcourtparser
import sys
from districtcourtopener import DistrictCourtOpener
from time import sleep

class DistrictCourtReader:
    def __init__(self):
        self.fips_code = ''
        self.opener = DistrictCourtOpener()
    
    def connect(self):
        soup = self.opener.open_welcome_page()
        self.court_names = districtcourtparser.parse_court_names(soup)
    
    def change_court(self, fips_code):
        if fips_code != self.fips_code:
            name = self.court_names[fips_code]
            self.opener.change_court(name, fips_code)
            self.fips_code = fips_code
    
    def get_cases_by_date(self, fips_code, date):
        self.change_court(fips_code)
        self.opener.open_hearing_date_search(fips_code)
        sleep(1)
        
        date = date.strftime('%m/%d/%Y')
        print '\tSearching ' + self.court_names[fips_code] + \
              ' for cases on ' + date
        soup = self.opener.do_hearing_date_search(fips_code, date, True)
        sleep(1)
        
        cases = []
        while True:
            cases.extend(districtcourtparser.parse_hearing_date_search(soup))
            print '\tFound ' + str(len(cases)) + ' cases\r',
            sys.stdout.flush()
            if not districtcourtparser.next_button_found(soup):
                break
            sleep(1)
            soup = self.opener.do_hearing_date_search(fips_code, date, False)
        return cases
    
    def get_case_details(self, case):
        sleep(1)
        soup = self.opener.open_case_details(case)
        return districtcourtparser.parse_case_details(soup)
