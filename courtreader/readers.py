import circuitcourtparser
import districtcourtparser
import logging
import sys
from circuitcourtopener import CircuitCourtOpener
from districtcourtopener import DistrictCourtOpener
from time import sleep

log = logging.getLogger('logentries')

class DistrictCourtReader:
    def __init__(self):
        self.fips_code = ''
        self.opener = DistrictCourtOpener()

    def connect(self):
        soup = self.opener.open_welcome_page()
        self.court_names = districtcourtparser.parse_court_names(soup)
        return self.court_names

    def change_court(self, fips_code):
        if fips_code != self.fips_code:
            name = self.court_names[fips_code]
            self.opener.change_court(name, fips_code)
            self.fips_code = fips_code
            sleep(1)

    def get_case_details_by_number(self, fips_code, case_number):
        self.change_court(fips_code)
        soup = self.opener.do_case_number_search(fips_code, case_number)
        return districtcourtparser.parse_case_details(soup)

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

    def get_cases_by_name(self, fips_code, case_type, name):
        self.change_court(fips_code)
        self.opener.open_name_search(fips_code)
        sleep(1)
        cases = []
        count = 0
        found_cases = None
        while True:
            soup = self.opener.do_name_search(fips_code, name, count, found_cases)
            found_cases = districtcourtparser.parse_name_search(soup)
            cases.extend(found_cases)
            if not districtcourtparser.next_names_button_found(soup):
                break
            log.info('Next Names Page')
            print 'Next Names Page'
            count += 1
            sleep(2)
        return cases

class CircuitCourtReader:
    def __init__(self):
        self.fips_code = ''
        self.opener = CircuitCourtOpener()
        self.searches_on_session = 0

    def manage_opener(self):
        self.searches_on_session += 1
        if self.searches_on_session > 25:
            print 'RESETTING OPENER'
            self.log_off()
            sleep(2)
            self.connect()
            self.searches_on_session = 0
            self.fips_code = ''
            print 'RESET SUCCESSFUL'

    def connect(self):
        soup = self.opener.open_welcome_page()
        self.courts = circuitcourtparser.parse_court_names(soup)
        return self.courts

    def log_off(self):
        self.opener.log_off()

    def change_court(self, fips_code):
        if fips_code != self.fips_code:
            print 'Changing court'
            self.opener.change_court(fips_code, \
                                     self.courts[fips_code]['full_name'])
            self.fips_code = fips_code
            sleep(1)

    def get_case_details_by_number(self, fips_code, case_type, case_number):
        self.manage_opener()
        category_code = 'R'
        if case_type == 'civil':
            category_code = 'CIVIL'
        self.change_court(fips_code)
        soup = self.opener.do_case_number_search(fips_code, case_number, category_code)
        if case_type == 'civil':
            return circuitcourtparser.parse_civil_case_details(soup)
        return circuitcourtparser.parse_case_details(soup)

    def get_cases_by_name(self, fips_code, case_type, name):
        self.manage_opener()
        category_code = 'R'
        if case_type == 'civil':
            category_code = 'CIVIL'
        self.change_court(fips_code)
        cases = []
        soup = self.opener.do_name_search(fips_code, name, category_code)
        all_found = circuitcourtparser.parse_name_search(soup, name, cases)
        while not all_found:
            sleep(1)
            soup = self.opener.continue_name_search(fips_code, category_code)
            all_found = circuitcourtparser.parse_name_search(soup, name, cases)
        return cases

    def get_cases_by_date(self, fips_code, case_type, date):
        self.manage_opener()
        category_code = 'R'
        if case_type == 'civil':
            category_code = 'CIVIL'
        self.change_court(fips_code)
        cases = []
        soup = self.opener.do_date_search(fips_code, date, category_code)
        all_found = circuitcourtparser.parse_date_search(soup, cases)
        print 'FINAL PAGE', all_found
        while not all_found:
            sleep(1)
            soup = self.opener.continue_date_search(fips_code, category_code)
            all_found = circuitcourtparser.parse_date_search(soup, cases)
            print 'FINAL PAGE', all_found
        return cases
