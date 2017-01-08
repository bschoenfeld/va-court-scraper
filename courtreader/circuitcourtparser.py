import re
from datetime import datetime

def handle_parse_exception(soup):
    print '\nException parsing HTML.', \
          'Probably contained something unexpected.', \
          'Check unexpected_output.html'
    with open('unexpected_output.html', 'wb') as output:
        output.write(soup.prettify().encode('UTF-8'))

def parse_court_names(soup):
    try:
        courts = {}
        for option in soup.find_all('option'):
            fips_code = option['value'][:3]
            courts[fips_code] = {
                'full_name': option['value'],
                'name': option['value'][5:]
            }
        return courts
    except:
        handle_parse_exception(soup)
        raise

def get_data_from_table(case, table):
    table_cells = table.find_all('td')
    for cell in table_cells:
        strings = list(cell.stripped_strings)
        if len(strings) < 2:
            continue
        name = strings[0].encode('ascii', 'ignore') \
                         .replace(':', '') \
                         .replace('/', '') \
                         .replace(' ', '')
        case[name] = strings[1].encode('ascii', 'ignore')

DATES = [
    'Filed',
    'OffenseDate',
    'ArrestDate',
    'DispositionDate',
    'RestrictionEffectiveDate',
    'RestrictionEndDate',
    'AppealedDate',
    'DateServed',
    'HearDate',
    'DOB',
    'DateOrderedToMediation',
    'FinalOrderDate'
]

TIME_SPANS = [
    'SentenceTime',
    'SentenceSuspended',
    'OperatorLicenseSuspensionTime',
    'ProbationTime'
]

MONETARY = [
    'Costs',
    'FineAmount',
    'RestitutionAmount'
]

BOOL = [
    'FinesCostPaid',
    'DriverImprovementClinic',
    'DrivingRestrictions',
    'VAAlcoholSafetyAction',
    'RestitutionPaid',
    'TrafficFatality',
    'FilingFeePaid'
]

def get_data_from_table_with_rows(table, court_type):
    date_format = '%m/%d/%Y'
    if court_type == 'civil':
        date_format = '%m/%d/%y'
    data = []
    rows = table.find_all('tr')
    col_names = [x.encode('ascii') for x in rows.pop(0).stripped_strings]
    if '#' in col_names[0]:
        col_names[0] = 'Number'
    for row in rows:
        item = {}
        for i, col in enumerate(row.find_all('td')):
            key = col_names[i].encode('ascii', 'ignore') \
                         .replace(':', '') \
                         .replace('/', '') \
                         .replace(' ', '')
            val = col.get_text(strip=True) \
                     .encode('ascii', 'ignore')
            if val == '':
                continue
            if key in DATES:
                val = datetime.strptime(val, date_format)
            item[key] = val
        if 'Time' in item:
            full_dt = '{} {}'.format(item['Date'], item['Time'])
            item['Date'] = datetime.strptime(full_dt, date_format + ' %I:%M%p')
            del item['Time']
        if 'Number' in item:
            del item['Number']
        if 'Jury' in item:
            item['Jury'] = True if item['Jury'].upper() == 'YES' else False
        data.append(item)
    return data

def parse_pleadings_table(soup, court_type):
    pleadings_table = soup.find(id='count')
    return get_data_from_table_with_rows(pleadings_table, court_type)

def parse_services_table(soup, court_type):
    services_table = soup.find(id='count')
    return get_data_from_table_with_rows(services_table, court_type)

def parse_case_details(soup):
    try:
        case_details = {}
        if soup.find(text=re.compile('Case not found')) is not None:
            case_details['error'] = 'case_not_found'
            return case_details
        if soup.find(text=re.compile('Please enter a valid Case Number')) is not None:
            case_details['error'] = 'case_not_found'
            return case_details
        tables = soup.find_all('table')
        details_table = tables[4]
        hearings_table = tables[6]
        final_disposition_table = tables[8]
        sentencing_table = tables[9]
        appeal_table = tables[10]

        get_data_from_table(case_details, details_table)
        get_data_from_table(case_details, final_disposition_table)
        get_data_from_table(case_details, sentencing_table)
        get_data_from_table(case_details, appeal_table)

        if 'DOB' in case_details:
            case_details['DOB'] = case_details['DOB'].replace('****', '1004')

        for key in DATES:
            if key in case_details:
                case_details[key] = datetime.strptime(case_details[key], '%m/%d/%Y')

        for key in TIME_SPANS:
            if key in case_details:
                case_details[key] = simplify_time_str_to_days(case_details[key])

        for key in MONETARY:
            if key in case_details:
                case_details[key] = float(case_details[key].replace('$', '').replace(',', ''))

        for key in BOOL:
            if key in case_details:
                case_details[key] = True if case_details[key].upper() == 'YES' else False

        if 'ConcurrentConsecutive' in case_details:
            if 'Consecutively' in case_details['ConcurrentConsecutive']:
                case_details['ConcurrentConsecutive'] = 'Consecutive'
            if 'Concurrently' in case_details['ConcurrentConsecutive']:
                case_details['ConcurrentConsecutive'] = 'Concurrent'

        if 'ProbationStarts' in case_details:
            if 'Release' in case_details['ProbationStarts']:
                case_details['ProbationStarts'] = 'Release'
            if 'Sentencing' in case_details['ProbationStarts']:
                case_details['ProbationStarts'] = 'Sentencing'

        case_details['Hearings'] = get_data_from_table_with_rows(hearings_table, 'criminal')

        return case_details
    except:
        handle_parse_exception(soup)
        raise

def parse_civil_case_details(soup):
    try:
        case_details = {}
        if soup.find(text=re.compile('Case not found')) is not None:
            case_details['error'] = 'case_not_found'
            return case_details
        tables = soup.find_all('table')
        details_table = tables[4]
        get_data_from_table(case_details, details_table)

        hearings_table = tables[12]
        case_details['Hearings'] = get_data_from_table_with_rows(hearings_table, 'civil')

        case_details['Plaintiffs'] = []
        case_details['Defendants'] = []

        for li in soup.find_all('li'):
            line = [x.encode('ascii', 'ignore') for x in li.stripped_strings]
            if len(line) < 2:
                continue
            key = line[0].replace(':', '')
            if 'Plaintiff' in key or 'Defendant' in key:
                party = {'Name': line[1]}
                for l in line:
                    if 'Trading as:' in l and l != 'Trading as:':
                        party['TradingAs'] = l.replace('Trading as:', '')
                    if 'Attorney:' in l and l != 'Attorney:':
                        party['Attorney'] = l.replace('Attorney:', '')

                if 'Plaintiff' in key:
                    case_details['Plaintiffs'].append(party)
                if 'Defendant' in key:
                    case_details['Defendants'].append(party)
            else:
                case_details[key] = line[1]

        for key in DATES:
            if key in case_details:
                case_details[key] = datetime.strptime(case_details[key], '%m/%d/%y')

        for key in BOOL:
            if key in case_details:
                case_details[key] = True if case_details[key].upper() == 'YES' else False

        if 'NumberofDefendants' in case_details:
            case_details['NumberofDefendants'] = int(case_details['NumberofDefendants'])
        if 'NumberofPlaintiffs' in case_details:
            case_details['NumberofPlaintiffs'] = int(case_details['NumberofPlaintiffs'])

        return case_details
    except:
        handle_parse_exception(soup)
        raise

def parse_name_search(soup, name, cases):
    try:
        for row in soup.find(class_='nameList').find_all('tr'):
            cols = row.find_all('td')
            if len(cols) > 4:
                if name not in cols[1].string:
                    return True
                cases.append({
                    'case_number': cols[0].span.a.string.strip(),
                    'name': cols[1].string.strip(),
                    'charge': cols[2].string.strip(),
                    'date': cols[3].string.strip(),
                    'status': cols[4].string.strip()
                })
            elif len(cols) > 3:
                if name not in cols[1].get_text() and name not in \
                        cols[2].get_text():
                    return True
                cases.append({
                    'case_number': cols[0].span.a.string.strip(),
                    'name': cols[1].get_text(),
                    'other_name': cols[2].get_text(),
                    'status': cols[3].string.strip()
                })
        return False
    except:
        handle_parse_exception(soup)
        raise

def parse_date_search(soup, cases):
    try:
        case_numbers = [case['case_number'] for case in cases]
        previous_cases_count = len(cases)
        for row in soup.find(class_='nameList').find_all('tr'):
            cols = row.find_all('td')
            if len(cols) < 4:
                continue
            case_number = cols[0].span.a.string
            defendant = cols[1].string
            if case_number is None or defendant is None:
                continue
            case_number = case_number.strip()
            if case_number in case_numbers:
                continue
            cases.append({
                'case_number': case_number,
                'defendant': defendant.strip()
            })
        return previous_cases_count == len(cases)
    except:
        handle_parse_exception(soup)
        raise

def simplify_time_str_to_days(time_string):
    time_string = time_string.replace(' Year(s)', 'Years ') \
                             .replace(' Month(s)', 'Months ') \
                             .replace(' Day(s)', 'Days ')
    days = 0
    string_parts = time_string.split(' ')
    for string_part in string_parts:
        if 'Years' in string_part:
            days += int(string_part.replace('Years', '')) * 365
        elif string_part == '12Months':
            days += 365
        elif 'Months' in string_part:
            days += int(string_part.replace('Months', '')) * 30
        elif 'Days' in string_part:
            days += int(string_part.replace('Days', ''))
        elif 'Hours' in string_part:
            hours = int(string_part.replace('Hours', ''))
            if hours > 0:
                days += 1
    return days
