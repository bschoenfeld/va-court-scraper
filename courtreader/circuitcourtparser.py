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
        if len(strings) < 1:
            continue
        name = strings[0].encode('ascii', 'ignore') \
                         .replace(':', '') \
                         .replace('/', '') \
                         .replace(' ', '')
        if len(strings) > 1:
            case[name] = strings[1].encode('ascii', 'ignore')
        else:
            case[name] = ''

def get_hearings_from_table(case, table):
    case['Hearings'] = []

    rows = table.find_all('tr')
    col_names = [x.encode('ascii') for x in rows.pop(0).stripped_strings]
    col_names[0] = 'Number'
    for row in rows:
        hearing = {}
        for i, col in enumerate(row.find_all('td')):
            val = col.get_text(strip=True) \
                     .encode('ascii', 'ignore')
            hearing[col_names[i]] = val
        #hearing_dt = hearing['Date'] + hearing['Time']
        #hearing['Datetime'] = datetime.strptime(hearing_dt, "%m/%d/%Y%I:%M%p")
        case['Hearings'].append(hearing)

def parse_case_details(soup):
    try:
        case_details = {}
        if soup.find(text=re.compile('Case not found')) is not None:
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
        get_hearings_from_table(case_details, hearings_table)

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

        for li in soup.find_all('li'):
            line = [x.encode('ascii', 'ignore') for x in li.stripped_strings]
            case_details[line[0].replace(':','')] = line[1] \
                if len(line) > 1 else ''

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
                return True
            cases.append({
                'case_number': case_number,
                'defendant': defendant.strip()
            })
        return previous_cases_count == len(cases)
    except:
        handle_parse_exception(soup)
        raise
