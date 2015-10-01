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
