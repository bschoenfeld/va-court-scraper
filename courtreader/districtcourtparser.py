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
        # Load list of courts and fips codes
        fips = [tag['value'] for tag in soup.find_all('input',
            {'name':'courtFips'})]
        names = [tag['value'] for tag in soup.find_all('input',
            {'name':'courtName'})]
        court_names = {}
        for f, c in zip(fips, names):
            court_names[f] = c
        return court_names
    except:
        handle_parse_exception(soup)
        raise

def parse_name_search(soup):
    try:
        no_results = re.compile(r'No results found for the search criteria')
        if soup.find('td', text=no_results) is not None:
            return []
        cases = []
        rows = soup.find('table', {'class':'tableborder'}).find_all('tr')
        for row in rows:
            cells = row.find_all('td')
            if cells[0]['class'][0] == 'gridheader':
                continue
            case_number = cells[1].a.text.strip()
            defendant = cells[2].string.strip()
            charge = cells[5].string.strip()
            cases.append({
                'case_number': case_number,
                'defendant': defendant,
                'charge': charge
            })
        return cases
    except:
        handle_parse_exception(soup)
        raise

def next_names_button_found(soup):
    try:
        return soup.find('input', {'title': 'Next'}) is not None
    except:
        handle_parse_exception(soup)
        raise

def parse_hearing_date_search(soup, case_type):
    try:
        no_results = re.compile(r'No results found for the search criteria')
        if soup.find('td', text=no_results) is not None:
            return []
        cases = []
        rows = soup.find('table', {'class':'tableborder'}).find_all('tr')
        for row in rows:
            cells = row.find_all('td')
            if cells[0]['class'][0] == 'gridheader':
                continue
            details_url = cells[1].a['href']
            case_number = list(cells[1].a.stripped_strings)[0]
            defendant_cell_content = list(cells[2].stripped_strings)
            defendant = defendant_cell_content[0] if len(defendant_cell_content) > 0 else ''
            if case_type == 'civil':
                plaintiff_cell_content = list(cells[3].stripped_strings)
                plaintiff = plaintiff_cell_content[0] if len(plaintiff_cell_content) > 0 else ''
                civil_case_type_content = list(cells[4].stripped_strings)
                civil_case_type = civil_case_type_content[0] if len(civil_case_type_content) > 0 else ''
                hearing_time_content = list(cells[5].stripped_strings)
                hearing_time = hearing_time_content[0] if len(hearing_time_content) > 0 else ''
                cases.append({
                    'case_number': case_number,
                    'details_url': details_url,
                    'defendant': defendant,
                    'plaintiff': plaintiff,
                    'civil_case_type': civil_case_type,
                    'hearing_time': hearing_time
                })
            else:
                status_cell_content = list(cells[6].stripped_strings)
                status = status_cell_content[0] if len(status_cell_content) > 0 else ''
                cases.append({
                    'case_number': case_number,
                    'details_url': details_url,
                    'defendant': defendant,
                    'status': status
                })
        return cases
    except:
        handle_parse_exception(soup)
        raise

def next_button_found(soup):
    try:
        return soup.find('input', {'name': 'caseInfoScrollForward'}) is not None
    except:
        handle_parse_exception(soup)
        raise

def parse_case_details(soup, case_type):
    case_details = {}
    try:
        case_details['CourtName'] = soup.find(id='headerCourtName') \
                                        .string.strip()
        # Parse grids
        for label_cell in soup.find_all(class_=re.compile('labelgrid')):
            value_cell = label_cell.next_sibling
            while value_cell.name != 'td':
                value_cell = value_cell.next_sibling
            label = get_string_from_cell(label_cell, True)
            value = get_string_from_cell(value_cell)
            case_details[label] = value
        # Parse tables
        if case_type == 'civil':
            # the table names really are backwards here
            case_details['Plaintiffs'] = parse_table(soup, 'toggleDef')
            case_details['Defendants'] = parse_table(soup, 'togglePlaintiff')
            case_details['Reports'] = parse_table(soup, 'toggleReports')
        case_details['Hearings'] = parse_table(soup, 'toggleHearing')
        case_details['Services'] = parse_table(soup, 'toggleServices')
        if 'CaseNumber' not in case_details:
            raise ValueError('Missing Case Number')
    except:
        handle_parse_exception(soup)
        raise
    return case_details

def get_string_from_cell(cell, is_label=False):
    values = list(cell.stripped_strings)
    if len(values) < 1: return ''
    value = values[0].encode('ascii', 'ignore') \
                     .strip() \
                     .replace('\t', '') \
                     .replace('\r', '') \
                     .replace('\n', '')
    if is_label:
        value = value.replace(':', '') \
                     .replace('/', '') \
                     .replace(' ', '')
    return value

def parse_table(soup, table_id):
    table_contents = []
    table_section = soup.find(id=table_id)
    table_headers = list(table_section.find(class_='gridheader') \
                                      .stripped_strings)
    for row in table_section.find_all(class_='gridrow'):
        table_contents.append(parse_table_row(row, table_headers))
    for row in table_section.find_all(class_='gridalternaterow'):
        table_contents.append(parse_table_row(row, table_headers))
    return table_contents

def parse_table_row(row, table_headers):
    return dict(zip(
        table_headers,
        [cell.string.strip()
         if cell.string is not None else ''
         for cell in row.find_all('td')]
    ))

