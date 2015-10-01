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

def parse_hearing_date_search(soup):
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
            status_cell_content = list(cells[6].stripped_strings)
            status = ''
            if len(status_cell_content) > 0:
                status = status_cell_content[0]
            cases.append({
                'details_url': details_url,
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

def parse_case_details(soup):
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
        case_details['Hearings'] = parse_table(soup, 'toggleHearing')
        case_details['Services'] = parse_table(soup, 'toggleServices')
    except:
        handle_parse_exception(soup)
        raise
    return case_details

def get_string_from_cell(cell, is_label=False):
    values = list(cell.stripped_strings)
    if len(values) < 1: return ''
    value = values[0].encode('ascii', 'ignore') \
                     .strip()
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
        table_contents.append(dict(zip( \
            table_headers, \
            [cell.string.strip() \
                if cell.string is not None else '' \
                for cell in row.find_all('td')] \
        )))
    return table_contents
