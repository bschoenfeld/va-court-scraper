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
    try:
        cells = list(soup.find('td', text=re.compile('Case Number')) \
                         .parent.find_all('td'))
        case_number = cells[1].text.strip()
        filed_date = datetime.strptime(cells[3].text.strip(), "%m/%d/%Y")
    
        cells = list(soup.find('td', text=re.compile('Name')) \
                         .parent.find_all('td'))
        name = cells[1].text.strip()
    
        cells = list(soup.find('td', text=re.compile('Case Type')) \
                         .parent.find_all('td'))
        case_type = cells[3].text.strip()
        offense_class = cells[5].text.strip()
        return {
            'case_number': case_number,
            'name': name,
            'filed_date': filed_date,
            'case_type': case_type,
            'offense_class': offense_class
        }
    except:
        handle_parse_exception(soup)
        raise
