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
        courts = []
        for option in soup.find_all('option'):
            courts.append({
                'fullName': option['value'],
                'fips_code': option['value'][:3],
                'name': option['value'][5:]
            })
        return courts
    except:
        handle_parse_exception(soup)
        raise
