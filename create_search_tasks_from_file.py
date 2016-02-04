import csv
import sys
from courtutils.database import Database

courts = list(Database.get_circuit_courts())
courts_by_name = {court['name']:court for court in courts}

def get_nearby_courts(court_name):
    if 'nearbyCourts' not in courts_by_name[court_name]:
        print 'LOOKING UP COURTS NEAR', court_name
        nearby_courts = Database.find_courts('circuit', court_name, 40)
        courts_by_name[court_name]['nearbyCourts'] = \
            [court['fips_code'] for court in nearby_courts]
    return courts_by_name[court_name]['nearbyCourts']

def get_courts_to_search(row, courts_by_name):
    if 'courtName' not in row or row['courtName'] not in courts_by_name:
        return [court['fips_code'] for court in courts]
    return get_nearby_courts(row['courtName'])

search_id = Database.insert_search()
search_terms = 0
search_tasks = 0
with open(sys.argv[1]) as csvfile:
    reader = csv.DictReader(csvfile)
    for row in reader:
        courts_to_search = get_courts_to_search(row, courts_by_name)
        name = row['last'] + ', ' + row['first']
        search_terms += 1
        print name, len(courts_to_search), 'courts'
        for fips_code in courts_to_search:
            search_tasks += 1
            Database.insert_task(search_id, 'circuit', \
                fips_code, 'civil', name.upper())
        break
Database.update_search(search_id, search_terms, search_tasks)
