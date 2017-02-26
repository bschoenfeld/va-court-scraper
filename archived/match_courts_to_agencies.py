import csv
import sys
from itertools import groupby
from geopy.geocoders import GoogleV3, Nominatim
from courtutils.database import Database

def sanitize_agency(agency):
    return agency.replace("`", "'") \
                 .replace("Police Department", "") \
                 .replace("Sheriff's Office", "") \
                 .replace("Sheriffs Office", "") \
                 .replace("PD", "") \
                 .replace("Pd", "") \
                 .replace("Police Dept.", "") \
                 .replace("Police Dept", "") \
                 .replace("Division of Police", "") \
                 .replace("Police", "") \
                 .replace("/ Security", "") \
                 .replace("& ", "") \
                 .strip()

geolocator_google = GoogleV3(api_key='AIzaSyApqcChB-VAfVx3KqzfW2NvYAfHFyrxRuc')
geolocator_osm = Nominatim()
courts = list(Database.get_circuit_courts())
court_names = [court['name'] for court in courts]

rows = []
with open(sys.argv[1]) as csvfile:
    reader = csv.DictReader(csvfile)
    for row in reader:
        rows.append(row)
fieldnames = rows[0].keys()

agencies = [key for key, group in groupby(rows, lambda x: x['agency'])]
agencies_to_court = {}
sanitized_agencies_to_court = {}
for agency in agencies:
    sanitized_agency = sanitize_agency(agency)
    if sanitized_agency in sanitized_agencies_to_court:
        print 'FOUND SANITIZED AGENCY'
        agencies_to_court[agency] = sanitized_agencies_to_court[sanitized_agency]
    agencies_to_court[agency] = None
    for court_name in court_names:
        if court_name.replace(' Circuit Court', '') == sanitized_agency.replace(' County', '') or \
           court_name.replace(' Circuit Court', '') == sanitized_agency.replace(' City', ''):
            agencies_to_court[agency] = court_name
            sanitized_agencies_to_court[sanitized_agency] = court_name
            break

for agency in agencies_to_court:
    if agencies_to_court[agency] is None:
        sanitized_agency = sanitize_agency(agency)
        if sanitized_agency in sanitized_agencies_to_court:
            print 'FOUND SANITIZED AGENCY'
            agencies_to_court[agency] = sanitized_agencies_to_court[sanitized_agency]
        geocoder = 'OSM'
        location = geolocator_osm.geocode(sanitized_agency + ', Virginia, USA')
        if location is None:
            print agency, 'GEOCODING FAILED'
            continue
        nearest_court = Database.get_closest_court('circuit', \
                            location.latitude, location.longitude)
        print agency, 'GEOCODED BY', geocoder, nearest_court['name']
        agencies_to_court[agency] = nearest_court['name']
        sanitized_agencies_to_court[sanitized_agency] = nearest_court['name']

fieldnames.append('courtName')
with open(sys.argv[1]) as csvinfile:
    with open(sys.argv[1] + 'out.csv', 'w') as csvoutfile:
        reader = csv.DictReader(csvinfile)
        writer = csv.DictWriter(csvoutfile, fieldnames=fieldnames)
        writer.writeheader()
        for row in reader:
            row['courtName'] = None
            if row['agency'] in agencies_to_court:
                row['courtName'] = agencies_to_court[row['agency']]
            writer.writerow(row)
