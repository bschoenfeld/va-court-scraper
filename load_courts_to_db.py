import os
import pymongo
from geopy.geocoders import GoogleV3
from courtreader import readers

# Connect to database
db = pymongo.MongoClient(os.environ['MONGO_DB'])['va_court_search']
geolocator = GoogleV3(api_key=os.environ['GOOGLE_API_KEY'])

print 'CIRCUIT COURT'
db.circuit_courts.drop()
reader = readers.CircuitCourtReader()
courts = reader.connect()
court_names = []
for fips_code, court in courts.iteritems():
    print court['name']
    court_locality = court['name'].replace(' Circuit Court', '')
    location = geolocator.geocode(court_locality + ', Virginia, USA')
    db.circuit_courts.insert_one({
        'name': court['name'],
        'fips_code': fips_code,
        'location': {'type': 'Point', 'coordinates': [
            location.longitude, location.latitude
        ]}
    })
    court_names.append(court['name'] + ' ' + fips_code)
db.circuit_courts.create_index([('location', pymongo.GEOSPHERE)], \
                                background=True)
'''
court_names.sort()
for court_name in court_names:
    print court_name
'''

print 'DISTRICT COURT'
db.district_courts.drop()
reader = readers.DistrictCourtReader()
courts = reader.connect()
court_names = []
for fips_code, court in courts.iteritems():
    print court
    court_locality = court.replace(' General District Court', '')\
                          .replace('-Criminal', '')\
                          .replace('-Civil', '')\
                          .replace('-Traffic', '')
    location = geolocator.geocode(court_locality + ', Virginia, USA')
    db.district_courts.insert_one({
        'name': court,
        'fips_code': fips_code,
        'location': {'type': 'Point', 'coordinates': [
            location.longitude, location.latitude
        ]}
    })
    court_names.append(court + ' ' + fips_code)
db.district_courts.create_index([('location', pymongo.GEOSPHERE)], \
                                 background=True)
'''
court_names.sort()
for court_name in court_names:
    print court_name
'''
