import os
import pymongo
from geopy.geocoders import GoogleV3
from courtreader import readers
#from courtutils.databases.mongo import MongoDatabase
from courtutils.databases.postgres import PostgresDatabase

geolocator = GoogleV3(api_key=os.environ['GOOGLE_API_KEY'])

print 'CIRCUIT COURT'
#circuit_db = MongoDatabase('va_court_search', 'circuit')
circuit_db = PostgresDatabase('va_court_search', 'circuit')
circuit_db.drop_courts()
reader = readers.CircuitCourtReader()
courts = reader.connect()
court_names = []
for fips_code, court in courts.iteritems():
    print court['name']
    court_locality = court['name'].replace(' Circuit Court', '')
    location = geolocator.geocode(court_locality + ', Virginia, USA')
    circuit_db.add_court(court['name'], fips_code, location)
    court_names.append(court['name'] + ' ' + fips_code)
circuit_db.add_court_location_index()
circuit_db.commit()
exit()
'''
court_names.sort()
for court_name in court_names:
    print court_name
'''

print 'DISTRICT COURT'
district_db = MongoDatabase('va_court_search', 'district')
district_db.drop_courts()
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
    district_db.add_court(court, fips_code, location)
    court_names.append(court + ' ' + fips_code)
district_db.add_court_location_index()

'''
court_names.sort()
for court_name in court_names:
    print court_name
'''
