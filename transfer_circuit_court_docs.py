import datetime
import pymongo
import os
from courtreader import readers

# Connect to database
client = pymongo.MongoClient(os.environ['CIRCUIT_DB'])
db = client.va_circuit_court_cases

reader = readers.CircuitCourtReader()
courts = reader.connect()
court_codes = {}
for fips_code, court in courts.iteritems():
    court_name = court['name'].replace(' Circuit Court', '')
    court_codes[court_name] = fips_code

cases = db.case_numbers.find()
cases_in_brief = []
for case in cases:
    case_in_brief = {
        'CaseNumber': case['case_number'],
        'FIPSCode': court_codes[case['court']]
    }
    cases_in_brief.append(case_in_brief)
    if len(cases_in_brief) >= 10000:
        print 10000
        db.cases.insert_many(cases_in_brief)
        del cases_in_brief[:]
print len(cases_in_brief)
db.cases.insert_many(cases_in_brief)
print 'Finished'
