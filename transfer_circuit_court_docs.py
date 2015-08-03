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
for court in courts:
    court_codes[court['name'].replace(' Circuit Court', '')] = court['fips_code']

cases = db.case_numbers.find({ \
                    'court': 'Arlington', \
                    'case_number': {'$regex': 'CR1[0-4].*'}\
                })
cases_in_brief = []
for case in cases:
    case_in_brief = {
        'CaseNumber': case['case_number'],
        'FIPSCode': court_codes[case['court']]
    }
    cases_in_brief.append(case_in_brief)
print len(cases_in_brief)
db.cases.insert_many(cases_in_brief)
print 'Finished'
