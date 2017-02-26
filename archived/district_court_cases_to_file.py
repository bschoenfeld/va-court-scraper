import csv
import os
import pymongo

fips_code = '013'

# Connect to database
client = pymongo.MongoClient(os.environ['DISTRICT_DB'])
db = client.va_district_court_cases
cases = db.cases.find({
    'FIPSCode': fips_code,
    'date_collected': {'$exists': True}
}, projection = {
    '_id': False,
    'error': False,
    'date_collected': False,
    'Hearings': False,
    'Services': False,
})
filename = 'district_court_' + fips_code + '.csv'
with open(filename, 'w') as f:
    fieldnames = [
        'FIPSCode', 'CaseNumber', 'Locality', 'CourtName', 'FiledDate',
        'Name', 'AKA1', 'AKA2', 'DOB', 'Gender', 'Race', 'Address',
        'OffenseDate', 'ArrestDate', 'Class', 'Status', 'Complainant',
        'CaseType', 'Charge', 'CodeSection',
        'AmendedCaseType', 'AmendedCharge', 'AmendedCode',
        'FinalDisposition', 'DefenseAttorney',
        'SentenceTime', 'SentenceSuspendedTime',
        'ProbationType', 'ProbationTime', 'ProbationStarts',
        'Fine', 'Costs', 'FineCostsDue', 'FineCostsPaid', 'FineCostsPaidDate',
        'OperatorLicenseRestrictionCodes', 'OperatorLicenseSuspensionTime',
        'RestrictionStartDate', 'RestrictionEndDate', 'VASAP'
    ]
    writer = csv.DictWriter(f, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerows(cases)
