import csv
import os
import pymongo

fips_code = '700'

# Connect to database
client = pymongo.MongoClient(os.environ['CIRCUIT_DB'])
db = client.va_circuit_court_cases
cases = db.cases.find({
    'FIPSCode': fips_code,
    'date_collected': {'$exists': True}
}, projection = {
    '_id': False,
    'error': False,
    'date_collected': False,
    'Hearings': False,
})
filename = 'circuit_court_' + fips_code + '.csv'
with open(filename, 'w') as f:
    fieldnames = [
        'FIPSCode', 'CaseNumber', 'Locality', 'Commencedby', 'Filed',
        'Defendant', 'AKA', 'AKA2', 'DOB', 'Sex', 'Race', 'Address',
        'OffenseDate', 'ArrestDate', 'Class', 'ConcludedBy',
        'Charge', 'ChargeType', 'AmendedCharge', 'AmendedChargeType',
        'CodeSection', 'AmendedCodeSection',
        'DispositionCode', 'DispositionDate',
        'LifeDeath', 'SentenceTime', 'SentenceSuspended',
        'ConcurrentConsecutive', 'JailPenitentiary',
        'ProbationTime', 'ProbationType', 'ProbationStarts',
        'RestitutionAmount', 'RestitutionPaid',
        'FineAmount', 'Costs', 'FinesCostPaid', 'TrafficFatality',
        'DriverImprovementClinic', 'CourtDMVSurrender',
        'OperatorLicenseSuspensionTime',
        'DrivingRestrictions', 'RestrictionStartDate', 'RestrictionEndDate',
        'VAAlcoholSafetyAction', 'ProgramType', 'Military'
    ]
    writer = csv.DictWriter(f, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerows(cases)
