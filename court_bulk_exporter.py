from courtreader import readers
from courtutils.database import Database
from courtutils.logger import get_logger
from datetime import datetime, timedelta
import csv
import pymongo
import os
import sys
import time

fieldnames = [
    'court_fips',
    'Court',
    'CaseNumber',
    'Locality',
    'Commencedby',
    'Filed',
    'Defendant',
    'AKA',
    'AKA2',
    'DOB',
    'Sex',
    'Race',
    'Address',
    'OffenseDate',
    'ArrestDate',
    'Class',
    'ConcludedBy',
    'Charge',
    'ChargeType',
    'AmendedCharge',
    'AmendedChargeType',
    'CodeSection',
    'AmendedCodeSection',
    'DispositionCode',
    'DispositionDate',
    'LifeDeath',
    'SentenceTime',
    'SentenceSuspended',
    'ConcurrentConsecutive',
    'JailPenitentiary',
    'ProbationTime',
    'ProbationType',
    'ProbationStarts',
    'RestitutionAmount',
    'RestitutionPaid',
    'FineAmount',
    'Costs',
    'FinesCostPaid',
    'TrafficFatality',
    'DriverImprovementClinic',
    'CourtDMVSurrender',
    'OperatorLicenseSuspensionTime',
    'DrivingRestrictions',
    'RestrictionStartDate',
    'RestrictionEndDate',
    'VAAlcoholSafetyAction',
    'ProgramType',
    'Military'
]

excluded_fields = [
    '_id', 'details', 'details_fetched', 'case_number',
    'details_fetched_for_hearing_date', 'Hearings', 'defendant'
]

def get_db_connection():
    return pymongo.MongoClient(os.environ['MONGO_DB'])['va_court_search']

courts = list(Database.get_circuit_courts())
courts_by_fips = {court['fips_code']:court for court in courts}

db = get_db_connection()
court_fips = '013'

cases = db.circuit_court_detailed_cases.find({
    'court_fips': court_fips
})

with open('./circuit_court_cases_' + court_fips + '.csv', 'w') as csvfile:
    writer = csv.DictWriter(csvfile, fieldnames=sorted(fieldnames))
    writer.writeheader()
    for case in cases:
        case['Court'] = courts_by_fips[case['court_fips']]['name']
        for detail in case['details']:
            new_key = detail.replace(' ', '')
            case[new_key] = case['details'][detail]
        for field in excluded_fields:
            if field in case:
                del case[field]
        writer.writerow(case)
