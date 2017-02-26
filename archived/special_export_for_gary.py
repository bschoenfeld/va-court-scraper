from courtutils.database import Database
from pprint import pprint
import csv

def clean_names(case, field):
    case[field] = ' '.join(case[field].split())
    name_parts = case[field].split(': ')
    case[name_parts[0]] = name_parts[1]

excluded_fields = [
    '_id', 'details', 'details_fetched', 'case_number',
    'status', 'name', 'other_name', 'search_id', 'fetched'
]

force_fieldnames = [
    'Plaintiff2', 'Plaintiff3', 'Defendant2', 'Defendant3', 'FilingFeePaid'
]

courts = list(Database.get_circuit_courts())
courts_by_fips = {court['fips_code']:court for court in courts}
fieldnames = None
writer = None

with open('./data.csv', 'w') as csvfile:
    for case in Database.get_all_cases():
        clean_names(case, 'name')
        clean_names(case, 'other_name')
        case['Status'] = case['status']
        case['Court'] = courts_by_fips[case['fips_code']]['name']
        for detail in case['details']:
            new_key = detail.replace(' ', '')
            if new_key == 'Plaintiff': new_key += '1'
            if new_key == 'Defendant': new_key += '1'
            case[new_key] = case['details'][detail]
        for field in excluded_fields:
            if field in case:
                del case[field]
        if fieldnames is None:
            fieldnames = case.keys()
            for fieldname in force_fieldnames:
                if fieldname not in fieldnames:
                    fieldnames.append(fieldname)
            writer = csv.DictWriter(csvfile, fieldnames=sorted(fieldnames))
            writer.writeheader()
        writer.writerow(case)
