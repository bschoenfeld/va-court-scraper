import csv
import os
import sys
from pprint import pprint

def get_parties(case_id, party_reader, last_party_read):
    parties = []
    party = last_party_read
    while True:
        if party is None:
            try:
                party = party_reader.next()
            except StopIteration:
                print 'EOF'
                return (parties, party)
        if party['case_id'] != case_id:
            return (parties, party)
        parties.append(party)
        party = None

def add_parties_to_case(case, parties, party_name):
    for i in range(0, 3):
        if len(parties) > i:
            case[party_name + str(i+1) + 'Name'] = parties[i]['Name']
            case[party_name + str(i+1) + 'Address'] = parties[i]['Address']
            case[party_name + str(i+1) + 'Attorney'] = parties[i]['Attorney']

def get_party_headers(party_name):
    party_headers = ['NumberOf' + party_name + 's']
    for i in range(0, 3):
        party_headers.extend([
            party_name + str(i+1) + 'Name',
            party_name + str(i+1) + 'Address',
            party_name + str(i+1) + 'Attorney'
        ])
    return party_headers

with open(sys.argv[1]) as cases_file, \
     open(sys.argv[2]) as plaintiffs_file, \
     open(sys.argv[3]) as defendants_file:

    case_reader = csv.DictReader(cases_file)
    plaintiff_reader = csv.DictReader(plaintiffs_file)
    defendant_reader = csv.DictReader(defendants_file)
    output_file = None
    output_writer = None
    file_count = 0
    case_count = 0

    last_plaintiff_read = None
    last_defendant_read = None

    for case in case_reader:
        del case['details_fetched_for_hearing_date']
        del case['collected']

        case_count += 1
        if case_count > 100000:
            output_file.close()
            output_writer = None
            case_count = 0

        if output_writer is None:
            headers = sorted(case.keys())
            headers.extend(get_party_headers('Plaintiff'))
            headers.extend(get_party_headers('Defendant'))
            file_count += 1
            filename_parts = os.path.splitext(sys.argv[4])
            filename = '{}_{}{}'.format(
                filename_parts[0],
                str(file_count).zfill(2),
                filename_parts[1]
            )
            output_file = open(filename, 'w')
            output_writer = csv.DictWriter(output_file, fieldnames=headers)
            output_writer.writeheader()

        plaintiffs, last_plaintiff_read = get_parties(
            case['id'], plaintiff_reader, last_plaintiff_read
        )
        defendants, last_defendant_read = get_parties(
            case['id'], defendant_reader, last_defendant_read
        )

        case['NumberOfPlaintiffs'] = len(plaintiffs)
        case['NumberOfDefendants'] = len(defendants)

        add_parties_to_case(case, plaintiffs, 'Plaintiff')
        add_parties_to_case(case, defendants, 'Defendant')

        output_writer.writerow(case)
    output_file.close()
