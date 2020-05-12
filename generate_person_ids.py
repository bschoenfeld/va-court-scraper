import os
import subprocess
import sys
import time
from calendar import monthrange
from csv import DictReader, DictWriter
from datetime import datetime, date, timedelta
from fuzzywuzzy import fuzz

def run(option):
    if option is None or option == 0:
        prepare_database()
    if option == 0:
        return

    if option is None:
        for i in range(1, 13):
            run_month(i)
    else:
        run_month(option)

def run_month(month):
    days = monthrange(1904, month)
    dates = [
        (date(1904, month, 1) + timedelta(days=x)).strftime('%Y-%m-%d').replace('1904', '1004')
        for x in range(0, days[1])
    ]
    process_data(dates)

GENDERS = ['Female', 'Male']
LETTERS = [chr(c) for c in xrange(ord('A'), ord('Z')+1)]

def prepare_database():
    cmd = 'DROP TABLE IF EXISTS person_ids;'
    print subprocess.check_output(['psql', '-c', cmd])

    cmd = 'CREATE TABLE person_ids (person_id bigint, circuit_id bigint, district_id bigint);'
    print subprocess.check_output(['psql', '-c', cmd])

class CourtDataProcessor:
    def __init__(self, court_type, dob_start, dob_end):
        self.court_type = court_type
        self.dob_start = dob_start
        self.in_filepath = '{}_{}_{}.csv'.format(dob_start, dob_end, court_type)

        self.download_data(dob_start, dob_end)

        self.in_file = open(self.in_filepath)
        self.data_reader = DictReader(self.in_file)

        self.last_person = None

    def download_data(self, dob_start, dob_end):
        # PGHOST, PGDATABASE, PGUSER, PGPASSWORD
        if self.court_type == 'district':
            gender_field = 'Gender'
            name_field = 'Name'
            table = 'DistrictCriminalCase'
        else:
            gender_field = 'Sex'
            name_field = 'Defendant'
            table = 'CircuitCriminalCase'

        copy_cmd = '\\copy (SELECT id, "{}", "{}", "DOB", "Address" FROM "{}"'.format(
            gender_field, name_field, table
        )
        copy_cmd += ' WHERE "DOB" >= \'{}\' AND "DOB" <= \'{}\''.format(dob_start, dob_end)
        copy_cmd += ' ORDER BY "{}", "DOB", "{}") To \'{}\' With CSV HEADER;'.format(
            gender_field, name_field, self.in_filepath
        )

        psql_cmd = ['psql', '-c', copy_cmd]
        print self.in_filepath, subprocess.check_output(psql_cmd)

    def close(self):
        self.in_file.close()
        os.remove(self.in_filepath)

    def next_people(self, gender_group, dob_group, letter_group):
        people = []
        while True:
            if self.last_person is not None:
                person = self.last_person
                self.last_person = None
            else:
                try:
                    person = self.data_reader.next()
                except StopIteration:
                    break

            gender = person['Gender'] if 'Gender' in person else person['Sex']
            name = person['Name'] if 'Name' in person else person['Defendant']
            dob = person['DOB']

            if gender not in GENDERS:
                continue
            if name[0] not in LETTERS:
                continue

            if gender == gender_group and dob == dob_group and name.startswith(letter_group):
                people.append({
                    'id': person['id'],
                    'name': name,
                    'address': person['Address'],
                    'courtType': self.court_type
                })
            else:
                self.last_person = person
                break
        return people

class CourtDataWriter:
    def __init__(self, dob_start, dob_end):
        self.out_filepath = '{}_{}_person_ids.csv'.format(dob_start, dob_end)
        self.out_file = open(self.out_filepath, 'w')
        self.data_writer = DictWriter(self.out_file, fieldnames=[
            'person_id', 'circuit_id', 'district_id'
        ], lineterminator='\n')

    def write(self, people):
        self.data_writer.writerows([
            {
                'person_id': person['personId'],
                'circuit_id': person['id'] if person['courtType'] == 'circuit' else None,
                'district_id': person['id'] if person['courtType'] == 'district' else None
            }
            for person in people
        ])

    def upload(self):
        self.out_file.close()

        # psql copy outfile to table
        cmd = '\\COPY person_ids FROM {} CSV;'.format(self.out_filepath)
        print subprocess.check_output(['psql', '-c', cmd])

        os.remove(self.out_filepath)

def process_data(dates):
    district_data_processor = CourtDataProcessor('district', dates[0], dates[-1])
    circuit_data_processor = CourtDataProcessor('circuit', dates[0], dates[-1])
    data_writer = CourtDataWriter(dates[0], dates[-1])

    for gender in GENDERS:
        for dob in dates:
            for letter in LETTERS:
                people = []
                people.extend(district_data_processor.next_people(gender, dob, letter))
                people.extend(circuit_data_processor.next_people(gender, dob, letter))
                people.sort(key=lambda p: p['name'])
                if len(people) > 0:
                    #print gender, dob, letter, '|', people[0]['name'], '|', people[-1]['name']
                    match_people(gender, dob, letter, people)
                    data_writer.write(people)

    district_data_processor.close()
    circuit_data_processor.close()
    data_writer.upload()

def match_people(gender, dob, letter, people):
    person_id = get_starting_person_id(dob, letter, gender)
    print dob, letter, gender, len(people), 'Cases'
    sys.stdout.flush()

    for person in people:
        person['sName'] = sanitize_name(person['name'])

    for i, person in enumerate(people):
        if 'personId' not in person:
            person['personId'] = person_id
            person['score'] = -1
            person['lastAddresses'] = set()
            person_id += 1
        j = i + 1
        while True:
            if j >= len(people):
                break
            if people[i]['name'][:2] != people[j]['name'][:2]:
                break
            name_a = people[i]['sName']
            name_b = people[j]['sName']
            score = fuzz.partial_ratio(name_a, name_b)
            if score >= 90:
                people[j]['personId'] = people[i]['personId']
                people[j]['score'] = score
                people[j]['lastAddresses'] = people[i]['lastAddresses']
                people[j]['lastAddresses'].add(people[i]['address'])
                break
            elif score >= 80:
                best_address_score = fuzz.partial_ratio(people[i]['address'], people[j]['address'])
                for address in people[i]['lastAddresses']:
                    new_address_score = fuzz.partial_ratio(address, people[j]['address'])
                    if new_address_score > best_address_score:
                        best_address_score = new_address_score
                if best_address_score >= 80:
                    people[j]['personId'] = people[i]['personId']
                    people[j]['score'] = score
                    people[j]['lastAddresses'] = people[i]['lastAddresses']
                    people[j]['lastAddresses'].add(people[i]['address'])
                    break
            j += 1
        del person['lastAddresses']
        #print person['personId'], person['name'], person['id']

# Person ID
# BigInt Max 9,223,372,036,854,775,807
# Day of year * (1 * 10^12)
# Letter * (1 * 10^10)
# Sex * (1 * 10^9)
def get_starting_person_id(dob, letter, sex):
    person_id = datetime.strptime(dob, '%Y-%m-%d').date().timetuple().tm_yday * pow(10, 12)
    person_id += (ord(letter) - ord('A')) * pow(10, 10)
    if sex == 'Female':
        person_id += 1 * pow(10, 9)
    return person_id

def sanitize_name(name):
    name = name.replace('3RD', 'III').replace('.', '')
    if ';' in name:
        name = name[:name.index(';')]
    if '  ' in name:
        name = name[:name.index('  ')]
    return name

if __name__ == '__main__':
    param = None
    if len(sys.argv) > 1:
        param = int(sys.argv[1])
    run(param)
