from datetime import datetime, date, timedelta
from itertools import groupby
from operator import itemgetter
from pprint import pprint
from courtutils.databases.postgres import PostgresDatabase
from fuzzywuzzy import fuzz
from joblib import Parallel, delayed
import multiprocessing

def run():
    cur_date = date(1004, 1, 1)
    dates = []
    while cur_date.year == 1004:
        dates.append(cur_date)
        cur_date += timedelta(days=1)

    cpus = multiprocessing.cpu_count()
    Parallel(n_jobs=1)(delayed(run_for_date)(cur_date) for cur_date in dates)

def run_for_date(cur_date):
    for letter in char_range('A', 'Z'):
        match_people(cur_date, letter, 'Male')
        match_people(cur_date, letter, 'Female')

def char_range(c1, c2):
    """Generates the characters from `c1` to `c2`, inclusive."""
    for c in xrange(ord(c1), ord(c2)+1):
        yield chr(c)

def match_people(date, letter, sex):
    print 'Connecting to DB'
    db = PostgresDatabase('circuit')
    print 'Connected'

    person_id = get_starting_person_id(date, letter, sex)
    people = db.list_people_to_id(date, letter, sex)
    people.sort(key=lambda p: p['name'])
    print date, letter, sex, len(people), 'Cases'

    last_id = 0
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
            name_a = sanitize_name(people[i]['name'])
            name_b = sanitize_name(people[j]['name'])
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
        if last_id != person['personId']:
            last_id = person['personId']

    print 'Writing to db'
    for key, group in groupby(people, key=itemgetter('courtType', 'personId')):
        case_ids = [x['id'] for x in group]
        db.set_person_id(key[0], case_ids, key[1])
    db.commit()

    print 'Disconnect'
    db.disconnect()

# Person ID
# BigInt Max 9,223,372,036,854,775,807
# Day of year * (1 * 10^12)
# Letter * (1 * 10^10)
# Sex * (1 * 10^9)
def get_starting_person_id(date, letter, sex):
    person_id = date.timetuple().tm_yday * pow(10, 12)
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

run()
