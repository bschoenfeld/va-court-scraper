import sys
from time import sleep
from datetime import datetime
from courtutils.databases.postgres import PostgresDatabase
from courtreader import readers
from courtutils.logger import get_logger

log = get_logger()
reader = readers.DistrictCourtReader()
db = PostgresDatabase('district')

def update_case(fips):
    cases_to_fix = db.get_cases_with_no_past_due(fips, 'criminal')
    for case_to_fix in cases_to_fix:
        time_cap_1 = datetime.now()
        case = {
            'fips': fips,
            'case_number': case_to_fix[0],
            'details_fetched_for_hearing_date': case_to_fix[1],
            'collected': datetime.now()
        }
        case['details'] = reader.get_case_details_by_number(
            fips, 'criminal', case_to_fix[0],
            case['details_url'] if 'details_url' in case else None)
        time_cap_2 = datetime.now()
        if 'error' in case['details']:
            log.warn('Could not collect case details for %s in %s',
                     case_to_fix[0], case['fips'])
        else:
            log.info('%s %s', fips, case['details']['CaseNumber'])
            last_case_collected = datetime.now()
            db.replace_case_details(case, 'criminal')
        log.info('%s %s',
                 int((time_cap_2 - time_cap_1).total_seconds()),
                 int((datetime.now() - time_cap_2).total_seconds()))

while True:
    try:
        reader.connect()
        if len(sys.argv) > 1:
            update_case(sys.argv[1])
        else:
            courts = list(db.get_courts())
            for court in courts:
                if court['fips'] == '059':
                    continue
                update_case(court['fips'])
    except Exception:
        print 'Exception. Starting over.'
    except KeyboardInterrupt:
        raise

    try:
        reader.log_off()
    except Exception:
        print 'Could not log off'
    except KeyboardInterrupt:
        raise

    sleep(10)
