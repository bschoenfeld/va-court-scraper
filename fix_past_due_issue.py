import sys
from datetime import datetime
from courtutils.databases.postgres import PostgresDatabase
from courtreader import readers
from courtutils.logger import get_logger

log = get_logger()
reader = readers.DistrictCourtReader()
reader.connect()
db = PostgresDatabase('district')

def update_case(fips):
    cases_to_fix = db.get_cases_with_no_past_due(fips, 'criminal')
    for case_to_fix in cases_to_fix:
        case = {
            'fips': fips,
            'case_number': case_to_fix[0],
            'details_fetched_for_hearing_date': case_to_fix[1],
            'collected': datetime.now()
        }
        case['details'] = reader.get_case_details_by_number(
            fips, 'criminal', case_to_fix[0],
            case['details_url'] if 'details_url' in case else None)
        if 'error' in case['details']:
            log.warn('Could not collect case details for %s in %s',
                     case_to_fix[0], case['fips'])
        else:
            log.info('%s %s', fips, case['details']['CaseNumber'])
            db.replace_case_details(case, 'criminal')

if len(sys.argv) > 2:
    update_case(sys.argv[2])
else:
    courts = list(db.get_courts())
    for court in courts:
        update_case(court['fips'])
