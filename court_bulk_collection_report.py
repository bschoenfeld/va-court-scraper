import argparse
from datetime import datetime
from courtutils.databases.postgres import PostgresDatabase

def generate_report(start_date_str, end_date_str):
    earlier_date = datetime.strptime(start_date_str, '%Y-%m-%d').date()
    later_date = datetime.strptime(end_date_str, '%Y-%m-%d').date()
    
    total_dates = (later_date - earlier_date).days + 1
    if total_dates <= 0:
        print("End date must be on or after start date.")
        return

    print(f"Generating report for {earlier_date} to {later_date} (Total dates: {total_dates})")
    print("Connecting to databases...\n")

    # Initialize databases
    circuit_db = PostgresDatabase('circuit')
    district_db = PostgresDatabase('district')

    # Get all courts
    circuit_courts_data = circuit_db.get_courts()
    district_courts_data = district_db.get_courts()

    courts_by_fips = {}
    
    for c in circuit_courts_data:
        courts_by_fips[c['fips']] = {'name': c['name'][:30], 'circuit': True, 'district': False}
    for c in district_courts_data:
        if c['fips'] not in courts_by_fips:
            courts_by_fips[c['fips']] = {'name': c['name'][:30], 'circuit': False, 'district': True}
        else:
            courts_by_fips[c['fips']]['district'] = True

    fips_codes = sorted(courts_by_fips.keys())

    # Header
    print(f"{'FIPS':<5} | {'Court Name':<30} | {'District Criminal':<17} | {'District Civil':<14} | {'Circuit Criminal':<16} | {'Circuit Civil':<13}")
    print("-" * 110)

    for fips in fips_codes:
        court = courts_by_fips[fips]
        
        def get_unsearched(db, case_type):
            # get_date_searches expects (fips, case_type, start_date (later), end_date (earlier))
            searched = set(db.get_date_searches(fips, case_type, later_date, earlier_date))
            return max(0, total_dates - len(searched))

        if court['district']:
            dc_crim = get_unsearched(district_db, 'criminal')
            dc_civil = get_unsearched(district_db, 'civil')
        else:
            dc_crim, dc_civil = "-", "-"

        if court['circuit']:
            cc_crim = get_unsearched(circuit_db, 'criminal')
            cc_civil = get_unsearched(circuit_db, 'civil')
        else:
            cc_crim, cc_civil = "-", "-"

        print(f"{fips:<5} | {court['name']:<30} | {str(dc_crim):<17} | {str(dc_civil):<14} | {str(cc_crim):<16} | {str(cc_civil):<13}")

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Generate court bulk collection report.')
    parser.add_argument('start_date', help='Start date (YYYY-MM-DD)')
    parser.add_argument('end_date', help='End date (YYYY-MM-DD)')
    args = parser.parse_args()
    
    generate_report(args.start_date, args.end_date)
