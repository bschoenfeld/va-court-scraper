import pymongo
import os

class MongoDatabase():
    def __init__(self, name, court_type):
        self.client = pymongo.MongoClient(os.environ['MONGO_DB'])[name]
        self.court_type = court_type

    def add_court(self, name, fips, location):
        self.client[self.court_type + '_courts'].insert_one({
            'name': name,
            'fips_code': fips,
            'location': {'type': 'Point', 'coordinates': [
                location.longitude, location.latitude
            ]}
        })

    def add_court_location_index(self):
        self.client[self.court_type + '_courts'].create_index( \
            [('location', pymongo.GEOSPHERE)], background=True)

    def drop_courts(self):
        self.client[self.court_type + '_courts'].drop()

    def get_courts(self):
        return self.client[self.court_type + '_courts'].find(None, {'_id':0})

    def add_date_tasks(self, tasks):
        self.client[self.court_type + '_court_date_tasks'].insert_many(tasks)

    def add_date_task(self, task):
        self.client[self.court_type + '_court_date_tasks'].insert_one(task)

    def get_and_delete_date_task(self):
        return self.client[self.court_type + '_court_date_tasks'].find_one_and_delete({})

    def add_date_search(self, search):
        self.client[self.court_type + '_court_dates_searched'].insert_one(search)

    def get_date_search(self, search):
        return self.client[self.court_type + '_court_dates_searched'].find_one(search)

    def get_more_recent_case_details(self, case, case_type, date):
        return self.client[self.court_type + '_court_detailed_cases'].find_one({
            'court_fips': case['court_fips'],
            'case_number': case['case_number'],
            'details_fetched_for_hearing_date': {'$gte': date}
        })

    def replace_case_details(self, case, case_type):
        self.client[self.court_type + '_court_detailed_cases'].find_one_and_replace({
            'court_fips': case['court_fips'],
            'case_number': case['case_number']
        }, case, upsert=True)

    def get_cases_by_hearing_date(self, start, end):
        return self.client[self.court_type + '_court_detailed_cases'].find({
            'details_fetched_for_hearing_date': {'$gte': start, '$lt': end}
        })
