import pymongo
import os

class Database():
    client = pymongo.MongoClient(os.environ['MONGO_DB'])['va_court_search']

    @classmethod
    def add_user(cls, email):
        cls.client.users.insert_one({'email': email})

    @classmethod
    def set_user_password(cls, email, password):
        cls.client.users.update_one({'email': email},
            {'$set':{'password': password}})

    @classmethod
    def get_user(cls, email):
        return cls.client.users.find_one({'email': email})

    @classmethod
    def confirm_credentials(cls, email, password):
        return cls.client.users.find_one({'email': email, 'password': password})

    @classmethod
    def get_circuit_courts(cls):
        return cls.client.circuit_courts.find(None, {'_id':0})

    @classmethod
    def get_district_courts(cls):
        return cls.client.district_courts.find(None, {'_id':0})

    @classmethod
    def find_courts(cls, court_type, lat, lng, miles):
        search = {
            'location': {
                '$nearSphere': {
                    '$geometry': {
                        'type': 'Point',
                        'coordinates': [lng, lat]
                    },
                    '$maxDistance': miles * 1609
                }
            }
        }
        if court_type == 'circuit':
            return cls.client.circuit_courts.find(search, {'_id':0})
        else:
            return cls.client.district_courts.find(search, {'_id':0})

    @classmethod
    def insert_tasks(cls, court_system, name):
        courts = cls.client[court_system + '_courts']
        tasks = cls.client[court_system + '_court_tasks']
        court_codes = [c['fips_code'] for c in courts.find()]
        for code in court_codes:
            tasks.insert_one({'type': 'name',
                              'court_fips': code,
                              'term': name})
