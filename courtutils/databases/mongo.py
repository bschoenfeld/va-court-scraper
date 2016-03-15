import pymongo
import os

class MongoDatabase():
    def __init__(self, name, court_type):
        self.client = pymongo.MongoClient(os.environ['MONGO_DB'])[name]
        self.court_type = court_type

    def get_courts(self):
        return self.client[self.court_type + '_courts'].find(None, {'_id':0})

    def add_date_tasks(self, tasks):
        self.client[self.court_type + '_court_date_tasks'].insert_many(tasks)
