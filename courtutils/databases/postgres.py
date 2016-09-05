import os
from sqlalchemy import create_engine, Column, Integer, String
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from geoalchemy2 import Geometry

Base = declarative_base()

class CircuitCourt(Base):
    __tablename__ = 'circuit_courts'
    id = Column(Integer, primary_key=True)
    name = Column(String)
    fipscode = Column(String)
    location = Column(Geometry('POINT'))
    
class DistrictCourt(Base):
    __tablename__ = 'district_courts'
    id = Column(Integer, primary_key=True)
    name = Column(String)
    fipscode = Column(String)
    location = Column(Geometry('POINT'))

class PostgresDatabase():
    def __init__(self, name, court_type):
        self.engine = create_engine("postgresql://" + os.environ['POSTGRES_DB'])
        self.session = sessionmaker(bind=self.engine)()
        self.court_type = court_type
    
    def commit(self):
        self.session.commit()
    
    def add_court(self, name, fips, location):
        if self.court_type == 'circuit':
            court = CircuitCourt(name=name, fipscode=fips, location='POINT({} {})'.format(location.longitude, location.latitude))
            self.session.add(court)
    
    def add_court_location_index(self):
        pass
    
    def drop_courts(self):
        if self.court_type == 'circuit':
            CircuitCourt.__table__.drop(self.engine, checkfirst=True)
            CircuitCourt.__table__.create(self.engine, checkfirst=False)
        
    def get_courts(self):
        return self.client[self.court_type + '_courts'].find(None, {'_id':0})

    def add_date_tasks(self, tasks):
        self.client[self.court_type + '_court_date_tasks'].insert_many(tasks)

    def get_cases_by_hearing_date(self, start, end):
        return self.client[self.court_type + '_court_detailed_cases'].find({
            'details_fetched_for_hearing_date': {'$gte': start, '$lt': end}
        })
