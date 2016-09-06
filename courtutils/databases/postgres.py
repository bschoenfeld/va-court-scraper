import os
from sqlalchemy import create_engine, Column, Date, Integer, String
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from geoalchemy2 import Geometry

Base = declarative_base()

class CircuitCourt(Base):
    __tablename__ = 'circuit_courts'
    id = Column(Integer, primary_key=True)
    name = Column(String)
    fips = Column(Integer)
    location = Column(Geometry('POINT'))
    
class DistrictCourt(Base):
    __tablename__ = 'district_courts'
    id = Column(Integer, primary_key=True)
    name = Column(String)
    fips = Column(Integer)
    location = Column(Geometry('POINT'))

class CircuitCourtDateTask(Base):
    __tablename__ = 'circuit_court_date_tasks'
    id = Column(Integer, primary_key=True)
    fips = Column(Integer)
    startdate = Column(Date)
    enddate = Column(Date)

class DistrictCourtDateTask(Base):
    __tablename__ = 'district_court_date_tasks'
    id = Column(Integer, primary_key=True)
    fips = Column(Integer)
    startdate = Column(Date)
    enddate = Column(Date)

class PostgresDatabase():
    def __init__(self, name, court_type):
        self.engine = create_engine("postgresql://" + os.environ['POSTGRES_DB'])
        self.session = sessionmaker(bind=self.engine)()
        self.court_type = court_type
        self.court_builder = CircuitCourt if court_type == 'circuit' else DistrictCourt
        self.date_task_builder = CircuitCourtDateTask if court_type == 'circuit' else DistrictCourtDateTask
        self.date_task_builder.__table__.create(self.engine, checkfirst=True)
    
    def commit(self):
        self.session.commit()
    
    def add_court(self, name, fips, location):
        self.session.add( \
            self.court_builder( \
                name=name, \
                fips=int(fips), \
                location='POINT({} {})'.format( \
                    location.longitude, location.latitude)))
    
    def add_court_location_index(self):
        pass
    
    def drop_courts(self):
        self.court_builder.__table__.drop(self.engine, checkfirst=True)
        self.court_builder.__table__.create(self.engine, checkfirst=False)
        
    def get_courts(self):
        return [{
            'name': court.name,
            'fips': str(court.fips).zfill(3)
        } for court in self.session.query(self.court_builder)]

    def add_date_tasks(self, tasks):
        for task in tasks:
            self.session.add( \
                self.date_task_builder( \
                    fips=int(task['fips']),
                    startdate=task['start_date'],
                    enddate=task['end_date']))
        self.session.commit()

    def get_cases_by_hearing_date(self, start, end):
        return self.client[self.court_type + '_court_detailed_cases'].find({
            'details_fetched_for_hearing_date': {'$gte': start, '$lt': end}
        })
