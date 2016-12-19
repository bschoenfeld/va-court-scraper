import os
from sqlalchemy import create_engine, Column, Date, Integer, String
from sqlalchemy.exc import IntegrityError
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
    casetype = Column(String)

class DistrictCourtDateTask(Base):
    __tablename__ = 'district_court_date_tasks'
    id = Column(Integer, primary_key=True)
    fips = Column(Integer)
    startdate = Column(Date)
    enddate = Column(Date)
    casetype = Column(String)

class CircuitCourtDateSearch(Base):
    __tablename__ = 'circuit_court_dates_searched'
    id = Column(Integer, primary_key=True)
    fips = Column(Integer)
    date = Column(Date)
    casetype = Column(String)

class DistrictCourtDateSearch(Base):
    __tablename__ = 'district_court_dates_searched'
    id = Column(Integer, primary_key=True)
    fips = Column(Integer)
    date = Column(Date)
    casetype = Column(String)

class DetailedCase():
    id = Column(Integer, primary_key=True)
    fips = Column(Integer)
    case_number = Column(String)
    details_fetched_for_hearing_date = Column(Date)

class CircuitCourtDetailedCriminalCase(Base, DetailedCase):
    __tablename__ = 'circuit_court_details_criminal_case'

class CircuitCourtDetailedCivilCase(Base, DetailedCase):
    __tablename__ = 'circuit_court_details_civil_case'

class DistrictCourtDetailedCriminalCase(Base, DetailedCase):
    __tablename__ = 'district_court_details_criminal_case'

class DistrictCourtDetailedCivilCase(Base, DetailedCase):
    __tablename__ = 'district_court_details_civil_case'

class PostgresDatabase():
    def __init__(self, court_type):
        self.engine = create_engine("postgresql://" + os.environ['POSTGRES_DB'])
        self.session = sessionmaker(bind=self.engine)()
        self.court_type = court_type
        if court_type == 'circuit':
            self.court_builder = CircuitCourt
            self.date_task_builder = CircuitCourtDateTask
            self.date_search_builder = CircuitCourtDateSearch
        else:
            self.court_builder = DistrictCourt
            self.date_task_builder = DistrictCourtDateTask
            self.date_search_builder = DistrictCourtDateSearch
        self.date_task_builder.__table__.create(self.engine, checkfirst=True)
        self.date_search_builder.__table__.create(self.engine, checkfirst=True)

        CircuitCourtDetailedCriminalCase.__table__.create(self.engine, checkfirst=True)
        CircuitCourtDetailedCivilCase.__table__.create(self.engine, checkfirst=True)
        DistrictCourtDetailedCriminalCase.__table__.create(self.engine, checkfirst=True)
        DistrictCourtDetailedCivilCase.__table__.create(self.engine, checkfirst=True)

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
                    enddate=task['end_date'],
                    casetype=task['case_type']))
        self.session.commit()

    def add_date_task(self, task):
        self.session.add( \
            self.date_task_builder( \
                fips=int(task['fips']),
                startdate=task['start_date'],
                enddate=task['end_date'],
                casetype=task['case_type']))
        self.session.commit()

    def get_and_delete_date_task(self):
        while True:
            try:
                task = self.session.query(self.date_task_builder).first()
                if task is None:
                    return None
                self.session.delete(task)
                self.session.commit()
                return {
                    'fips': str(task.fips).zfill(3),
                    'start_date': task.startdate,
                    'end_date': task.enddate,
                    'case_type': task.casetype
                }
            except IntegrityError:
                self.session.rollback()

    def add_date_search(self, search):
        self.session.add(
            self.date_search_builder(
                fips=int(search['fips']),
                date=search['date'],
                casetype=search['case_type']
            )
        )
        self.session.commit()

    def get_date_search(self, search):
        result = self.session.query(self.date_search_builder).filter_by(
            fips=int(search['fips']),
            date=search['date'],
            casetype=search['case_type']
        ).first()
        if result is None:
            return None
        return search

    def get_case_builder(self, case_type):
        if self.court_type == 'circuit':
            if case_type == 'criminal':
                return CircuitCourtDetailedCriminalCase
            else:
                return CircuitCourtDetailedCivilCase
        else:
            if case_type == 'criminal':
                return DistrictCourtDetailedCriminalCase
            else:
                return DistrictCourtDetailedCivilCase

    def get_more_recent_case_details(self, case, case_type, date):
        case_builder = self.get_case_builder(case_type)
        result = self.session.query(case_builder).filter(
            case_builder.fips == int(case['fips']),
            case_builder.case_number == case['case_number'],
            case_builder.details_fetched_for_hearing_date >= date
        ).first()
        if result is None:
            return result
        return  {
            'details_fetched_for_hearing_date': result.details_fetched_for_hearing_date
        }

    def replace_case_details(self, case, case_type):
        print case
        case_builder = self.get_case_builder(case_type)
        self.session.query(case_builder).filter_by(
            fips=int(case['fips']),
            case_number=case['case_number']
        ).delete()
        self.session.add(case_builder(
            fips=int(case['fips']),
            case_number=case['case_number'],
            details_fetched_for_hearing_date=case['details_fetched_for_hearing_date']
        ))
        self.session.commit()

    def get_cases_by_hearing_date(self, start, end):
        return self.client[self.court_type + '_court_detailed_cases'].find({
            'details_fetched_for_hearing_date': {'$gte': start, '$lt': end}
        })
