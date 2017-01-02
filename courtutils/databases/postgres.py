import os
from datetime import datetime
from sqlalchemy import create_engine, Column, Date, Integer, String, ForeignKey
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relationship
from geoalchemy2 import Geometry
from pprint import pprint

Base = declarative_base()

class Court():
    id = Column(Integer, primary_key=True)
    name = Column(String)
    fips = Column(Integer)
    location = Column(Geometry('POINT'))

class CircuitCourt(Base, Court):
    __tablename__ = 'circuit_courts'

class DistrictCourt(Base, Court):
    __tablename__ = 'district_courts'


class DateTask():
    id = Column(Integer, primary_key=True)
    fips = Column(Integer)
    startdate = Column(Date)
    enddate = Column(Date)
    casetype = Column(String)

class CircuitCourtDateTask(Base, DateTask):
    __tablename__ = 'circuit_court_date_tasks'

class DistrictCourtDateTask(Base, DateTask):
    __tablename__ = 'district_court_date_tasks'


class DateSearch():
    id = Column(Integer, primary_key=True)
    fips = Column(Integer)
    date = Column(Date)
    casetype = Column(String)

class CircuitCourtDateSearch(Base, DateSearch):
    __tablename__ = 'circuit_court_dates_searched'

class DistrictCourtDateSearch(Base, DateSearch):
    __tablename__ = 'district_court_dates_searched'


CIRCUIT_CRIMINAL = 'CircuitCriminal'
CIRCUIT_CIVIL = 'CircuitCivil'
DISTRICT_CRIMINAL = 'DistrictCriminal'
DISTRICT_CIVIL = 'DistrictCivil'

DATES = [
    'Filed',
    'OffenseDate',
    'ArrestDate',
    'DispositonDate',
    'AppealedDate',
    'Date'
]

def load(k, d):
    if k not in d or d[k] == '':
        return None
    if k in DATES:
        return datetime.strptime(d[k], '%m/%d/%Y')
    return d[k]
#
# Case Tables
#
class Case():
    id = Column(Integer, primary_key=True)
    fips = Column(Integer)
    case_number = Column(String)
    details_fetched_for_hearing_date = Column(Date)

class CircuitCriminalCase(Base, Case):
    prefix = CIRCUIT_CRIMINAL
    __tablename__ = prefix + 'Case'
    hearings = relationship(prefix + 'Hearing', back_populates='case')
    pleadings = relationship(prefix + 'Pleading', back_populates='case')
    services = relationship(prefix + 'Service', back_populates='case')

    filed = Column(Date)
    commencedBy = Column(String)
    locality = Column(String)

    defendant = Column(String)
    sex = Column(String)
    race = Column(String)
    dob = Column(String)
    address = Column(String)

    charge = Column(String)
    codeSection = Column(String)
    chargeType = Column(String)
    chargeClass = Column(String)
    offenseDate = Column(Date)
    arrestDate = Column(Date)

    dispositionCode = Column(String)
    dispositonDate = Column(Date)
    concludedBy = Column(String)
    amendedCharge = Column(String)
    amendedCodeSection = Column(String)
    amendedChargeType = Column(String)

    jail = Column(String)
    concurrent = Column(String)
    lifeDeath = Column(String)
    sentenceTime = Column(String)
    sentenceSuspended = Column(String)
    operatorLicenseSuspensionTime = Column(String)
    fine = Column(String)
    costs = Column(String)
    fineCostsPaid = Column(String)
    programType = Column(String)
    probationType = Column(String)
    probationTime = Column(String)
    probationStarts = Column(String)
    courtDmvSurrender = Column(String)
    driverImprovementClinic = Column(String)
    drivingRestrictions = Column(String)
    drivingRestrictionEffectiveDate = Column(String)
    alcoholSafetyAction = Column(String)
    restitutionPaid = Column(String)
    restitutionAmount = Column(String)
    military = Column(String)
    trafficFatality = Column(String)

    appealedDate = Column(Date)

    @staticmethod
    def create(case):
        details = case['details']
        case = CircuitCriminalCase(
            fips=int(case['fips']),
            case_number=case['case_number'],
            details_fetched_for_hearing_date=case['details_fetched_for_hearing_date'],

            filed=load('Filed', details),
            commencedBy=load('CommencedBy', details),
            locality=load('Locality', details),

            defendant=load('Defendant', details),
            sex=load('Sex', details),
            race=load('Race', details),
            dob=load('DOB', details),
            address=load('Address', details),

            charge=load('Charge', details),
            codeSection=load('CodeSection', details),
            chargeType=load('ChargeType', details),
            chargeClass=load('ChargeClass', details),
            offenseDate=load('OffenseDate', details),
            arrestDate=load('ArrestDate', details),

            dispositionCode=load('DispositionCode', details),
            dispositonDate=load('DispositonDate', details),
            concludedBy=load('ConcludedBy', details),
            amendedCharge=load('AmendedCharge', details),
            amendedCodeSection=load('AmendedCodeSection', details),
            amendedChargeType=load('AmendedChargeType', details),

            jail=load('JailPenitentiary', details),
            concurrent=load('ConcurrentConsecutive', details),
            lifeDeath=load('LifeDeath', details),
            sentenceTime=load('SentenceTime', details),
            sentenceSuspended=load('SentenceSuspended', details),
            operatorLicenseSuspensionTime=load('OperatorLicenseSuspensionTime', details),
            fine=load('Fine', details),
            costs=load('Costs', details),
            fineCostsPaid=load('FineCostsPaid', details),
            programType=load('ProgramType', details),
            probationType=load('ProbationType', details),
            probationTime=load('ProbationTime', details),
            probationStarts=load('ProbationStarts', details),
            courtDmvSurrender=load('CourtDmvSurrender', details),
            driverImprovementClinic=load('DriverImprovementClinic', details),
            drivingRestrictions=load('DrivingRestrictions', details),
            drivingRestrictionEffectiveDate=load('DrivingRestrictionEffectiveDate', details),
            alcoholSafetyAction=load('AlcoholSafetyAction', details),
            restitutionPaid=load('RestitutionPaid', details),
            restitutionAmount=load('RestitutionAmount', details),
            military=load('Military', details),
            trafficFatality=load('TrafficFatality', details),

            appealedDate=load('AppealedDate', details)
        )

        if 'Hearings' in details:
            case.hearings = [
                CircuitCriminalHearing(
                    date=load('Date', hearing),
                    hearing_type=load('Type', hearing),
                    room=load('Room', hearing),
                    result=load('Result', hearing),
                    duration=load('Duration', hearing),
                    jury=load('Jury', hearing),
                    plea=load('Plea', hearing),
                ) for hearing in details['Hearings']
            ]
        return case

class CircuitCivilCase(Base, Case):
    prefix = CIRCUIT_CIVIL
    __tablename__ = prefix + 'Case'
    hearings = relationship(prefix + 'Hearing', back_populates='case')
    pleadings = relationship(prefix + 'Pleading', back_populates='case')
    services = relationship(prefix + 'Service', back_populates='case')

class DistrictCriminalCase(Base, Case):
    prefix = DISTRICT_CRIMINAL
    __tablename__ = prefix + 'Case'
    hearings = relationship(prefix + 'Hearing', back_populates='case')
    services = relationship(prefix + 'Service', back_populates='case')

class DistrictCivilCase(Base, Case):
    prefix = DISTRICT_CIVIL
    __tablename__ = prefix + 'Case'
    hearings = relationship(prefix + 'Hearing', back_populates='case')
    services = relationship(prefix + 'Service', back_populates='case')
    reports = relationship(prefix + 'Report', back_populates='case')
    plaintiffs = relationship(prefix + 'Plaintiff', back_populates='case')
    defendants = relationship(prefix + 'Defendant', back_populates='case')

#
# Hearing Tables
#
class Hearing():
    id = Column(Integer, primary_key=True)
    date = Column(Date)
    hearing_type = Column(String)
    room = Column(String)
    result = Column(String)

class CircuitCriminalHearing(Base, Hearing):
    prefix = CIRCUIT_CRIMINAL
    __tablename__ = prefix + 'Hearing'
    case_id = Column(Integer, ForeignKey(prefix + 'Case.id'))
    case = relationship(prefix + 'Case', back_populates='hearings')
    duration = Column(String)
    jury = Column(String)
    plea = Column(String)

class CircuitCivilHearing(Base, Hearing):
    prefix = CIRCUIT_CIVIL
    __tablename__ = prefix + 'Hearing'
    case_id = Column(Integer, ForeignKey(prefix + 'Case.id'))
    case = relationship(prefix + 'Case', back_populates='hearings')
    duration = Column(String)
    jury = Column(String)

class DistrictCriminalHearing(Base, Hearing):
    prefix = DISTRICT_CRIMINAL
    __tablename__ = prefix + 'Hearing'
    case_id = Column(Integer, ForeignKey(prefix + 'Case.id'))
    case = relationship(prefix + 'Case', back_populates='hearings')
    plea = Column(String)
    continuance_code = Column(String)

class DistrictCivilHearing(Base, Hearing):
    prefix = DISTRICT_CIVIL
    __tablename__ = prefix + 'Hearing'
    case_id = Column(Integer, ForeignKey(prefix + 'Case.id'))
    case = relationship(prefix + 'Case', back_populates='hearings')

#
# Pleading Tables
#
class Pleading():
    id = Column(Integer, primary_key=True)
    filed_date = Column(Date)
    pleading_type = Column(String)
    party = Column(String)
    judge = Column(String)
    book = Column(String)
    page = Column(String)
    remarks = Column(String)

class CircuitCriminalPleading(Base, Pleading):
    prefix = CIRCUIT_CRIMINAL
    __tablename__ = prefix + 'Pleading'
    case_id = Column(Integer, ForeignKey(prefix + 'Case.id'))
    case = relationship(prefix + 'Case', back_populates='pleadings')

class CircuitCivilPleading(Base, Pleading):
    prefix = CIRCUIT_CIVIL
    __tablename__ = prefix + 'Pleading'
    case_id = Column(Integer, ForeignKey(prefix + 'Case.id'))
    case = relationship(prefix + 'Case', back_populates='pleadings')

#
# Service Tables
#
class Service():
    id = Column(Integer, primary_key=True)
    name = Column(String)
    service_type = Column(String)
    how_served = Column(String)

class CircuitCriminalService(Base, Service):
    prefix = CIRCUIT_CRIMINAL
    __tablename__ = prefix + 'Service'
    case_id = Column(Integer, ForeignKey(prefix + 'Case.id'))
    case = relationship(prefix + 'Case', back_populates='services')
    hear_date = Column(Date)
    date_served = Column(Date)

class CircuitCivilService(Base, Service):
    prefix = CIRCUIT_CIVIL
    __tablename__ = prefix + 'Service'
    case_id = Column(Integer, ForeignKey(prefix + 'Case.id'))
    case = relationship(prefix + 'Case', back_populates='services')
    hear_date = Column(Date)
    date_served = Column(Date)

class DistrictCriminalService(Base, Service):
    prefix = DISTRICT_CRIMINAL
    __tablename__ = prefix + 'Service'
    case_id = Column(Integer, ForeignKey(prefix + 'Case.id'))
    case = relationship(prefix + 'Case', back_populates='services')
    date_issued = Column(Date)
    date_returned = Column(Date)
    plaintiff = Column(String)

class DistrictCivilService(Base, Service):
    prefix = DISTRICT_CIVIL
    __tablename__ = prefix + 'Service'
    case_id = Column(Integer, ForeignKey(prefix + 'Case.id'))
    case = relationship(prefix + 'Case', back_populates='services')
    date_issued = Column(Date)
    date_returned = Column(Date)
    plaintiff = Column(String)

#
# Report Tables
#
class Report():
    id = Column(Integer, primary_key=True)
    report_type = Column(String)
    agency = Column(String)
    date_ordered = Column(Date)
    date_due = Column(Date)
    date_received = Column(Date)

class DistrictCivilReport(Base, Report):
    prefix = DISTRICT_CIVIL
    __tablename__ = prefix + 'Report'
    case_id = Column(Integer, ForeignKey(prefix + 'Case.id'))
    case = relationship(prefix + 'Case', back_populates='reports')

#
# Party Tables
#
class Party():
    id = Column(Integer, primary_key=True)
    name = Column(String)
    dba = Column(String)
    address = Column(String)
    judgement = Column(String)
    attorney = Column(String)

class DistrictCivilPlaintiff(Base, Party):
    prefix = DISTRICT_CIVIL
    __tablename__ = prefix + 'Plaintiff'
    case_id = Column(Integer, ForeignKey(prefix + 'Case.id'))
    case = relationship(prefix + 'Case', back_populates='plaintiffs')

class DistrictCivilDefendant(Base, Party):
    prefix = DISTRICT_CIVIL
    __tablename__ = prefix + 'Defendant'
    case_id = Column(Integer, ForeignKey(prefix + 'Case.id'))
    case = relationship(prefix + 'Case', back_populates='defendants')


TABLES = [
    # Courts
    CircuitCourt,
    DistrictCourt,

    # Tasks
    CircuitCourtDateTask,
    DistrictCourtDateTask,

    # Searches
    CircuitCourtDateSearch,
    DistrictCourtDateSearch,

    # Cases
    CircuitCriminalCase,
    CircuitCivilCase,
    DistrictCriminalCase,
    DistrictCivilCase,

    # Hearings
    CircuitCriminalHearing,
    CircuitCivilHearing,
    DistrictCriminalHearing,
    DistrictCivilHearing,

    # Pleadings
    CircuitCriminalPleading,
    CircuitCivilPleading,

    # Services
    CircuitCriminalService,
    CircuitCivilService,
    DistrictCriminalService,
    DistrictCivilService,

    # Reports and Parties
    DistrictCivilReport,
    DistrictCivilPlaintiff,
    DistrictCivilDefendant,
]

#
# Database class
#
class PostgresDatabase():
    def __init__(self, court_type):
        self.engine = create_engine("postgresql://" + os.environ['POSTGRES_DB'])
        self.session = sessionmaker(bind=self.engine)()

        for table in TABLES:
            table.__table__.create(self.engine, checkfirst=True) #pylint: disable=E1101

        self.court_type = court_type
        if court_type == 'circuit':
            self.court_builder = CircuitCourt
            self.date_task_builder = CircuitCourtDateTask
            self.date_search_builder = CircuitCourtDateSearch
        else:
            self.court_builder = DistrictCourt
            self.date_task_builder = DistrictCourtDateTask
            self.date_search_builder = DistrictCourtDateSearch

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
                return CircuitCriminalCase
            else:
                return CircuitCivilCase
        else:
            if case_type == 'criminal':
                return DistrictCriminalCase
            else:
                return DistrictCivilCase

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
        pprint(case)
        case_builder = self.get_case_builder(case_type)
        self.session.query(case_builder).filter_by(
            fips=int(case['fips']),
            case_number=case['case_number']
        ).delete()
        self.session.add(case_builder.create(case))
        self.session.commit()

