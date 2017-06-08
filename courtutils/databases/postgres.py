import os
from datetime import datetime, date
from sqlalchemy import (create_engine, Boolean, Column,
                        Date, DateTime, Integer, BigInteger,
                        Float, String, ForeignKey, Index)
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relationship
from sqlalchemy.pool import NullPool
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

class ActiveDateTask():
    id = Column(Integer, primary_key=True)
    fips = Column(Integer)
    startdate = Column(Date)
    enddate = Column(Date)
    casetype = Column(String)

class CircuitCourtActiveDateTask(Base, DateTask):
    __tablename__ = 'circuit_court_active_date_tasks'

class DistrictCourtActiveDateTask(Base, DateTask):
    __tablename__ = 'district_court_active_date_tasks'


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

#
# Case Tables
#
class Case():
    id = Column(BigInteger, primary_key=True)
    fips = Column(Integer)
    details_fetched_for_hearing_date = Column(Date)
    collected = Column(Date)
    CaseNumber = Column(String)

class CircuitCriminalCase(Base, Case):
    prefix = CIRCUIT_CRIMINAL
    __tablename__ = prefix + 'Case'
    Hearings = relationship(prefix + 'Hearing')
    Pleadings = relationship(prefix + 'Pleading')
    Services = relationship(prefix + 'Service')

    Filed = Column(Date)
    Commencedby = Column(String)
    Locality = Column(String)

    person_id = Column(BigInteger)
    Defendant = Column(String)
    AKA = Column(String)
    AKA2 = Column(String)
    Sex = Column(String)
    Race = Column(String)
    DOB = Column(Date)
    Address = Column(String)

    Charge = Column(String)
    CodeSection = Column(String)
    ChargeType = Column(String)
    Class = Column(String)
    OffenseDate = Column(Date)
    ArrestDate = Column(Date)

    DispositionCode = Column(String)
    DispositionDate = Column(Date)
    ConcludedBy = Column(String)
    AmendedCharge = Column(String)
    AmendedCodeSection = Column(String)
    AmendedChargeType = Column(String)

    JailPenitentiary = Column(String)
    ConcurrentConsecutive = Column(String)
    LifeDeath = Column(String)
    SentenceTime = Column(Integer)
    SentenceSuspended = Column(Integer)
    OperatorLicenseSuspensionTime = Column(Integer)
    FineAmount = Column(Float)
    Costs = Column(Float)
    FinesCostPaid = Column(Boolean)
    ProgramType = Column(String)
    ProbationType = Column(String)
    ProbationTime = Column(Integer)
    ProbationStarts = Column(String)
    CourtDMVSurrender = Column(String)
    DriverImprovementClinic = Column(Boolean)
    DrivingRestrictions = Column(Boolean)
    RestrictionEffectiveDate = Column(Date)
    RestrictionEndDate = Column(Date)
    VAAlcoholSafetyAction = Column(Boolean)
    RestitutionPaid = Column(Boolean)
    RestitutionAmount = Column(String)
    Military = Column(String)
    TrafficFatality = Column(Boolean)

    AppealedDate = Column(Date)

    @staticmethod
    def create(case):
        details = case['details']
        hearings = []
        pleadings = []
        services = []

        if 'Hearings' in details:
            hearings = details['Hearings']
            del details['Hearings']
        if 'Pleadings' in details:
            pleadings = details['Pleadings']
            del details['Pleadings']
        if 'Services' in details:
            services = details['Services']
            del details['Services']

        db_case = CircuitCriminalCase(**details)
        db_case.fips = int(case['fips'])
        db_case.details_fetched_for_hearing_date = case['details_fetched_for_hearing_date']
        db_case.collected = case['collected']

        db_case.Hearings = [
            CircuitCriminalHearing(**hearing)
            for hearing in hearings
        ]
        db_case.Pleadings = [
            CircuitCriminalPleading(**pleading)
            for pleading in pleadings
        ]
        db_case.Services = [
            CircuitCriminalService(**service)
            for service in services
        ]
        return db_case

class CircuitCivilCase(Base, Case):
    prefix = CIRCUIT_CIVIL
    __tablename__ = prefix + 'Case'
    Hearings = relationship(prefix + 'Hearing')
    Pleadings = relationship(prefix + 'Pleading')
    Services = relationship(prefix + 'Service')
    Plaintiffs = relationship(prefix + 'Plaintiff')
    Defendants = relationship(prefix + 'Defendant')

    Filed = Column(Date)
    FilingType = Column(String)
    FilingFeePaid = Column(Boolean)
    NumberofPlaintiffs = Column(Integer)
    NumberofDefendants = Column(Integer)
    CommencedBy = Column(String)
    Bond = Column(String)
    ComplexCase = Column(String)

    DateOrderedToMediation = Column(Date)

    Judgment = Column(String)
    FinalOrderDate = Column(Date)
    AppealedDate = Column(Date)
    ConcludedBy = Column(String)

    @staticmethod
    def create(case):
        details = case['details']
        hearings = []
        pleadings = []
        services = []
        plaintiffs = []
        defendants = []

        if 'Hearings' in details:
            hearings = details['Hearings']
            del details['Hearings']
        if 'Pleadings' in details:
            pleadings = details['Pleadings']
            del details['Pleadings']
        if 'Services' in details:
            services = details['Services']
            del details['Services']
        if 'Plaintiffs' in details:
            plaintiffs = details['Plaintiffs']
            del details['Plaintiffs']
        if 'Defendants' in details:
            defendants = details['Defendants']
            del details['Defendants']

        db_case = CircuitCivilCase(**details)
        db_case.fips = int(case['fips'])
        db_case.details_fetched_for_hearing_date = case['details_fetched_for_hearing_date']
        db_case.collected = case['collected']

        db_case.Hearings = [
            CircuitCivilHearing(**hearing)
            for hearing in hearings
        ]
        db_case.Pleadings = [
            CircuitCivilPleading(**pleading)
            for pleading in pleadings
        ]
        db_case.Services = [
            CircuitCivilService(**service)
            for service in services
        ]
        db_case.Plaintiffs = [
            CircuitCivilPlaintiff(**plaintiff)
            for plaintiff in plaintiffs
        ]
        db_case.Defendants = [
            CircuitCivilDefendant(**defendant)
            for defendant in defendants
        ]
        return db_case

class DistrictCriminalCase(Base, Case):
    prefix = DISTRICT_CRIMINAL
    __tablename__ = prefix + 'Case'
    Hearings = relationship(prefix + 'Hearing')
    Services = relationship(prefix + 'Service')

    person_id = Column(BigInteger)
    FiledDate = Column(Date)
    Locality = Column(String)
    Name = Column(String)
    Status = Column(String)
    DefenseAttorney = Column(String)
    Address = Column(String)
    AKA1 = Column(String)
    AKA2 = Column(String)
    Gender = Column(String)
    Race = Column(String)
    DOB = Column(Date)

    Charge = Column(String)
    CodeSection = Column(String)
    CaseType = Column(String)
    Class = Column(String)
    OffenseDate = Column(Date)
    ArrestDate = Column(Date)
    Complainant = Column(String)
    AmendedCharge = Column(String)
    AmendedCode = Column(String)
    AmendedCaseType = Column(String)

    FinalDisposition = Column(String)
    SentenceTime = Column(Integer)
    SentenceSuspendedTime = Column(Integer)
    ProbationType = Column(String)
    ProbationTime = Column(Integer)
    ProbationStarts = Column(String)
    OperatorLicenseSuspensionTime = Column(Integer)
    RestrictionEffectiveDate = Column(Date)
    RestrictionEndDate = Column(Date)
    OperatorLicenseRestrictionCodes = Column(String)
    Fine = Column(Float)
    Costs = Column(Float)
    FineCostsDue = Column(Date)
    FineCostsPastDue = Column(Boolean)
    FineCostsPaid = Column(Boolean)
    FineCostsPaidDate = Column(Date)
    VASAP = Column(Boolean)

    @staticmethod
    def create(case):
        details = case['details']
        hearings = []
        services = []

        if 'Hearings' in details:
            hearings = details['Hearings']
            del details['Hearings']
        if 'Services' in details:
            services = details['Services']
            del details['Services']

        db_case = DistrictCriminalCase(**details)
        db_case.fips = int(case['fips'])
        db_case.details_fetched_for_hearing_date = case['details_fetched_for_hearing_date']
        db_case.collected = case['collected']

        db_case.Hearings = [
            DistrictCriminalHearing(**hearing)
            for hearing in hearings
        ]
        db_case.Services = [
            DistrictCriminalService(**service)
            for service in services
        ]
        return db_case

class DistrictCivilCase(Base, Case):
    prefix = DISTRICT_CIVIL
    __tablename__ = prefix + 'Case'
    Hearings = relationship(prefix + 'Hearing')
    Services = relationship(prefix + 'Service')
    Reports = relationship(prefix + 'Report')
    Plaintiffs = relationship(prefix + 'Plaintiff')
    Defendants = relationship(prefix + 'Defendant')

    FiledDate = Column(Date)
    CaseType = Column(String)
    DebtType = Column(String)

    Judgment = Column(String)
    Costs = Column(Float)
    AttorneyFees = Column(Float)
    PrincipalAmount = Column(Float)
    OtherAmount = Column(Float)
    InterestAward = Column(String)
    Possession = Column(String)
    WritIssuedDate = Column(Date)
    HomesteadExemptionWaived = Column(Boolean)
    IsJudgmentSatisfied = Column(String)
    DateSatisfactionFiled = Column(Date)
    OtherAwarded = Column(String)
    FurtherCaseInformation = Column(String)

    Garnishee = Column(String)
    Address = Column(String)
    GarnisheeAnswer = Column(String)
    AnswerDate = Column(Date)
    NumberofChecksReceived = Column(Integer)

    AppealDate = Column(Date)
    AppealedBy = Column(String)

    @staticmethod
    def create(case):
        details = case['details']
        hearings = []
        services = []
        reports = []
        plaintiffs = []
        defendants = []

        if 'Hearings' in details:
            hearings = details['Hearings']
            del details['Hearings']
        if 'Services' in details:
            services = details['Services']
            del details['Services']
        if 'Reports' in details:
            reports = details['Reports']
            del details['Reports']
        if 'Plaintiffs' in details:
            plaintiffs = details['Plaintiffs']
            del details['Plaintiffs']
        if 'Defendants' in details:
            defendants = details['Defendants']
            del details['Defendants']

        db_case = DistrictCivilCase(**details)
        db_case.fips = int(case['fips'])
        db_case.details_fetched_for_hearing_date = case['details_fetched_for_hearing_date']
        db_case.collected = case['collected']

        db_case.Hearings = [
            DistrictCivilHearing(**hearing)
            for hearing in hearings
        ]
        db_case.Services = [
            DistrictCivilService(**service)
            for service in services
        ]
        db_case.Reports = [
            DistrictCivilReport(**report)
            for report in reports
        ]
        db_case.Plaintiffs = [
            DistrictCivilPlaintiff(**plaintiff)
            for plaintiff in plaintiffs
        ]
        db_case.Defendants = [
            DistrictCivilDefendant(**defendant)
            for defendant in defendants
        ]
        return db_case

#
# Hearing Tables
#
class Hearing():
    id = Column(BigInteger, primary_key=True)
    Date = Column(DateTime)
    Result = Column(String)

class CircuitCriminalHearing(Base, Hearing):
    prefix = CIRCUIT_CRIMINAL
    __tablename__ = prefix + 'Hearing'
    case_id = Column(BigInteger, ForeignKey(prefix + 'Case.id', ondelete='CASCADE'))
    Duration = Column(String)
    Jury = Column(Boolean)
    Plea = Column(String)
    Type = Column(String)
    Room = Column(String)

class CircuitCivilHearing(Base, Hearing):
    prefix = CIRCUIT_CIVIL
    __tablename__ = prefix + 'Hearing'
    case_id = Column(BigInteger, ForeignKey(prefix + 'Case.id', ondelete='CASCADE'))
    Duration = Column(String)
    Jury = Column(String)
    Type = Column(String)
    Room = Column(String)

class DistrictCriminalHearing(Base, Hearing):
    prefix = DISTRICT_CRIMINAL
    __tablename__ = prefix + 'Hearing'
    case_id = Column(BigInteger, ForeignKey(prefix + 'Case.id', ondelete='CASCADE'))
    Plea = Column(String)
    ContinuanceCode = Column(String)
    HearingType = Column(String)
    Courtroom = Column(String)

class DistrictCivilHearing(Base, Hearing):
    prefix = DISTRICT_CIVIL
    __tablename__ = prefix + 'Hearing'
    case_id = Column(BigInteger, ForeignKey(prefix + 'Case.id', ondelete='CASCADE'))
    HearingType = Column(String)
    Courtroom = Column(String)

#
# Pleading Tables
#
class Pleading():
    id = Column(BigInteger, primary_key=True)
    Filed = Column(Date)
    Type = Column(String)
    Party = Column(String)
    Judge = Column(String)
    Book = Column(String)
    Page = Column(String)
    Remarks = Column(String)

class CircuitCriminalPleading(Base, Pleading):
    prefix = CIRCUIT_CRIMINAL
    __tablename__ = prefix + 'Pleading'
    case_id = Column(BigInteger, ForeignKey(prefix + 'Case.id', ondelete='CASCADE'))

class CircuitCivilPleading(Base, Pleading):
    prefix = CIRCUIT_CIVIL
    __tablename__ = prefix + 'Pleading'
    case_id = Column(BigInteger, ForeignKey(prefix + 'Case.id', ondelete='CASCADE'))

#
# Service Tables
#
class Service():
    id = Column(BigInteger, primary_key=True)
    HowServed = Column(String)

class CircuitCriminalService(Base, Service):
    prefix = CIRCUIT_CRIMINAL
    __tablename__ = prefix + 'Service'
    case_id = Column(BigInteger, ForeignKey(prefix + 'Case.id', ondelete='CASCADE'))
    HearDate = Column(Date)
    DateServed = Column(Date)
    Name = Column(String)
    Type = Column(String)

class CircuitCivilService(Base, Service):
    prefix = CIRCUIT_CIVIL
    __tablename__ = prefix + 'Service'
    case_id = Column(BigInteger, ForeignKey(prefix + 'Case.id', ondelete='CASCADE'))
    HearDate = Column(Date)
    DateServed = Column(Date)
    Name = Column(String)
    Type = Column(String)

class DistrictCriminalService(Base, Service):
    prefix = DISTRICT_CRIMINAL
    __tablename__ = prefix + 'Service'
    case_id = Column(BigInteger, ForeignKey(prefix + 'Case.id', ondelete='CASCADE'))
    DateIssued = Column(Date)
    DateReturned = Column(Date)
    Plaintiff = Column(String)
    PersonServed = Column(String)
    ProcessType = Column(String)

class DistrictCivilService(Base, Service):
    prefix = DISTRICT_CIVIL
    __tablename__ = prefix + 'Service'
    case_id = Column(BigInteger, ForeignKey(prefix + 'Case.id', ondelete='CASCADE'))
    DateIssued = Column(Date)
    DateReturned = Column(Date)
    Plaintiff = Column(String)
    PersonServed = Column(String)
    ProcessType = Column(String)

#
# Report Tables
#
class Report():
    id = Column(BigInteger, primary_key=True)
    ReportType = Column(String)
    ReportingAgency = Column(String)
    DateOrdered = Column(Date)
    DateDue = Column(Date)
    DateReceived = Column(Date)

class DistrictCivilReport(Base, Report):
    prefix = DISTRICT_CIVIL
    __tablename__ = prefix + 'Report'
    case_id = Column(BigInteger, ForeignKey(prefix + 'Case.id', ondelete='CASCADE'))

#
# Party Tables
#
class CircuitCivilParty():
    id = Column(BigInteger, primary_key=True)
    Name = Column(String)
    TradingAs = Column(String)
    Attorney = Column(String)

class CircuitCivilPlaintiff(Base, CircuitCivilParty):
    prefix = CIRCUIT_CIVIL
    __tablename__ = prefix + 'Plaintiff'
    case_id = Column(BigInteger, ForeignKey(prefix + 'Case.id', ondelete='CASCADE'))

class CircuitCivilDefendant(Base, CircuitCivilParty):
    prefix = CIRCUIT_CIVIL
    __tablename__ = prefix + 'Defendant'
    case_id = Column(BigInteger, ForeignKey(prefix + 'Case.id', ondelete='CASCADE'))

class DistrictCivilParty():
    id = Column(BigInteger, primary_key=True)
    Name = Column(String)
    DBATA = Column(String)
    Address = Column(String)
    Judgment = Column(String)
    Attorney = Column(String)

class DistrictCivilPlaintiff(Base, DistrictCivilParty):
    prefix = DISTRICT_CIVIL
    __tablename__ = prefix + 'Plaintiff'
    case_id = Column(BigInteger, ForeignKey(prefix + 'Case.id', ondelete='CASCADE'))

class DistrictCivilDefendant(Base, DistrictCivilParty):
    prefix = DISTRICT_CIVIL
    __tablename__ = prefix + 'Defendant'
    case_id = Column(BigInteger, ForeignKey(prefix + 'Case.id', ondelete='CASCADE'))


TABLES = [
    # Courts
    CircuitCourt,
    DistrictCourt,

    # Tasks
    CircuitCourtDateTask,
    DistrictCourtDateTask,
    CircuitCourtActiveDateTask,
    DistrictCourtActiveDateTask,

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
    CircuitCivilPlaintiff,
    CircuitCivilDefendant
]

#
# Database class
#
class PostgresDatabase():
    def __init__(self, court_type):
        self.engine = create_engine("postgresql://" + os.environ['POSTGRES_DB'], poolclass=NullPool)
        self.session = sessionmaker(bind=self.engine)()

        Index('circuit_court_active_date_tasks_fips_casetype_idx',
              CircuitCourtActiveDateTask.__table__.c.fips,
              CircuitCourtActiveDateTask.__table__.c.casetype,
              unique=True)
        Index('district_court_active_date_tasks_fips_casetype_idx',
              DistrictCourtActiveDateTask.__table__.c.fips,
              DistrictCourtActiveDateTask.__table__.c.casetype,
              unique=True)

        for table in TABLES:
            table.__table__.create(self.engine, checkfirst=True) #pylint: disable=E1101

        self.court_type = court_type
        if court_type == 'circuit':
            self.court_builder = CircuitCourt
            self.date_task_builder = CircuitCourtDateTask
            self.active_date_task_builder = CircuitCourtActiveDateTask
            self.date_search_builder = CircuitCourtDateSearch
        else:
            self.court_builder = DistrictCourt
            self.date_task_builder = DistrictCourtDateTask
            self.active_date_task_builder = DistrictCourtActiveDateTask
            self.date_search_builder = DistrictCourtDateSearch

    def commit(self):
        self.session.commit()

    def add_court(self, name, fips, location):
        longitude = 0 if location is None else location.longitude
        latitude = 0 if location is None else location.latitude
        self.session.add(
            self.court_builder(
                name=name,
                fips=int(fips),
                location='POINT({} {})'.format(longitude, latitude)))

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

    def count_courts(self):
        return self.session.query(self.court_builder).count()

    def add_date_tasks(self, tasks):
        for task in tasks:
            self.session.add(
                self.date_task_builder(
                    fips=int(task['fips']),
                    startdate=task['start_date'],
                    enddate=task['end_date'],
                    casetype=task['case_type']
                )
            )
        self.session.commit()

    def add_date_task(self, task, stopping_work=False):
        self.session.add(
            self.date_task_builder(
                fips=int(task['fips']),
                startdate=task['start_date'],
                enddate=task['end_date'],
                casetype=task['case_type']
            )
        )
        if stopping_work:
            self.session \
                .query(self.active_date_task_builder) \
                .filter(
                    self.active_date_task_builder.fips == int(task['fips']),
                    self.active_date_task_builder.casetype == task['case_type']
                ) \
                .delete()
        self.session.commit()

    def get_and_delete_date_task(self, finished_task=None):
        while True:
            try:
                if finished_task is not None:
                    self.session \
                        .query(self.active_date_task_builder) \
                        .filter(
                            self.active_date_task_builder.fips == int(finished_task['fips']),
                            self.active_date_task_builder.casetype == finished_task['case_type']
                        ) \
                        .delete()
                    self.session.commit()

                task = self.session \
                           .query(self.date_task_builder) \
                           .order_by(self.date_task_builder.startdate.desc()) \
                           .first()
                if task is None:
                    return None
                self.session.delete(task)
                self.session.commit()

                self.session.add(
                    self.active_date_task_builder(
                        fips=task.fips,
                        startdate=task.startdate,
                        enddate=task.enddate,
                        casetype=task.casetype
                    )
                )
                self.session.commit()

                return {
                    'fips': str(task.fips).zfill(3),
                    'start_date': task.startdate,
                    'end_date': task.enddate,
                    'case_type': task.casetype
                }
            except IntegrityError:
                print 'WARNING - FAILED TO GET NEW TASK'
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

    def count_dates_searched_for_year(self, case_type, year):
        start_date = date(year, 1, 1)
        end_date = date(year+1, 1, 1)
        return self.session.query(self.date_search_builder).filter(
            self.date_search_builder.date >= start_date,
            self.date_search_builder.date < end_date,
            self.date_search_builder.casetype == case_type
        ).distinct(self.date_search_builder.fips, self.date_search_builder.date).count()

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
            case_builder.CaseNumber == case['case_number'],
            case_builder.details_fetched_for_hearing_date >= date
        ).first()
        if result is None:
            return result
        return  {
            'details_fetched_for_hearing_date': result.details_fetched_for_hearing_date
        }

    def replace_case_details(self, case, case_type):
        #pprint(case)
        case_builder = self.get_case_builder(case_type)
        self.session.query(case_builder).filter_by(
            fips=int(case['fips']),
            CaseNumber=case['case_number']
        ).delete()
        self.session.add(case_builder.create(case))
        self.session.commit()

    def list_people_to_id(self, date, letter, sex):
        people = []

        circuit_people = self.session.query(CircuitCriminalCase).filter(
            CircuitCriminalCase.DOB == date,
            CircuitCriminalCase.Defendant.startswith(letter),
            CircuitCriminalCase.Sex == sex
        ).with_entities(
            CircuitCriminalCase.id,
            CircuitCriminalCase.Defendant,
            CircuitCriminalCase.Address
        ).all()
        people.extend([{
            'id': p[0],
            'name': p[1],
            'address': p[2],
            'courtType': 'circuit'
        } for p in circuit_people])

        district_people = self.session.query(DistrictCriminalCase).filter(
            DistrictCriminalCase.DOB == date,
            DistrictCriminalCase.Name.startswith(letter),
            DistrictCriminalCase.Gender == sex
        ).with_entities(
            DistrictCriminalCase.id,
            DistrictCriminalCase.Name,
            DistrictCriminalCase.Address
        ).all()
        people.extend([{
            'id': p[0],
            'name': p[1],
            'address': p[2],
            'courtType': 'district'
        } for p in district_people])

        return people

    def set_person_id(self, court_type, case_ids, person_id):
        case_builder = CircuitCriminalCase if court_type == 'circuit' else DistrictCriminalCase
        self.session.query(case_builder).filter(
            case_builder.id.in_(case_ids)
        ).update({'person_id':person_id}, synchronize_session=False)

    def rollback(self):
        self.session.rollback()

    def disconnect(self):
        self.session.close()

    def get_cases_with_no_past_due(self, fips, case_type):
        case_builder = self.get_case_builder(case_type)
        cases = self.session.query(case_builder).filter(
            case_builder.fips == int(fips),
            case_builder.collected == None,
            case_builder.FineCostsDue != None
        )
        case_numbers = [(case.CaseNumber, case.details_fetched_for_hearing_date) for case in cases]
        return case_numbers

