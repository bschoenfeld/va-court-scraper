from __future__ import absolute_import
from courtreader import readers
from time import sleep
import datetime
import os
import sys
import time
import traceback
import socket
import threading
from sqlalchemy import create_engine, text

# Prevent infinite hangs on network sockets (which blocks Ctrl-C in Windows)
socket.setdefaulttimeout(60)

MONGO = False
POSTGRES = True

if MONGO:
    import pymongo
    from courtutils.databases.mongo import MongoDatabase
if POSTGRES:
    from courtutils.databases.postgres import PostgresDatabase

# configure logging
print('Worker running')

COURT_TYPE = sys.argv[1]
if COURT_TYPE != 'circuit' and COURT_TYPE != 'district':
    raise ValueError('Unknown court type')

def get_db_connection():
    if MONGO:
        return MongoDatabase('va_court_search', COURT_TYPE)
    if POSTGRES:
        return PostgresDatabase(COURT_TYPE)
    return None

# Lightweight engine for cheap "is there work?" checks while idle, so we don't
# rebuild a full PostgresDatabase (which re-checks the whole schema) every poll.
_idle_engine = None

def has_pending_work():
    global _idle_engine
    try:
        if _idle_engine is None:
            _idle_engine = create_engine('postgresql://' + os.environ['POSTGRES_DB'])
        table = '%s_court_date_tasks' % COURT_TYPE
        with _idle_engine.connect() as conn:
            count = conn.execute(text('SELECT COUNT(*) FROM %s' % table)).scalar()
        return (count or 0) > 0
    except Exception:
        # On any error, fall through to the normal (full) path.
        if _idle_engine is not None:
            try:
                _idle_engine.dispose()
            except Exception:
                pass
            _idle_engine = None
        return True

# Identify this collector process so the dashboard can show it even when idle.
WORKER_ID = '%s-%d' % (socket.gethostname(), os.getpid())
_worker_state = {'status': 'idle', 'fips': None, 'case_type': None}
_worker_state_lock = threading.Lock()
# Set whenever the worker state changes, so the presence thread writes the new
# status immediately instead of waiting for its next interval.
_worker_state_changed = threading.Event()

def set_worker_state(status, fips=None, case_type=None):
    with _worker_state_lock:
        _worker_state['status'] = status
        _worker_state['fips'] = int(fips) if fips else None
        _worker_state['case_type'] = case_type
    _worker_state_changed.set()

def start_worker_presence(interval=15):
    # Periodically record this process in the workers table (upsert keyed on
    # worker_id) so the dashboard sees it whether working or idle. Reuses one
    # lightweight engine.
    upsert_sql = text(
        'INSERT INTO workers (worker_id, court_type, status, fips, case_type, last_alive) '
        'VALUES (:wid, :ct, :status, :fips, :case_type, :now) '
        'ON CONFLICT (worker_id) DO UPDATE SET '
        'status = EXCLUDED.status, fips = EXCLUDED.fips, '
        'case_type = EXCLUDED.case_type, last_alive = EXCLUDED.last_alive'
    )
    def beat():
        engine = None
        while True:
            try:
                if engine is None:
                    engine = create_engine('postgresql://' + os.environ['POSTGRES_DB'])
                with _worker_state_lock:
                    st = dict(_worker_state)
                with engine.begin() as conn:
                    conn.execute(upsert_sql, {
                        'wid': WORKER_ID,
                        'ct': COURT_TYPE,
                        'status': st['status'],
                        'fips': st['fips'],
                        'case_type': st['case_type'],
                        'now': datetime.datetime.now(),
                    })
            except Exception:
                if engine is not None:
                    try:
                        engine.dispose()
                    except Exception:
                        pass
                    engine = None
            # Wake early (and write again) as soon as the state changes.
            if _worker_state_changed.wait(interval):
                _worker_state_changed.clear()
    t = threading.Thread(target=beat, daemon=True)
    t.start()

def get_cases_on_date(db, reader, fips, case_type, date, dateStr, batch_id=None):
    print('Getting cases on ' + dateStr)
    sleep(1)
    cases = reader.get_cases_by_date(fips, case_type, dateStr)
    total_cases = len(cases)
    for i, case in enumerate(cases, 1):
        try:
            case['details_fetched_for_hearing_date'] = date
            case['fips'] = fips
            case['collected'] = datetime.datetime.now()

            # If the hearing is in the future, add to the docket table - don't get details
            if date > datetime.datetime.now().date():
                print('[%s] [%d/%d] Docket %s %s' % (fips, i, total_cases, case['case_number'], case['defendant']))
                case['CaseNumber'] = case['case_number']
                case['Defendant'] = case['defendant']
                if case_type == 'civil':
                    case['CaseType'] = case['civil_case_type']
                    case['Plaintiff'] = case['plaintiff']
                db.add_case_to_docket(case, case_type)
                db.record_retrieved_case(batch_id, case_type, fips, case['case_number'])
                continue

            case_details = db.get_more_recent_case_details(case, case_type, date)
            if case_details != None:
                last_date = case_details['details_fetched_for_hearing_date'].strftime('%m/%d/%Y')
                collected_date = case_details['collected'].strftime('%m/%d/%Y')
                if case_details['details_fetched_for_hearing_date'] < case_details['collected']:
                    print('[%s] [%d/%d] %s details collected for hearing on %s' % (fips, i, total_cases, case['case_number'], last_date))
                    continue
                else:
                    print('[%s] [%d/%d] %s details were collected on %s before hearing date on %s - updating now' % (fips, i, total_cases, case['case_number'], collected_date, last_date))
            if '--' in case['case_number']:
                if case_type == 'civil':
                    case['details'] = {
                        'CaseNumber': case['case_number']
                    }
                elif 'defendant' in case:
                    case['details'] = {
                        'CaseNumber': case['case_number'],
                        'Defendant': case['defendant']
                    }
            else:
                if len(case['case_number']) < 13:
                    print('[%s] is an invalid case number' % (case['case_number'],))
                    continue
                case['details'] = reader.get_case_details_by_number(
                    fips, case_type, case['case_number'],
                    case['details_url'] if 'details_url' in case else None)
            if 'error' in case['details']:
                print('Could not collect case details for %s in %s' % (
                         case['case_number'], case['fips']))
            else:
                print('[%s] [%d/%d] %s %s' % (fips, i, total_cases, case['case_number'], case['defendant']))
                db.replace_case_details(case, case_type)
                db.record_retrieved_case(batch_id, case_type, fips, case['case_number'])
        except Exception as err:
            # Let timeouts bubble up so the whole date is retried later; skip a
            # single problematic case rather than abandoning the entire task.
            if isinstance(err, socket.timeout) or 'timeout' in str(err).lower() or 'read operation' in str(err).lower():
                raise
            print('Error collecting case %s in %s: %s. Skipping case.' % (
                case.get('case_number', '?'), fips, err))
            try:
                db.rollback()
            except Exception:
                pass

def start_heartbeat(task, interval=30):
    stop_event = threading.Event()
    active_table = '%s_court_active_date_tasks' % COURT_TYPE
    update_sql = text(
        'UPDATE %s SET last_alive = :now WHERE fips = :fips AND casetype = :ct'
        % active_table
    )
    def heartbeat():
        # Build one lightweight engine for the whole heartbeat and reuse it.
        # This avoids reconstructing PostgresDatabase every tick (which re-runs
        # table-creation/migration checks) and issues only a minimal UPDATE.
        engine = None
        params = {
            'fips': int(task['fips']),
            'ct': task['case_type'],
        }
        while not stop_event.wait(interval):
            try:
                if engine is None:
                    engine = create_engine('postgresql://' + os.environ['POSTGRES_DB'])
                params['now'] = datetime.datetime.now()
                with engine.begin() as conn:
                    conn.execute(update_sql, params)
            except Exception:
                if engine is not None:
                    try:
                        engine.dispose()
                    except Exception:
                        pass
                    engine = None
        if engine is not None:
            try:
                engine.dispose()
            except Exception:
                pass
    t = threading.Thread(target=heartbeat, daemon=True)
    t.start()
    return stop_event

def run_collector(reader, last_task):
    # Cheap idle check: with no finished task to clean up and no pending work,
    # skip building a full PostgresDatabase and just wait.
    if last_task is None and not has_pending_work():
        set_worker_state('idle')
        print('Nothing to do. Sleeping for 30 seconds.')
        sleep(30)
        return None

    db = get_db_connection()

    task = db.get_and_delete_date_task(last_task)
    if task is None:
        set_worker_state('idle')
        print('Nothing to do. Sleeping for 30 seconds.')
        sleep(30)
        db.disconnect()
        return

    set_worker_state('working', task['fips'], task['case_type'])
    heartbeat_stop = start_heartbeat(task)

    try:
        reader_connected = False

        fips = task['fips']
        start_date = task['start_date']
        end_date = task['end_date']
        case_type = task['case_type']
        batch_id = task.get('batch_id')

        print('Start %s %s %s-%s' % (
                 fips,
                 case_type,
                 start_date.strftime('%m/%d/%Y'),
                 end_date.strftime('%m/%d/%Y')))
        date = start_date

        searched_dates = set([
            d.strftime('%m/%d/%Y') if hasattr(d, 'strftime') else d 
            for d in db.get_date_searches(fips, case_type, start_date, end_date)
        ])

        while date >= end_date:
            date_search = {
                'fips': fips,
                'case_type': case_type,
                'date': date
            }
            date_str = date.strftime('%m/%d/%Y')
            if date_str in searched_dates:
                print(date_str + ' already searched')
            else:
                if not reader_connected:
                    reader.connect()
                    reader_connected = True
                try:
                    get_cases_on_date(db, reader, fips, case_type, date, date_str, batch_id)
                    db.add_date_search(date_search)
                    searched_dates.add(date_str)
                except Exception as err:
                    if isinstance(err, socket.timeout) or "timeout" in str(err).lower() or "read operation" in str(err).lower():
                        print('Timeout fetching cases for %s on %s. Skipping.' % (fips, date_str))
                    else:
                        raise
            date += datetime.timedelta(days=-1)

        if reader_connected:
            reader.log_off()
    except Exception as err:
        heartbeat_stop.set()
        print(traceback.format_exc())
        print('Putting task back')
        db.rollback()
        db.add_date_task(task, True)
        db.disconnect()
        try:
            reader.log_off()
        except:
            pass
        raise
    except KeyboardInterrupt:
        heartbeat_stop.set()
        print('Putting task back')
        db.rollback()
        db.add_date_task(task, True)
        db.disconnect()
        try:
            reader.log_off()
        except:
            pass
        raise

    heartbeat_stop.set()
    # Record the task as completed before disconnecting
    try:
        db.add_completed_date_task(task)
    except Exception:
        print('Warning: failed to record completed task')
    db.disconnect()
    return task

def get_reader():
    return readers.CircuitCourtReader() if 'circuit' in COURT_TYPE else \
            readers.DistrictCourtReader()

def run():
    # Make sure the schema (incl. the workers table) exists, then start
    # reporting this process's presence so the dashboard can show it when idle.
    try:
        get_db_connection().disconnect()
    except Exception:
        pass
    start_worker_presence()

    reader = None
    finished_task = None
    while True:
        try:
            if reader is None:
                reader = get_reader()
            finished_task = run_collector(reader, finished_task)
        except Exception as err:
            set_worker_state('idle')
            # The failed task was already put back inside run_collector, so don't
            # carry it forward as "finished" - reusing it would delete whatever
            # active row now holds that court (possibly another worker's claim).
            finished_task = None
            try:
                reader.log_off()
            except:
                pass
            reader = None
            print(traceback.format_exc())
            if isinstance(err, socket.timeout) or "timeout" in str(err).lower() or "read operation timed out" in str(err).lower():
                print('Network timeout occurred. Sleeping for 30 seconds before retrying...')
                sleep(30)
            else:
                print('Unexpected error. Sleeping for 60 seconds.')
                sleep(60)
run()
