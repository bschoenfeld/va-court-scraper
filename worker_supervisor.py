from __future__ import absolute_import
from __future__ import print_function
import os
import sys
import time
import socket
import atexit
import subprocess
from datetime import datetime

from sqlalchemy import create_engine, text

# Runs on each machine that should host collectors. Polls the worker_targets
# table (set from the dashboard) and starts/stops local court_bulk_collector.py
# processes so the number running matches the desired count. The dashboard never
# spawns processes itself - it only writes the desired number here.
#
# Only one supervisor may run per host (enforced by a lock file). On startup it
# kills any collectors already registered for this host, so it always begins from
# a known state and cleans up orphans left by a previous run.

COURT_TYPES = ['district', 'circuit']
POLL_SECONDS = 5
MAX_WORKERS = 10  # the court site becomes unstable past ~10 collectors
LOG_DIR = 'worker_logs'  # each collector's output goes to its own file here
LOCK_FILE = 'worker_supervisor.lock'

engine = create_engine('postgresql://' + os.environ['POSTGRES_DB'])

# Child collector processes we started, keyed by court type.
children = {ct: [] for ct in COURT_TYPES}


def pid_alive(pid):
    if os.name == 'nt':
        try:
            out = subprocess.run(
                ['tasklist', '/FI', 'PID eq %d' % pid],
                capture_output=True, text=True)
            return ('No tasks' not in out.stdout) and (str(pid) in out.stdout)
        except Exception:
            return False
    try:
        os.kill(pid, 0)
        return True
    except Exception:
        return False


def kill_pid(pid):
    try:
        if os.name == 'nt':
            subprocess.call(['taskkill', '/F', '/PID', str(pid)],
                            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        else:
            os.kill(pid, 15)
    except Exception:
        pass


def acquire_lock():
    """Refuse to start if another live supervisor holds the lock on this host."""
    if os.path.exists(LOCK_FILE):
        try:
            with open(LOCK_FILE) as f:
                old_pid = int(f.read().strip())
            if old_pid != os.getpid() and pid_alive(old_pid):
                print('Another supervisor (pid=%d) is already running. Exiting.' % old_pid)
                return False
        except Exception:
            pass  # stale/unreadable lock - take it over
    try:
        with open(LOCK_FILE, 'w') as f:
            f.write(str(os.getpid()))
        atexit.register(_release_lock)
    except Exception:
        pass
    return True


def _release_lock():
    try:
        if os.path.exists(LOCK_FILE):
            os.remove(LOCK_FILE)
    except Exception:
        pass


def registered_pids():
    """Collector pids registered in the workers table for this host (worker_id is
    'hostname-pid'), keyed by court type."""
    host = socket.gethostname()
    pids = {}
    try:
        with engine.connect() as conn:
            rows = conn.execute(text(
                'SELECT worker_id, court_type FROM workers WHERE worker_id LIKE :pfx'
            ), {'pfx': host + '-%'}).fetchall()
        for worker_id, court_type in rows:
            try:
                pids.setdefault(court_type, []).append(int(worker_id.rsplit('-', 1)[1]))
            except Exception:
                pass
    except Exception:
        pass
    return pids


def clean_slate():
    """Kill any collectors already registered for this host so we start fresh."""
    killed = 0
    for court_type, pids in registered_pids().items():
        for pid in pids:
            if pid_alive(pid):
                kill_pid(pid)
                killed += 1
    if killed:
        print('Cleaned up %d pre-existing collector(s) on startup' % killed)


def ensure_target_table():
    try:
        with engine.begin() as conn:
            conn.execute(text(
                'CREATE TABLE IF NOT EXISTS worker_targets ('
                ' court_type VARCHAR PRIMARY KEY, desired_count INTEGER, updated_at TIMESTAMP)'
            ))
    except Exception:
        pass


def get_desired(court_type):
    try:
        with engine.connect() as conn:
            val = conn.execute(text(
                'SELECT desired_count FROM worker_targets WHERE court_type = :ct'
            ), {'ct': court_type}).scalar()
        return max(0, min(MAX_WORKERS, int(val))) if val is not None else 0
    except Exception:
        return None  # DB unreachable - leave things as they are


def reap(court_type):
    alive = []
    for p in children[court_type]:
        if p.poll() is None:
            alive.append(p)
        else:
            logf = getattr(p, '_logf', None)
            if logf is not None:
                try:
                    logf.close()
                except Exception:
                    pass
    children[court_type] = alive


def start_worker(court_type):
    if not os.path.isdir(LOG_DIR):
        try:
            os.makedirs(LOG_DIR)
        except Exception:
            pass
    log_path = os.path.join(
        LOG_DIR, '%s-%s.log' % (court_type, datetime.now().strftime('%Y%m%d-%H%M%S-%f')))
    logf = open(log_path, 'a')
    # Run windowless and send output to the log file instead of a new console.
    kwargs = {'stdout': logf, 'stderr': subprocess.STDOUT}
    if os.name == 'nt':
        kwargs['creationflags'] = subprocess.CREATE_NO_WINDOW
    p = subprocess.Popen([sys.executable, 'court_bulk_collector.py', court_type], **kwargs)
    p._logf = logf  # keep the file handle alive until the process is reaped
    children[court_type].append(p)
    print('[%s] started %s collector pid=%d -> %s' % (
        datetime.now().strftime('%H:%M:%S'), court_type, p.pid, log_path))


def stop_worker(court_type):
    if children[court_type]:
        p = children[court_type].pop()
        try:
            p.terminate()
            print('[%s] stopped %s collector pid=%d' % (
                datetime.now().strftime('%H:%M:%S'), court_type, p.pid))
        except Exception:
            pass


def main():
    if not acquire_lock():
        return
    ensure_target_table()
    clean_slate()
    print('Supervisor running on %s (poll every %ds)' % (socket.gethostname(), POLL_SECONDS))
    while True:
        for court_type in COURT_TYPES:
            reap(court_type)
            desired = get_desired(court_type)
            if desired is None:
                continue
            running = len(children[court_type])
            while running < desired:
                start_worker(court_type)
                running += 1
            while running > desired:
                stop_worker(court_type)
                running -= 1
        time.sleep(POLL_SECONDS)


if __name__ == '__main__':
    main()
