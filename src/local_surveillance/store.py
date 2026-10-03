"""Run-isolated, transactional SQLite persistence and bounded evidence reads."""

from contextlib import contextmanager
import json
from pathlib import Path
import sqlite3
import time
import uuid

from .models import Event, dumps


class Store:
    def __init__(self, path):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.db = sqlite3.connect(self.path)
        self.db.row_factory = sqlite3.Row
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.execute("PRAGMA foreign_keys=ON")
        self.db.executescript("""
        CREATE TABLE IF NOT EXISTS runs (
          run_id TEXT PRIMARY KEY, mode TEXT NOT NULL, created REAL NOT NULL,
          config TEXT NOT NULL, status TEXT NOT NULL DEFAULT 'running',
          cursor INTEGER NOT NULL DEFAULT 0, error TEXT);
        CREATE TABLE IF NOT EXISTS raw_messages (
          seq INTEGER PRIMARY KEY, run_id TEXT NOT NULL REFERENCES runs,
          received_at REAL NOT NULL, connection_id TEXT NOT NULL, raw TEXT NOT NULL);
        CREATE INDEX IF NOT EXISTS raw_run ON raw_messages(run_id,seq);
        CREATE TABLE IF NOT EXISTS events (
          seq INTEGER PRIMARY KEY, run_id TEXT NOT NULL REFERENCES runs,
          event_id TEXT NOT NULL, ts REAL NOT NULL, received_at REAL NOT NULL,
          kind TEXT NOT NULL, symbol TEXT, source TEXT NOT NULL, payload TEXT NOT NULL,
          UNIQUE(run_id,event_id));
        CREATE INDEX IF NOT EXISTS event_run ON events(run_id,seq);
        CREATE INDEX IF NOT EXISTS event_time ON events(run_id,ts);
        CREATE TABLE IF NOT EXISTS reference_data (
          run_id TEXT NOT NULL REFERENCES runs, kind TEXT NOT NULL, item_id TEXT NOT NULL,
          payload TEXT NOT NULL, PRIMARY KEY(run_id,kind,item_id));
        CREATE TABLE IF NOT EXISTS alerts (
          run_id TEXT NOT NULL REFERENCES runs, alert_id TEXT NOT NULL,
          rule TEXT NOT NULL, updated_at REAL NOT NULL, payload TEXT NOT NULL,
          PRIMARY KEY(run_id,alert_id));
        CREATE TABLE IF NOT EXISTS alert_versions (
          run_id TEXT NOT NULL, alert_id TEXT NOT NULL, as_of_seq INTEGER NOT NULL,
          as_of_time REAL NOT NULL, payload TEXT NOT NULL,
          PRIMARY KEY(run_id,alert_id,as_of_seq));
        """)

    def close(self):
        self.db.close()

    @contextmanager
    def transaction(self):
        with self.db:
            yield

    def create_run(self, mode, config, run_id=None):
        rid = run_id or uuid.uuid4().hex
        with self.db:
            self.db.execute("INSERT INTO runs(run_id,mode,created,config) VALUES(?,?,?,?)",
                            (rid, mode, time.time(), dumps(config)))
        return rid

    def run(self, run_id):
        row = self.db.execute("SELECT * FROM runs WHERE run_id=?", (run_id,)).fetchone()
        if row is None:
            raise ValueError(f"Unknown run {run_id}")
        out = dict(row)
        out["config"] = json.loads(out["config"])
        return out

    def runs(self):
        return [self.run(r[0]) for r in self.db.execute("SELECT run_id FROM runs ORDER BY created")]

    def set_status(self, run_id, status, error=None):
        with self.db:
            self.db.execute("UPDATE runs SET status=?,error=? WHERE run_id=?", (status, error, run_id))

    def set_cursor(self, run_id, cursor):
        self.db.execute("UPDATE runs SET cursor=? WHERE run_id=?", (cursor, run_id))

    def save_raw(self, run_id, received_at, connection_id, raw):
        return self.db.execute("INSERT INTO raw_messages(run_id,received_at,connection_id,raw) VALUES(?,?,?,?)",
                               (run_id, received_at, connection_id, raw)).lastrowid

    def raw(self, run_id, after=0):
        for row in self.db.execute("SELECT * FROM raw_messages WHERE run_id=? AND seq>? ORDER BY seq",
                                   (run_id, after)):
            yield dict(row)

    def add_event(self, run_id, event):
        cur = self.db.execute("""INSERT OR IGNORE INTO events
          (run_id,event_id,ts,received_at,kind,symbol,source,payload) VALUES(?,?,?,?,?,?,?,?)""",
          (run_id, event.event_id, event.ts, event.received_at, event.kind,
           event.symbol, event.source, dumps(event.payload)))
        return cur.lastrowid if cur.rowcount else None

    def latest_seq(self, run_id):
        return self.db.execute("SELECT COALESCE(MAX(seq),0) FROM events WHERE run_id=?", (run_id,)).fetchone()[0]

    def events(self, run_id, *, as_of_seq, as_of_time, kinds=None, start=None, end=None,
               symbol=None, limit=None):
        clauses = ["run_id=?", "seq<=?", "ts<=?", "received_at<=?"]
        args = [run_id, as_of_seq, as_of_time, as_of_time]
        if kinds:
            clauses.append("kind IN (" + ",".join("?" for _ in kinds) + ")")
            args.extend(kinds)
        if start is not None:
            clauses.append("ts>=?"); args.append(start)
        if end is not None:
            clauses.append("ts<=?"); args.append(end)
        if symbol:
            clauses.append("symbol=?"); args.append(symbol)
        sql = "SELECT * FROM events WHERE " + " AND ".join(clauses) + " ORDER BY ts,seq"
        if limit is not None:
            sql += " LIMIT ?"; args.append(limit)
        for row in self.db.execute(sql, args):
            result = dict(row)
            result["payload"] = json.loads(result["payload"])
            yield result

    def put_reference(self, run_id, kind, item_id, value):
        payload = dumps(value)
        existing = self.db.execute("SELECT payload FROM reference_data WHERE run_id=? AND kind=? AND item_id=?",
                                   (run_id, kind, item_id)).fetchone()
        if existing:
            if existing[0] != payload:
                raise ValueError("Run-start references are immutable; start a new run to change them")
            return
        if self.latest_seq(run_id):
            raise ValueError("Reference records must be installed before ingestion starts")
        self.db.execute("INSERT INTO reference_data VALUES(?,?,?,?)",
                        (run_id, kind, item_id, payload))

    def references(self, run_id):
        result = {}
        for row in self.db.execute("SELECT * FROM reference_data WHERE run_id=?", (run_id,)):
            result.setdefault(row["kind"], {})[row["item_id"]] = json.loads(row["payload"])
        return result

    def save_alert(self, run_id, alert, seq, clock):
        payload = dumps(alert)
        self.db.execute("INSERT OR REPLACE INTO alerts VALUES(?,?,?,?,?)",
                        (run_id, alert["alert_id"], alert["rule"], clock, payload))
        self.db.execute("INSERT OR REPLACE INTO alert_versions VALUES(?,?,?,?,?)",
                        (run_id, alert["alert_id"], seq, clock, payload))

    def alerts(self, run_id, *, as_of_seq, as_of_time):
        rows = self.db.execute("""SELECT a.payload FROM alert_versions a JOIN (
          SELECT alert_id,MAX(as_of_seq) AS seq FROM alert_versions
          WHERE run_id=? AND as_of_seq<=? AND as_of_time<=? GROUP BY alert_id
        ) b ON a.alert_id=b.alert_id AND a.as_of_seq=b.seq WHERE a.run_id=? ORDER BY a.alert_id""",
                              (run_id, as_of_seq, as_of_time, run_id))
        return [json.loads(r[0]) for r in rows]
