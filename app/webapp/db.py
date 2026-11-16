"""Persistence layer for the FastAPI web platform."""
from __future__ import annotations

import json
import sqlite3
import time
import uuid
from typing import Any, Dict, List, Optional

from ..avro_codec import DEFAULT_SCHEMA_V1, fingerprint
from ..db_common import get_conn

_DDL = """
CREATE TABLE IF NOT EXISTS schema_versions (
    fp                  TEXT PRIMARY KEY,
    kind                TEXT NOT NULL CHECK (kind IN ('writer', 'reader')),
    version             INTEGER NOT NULL,
    schema_json         TEXT NOT NULL,
    created_at          TEXT NOT NULL,
    UNIQUE(kind, version)
);
CREATE TABLE IF NOT EXISTS writer_state (
    id                  INTEGER PRIMARY KEY CHECK (id = 1),
    effective_fp        TEXT NOT NULL,
    effective_version   INTEGER NOT NULL,
    updated_at          TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS events (
    id                  TEXT PRIMARY KEY,
    entity_id           TEXT,
    revision            INTEGER NOT NULL DEFAULT 1,
    writer_fp           TEXT NOT NULL,
    writer_version      INTEGER NOT NULL,
    payload_json        TEXT NOT NULL,
    blob_b64            TEXT NOT NULL,
    ingested_at         TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS event_current (
    entity_id           TEXT PRIMARY KEY,
    event_id            TEXT NOT NULL,
    revision            INTEGER NOT NULL,
    updated_at          TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS event_corrections (
    correction_id       TEXT PRIMARY KEY,
    entity_id           TEXT NOT NULL,
    old_event_id        TEXT NOT NULL,
    new_event_id        TEXT NOT NULL,
    seen_revision       INTEGER NOT NULL,
    new_revision        INTEGER NOT NULL,
    writer_fp           TEXT NOT NULL,
    writer_version      INTEGER NOT NULL,
    reason              TEXT NOT NULL,
    created_at          TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS writer_switches (
    switch_id           TEXT PRIMARY KEY,
    version             INTEGER NOT NULL,
    base_version        INTEGER NOT NULL,
    new_fp              TEXT NOT NULL,
    schema_json         TEXT NOT NULL,
    status              TEXT NOT NULL CHECK (status IN ('pending', 'confirmed', 'rejected')),
    target              TEXT NOT NULL DEFAULT 'local',
    last_error          TEXT,
    created_at          TEXT NOT NULL,
    updated_at          TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS reader_switches (
    switch_id           TEXT PRIMARY KEY,
    analyzer_id         TEXT NOT NULL,
    version             INTEGER NOT NULL,
    base_version        INTEGER NOT NULL,
    schema_json         TEXT NOT NULL,
    writer_fp           TEXT NOT NULL,
    status              TEXT NOT NULL CHECK (status IN ('pending', 'confirmed', 'rejected')),
    last_error          TEXT,
    created_at          TEXT NOT NULL,
    updated_at          TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS decode_results (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    event_id            TEXT NOT NULL,
    analyzer_id         TEXT NOT NULL,
    reader_version      INTEGER,
    decoded_ok          INTEGER NOT NULL,
    datum_json          TEXT,
    error               TEXT,
    decoded_at          TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS sim_config (
    id                  INTEGER PRIMARY KEY CHECK (id = 1),
    delay_ms            INTEGER NOT NULL DEFAULT 0,
    drop_writer_switch  INTEGER NOT NULL DEFAULT 0,
    drop_reader_switch  INTEGER NOT NULL DEFAULT 0
);
"""


def _now() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def init(db_path: str) -> None:
    conn = get_conn(db_path)
    try:
        conn.executescript(_DDL)
        _migrate_events(conn)
        fp = fingerprint(DEFAULT_SCHEMA_V1)
        conn.execute(
            "INSERT OR IGNORE INTO schema_versions(fp, kind, version, schema_json, created_at) "
            "VALUES (?, 'writer', 1, ?, ?)",
            (fp, json.dumps(DEFAULT_SCHEMA_V1), _now()),
        )
        conn.execute(
            "INSERT OR IGNORE INTO writer_state(id, effective_fp, effective_version, updated_at) "
            "VALUES (1, ?, 1, ?)", (fp, _now()),
        )
        conn.execute(
            "INSERT OR IGNORE INTO sim_config(id, delay_ms, drop_writer_switch, "
            "drop_reader_switch) VALUES (1, 0, 0, 0)"
        )
        conn.commit()
    finally:
        conn.close()


def _migrate_events(conn: sqlite3.Connection) -> None:
    """Bring pre-correction-era databases forward: add the entity/revision
    columns, treat every retained event as revision 1 of its own entity and
    backfill the current-pointer table. Old bytes and formats are untouched."""
    cols = {r["name"] for r in conn.execute("PRAGMA table_info(events)")}
    if "entity_id" not in cols:
        conn.execute("ALTER TABLE events ADD COLUMN entity_id TEXT")
    if "revision" not in cols:
        conn.execute(
            "ALTER TABLE events ADD COLUMN revision INTEGER NOT NULL DEFAULT 1")
    conn.execute("UPDATE events SET entity_id = id WHERE entity_id IS NULL")
    conn.execute(
        "INSERT OR IGNORE INTO event_current(entity_id, event_id, revision, updated_at) "
        "SELECT entity_id, id, revision, ingested_at FROM events")


# ----- schemas --------------------------------------------------------------

def register_schema(conn: sqlite3.Connection, kind: str, version: int,
                    schema: Dict[str, Any]) -> str:
    fp = fingerprint(schema)
    conn.execute(
        "INSERT OR IGNORE INTO schema_versions(fp, kind, version, schema_json, created_at) "
        "VALUES (?, ?, ?, ?, ?)",
        (fp, kind, version, json.dumps(schema), _now()),
    )
    return fp


def next_version(conn: sqlite3.Connection, kind: str) -> int:
    row = conn.execute(
        "SELECT COALESCE(MAX(version), 0) AS m FROM schema_versions WHERE kind = ?", (kind,)
    ).fetchone()
    # Both formats start at v1 (the default schema shipped in every service),
    # so the first *switch* must propose v2.
    return max(2, row["m"] + 1)


def get_schema_by_fp(conn: sqlite3.Connection, fp: str) -> Optional[Dict[str, Any]]:
    row = conn.execute(
        "SELECT schema_json FROM schema_versions WHERE fp = ?", (fp,)
    ).fetchone()
    return json.loads(row["schema_json"]) if row else None


def all_writer_schemas(conn: sqlite3.Connection) -> List[Dict[str, Any]]:
    rows = conn.execute(
        "SELECT fp, version, schema_json, created_at FROM schema_versions "
        "WHERE kind = 'writer' ORDER BY version"
    ).fetchall()
    return [dict(fp=r["fp"], version=r["version"],
                 schema=json.loads(r["schema_json"]), created_at=r["created_at"])
            for r in rows]


# ----- writer state ---------------------------------------------------------

def get_writer_state(conn: sqlite3.Connection) -> sqlite3.Row:
    return conn.execute("SELECT * FROM writer_state WHERE id = 1").fetchone()


def set_writer_state(conn: sqlite3.Connection, fp: str, version: int) -> None:
    conn.execute(
        "UPDATE writer_state SET effective_fp = ?, effective_version = ?, updated_at = ? "
        "WHERE id = 1", (fp, version, _now()),
    )


# ----- events ---------------------------------------------------------------

def insert_event(conn: sqlite3.Connection, *, writer_fp: str, writer_version: int,
                 payload: Dict[str, Any], blob_b64: str,
                 entity_id: Optional[str] = None, revision: int = 1) -> str:
    event_id = "evt_" + uuid.uuid4().hex[:12]
    conn.execute(
        "INSERT INTO events(id, entity_id, revision, writer_fp, writer_version, "
        "payload_json, blob_b64, ingested_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        (event_id, entity_id or event_id, revision, writer_fp, writer_version,
         json.dumps(payload, ensure_ascii=False), blob_b64, _now()),
    )
    return event_id


def list_events(conn: sqlite3.Connection) -> List[sqlite3.Row]:
    return conn.execute(
        "SELECT * FROM events ORDER BY ingested_at DESC, rowid DESC"
    ).fetchall()


def get_event(conn: sqlite3.Connection, event_id: str) -> Optional[sqlite3.Row]:
    return conn.execute("SELECT * FROM events WHERE id = ?", (event_id,)).fetchone()


def list_entity_events(conn: sqlite3.Connection, entity_id: str) -> List[sqlite3.Row]:
    """Every retained revision of one entity, oldest first (the chain)."""
    return conn.execute(
        "SELECT * FROM events WHERE entity_id = ? ORDER BY revision, rowid",
        (entity_id,),
    ).fetchall()


# ----- current pointer (analysts see only the latest revision) ---------------

def set_current(conn: sqlite3.Connection, entity_id: str, event_id: str,
                revision: int) -> None:
    conn.execute(
        "INSERT INTO event_current(entity_id, event_id, revision, updated_at) "
        "VALUES (?, ?, ?, ?) ON CONFLICT(entity_id) DO UPDATE SET "
        "event_id = excluded.event_id, revision = excluded.revision, "
        "updated_at = excluded.updated_at",
        (entity_id, event_id, revision, _now()),
    )


def get_current(conn: sqlite3.Connection, entity_id: str) -> Optional[sqlite3.Row]:
    return conn.execute(
        "SELECT * FROM event_current WHERE entity_id = ?", (entity_id,),
    ).fetchone()


def advance_current(conn: sqlite3.Connection, entity_id: str, new_event_id: str,
                    seen_revision: int) -> bool:
    """Compare-and-swap the current pointer; False iff the revision moved."""
    cur = conn.execute(
        "UPDATE event_current SET event_id = ?, revision = revision + 1, "
        "updated_at = ? WHERE entity_id = ? AND revision = ?",
        (new_event_id, _now(), entity_id, seen_revision),
    )
    return cur.rowcount == 1


def list_current_events(conn: sqlite3.Connection) -> List[sqlite3.Row]:
    """One row per entity: the event the current pointer references."""
    return conn.execute(
        "SELECT e.* FROM event_current c JOIN events e ON e.id = c.event_id "
        "ORDER BY c.updated_at DESC, e.rowid DESC"
    ).fetchall()


# ----- corrections (revision relation audit) ---------------------------------

def insert_correction(conn: sqlite3.Connection, *, entity_id: str,
                      old_event_id: str, new_event_id: str, seen_revision: int,
                      new_revision: int, writer_fp: str, writer_version: int,
                      reason: str) -> str:
    correction_id = "cor_" + uuid.uuid4().hex[:12]
    conn.execute(
        "INSERT INTO event_corrections(correction_id, entity_id, old_event_id, "
        "new_event_id, seen_revision, new_revision, writer_fp, writer_version, "
        "reason, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (correction_id, entity_id, old_event_id, new_event_id, seen_revision,
         new_revision, writer_fp, writer_version, reason, _now()),
    )
    return correction_id


def list_corrections(conn: sqlite3.Connection, entity_id: Optional[str] = None,
                     limit: int = 50) -> List[sqlite3.Row]:
    if entity_id is not None:
        return conn.execute(
            "SELECT * FROM event_corrections WHERE entity_id = ? "
            "ORDER BY created_at DESC, rowid DESC LIMIT ?",
            (entity_id, limit),
        ).fetchall()
    return conn.execute(
        "SELECT * FROM event_corrections ORDER BY created_at DESC, rowid DESC "
        "LIMIT ?", (limit,),
    ).fetchall()


# ----- switches -------------------------------------------------------------

def insert_writer_switch(conn: sqlite3.Connection, *, version: int, base_version: int,
                         new_fp: str, schema: Dict[str, Any], target: str) -> str:
    switch_id = "wsw_" + uuid.uuid4().hex[:12]
    conn.execute(
        "INSERT INTO writer_switches(switch_id, version, base_version, new_fp, schema_json, "
        "status, target, last_error, created_at, updated_at) VALUES (?, ?, ?, ?, ?, 'pending', "
        "?, NULL, ?, ?)",
        (switch_id, version, base_version, new_fp, json.dumps(schema), target, _now(), _now()),
    )
    return switch_id


def update_writer_switch(conn: sqlite3.Connection, switch_id: str, status: str,
                         error: Optional[str] = None) -> None:
    conn.execute(
        "UPDATE writer_switches SET status = ?, last_error = ?, updated_at = ? "
        "WHERE switch_id = ?", (status, error, _now(), switch_id),
    )


def get_writer_switch(conn: sqlite3.Connection, switch_id: str) -> Optional[sqlite3.Row]:
    return conn.execute(
        "SELECT * FROM writer_switches WHERE switch_id = ?", (switch_id,)
    ).fetchone()


def pending_writer_switches(conn: sqlite3.Connection) -> List[sqlite3.Row]:
    return conn.execute(
        "SELECT * FROM writer_switches WHERE status = 'pending' ORDER BY version"
    ).fetchall()


def insert_reader_switch(conn: sqlite3.Connection, *, analyzer_id: str, version: int,
                         base_version: int, schema: Dict[str, Any], writer_fp: str) -> str:
    switch_id = "rsw_" + uuid.uuid4().hex[:12]
    conn.execute(
        "INSERT INTO reader_switches(switch_id, analyzer_id, version, base_version, schema_json, "
        "writer_fp, status, last_error, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, "
        "'pending', NULL, ?, ?)",
        (switch_id, analyzer_id, version, base_version, json.dumps(schema),
         writer_fp, _now(), _now()),
    )
    return switch_id


def update_reader_switch(conn: sqlite3.Connection, switch_id: str, status: str,
                         error: Optional[str] = None) -> None:
    conn.execute(
        "UPDATE reader_switches SET status = ?, last_error = ?, updated_at = ? "
        "WHERE switch_id = ?", (status, error, _now(), switch_id),
    )


def get_reader_switch(conn: sqlite3.Connection, switch_id: str) -> Optional[sqlite3.Row]:
    return conn.execute(
        "SELECT * FROM reader_switches WHERE switch_id = ?", (switch_id,)
    ).fetchone()


def pending_reader_switches(conn: sqlite3.Connection) -> List[sqlite3.Row]:
    return conn.execute(
        "SELECT * FROM reader_switches WHERE status = 'pending' ORDER BY created_at"
    ).fetchall()


# ----- decode results -------------------------------------------------------

def insert_decode_result(conn: sqlite3.Connection, *, event_id: str, analyzer_id: str,
                         reader_version: Optional[int], decoded_ok: bool,
                         datum: Any, error: Optional[str]) -> None:
    conn.execute(
        "INSERT INTO decode_results(event_id, analyzer_id, reader_version, decoded_ok, "
        "datum_json, error, decoded_at) VALUES (?, ?, ?, ?, ?, ?, ?)",
        (event_id, analyzer_id, reader_version, int(decoded_ok),
         json.dumps(datum, ensure_ascii=False) if datum is not None else None, error, _now()),
    )


def latest_decode_results(conn: sqlite3.Connection, limit: int = 20) -> List[sqlite3.Row]:
    return conn.execute(
        "SELECT * FROM decode_results ORDER BY id DESC LIMIT ?", (limit,)
    ).fetchall()


# ----- simulation -----------------------------------------------------------

def get_sim(conn: sqlite3.Connection) -> Dict[str, Any]:
    row = conn.execute("SELECT * FROM sim_config WHERE id = 1").fetchone()
    return {
        "delay_ms": row["delay_ms"],
        "drop_writer_switch": bool(row["drop_writer_switch"]),
        "drop_reader_switch": bool(row["drop_reader_switch"]),
    }


def set_sim(conn: sqlite3.Connection, delay_ms: int, drop_writer: bool,
            drop_reader: bool) -> None:
    conn.execute(
        "UPDATE sim_config SET delay_ms = ?, drop_writer_switch = ?, "
        "drop_reader_switch = ? WHERE id = 1",
        (max(0, int(delay_ms)), int(bool(drop_writer)), int(bool(drop_reader))),
    )


def consume_drop_flag(conn: sqlite3.Connection, flag: str) -> bool:
    """One-shot read-and-reset inside the same transaction."""
    assert flag in {"drop_writer_switch", "drop_reader_switch"}
    row = conn.execute(f"SELECT {flag} AS v FROM sim_config WHERE id = 1").fetchone()
    armed = bool(row["v"])
    if armed:
        conn.execute(f"UPDATE sim_config SET {flag} = 0 WHERE id = 1")
    return armed
