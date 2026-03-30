"""Persistent store for locked monthly rep and team summaries.

Storage: SQLite at data/summaries.db (auto-created on first write).

Schema
------
summaries(
  id            INTEGER PRIMARY KEY AUTOINCREMENT,
  year          INTEGER NOT NULL,
  month         INTEGER NOT NULL,
  entity_type   TEXT    NOT NULL,  -- 'rep' | 'team'
  entity_id     TEXT    NOT NULL,  -- owner_id for reps; 'team' for the team record
  entity_name   TEXT,
  grade         TEXT,
  snapshot_json TEXT,              -- JSON: month-end metric snapshot
  summary_json  TEXT,              -- JSON: {takeaway, why, next_focus}
  generated_at  TEXT,              -- ISO-8601 UTC timestamp
  UNIQUE (year, month, entity_type, entity_id)
)
"""
import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

DB_PATH = Path(__file__).parent / "data" / "summaries.db"


def _connect() -> sqlite3.Connection:
    DB_PATH.parent.mkdir(exist_ok=True)
    con = sqlite3.connect(str(DB_PATH), check_same_thread=False)
    con.row_factory = sqlite3.Row
    return con


def _init(con: sqlite3.Connection) -> None:
    con.execute("""
        CREATE TABLE IF NOT EXISTS summaries (
            id            INTEGER PRIMARY KEY AUTOINCREMENT,
            year          INTEGER NOT NULL,
            month         INTEGER NOT NULL,
            entity_type   TEXT    NOT NULL,
            entity_id     TEXT    NOT NULL,
            entity_name   TEXT,
            grade         TEXT,
            snapshot_json TEXT,
            summary_json  TEXT,
            generated_at  TEXT,
            UNIQUE (year, month, entity_type, entity_id)
        )
    """)
    con.commit()


def save(
    year: int, month: int,
    entity_type: str, entity_id: str, entity_name: str,
    grade: str, snapshot: dict, summary: dict,
) -> None:
    """Upsert a monthly summary record."""
    with _connect() as con:
        _init(con)
        con.execute("""
            INSERT INTO summaries
                (year, month, entity_type, entity_id, entity_name, grade,
                 snapshot_json, summary_json, generated_at)
            VALUES (?,?,?,?,?,?,?,?,?)
            ON CONFLICT (year, month, entity_type, entity_id) DO UPDATE SET
                entity_name   = excluded.entity_name,
                grade         = excluded.grade,
                snapshot_json = excluded.snapshot_json,
                summary_json  = excluded.summary_json,
                generated_at  = excluded.generated_at
        """, (
            year, month, entity_type, entity_id, entity_name, grade,
            json.dumps(snapshot), json.dumps(summary),
            datetime.now(timezone.utc).isoformat(),
        ))


def get(year: int, month: int, entity_type: str, entity_id: str) -> dict | None:
    """Return a single summary record or None if not found."""
    with _connect() as con:
        _init(con)
        row = con.execute("""
            SELECT * FROM summaries
            WHERE year=? AND month=? AND entity_type=? AND entity_id=?
        """, (year, month, entity_type, entity_id)).fetchone()
    return _parse(row) if row else None


def history(entity_type: str, entity_id: str, limit: int = 24) -> list[dict]:
    """Return summaries for one entity, newest first."""
    with _connect() as con:
        _init(con)
        rows = con.execute("""
            SELECT * FROM summaries
            WHERE entity_type=? AND entity_id=?
            ORDER BY year DESC, month DESC
            LIMIT ?
        """, (entity_type, entity_id, limit)).fetchall()
    return [_parse(r) for r in rows]


def exists(year: int, month: int) -> bool:
    """Return True if at least one summary record exists for this year/month."""
    with _connect() as con:
        _init(con)
        n = con.execute(
            "SELECT COUNT(*) FROM summaries WHERE year=? AND month=?",
            (year, month),
        ).fetchone()[0]
    return n > 0


def _parse(row: sqlite3.Row) -> dict:
    d = dict(row)
    d["snapshot"] = json.loads(d.pop("snapshot_json") or "{}")
    d["summary"]  = json.loads(d.pop("summary_json")  or "{}")
    return d
