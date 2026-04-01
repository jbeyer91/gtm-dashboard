"""
Persistent storage for locked monthly scorecard summaries.

Summaries are generated once per month (for the just-completed prior month),
written once, and never overwritten.  The store survives process restarts
because it is a JSON file on disk.

Default location: $SUMMARY_DIR/monthly_summaries.json
Override SUMMARY_DIR env var to point at a mounted persistent disk on Render
so history survives redeploys.

──────────────────────────────────────────
Record schema (one record per entity/month)
──────────────────────────────────────────
{
  "year":                 2026,          # int
  "month":                2,             # int  1-12
  "entity_type":          "rep",         # "rep" | "team"
  "entity_id":            "12345",       # HubSpot owner_id, or "team"
  "entity_label":         "Smith",       # display name
  "final_grade":          "B+",          # letter grade string, or ""
  "metrics":              { ... },       # raw numbers snapshotted at generation
  "main_takeaway":        "...",         # one sentence
  "why":                  ["1. ...", "2. ..."],   # 2-3 items
  "next_focus":           ["...", "..."],          # 2-3 items
  "generation_timestamp": "2026-03-01T00:05:00Z"
}

Storage layout inside monthly_summaries.json
---------------------------------------------
{
  "records": {
    "<entity_type>:<entity_id>:<YYYY-MM>": { ...record }
  }
}
"""

import json
import os
import threading
from datetime import datetime, timezone
from typing import Optional

SUMMARY_DIR = os.environ.get("SUMMARY_DIR", "/tmp/gtm_summaries")
_STORE_PATH = os.path.join(SUMMARY_DIR, "monthly_summaries.json")
_lock       = threading.Lock()

os.makedirs(SUMMARY_DIR, exist_ok=True)

_REQUIRED_FIELDS = {
    "year", "month", "entity_type", "entity_id", "entity_label",
    "main_takeaway", "why", "next_focus",
}


# ── Internal helpers ──────────────────────────────────────────────────────────

def _load() -> dict:
    """Read the store from disk; return an empty structure on any error."""
    try:
        with open(_STORE_PATH, "r", encoding="utf-8") as fh:
            data = json.load(fh)
        data.setdefault("records", {})
        data.setdefault("settings", {})
        return data
    except (FileNotFoundError, json.JSONDecodeError):
        return {"records": {}, "settings": {}}


def _persist(data: dict) -> None:
    """Atomically write the store to disk (no partial files on crash)."""
    tmp = _STORE_PATH + ".tmp"
    with open(tmp, "w", encoding="utf-8") as fh:
        json.dump(data, fh, indent=2, ensure_ascii=False)
    os.replace(tmp, _STORE_PATH)


def _record_key(entity_type: str, entity_id: str, year: int, month: int) -> str:
    return f"{entity_type}:{entity_id}:{year:04d}-{month:02d}"


# ── Public API ────────────────────────────────────────────────────────────────

def delete_month(year: int, month: int) -> int:
    """Delete all records for a given month (both team and all reps).

    Returns the number of records deleted.  Use this to correct summaries
    that were locked against stale data — delete then re-generate.
    """
    suffix = f":{year:04d}-{month:02d}"
    with _lock:
        data = _load()
        keys_to_delete = [k for k in data["records"] if k.endswith(suffix)]
        for k in keys_to_delete:
            del data["records"][k]
        if keys_to_delete:
            _persist(data)
    return len(keys_to_delete)


def save_summary(record: dict) -> bool:
    """Persist a monthly summary record.

    Returns True if saved, False if a record for this entity/month already
    exists (locked — never overwritten).

    The caller must supply all _REQUIRED_FIELDS.  Optional fields
    (final_grade, metrics) default to empty string / empty dict if absent.
    """
    missing = _REQUIRED_FIELDS - record.keys()
    if missing:
        raise ValueError(f"save_summary: missing required fields: {missing}")

    entity_type = record["entity_type"]
    entity_id   = record["entity_id"]
    year        = int(record["year"])
    month       = int(record["month"])
    key         = _record_key(entity_type, entity_id, year, month)

    with _lock:
        data = _load()
        if key in data["records"]:
            return False  # already locked — never overwrite

        data["records"][key] = {
            "year":                 year,
            "month":                month,
            "entity_type":          entity_type,
            "entity_id":            entity_id,
            "entity_label":         record["entity_label"],
            "final_grade":          record.get("final_grade", ""),
            "metrics":              record.get("metrics", {}),
            "main_takeaway":        record["main_takeaway"],
            "why":                  record["why"],
            "next_focus":           record["next_focus"],
            "generation_timestamp": datetime.now(timezone.utc).strftime(
                                        "%Y-%m-%dT%H:%M:%SZ"
                                    ),
        }
        _persist(data)
        return True


def get_latest_rep_summary(owner_id: str) -> Optional[dict]:
    """Return the most recent locked summary for a rep, or None."""
    history = get_rep_history(owner_id)
    return history[0] if history else None


def get_latest_team_summary() -> Optional[dict]:
    """Return the most recent locked whole-team summary, or None."""
    history = get_team_history()
    return history[0] if history else None


def get_rep_history(owner_id: str) -> list:
    """Return all locked summaries for a rep, sorted newest-first."""
    with _lock:
        store = _load()
    prefix = f"rep:{owner_id}:"
    records = [v for k, v in store["records"].items() if k.startswith(prefix)]
    records.sort(key=lambda r: (r["year"], r["month"]), reverse=True)
    return records


def get_team_history() -> list:
    """Return all locked whole-team summaries, sorted newest-first."""
    with _lock:
        store = _load()
    records = [v for v in store["records"].values() if v["entity_type"] == "team"]
    records.sort(key=lambda r: (r["year"], r["month"]), reverse=True)
    return records


def last_completed_month() -> tuple:
    """Return (year, month) of the most recently completed calendar month."""
    now = datetime.now(timezone.utc)
    if now.month == 1:
        return now.year - 1, 12
    return now.year, now.month - 1


# ── Departed rep grace period ─────────────────────────────────────────────────
# Reps added here continue to pass the analytics team filter through month-end
# so their summary is generated correctly even after HubSpot team removal.

def add_grace_rep(owner_id: str, label: str) -> None:
    """Mark a departed rep to stay in analytics through end of current month."""
    with _lock:
        data = _load()
        data.setdefault("grace_reps", {})[owner_id] = label
        _persist(data)


def remove_grace_rep(owner_id: str) -> None:
    """Remove a rep from the grace list (call after their month-end summary is locked)."""
    with _lock:
        data = _load()
        data.get("grace_reps", {}).pop(owner_id, None)
        _persist(data)


def get_grace_rep_ids() -> frozenset:
    """Return the set of owner IDs currently in the grace period."""
    with _lock:
        data = _load()
    return frozenset(data.get("grace_reps", {}).keys())


def get_grace_reps() -> dict:
    """Return {owner_id: label} for all reps currently in the grace period."""
    with _lock:
        data = _load()
    return dict(data.get("grace_reps", {}))


def get_all_rep_ids_with_history() -> dict:
    """Return {owner_id: label} for every rep that has at least one locked record.

    Used by the history page to keep departed reps visible even after they are
    removed from the active HubSpot team.
    """
    with _lock:
        data = _load()
    result = {}
    for v in data["records"].values():
        if v["entity_type"] == "rep":
            result[v["entity_id"]] = v["entity_label"]
    return result


def get_admin_settings() -> dict:
    """Return persisted admin allowlists."""
    with _lock:
        data = _load()
    settings = data.get("settings", {})
    return {
        "admin_emails": list(settings.get("admin_emails", [])),
        "admin_owner_ids": list(settings.get("admin_owner_ids", [])),
    }


def update_admin_settings(admin_emails: list[str], admin_owner_ids: list[str]) -> dict:
    """Persist admin allowlists and return the normalized values."""
    normalized = {
        "admin_emails": sorted({(email or "").strip().lower() for email in admin_emails if (email or "").strip()}),
        "admin_owner_ids": sorted({(owner_id or "").strip() for owner_id in admin_owner_ids if (owner_id or "").strip()}),
    }
    with _lock:
        data = _load()
        data.setdefault("settings", {})
        data["settings"]["admin_emails"] = normalized["admin_emails"]
        data["settings"]["admin_owner_ids"] = normalized["admin_owner_ids"]
        _persist(data)
    return normalized
