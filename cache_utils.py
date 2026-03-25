"""
Two-layer TTL cache for HubSpot analytics results.

Layer 1 — memory : plain dict, fast, reset on process restart.
Layer 2 — disk   : pickle files in CACHE_DIR, survive OOM crashes and
                   worker restarts within the same Render deployment.

On startup the module scans the disk cache so a worker that was killed
by OOM and restarted can serve stale-but-valid data immediately while
the background scheduler rebuilds fresh data.  Users never experience
a full cold-cache rebuild after a crash.

No external dependencies — stdlib only (pickle, hashlib, threading).
"""

import gc
import hashlib
import logging
import os
import pickle
import threading
import time
from functools import wraps

log = logging.getLogger(__name__)

TTL       = int(os.environ.get("CACHE_TTL_SECONDS", 3600))   # default: 1 hour
CACHE_DIR = os.environ.get("CACHE_DIR", "/tmp/gtm_cache")

os.makedirs(CACHE_DIR, exist_ok=True)

_store: dict = {}           # key → (result, expires_at)
_last_refreshed: list = [0.0]
_disk_lock = threading.Lock()


# ── Helpers ──────────────────────────────────────────────────────────────────

def _to_hashable(v):
    """Recursively convert unhashable types so they can be part of a cache key."""
    if isinstance(v, list):
        return tuple(_to_hashable(i) for i in v)
    if isinstance(v, dict):
        return tuple(sorted((k, _to_hashable(val)) for k, val in v.items()))
    return v


def _key_to_path(key: tuple) -> str:
    """Stable, filesystem-safe path for a cache key."""
    digest = hashlib.sha256(repr(key).encode()).hexdigest()[:20]
    return os.path.join(CACHE_DIR, f"{digest}.pkl")


def _read_disk(key: tuple):
    """Return (result, expires_at) from disk, or None on miss / any error."""
    path = _key_to_path(key)
    try:
        with _disk_lock:
            if not os.path.exists(path):
                return None
            with open(path, "rb") as fh:
                return pickle.load(fh)          # (result, expires_at)
    except Exception:
        return None


def _write_disk(key: tuple, result, expires_at: float) -> None:
    """Atomically write (result, expires_at) to disk. Failures are non-fatal."""
    path = _key_to_path(key)
    tmp  = path + ".tmp"
    try:
        with _disk_lock:
            with open(tmp, "wb") as fh:
                pickle.dump((result, expires_at), fh,
                            protocol=pickle.HIGHEST_PROTOCOL)
            os.replace(tmp, path)           # atomic rename → no partial files
    except Exception:
        pass


def _restore_last_refreshed() -> None:
    """Infer last_refreshed from the newest valid disk cache file.

    After an OOM crash the nav badge shows e.g. '45m ago' instead of
    'not yet loaded', so leadership doesn't see a blank / broken indicator.
    """
    try:
        pkls = [
            os.path.join(CACHE_DIR, f)
            for f in os.listdir(CACHE_DIR)
            if f.endswith(".pkl")
        ]
        if not pkls:
            return
        newest_mtime = max(os.path.getmtime(p) for p in pkls)
        if time.time() - newest_mtime < TTL:        # only if still within TTL
            _last_refreshed[0] = newest_mtime
    except Exception:
        pass


# Run once at import time
_restore_last_refreshed()


# ── Logging helpers ───────────────────────────────────────────────────────────

def _log_key(args) -> str:
    """Compact representation of positional args for log lines, e.g. '(this_month)'."""
    return f"({', '.join(repr(a) for a in args)})" if args else ""


# ── Public decorator ──────────────────────────────────────────────────────────

def ttl_cache(func):
    """Cache the return value of *func* for TTL seconds, keyed on all arguments.

    Pass ``_force=True`` to bypass TTL and always fetch fresh data.
    The ``_force`` kwarg is consumed here and never forwarded to *func*.

    Read path  : memory → disk → live fetch
    Write path : memory + disk (every time fresh data is fetched)

    ``_last_refreshed`` is updated **only** on ``_force=True`` writes so that
    the "last refreshed" badge reflects the most recent scheduled sync, not
    incidental user-triggered cache misses.
    """
    @wraps(func)
    def wrapper(*args, **kwargs):
        force = kwargs.pop("_force", False)
        key = (func.__name__,) + tuple(_to_hashable(a) for a in args) + tuple(
            (k, _to_hashable(v)) for k, v in sorted(kwargs.items())
        )
        now = time.time()

        if not force:
            # 1. Memory hit (fastest path)
            entry = _store.get(key)
            if entry is not None:
                result, expires_at = entry
                if now < expires_at:
                    log.debug("cache HIT  mem  %s%s", func.__name__, _log_key(args))
                    return result

            # 2. Disk hit — promote to memory so subsequent requests are instant
            disk_entry = _read_disk(key)
            if disk_entry is not None:
                result, expires_at = disk_entry
                if now < expires_at:
                    _store[key] = (result, expires_at)
                    log.debug("cache HIT  disk %s%s", func.__name__, _log_key(args))
                    # Restore last_refreshed if this is the first hit after restart
                    if not _last_refreshed[0]:
                        _last_refreshed[0] = expires_at - TTL
                    return result

        # 3. Cache miss (or _force=True) — call the real function
        reason = "forced" if force else "cold  "
        log.info("cache MISS %s %s%s", reason, func.__name__, _log_key(args))
        t0     = time.monotonic()
        result = func(*args, **kwargs)
        log.info("cache FILL %s %s%s  %.1fs", reason, func.__name__, _log_key(args),
                 time.monotonic() - t0)
        expires_at = now + TTL
        _store[key] = (result, expires_at)
        _write_disk(key, result, expires_at)
        # Only the scheduler uses _force=True; user-triggered TTL misses must
        # not reset the badge — it should always reflect the last scheduled sync.
        if force:
            _last_refreshed[0] = now
        return result

    return wrapper


# ── Cache management ──────────────────────────────────────────────────────────

def clear_cache() -> None:
    """Invalidate all cached results (memory and disk)."""
    _store.clear()
    _last_refreshed[0] = 0.0
    # Delete disk files in a background thread so the HTTP response isn't
    # blocked by slow Render disk I/O.
    def _delete_disk():
        try:
            with _disk_lock:
                for fname in os.listdir(CACHE_DIR):
                    if fname.endswith(".pkl"):
                        try:
                            os.remove(os.path.join(CACHE_DIR, fname))
                        except Exception:
                            pass
        except Exception:
            pass
    threading.Thread(target=_delete_disk, daemon=True).start()


def last_refreshed_str() -> str:
    """Human-friendly string for when data was last fetched."""
    ts = _last_refreshed[0]
    if not ts:
        return "not yet loaded"
    ago = int(time.time() - ts)
    if ago < 60:
        return f"{ago}s ago"
    if ago < 3600:
        return f"{ago // 60}m ago"
    hours = ago // 3600
    mins  = (ago % 3600) // 60
    return f"{hours}h {mins}m ago" if mins else f"{hours}h ago"


def last_refreshed_ts() -> float:
    """Raw Unix timestamp of the last cache population (0 if never)."""
    return _last_refreshed[0]
