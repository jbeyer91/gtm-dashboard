"""
Simple in-memory TTL cache for HubSpot analytics results.
No external dependencies — just a plain dict with expiry timestamps.

Cache lifetime defaults to 60 minutes (matching the Google Sheets refresh cadence).
Override by setting CACHE_TTL_SECONDS in the environment.
"""
import os
import time
from functools import wraps

TTL = int(os.environ.get("CACHE_TTL_SECONDS", 3600))  # default: 1 hour

_store: dict = {}          # key → (result, expires_at)
_last_refreshed: list = [0.0]  # mutable so the decorator closure can update it


def ttl_cache(func):
    """Cache the return value of a function for TTL seconds, keyed on all arguments."""
    @wraps(func)
    def wrapper(*args, **kwargs):
        key = (func.__name__,) + args + tuple(sorted(kwargs.items()))
        entry = _store.get(key)
        if entry is not None:
            result, expires_at = entry
            if time.time() < expires_at:
                return result
        # Cache miss — fetch fresh data
        result = func(*args, **kwargs)
        _store[key] = (result, time.time() + TTL)
        _last_refreshed[0] = time.time()
        return result
    return wrapper


def clear_cache() -> None:
    """Invalidate all cached results so the next request fetches fresh data."""
    _store.clear()
    # Keep _last_refreshed[0] so the template still shows the old timestamp
    # until new data is actually loaded.
    _last_refreshed[0] = 0.0


def last_refreshed_str() -> str:
    """Return a human-friendly string describing when data was last fetched."""
    ts = _last_refreshed[0]
    if not ts:
        return "not yet loaded"
    ago = int(time.time() - ts)
    if ago < 60:
        return f"{ago}s ago"
    if ago < 3600:
        return f"{ago // 60}m ago"
    hours = ago // 3600
    mins = (ago % 3600) // 60
    return f"{hours}h {mins}m ago" if mins else f"{hours}h ago"


def last_refreshed_ts() -> float:
    """Return the raw Unix timestamp of the last cache population (0 if never)."""
    return _last_refreshed[0]
