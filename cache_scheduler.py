"""
Background cache-warming scheduler.

Runs a daemon thread that silently re-fetches HubSpot data every
CACHE_SYNC_HOURS hours (default: 1). Every period × view combination
is pre-warmed so users always get instant page loads regardless of
which date range they select.

If any individual fetch fails, the rest still run and the error is
silently swallowed — the next scheduled sync will try again.
"""

import os
import logging
import threading
import analytics

log = logging.getLogger(__name__)

# How often to re-sync (configurable via env var, default 1 hour)
SYNC_INTERVAL_S = float(os.environ.get("CACHE_SYNC_HOURS", "1")) * 3600

# All selectable periods (must match app.py PERIODS)
ALL_PERIODS = [
    "this_month",
    "last_month",
    "last_30",
    "last_60",
    "last_90",
    "this_quarter",
    "last_quarter",
    "ytd",
]

# All views to warm — listed most-visited first so the dashboard is
# usable as quickly as possible after boot.
_VIEWS = [
    analytics.compute_call_stats,
    analytics.compute_pipeline_coverage,
    analytics.compute_pipeline_generated,
    analytics.compute_deals_won,
    analytics.compute_deals_lost,
    analytics.compute_deal_advancement,
    analytics.compute_inbound_funnel,
    analytics.compute_win_rate_by_source,
]

_timer: threading.Timer = None  # keeps a reference so we can cancel if needed


def _sync():
    """Force-refresh every view × period combination without clearing the cache.

    Using _force=True bypasses the TTL check so each entry is always refreshed,
    but existing cached entries remain readable by users throughout the sync.
    This means users never land on a cold cache during a background refresh.

    Total: 8 views × 8 periods = 64 calls, run sequentially in the background.
    get_owners() and get_deal_contact_windows() are cached after the first
    call so subsequent compute functions in the same sync reuse them instantly.
    """
    log.info("Cache sync starting — %d views × %d periods…", len(_VIEWS), len(ALL_PERIODS))
    total, failed = 0, 0
    for period in ALL_PERIODS:
        for fn in _VIEWS:
            try:
                fn(period, _force=True)
                log.debug("  ✓ %s(%s)", fn.__name__, period)
                total += 1
            except Exception as exc:
                log.warning("  ✗ %s(%s): %s", fn.__name__, period, exc)
                failed += 1
    log.info(
        "Cache sync complete (%d ok, %d failed). Next sync in %.0f min.",
        total, failed, SYNC_INTERVAL_S / 60,
    )
    _schedule_next()


def _schedule_next():
    global _timer
    _timer = threading.Timer(SYNC_INTERVAL_S, _sync)
    _timer.daemon = True
    _timer.start()


def start(initial_delay_s: float = 0):
    """
    Start the background scheduler.

    initial_delay_s=0  → sync immediately on startup (warms cache before
                          the first user arrives).
    Pass a positive value if you want to delay the first sync.
    """
    global _timer
    _timer = threading.Timer(initial_delay_s, _sync)
    _timer.daemon = True
    _timer.start()
    log.info(
        "Cache scheduler started — syncing every %.0f min, %d views × %d periods (first sync in %.0fs).",
        SYNC_INTERVAL_S / 60,
        len(_VIEWS),
        len(ALL_PERIODS),
        initial_delay_s,
    )
