"""
Background cache-warming scheduler.

Runs a daemon thread that silently re-fetches HubSpot data every
CACHE_SYNC_HOURS hours (default: 1). Users always hit a warm cache
and get instant page loads.

The sync order is chosen so the most-visited pages warm up first.
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

# Default period for each view (matches each route's default)
_VIEWS = [
    (analytics.compute_call_stats,          "last_90"),
    (analytics.compute_pipeline_coverage,   "this_month"),
    (analytics.compute_pipeline_generated,  "this_month"),
    (analytics.compute_deals_won,           "this_month"),
    (analytics.compute_deals_lost,          "this_month"),
    (analytics.compute_deal_advancement,    "this_month"),
    (analytics.compute_inbound_funnel,      "this_month"),
    (analytics.compute_win_rate_by_source,  "this_quarter"),
]

_timer: threading.Timer = None  # keeps a reference so we can cancel if needed


def _sync():
    """Force-refresh each view's default period without clearing the cache first.

    Using _force=True bypasses the TTL check so each entry is always refreshed,
    but existing cached entries remain readable by users throughout the sync.
    This means users never land on a cold cache during a background refresh.
    """
    log.info("Cache sync starting…")
    for fn, period in _VIEWS:
        try:
            fn(period, _force=True)
            log.info("  ✓ %s(%s)", fn.__name__, period)
        except Exception as exc:
            log.warning("  ✗ %s(%s): %s", fn.__name__, period, exc)
    log.info("Cache sync complete. Next sync in %.0f min.", SYNC_INTERVAL_S / 60)
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
        "Cache scheduler started — syncing every %.0f min (first sync in %.0fs).",
        SYNC_INTERVAL_S / 60,
        initial_delay_s,
    )
