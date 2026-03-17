"""
Background cache-warming scheduler.

Runs a daemon thread that silently re-fetches HubSpot data every
CACHE_SYNC_HOURS hours (default: 1). Every period × view combination
is pre-warmed so users always get instant page loads regardless of
which date range they select.

Sync order per period:
  1. Force-refresh all raw HubSpot API calls (get_deals, get_calls, etc.)
     so the compute layer actually receives fresh data from HubSpot.
  2. Force-refresh all compute_* functions so cached analytics results
     are rebuilt from the fresh API data.

If any individual fetch fails, the rest still run and the error is
silently swallowed — the next scheduled sync will try again.
"""

import os
import logging
import threading
import hubspot
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
    "next_month",
]

# All compute views to warm — most-visited first
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

_timer: threading.Timer = None


def _refresh_base_data():
    """Force-refresh HubSpot API calls that don't depend on period."""
    for fn in (hubspot.get_owners, hubspot.get_team_owner_ids, hubspot.get_deal_contact_windows):
        try:
            fn(_force=True)
        except Exception as exc:
            log.warning("  ✗ %s: %s", fn.__name__, exc)


def _refresh_period_data(period: str):
    """Force-refresh all raw HubSpot API calls for a given period.

    This ensures the compute layer receives fresh data from HubSpot rather
    than reusing stale sub-call cache entries from the previous sync cycle.
    """
    try:
        start, end = hubspot.get_date_range(period, _force=True)
    except Exception as exc:
        log.warning("  ✗ get_date_range(%s): %s", period, exc)
        return

    for fn, extra_kwargs in [
        (hubspot.get_deals,            {"date_field": "createdate"}),
        (hubspot.get_deals,            {"date_field": "closedate"}),
        (hubspot.get_calls,            {}),
        (hubspot.get_all_open_deals,   {}),
        (hubspot.get_contacts_inbound, {}),
    ]:
        try:
            fn(start, end, _force=True, **extra_kwargs)
        except Exception as exc:
            log.warning("  ✗ %s(%s, ...): %s", fn.__name__, period, exc)




def _sync():
    """Full sync: refresh raw HubSpot data then rebuild all analytics cache entries.

    Using _force=True on every call bypasses TTL checks while leaving existing
    cache entries readable — users never land on a cold cache mid-sync.
    """
    log.info("Cache sync starting — %d periods × %d views…", len(ALL_PERIODS), len(_VIEWS))
    total, failed = 0, 0

    # Step 1: refresh period-agnostic data (owners, deal-contact graph)
    _refresh_base_data()

    # Step 2: for each period, refresh raw API calls then compute analytics
    for period in ALL_PERIODS:
        _refresh_period_data(period)          # fetch fresh HubSpot data
        for fn in _VIEWS:                     # rebuild analytics from fresh data
            try:
                fn(period, _force=True)
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
    """Start the background scheduler.

    initial_delay_s=0  → sync immediately on startup so cache is warm
                          before the first user arrives.
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
