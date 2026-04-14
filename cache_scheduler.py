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

import gc
import os
import logging
import threading
import hubspot
import analytics
from analytics import _coverage_end
from hubspot import TEAM_FILTER

log = logging.getLogger(__name__)

# How often to re-sync (configurable via env var, default 1 hour)
SYNC_INTERVAL_S = float(os.environ.get("CACHE_SYNC_HOURS", "1")) * 3600

# All selectable periods (must match app.py PERIODS).
# last_60 is intentionally omitted — it's the least-used range and its data
# is fully bracketed by last_30 and last_90, so dropping it saves one full
# set of raw API caches (calls, deals×2, open_deals, contacts) from RAM.
ALL_PERIODS = [
    "this_month",
    "last_month",
    "last_30",
    "last_90",
    "this_quarter",
    "last_quarter",
    "ytd",
    "next_month",
]

# Prior-period comparison variants (all selectable periods except next_month,
# which has no meaningful prior period).  Historical data never changes so we
# don't force-refresh the underlying raw API calls, but we DO pre-warm the
# analytics results so page loads never trigger a live HubSpot request.
PRIOR_PERIODS = ["prior_" + p for p in ALL_PERIODS if p not in ("next_month",)]

# today / this_week / last_week are call-stats-only short periods that need
# warming so the Call Stats tab never triggers a live HubSpot request.
CALL_STATS_EXTRA = [
    "today",       "this_week",       "last_week",
    "prior_today", "prior_this_week", "prior_last_week",
]

# this_week / last_week are also selectable on deal pages (DEAL_PERIODS in
# app.py).  They're too short to belong in ALL_PERIODS but must be pre-warmed
# so clicking "This Week" / "Last Week" never hits HubSpot live.
DEAL_WEEK_PERIODS = ["this_week", "last_week"]

# All compute views to warm — most-visited first.
# compute_scorecard is included here so the home page unblocks immediately
# after this_month data is ready, not at the end of the full sync cycle.
_VIEWS = [
    analytics.compute_call_stats,
    analytics.compute_scorecard,          # 2nd: unblocks /, /scorecard ASAP (depends on call_stats)
    analytics.compute_connect_diagnostics,
    # compute_connect_rate_drivers is warmed separately below with explicit positional
    # args so the cache key matches what the route calls (period, "all", "all", "all").
    analytics.compute_dial_pipeline,
    analytics.compute_pipeline_coverage,
    analytics.compute_pipeline_generated,
    analytics.compute_deals_won,
    analytics.compute_deals_lost,
    analytics.compute_deal_advancement,
    analytics.compute_inbound_funnel,
    analytics.compute_deal_flow,
    analytics.compute_revenue_chart,
]

# Subset of _VIEWS relevant for weekly deal periods (pipeline_coverage and
# inbound_funnel don't expose week-level period selectors).
_DEAL_WEEK_VIEWS = [
    analytics.compute_pipeline_generated,
    analytics.compute_deals_won,
    analytics.compute_deals_lost,
    analytics.compute_deal_advancement,
]

_timer: threading.Timer = None
_sync_lock = threading.Lock()   # prevents concurrent syncs from doubling memory


def _refresh_base_data():
    """Force-refresh HubSpot API calls that don't depend on period."""
    for fn in (
        hubspot.get_owners,
        hubspot.get_team_owner_ids,
        hubspot.get_lost_reason_labels,
        hubspot.get_deal_contact_windows,
        hubspot.get_companies_for_coverage,
        hubspot.get_sequence_enrolled_company_ids,
        hubspot.get_overdue_sequence_tasks,
    ):
        try:
            fn(_force=True)
        except Exception as exc:
            log.warning("  ✗ %s: %s", fn.__name__, exc)


_RAW_FN_NAMES = frozenset({
    "get_deals", "get_calls", "get_calls_enriched", "get_contacts_inbound",
    "get_quotas", "get_all_open_deals",
})


def _evict_raw_from_memory():
    """Remove raw HubSpot API results from the in-memory cache.

    After analytics for a period are computed the large raw lists (deals,
    calls, contacts) are no longer needed in RAM — they're already on disk.
    Evicting them keeps steady-state RSS well below the 512 MB limit.
    """
    from cache_utils import _store
    keys_to_drop = [k for k in list(_store) if k and k[0] in _RAW_FN_NAMES]
    for k in keys_to_drop:
        _store.pop(k, None)
    if keys_to_drop:
        log.debug("Evicted %d raw API entries from memory.", len(keys_to_drop))


def _refresh_period_data(period: str):
    """Force-refresh all raw HubSpot API calls for a given period.

    This ensures the compute layer receives fresh data from HubSpot rather
    than reusing stale sub-call cache entries from the previous sync cycle.
    """
    try:
        start, end = hubspot.get_date_range(period)
    except Exception as exc:
        log.warning("  ✗ get_date_range(%s): %s", period, exc)
        return

    # get_all_open_deals uses the extended coverage boundary so deals with
    # close dates later in the period (e.g. March 18-31) are included.
    try:
        coverage = _coverage_end(period, start, end)
    except Exception as exc:
        log.warning("  ✗ _coverage_end(%s): %s", period, exc)
        coverage = end

    for fn, kwargs in [
        (hubspot.get_deals,            {"date_field": "createdate"}),
        (hubspot.get_deals,            {"date_field": "closedate"}),
        (hubspot.get_deals,            {"date_field": "hs_v2_date_entered_71300358"}),
        (hubspot.get_deals,            {"date_field": "hs_v2_date_entered_71300363"}),
        (hubspot.get_calls,            {}),
        (hubspot.get_contacts_inbound, {}),
        (hubspot.get_quotas,           {}),
    ]:
        try:
            fn(start, end, _force=True, **kwargs)
        except Exception as exc:
            log.warning("  ✗ %s(%s, ...): %s", fn.__name__, period, exc)
        import time; time.sleep(0.5)  # avoid HubSpot rate limit between fetches

    # Warm open-deals cache with the extended boundary
    try:
        hubspot.get_all_open_deals(start, coverage, _force=True)
    except Exception as exc:
        log.warning("  ✗ get_all_open_deals(%s, coverage_end): %s", period, exc)




def _sync():
    """Full sync: refresh raw HubSpot data then rebuild all analytics cache entries.

    Using _force=True on every call bypasses TTL checks while leaving existing
    cache entries readable — users never land on a cold cache mid-sync.
    """
    if not _sync_lock.acquire(blocking=False):
        log.info("Cache sync already in progress — skipping duplicate trigger.")
        return
    try:
        _sync_body()
    except Exception as exc:
        log.error("Cache sync crashed unexpectedly: %s", exc, exc_info=True)
    finally:
        _sync_lock.release()
        _schedule_next()


def _sync_body():
    log.info(
        "Cache sync starting — %d periods × %d views + %d week periods…",
        len(ALL_PERIODS), len(_VIEWS), len(DEAL_WEEK_PERIODS),
    )
    total, failed = 0, 0

    # Step 1: refresh period-agnostic data (owners, deal-contact graph, book coverage)
    _refresh_base_data()
    try:
        analytics.compute_book_coverage(_force=True)
        total += 1
    except Exception as exc:
        log.warning("  ✗ compute_book_coverage: %s", exc)
        failed += 1

    # Step 2: for each period, refresh raw API calls then compute analytics.
    # gc.collect() between periods lets Python reclaim the temporary objects
    # created during _refresh_period_data before the next period's data loads,
    # keeping the peak RSS lower during the sync spike.
    for period in ALL_PERIODS:
        _refresh_period_data(period)          # fetch fresh HubSpot data
        for fn in _VIEWS:                     # rebuild analytics from fresh data
            try:
                fn(period, _force=True)
                total += 1
            except Exception as exc:
                log.warning("  ✗ %s(%s): %s", fn.__name__, period, exc)
                failed += 1
        try:
            # Warm with explicit positional defaults to match the route's cache key:
            # compute_connect_rate_drivers(period, "all", "all", "all")
            analytics.compute_connect_rate_drivers(period, "all", "all", "all", _force=True)
            total += 1
        except Exception as exc:
            log.warning("  ✗ compute_connect_rate_drivers(%s): %s", period, exc)
            failed += 1
        for _team in TEAM_FILTER:
            try:
                analytics.compute_connect_rate_drivers(period, _team, "all", "all", _force=True)
                total += 1
            except Exception as exc:
                log.warning("  ✗ compute_connect_rate_drivers(%s, %s): %s", period, _team, exc)
                failed += 1
        _evict_raw_from_memory()              # drop large raw lists; they're on disk
        gc.collect()                          # release temporaries before next period

    # Step 3: warm prior-period analytics.  Historical data is stable so we
    # skip _refresh_period_data and let the analytics cache handle misses.
    for period in PRIOR_PERIODS:
        for fn in _VIEWS:
            try:
                fn(period, _force=True)
                total += 1
            except Exception as exc:
                log.warning("  ✗ %s(%s): %s", fn.__name__, period, exc)
                failed += 1
        try:
            analytics.compute_connect_rate_drivers(period, "all", "all", "all", _force=True)
            total += 1
        except Exception as exc:
            log.warning("  ✗ compute_connect_rate_drivers(%s): %s", period, exc)
            failed += 1
        for _team in TEAM_FILTER:
            try:
                analytics.compute_connect_rate_drivers(period, _team, "all", "all", _force=True)
                total += 1
            except Exception as exc:
                log.warning("  ✗ compute_connect_rate_drivers(%s, %s): %s", period, _team, exc)
                failed += 1
        _evict_raw_from_memory()
        gc.collect()

    # Step 4: call-stats-only short periods (today, this_week, last_week) + priors.
    for period in CALL_STATS_EXTRA:
        for fn in (
            analytics.compute_call_stats,
            analytics.compute_connect_diagnostics,
        ):
            try:
                fn(period, _force=True)
                total += 1
            except Exception as exc:
                log.warning("  ✗ %s(%s): %s", fn.__name__, period, exc)
                failed += 1
        try:
            analytics.compute_connect_rate_drivers(period, "all", "all", "all", _force=True)
            total += 1
        except Exception as exc:
            log.warning("  ✗ compute_connect_rate_drivers(%s): %s", period, exc)
            failed += 1
        for _team in TEAM_FILTER:
            try:
                analytics.compute_connect_rate_drivers(period, _team, "all", "all", _force=True)
                total += 1
            except Exception as exc:
                log.warning("  ✗ compute_connect_rate_drivers(%s, %s): %s", period, _team, exc)
                failed += 1

    # Step 4b: this_week / last_week for deal-page views.
    # Raw API data is refreshed first so compute functions receive fresh data
    # rather than hitting HubSpot live on the request thread.
    for period in DEAL_WEEK_PERIODS:
        log.info("  warming deal-week period: %s", period)
        _refresh_period_data(period)
        for fn in _DEAL_WEEK_VIEWS:
            try:
                fn(period, _force=True)
                total += 1
            except Exception as exc:
                log.warning("  ✗ %s(%s): %s", fn.__name__, period, exc)
                failed += 1
        _evict_raw_from_memory()
        gc.collect()

    # Step 5: scorecard prior period (this_month is already covered by _VIEWS).
    try:
        analytics.compute_scorecard("prior_this_month", _force=True)
        total += 1
    except Exception as exc:
        log.warning("  ✗ compute_scorecard(prior_this_month): %s", exc)

    log.info(
        "Cache sync complete (%d ok, %d failed). Next sync in %.0f min.",
        total, failed, SYNC_INTERVAL_S / 60,
    )
    _maybe_generate_summaries()


# First month that should ever appear in the scorecard history.
# All months from this point through last_completed_month() are required.
_HISTORY_START = (2026, 1)


def _required_history_months():
    """Return every (year, month) from _HISTORY_START through last_completed_month()."""
    import monthly_store
    end_year, end_month = monthly_store.last_completed_month()
    months = []
    y, m = _HISTORY_START
    while (y, m) <= (end_year, end_month):
        months.append((y, m))
        m += 1
        if m > 12:
            m = 1
            y += 1
    return months


def _maybe_generate_summaries():
    """Generate locked monthly summaries after each cache sync if not yet present.

    Checks every month from _HISTORY_START through last_completed_month() and
    backfills any that are missing. New months are picked up automatically —
    no code change needed when a new month closes.

    Under normal operation with a persistent store this is a no-op after the
    first successful sync — the team-record existence check is a single dict
    lookup.  After a store wipe (e.g. Render redeploy on ephemeral disk) every
    missing month is recovered automatically on the next hourly sync.

    Wrapped in a broad try/except so a summary failure never breaks the
    hourly data sync that users depend on.
    """
    try:
        import monthly_store
        import summary_engine

        team_history = monthly_store.get_team_history()
        done_months  = {(r["year"], r["month"]) for r in team_history}

        cur_year, cur_month = monthly_store.last_completed_month()
        missing = [(y, m) for y, m in _required_history_months() if (y, m) not in done_months]
        if not missing:
            return

        for y, m in missing:
            log.info(
                "Monthly summaries absent for %d-%02d — generating after cache sync…",
                y, m,
            )
            try:
                # Historical months use explicit "month:YYYY-MM" period keys whose
                # HubSpot data isn't refreshed by the main sync loop.  Refresh now
                # so generate_all_for_month has fresh inputs to work from.
                if not (y == cur_year and m == cur_month):
                    _refresh_period_data(f"month:{y:04d}-{m:02d}")

                result  = summary_engine.generate_all_for_month(y, m)
                n_saved = sum(1 for v in result["reps"].values() if v) + (1 if result["team"] else 0)
                log.info(
                    "Monthly summary generation for %d-%02d complete — %d new records locked.",
                    y, m, n_saved,
                )
            except Exception as exc:
                log.warning(
                    "Monthly summary generation for %d-%02d failed (will retry next sync): %s",
                    y, m, exc,
                )
    except Exception as exc:
        log.warning("Monthly summary generation failed (will retry next sync): %s", exc)


def _schedule_next():
    global _timer
    _timer = threading.Timer(SYNC_INTERVAL_S, _sync)
    _timer.daemon = True
    _timer.start()


def is_syncing() -> bool:
    """Return True if a cache sync is currently running."""
    acquired = _sync_lock.acquire(blocking=False)
    if acquired:
        _sync_lock.release()
        return False
    return True


def trigger():
    """Immediately kick off a background cache sync (e.g. after a manual refresh).

    Cancels any pending scheduled sync, then fires _sync in a new daemon
    thread so the HTTP request thread returns instantly.
    """
    global _timer
    if _timer is not None:
        _timer.cancel()
    t = threading.Thread(target=_sync, daemon=True)
    t.start()
    log.info("Cache sync triggered manually.")


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
