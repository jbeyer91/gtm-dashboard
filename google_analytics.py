"""
Google Analytics 4 Data API integration for web traffic insights.

Pulls session and engagement metrics by channel group (Organic Search,
Paid Search, Direct, Referral, Paid Social, Display, etc.) and daily
session trends for the GTM dashboard Traffic Sources page.

Required env vars:
    GA4_PROPERTY_ID         — numeric GA4 property ID (e.g. 123456789)
    GA4_SERVICE_ACCOUNT_JSON — full contents of a service account JSON key
                               that has Viewer access on the GA4 property
"""

import json
import logging
import os
from datetime import date, timedelta

log = logging.getLogger(__name__)

PROPERTY_ID          = os.environ.get("GA4_PROPERTY_ID", "").strip()
SERVICE_ACCOUNT_JSON = os.environ.get("GA4_SERVICE_ACCOUNT_JSON", "").strip()


def is_configured() -> bool:
    return bool(PROPERTY_ID and SERVICE_ACCOUNT_JSON)


def _client():
    from google.analytics.data_v1beta import BetaAnalyticsDataClient
    from google.oauth2 import service_account
    credentials = service_account.Credentials.from_service_account_info(
        json.loads(SERVICE_ACCOUNT_JSON),
        scopes=["https://www.googleapis.com/auth/analytics.readonly"],
    )
    return BetaAnalyticsDataClient(credentials=credentials)


def _date_range(period: str) -> tuple:
    """Map an app period key to a (start_date, end_date) pair for the GA4 API."""
    today = date.today()

    if period == "last_7":
        return "7daysAgo", "today"
    if period == "last_30":
        return "30daysAgo", "today"
    if period == "last_90":
        return "90daysAgo", "today"
    if period == "this_month":
        return today.replace(day=1).strftime("%Y-%m-%d"), "today"
    if period == "last_month":
        first_this = today.replace(day=1)
        last_prev  = first_this - timedelta(days=1)
        first_prev = last_prev.replace(day=1)
        return first_prev.strftime("%Y-%m-%d"), last_prev.strftime("%Y-%m-%d")
    if period == "this_quarter":
        q_month = ((today.month - 1) // 3) * 3 + 1
        return today.replace(month=q_month, day=1).strftime("%Y-%m-%d"), "today"
    if period == "last_quarter":
        q_month = ((today.month - 1) // 3) * 3 + 1
        if q_month == 1:
            lq_start = date(today.year - 1, 10, 1)
            lq_end   = date(today.year - 1, 12, 31)
        else:
            lq_start = today.replace(month=q_month - 3, day=1)
            lq_end   = today.replace(month=q_month, day=1) - timedelta(days=1)
        return lq_start.strftime("%Y-%m-%d"), lq_end.strftime("%Y-%m-%d")
    return "30daysAgo", "today"


def fetch_channel_performance(period: str = "last_30") -> dict:
    """Sessions, engagement, and key events by channel group.

    Returns:
        {
          "rows":   [{"channel", "sessions", "engaged_sessions",
                      "engagement_rate", "avg_session_duration",
                      "events_per_session", "key_events"}, ...],
          "totals": {...same keys aggregated...},
          "period": str,
          "error":  str | None,
        }
    """
    if not is_configured():
        return {"rows": [], "totals": _zero_totals(), "period": period,
                "error": "not_configured"}

    start, end = _date_range(period)

    try:
        from google.analytics.data_v1beta.types import (
            DateRange, Dimension, Metric, RunReportRequest,
        )
        req = RunReportRequest(
            property=f"properties/{PROPERTY_ID}",
            dimensions=[Dimension(name="sessionDefaultChannelGroup")],
            metrics=[
                Metric(name="sessions"),
                Metric(name="engagedSessions"),
                Metric(name="engagementRate"),
                Metric(name="averageSessionDuration"),
                Metric(name="eventsPerSession"),
                Metric(name="keyEvents"),
            ],
            date_ranges=[DateRange(start_date=start, end_date=end)],
        )
        resp = _client().run_report(req)

        rows = []
        for row in resp.rows:
            sessions = int(row.metric_values[0].value)
            engaged  = int(row.metric_values[1].value)
            rows.append({
                "channel":              row.dimension_values[0].value,
                "sessions":             sessions,
                "engaged_sessions":     engaged,
                "engagement_rate":      round(float(row.metric_values[2].value) * 100, 1),
                "avg_session_duration": round(float(row.metric_values[3].value), 0),
                "events_per_session":   round(float(row.metric_values[4].value), 2),
                "key_events":           int(float(row.metric_values[5].value)),
            })

        rows.sort(key=lambda r: r["sessions"], reverse=True)
        return {"rows": rows, "totals": _totals(rows), "period": period, "error": None}

    except Exception as exc:
        log.error("GA4 channel fetch failed: %s", exc)
        return {"rows": [], "totals": _zero_totals(), "period": period, "error": str(exc)}


def fetch_daily_sessions(period: str = "last_30") -> list:
    """Daily sessions and engaged sessions for a trend chart.

    Returns a list of {"date": "YYYY-MM-DD", "sessions": int,
    "engaged_sessions": int}, sorted ascending.
    """
    if not is_configured():
        return []

    start, end = _date_range(period)

    try:
        from google.analytics.data_v1beta.types import (
            DateRange, Dimension, Metric, RunReportRequest,
        )
        req = RunReportRequest(
            property=f"properties/{PROPERTY_ID}",
            dimensions=[Dimension(name="date")],
            metrics=[Metric(name="sessions"), Metric(name="engagedSessions")],
            date_ranges=[DateRange(start_date=start, end_date=end)],
        )
        resp = _client().run_report(req)

        daily = []
        for row in resp.rows:
            raw = row.dimension_values[0].value  # YYYYMMDD
            daily.append({
                "date":             f"{raw[:4]}-{raw[4:6]}-{raw[6:]}",
                "sessions":         int(row.metric_values[0].value),
                "engaged_sessions": int(row.metric_values[1].value),
            })

        daily.sort(key=lambda d: d["date"])
        return daily

    except Exception as exc:
        log.error("GA4 daily sessions fetch failed: %s", exc)
        return []


def _totals(rows: list) -> dict:
    sessions   = sum(r["sessions"]         for r in rows)
    engaged    = sum(r["engaged_sessions"] for r in rows)
    key_events = sum(r["key_events"]       for r in rows)
    eps = (
        sum(r["events_per_session"] * r["sessions"] for r in rows) / sessions
        if sessions else 0.0
    )
    return {
        "sessions":             sessions,
        "engaged_sessions":     engaged,
        "engagement_rate":      round(engaged / sessions * 100, 1) if sessions else 0.0,
        "events_per_session":   round(eps, 2),
        "key_events":           key_events,
    }


def _zero_totals() -> dict:
    return {
        "sessions": 0, "engaged_sessions": 0, "engagement_rate": 0.0,
        "events_per_session": 0.0, "key_events": 0,
    }
