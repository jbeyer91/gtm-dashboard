"""
Google Ads API integration for paid media performance data.

Reads campaign-level metrics (spend, impressions, clicks, conversions, CTR,
CPC, CPL) via the Google Ads API using OAuth2 refresh-token auth.

Required env vars:
    GOOGLE_ADS_DEVELOPER_TOKEN  — from Google Ads API Center
    GOOGLE_ADS_CLIENT_ID        — OAuth2 client ID
    GOOGLE_ADS_CLIENT_SECRET    — OAuth2 client secret
    GOOGLE_ADS_REFRESH_TOKEN    — long-lived refresh token
    GOOGLE_ADS_CUSTOMER_ID      — Google Ads account ID (with or without dashes)
"""

import logging
import os

log = logging.getLogger(__name__)

DEVELOPER_TOKEN = os.environ.get("GOOGLE_ADS_DEVELOPER_TOKEN", "")
CLIENT_ID       = os.environ.get("GOOGLE_ADS_CLIENT_ID", "")
CLIENT_SECRET   = os.environ.get("GOOGLE_ADS_CLIENT_SECRET", "")
REFRESH_TOKEN   = os.environ.get("GOOGLE_ADS_REFRESH_TOKEN", "")
CUSTOMER_ID     = os.environ.get("GOOGLE_ADS_CUSTOMER_ID", "").replace("-", "")

# Maps app period keys → Google Ads GAQL date-range literals
_DATE_RANGE_MAP = {
    "last_7":       "LAST_7_DAYS",
    "last_30":      "LAST_30_DAYS",
    "last_90":      "LAST_90_DAYS",
    "this_month":   "THIS_MONTH",
    "last_month":   "LAST_MONTH",
    "this_quarter": "THIS_QUARTER",
    "last_quarter": "LAST_QUARTER",
}


def is_configured() -> bool:
    return all([DEVELOPER_TOKEN, CLIENT_ID, CLIENT_SECRET, REFRESH_TOKEN, CUSTOMER_ID])


def _client():
    from google.ads.googleads.client import GoogleAdsClient
    return GoogleAdsClient.load_from_dict({
        "developer_token": DEVELOPER_TOKEN,
        "client_id":       CLIENT_ID,
        "client_secret":   CLIENT_SECRET,
        "refresh_token":   REFRESH_TOKEN,
        "use_proto_plus":  True,
    })


def fetch_campaign_performance(period: str = "last_30") -> dict:
    """Campaign-level spend and performance metrics for the given period.

    Returns:
        {
          "rows":   [{"id", "name", "status", "spend", "impressions", "clicks",
                      "conversions", "ctr", "avg_cpc", "cpl"}, ...],
          "totals": {...same keys aggregated...},
          "period": str,
          "error":  str | None,
        }
    """
    if not is_configured():
        return {
            "rows": [], "totals": _zero_totals(),
            "period": period, "error": "not_configured",
        }

    date_range = _DATE_RANGE_MAP.get(period, "LAST_30_DAYS")

    try:
        gas = _client().get_service("GoogleAdsService")
        query = f"""
            SELECT
                campaign.id,
                campaign.name,
                campaign.status,
                metrics.cost_micros,
                metrics.impressions,
                metrics.clicks,
                metrics.conversions,
                metrics.ctr,
                metrics.average_cpc
            FROM campaign
            WHERE segments.date DURING {date_range}
              AND campaign.status != 'REMOVED'
            ORDER BY metrics.cost_micros DESC
        """
        rows = []
        for row in gas.search(customer_id=CUSTOMER_ID, query=query):
            spend       = row.metrics.cost_micros / 1_000_000
            conversions = row.metrics.conversions
            rows.append({
                "id":          str(row.campaign.id),
                "name":        row.campaign.name,
                "status":      row.campaign.status.name,
                "spend":       spend,
                "impressions": row.metrics.impressions,
                "clicks":      row.metrics.clicks,
                "conversions": conversions,
                "ctr":         row.metrics.ctr * 100,
                "avg_cpc":     row.metrics.average_cpc / 1_000_000,
                "cpl":         spend / conversions if conversions > 0 else None,
            })

        rows = [r for r in rows if r["spend"] > 0 or r["impressions"] > 0]
        return {"rows": rows, "totals": _totals(rows), "period": period, "error": None}

    except Exception as exc:
        log.error("Google Ads campaign fetch failed: %s", exc)
        return {"rows": [], "totals": _zero_totals(), "period": period, "error": str(exc)}


def fetch_daily_spend(period: str = "last_30") -> list:
    """Daily aggregated spend for a trend chart.

    Returns a list of {"date": "YYYY-MM-DD", "spend": float}, sorted ascending.
    """
    if not is_configured():
        return []

    date_range = _DATE_RANGE_MAP.get(period, "LAST_30_DAYS")

    try:
        gas = _client().get_service("GoogleAdsService")
        query = f"""
            SELECT
                segments.date,
                metrics.cost_micros
            FROM campaign
            WHERE segments.date DURING {date_range}
              AND campaign.status != 'REMOVED'
            ORDER BY segments.date ASC
        """
        daily: dict = {}
        for row in gas.search(customer_id=CUSTOMER_ID, query=query):
            d = row.segments.date
            daily[d] = daily.get(d, 0.0) + row.metrics.cost_micros / 1_000_000

        return [{"date": d, "spend": v} for d, v in sorted(daily.items())]

    except Exception as exc:
        log.error("Google Ads daily spend fetch failed: %s", exc)
        return []


def _totals(rows: list) -> dict:
    spend       = sum(r["spend"]       for r in rows)
    impressions = sum(r["impressions"] for r in rows)
    clicks      = sum(r["clicks"]      for r in rows)
    conversions = sum(r["conversions"] for r in rows)
    return {
        "spend":       spend,
        "impressions": impressions,
        "clicks":      clicks,
        "conversions": conversions,
        "ctr":         (clicks / impressions * 100) if impressions else 0.0,
        "avg_cpc":     (spend / clicks)             if clicks      else 0.0,
        "cpl":         (spend / conversions)        if conversions else None,
    }


def _zero_totals() -> dict:
    return {
        "spend": 0, "impressions": 0, "clicks": 0,
        "conversions": 0, "ctr": 0.0, "avg_cpc": 0.0, "cpl": None,
    }
