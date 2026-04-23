"""
LinkedIn Marketing API integration for campaign spend and lead gen performance.

Pulls spend, impressions, clicks, and Lead Gen Form leads by campaign
from LinkedIn Campaign Manager via the versioned Marketing Analytics API.

Required env vars:
    LINKEDIN_ACCESS_TOKEN  — OAuth2 bearer token with r_ads + r_ads_reporting scopes
    LINKEDIN_AD_ACCOUNT_ID — numeric Campaign Manager account ID (from the URL in Campaign Manager)
"""

import logging
import os
import urllib.parse
from datetime import date, timedelta

import requests

log = logging.getLogger(__name__)

ACCESS_TOKEN  = os.environ.get("LINKEDIN_ACCESS_TOKEN",  "").strip()
AD_ACCOUNT_ID = os.environ.get("LINKEDIN_AD_ACCOUNT_ID", "").strip()
BASE_URL      = "https://api.linkedin.com/rest"
LI_VERSION    = "202501"


def is_configured() -> bool:
    return bool(ACCESS_TOKEN and AD_ACCOUNT_ID)


def _headers() -> dict:
    return {
        "Authorization":             f"Bearer {ACCESS_TOKEN}",
        "X-Restli-Protocol-Version": "2.0.0",
        "LinkedIn-Version":          LI_VERSION,
    }


def _get(path: str, params: dict | None = None) -> dict:
    # RestLi 2.0 complex values (dateRange, List()) must not have their
    # parens/colons percent-encoded, so we build the query string manually.
    url = f"{BASE_URL}{path}"
    if params:
        parts = []
        for k, v in params.items():
            parts.append(
                f"{urllib.parse.quote(str(k))}="
                f"{urllib.parse.quote(str(v), safe='():,')}"
            )
        url = f"{url}?{'&'.join(parts)}"

    resp = requests.get(url, headers=_headers(), timeout=30)
    resp.raise_for_status()
    return resp.json()


def _date_range(period: str) -> tuple[date, date]:
    today = date.today()
    if period == "last_7":
        return today - timedelta(days=7), today
    if period == "last_30":
        return today - timedelta(days=30), today
    if period == "last_90":
        return today - timedelta(days=90), today
    if period == "this_month":
        return today.replace(day=1), today
    if period == "last_month":
        first_this = today.replace(day=1)
        last_prev  = first_this - timedelta(days=1)
        return last_prev.replace(day=1), last_prev
    if period == "this_quarter":
        q_month = ((today.month - 1) // 3) * 3 + 1
        return today.replace(month=q_month, day=1), today
    if period == "last_quarter":
        q_month = ((today.month - 1) // 3) * 3 + 1
        if q_month == 1:
            return date(today.year - 1, 10, 1), date(today.year - 1, 12, 31)
        lq_start = today.replace(month=q_month - 3, day=1)
        lq_end   = today.replace(month=q_month, day=1) - timedelta(days=1)
        return lq_start, lq_end
    return today - timedelta(days=30), today


def _campaign_name(campaign_id: str) -> str:
    try:
        data = _get(f"/adCampaigns/{campaign_id}")
        return data.get("name", campaign_id)
    except Exception:
        return campaign_id


def fetch_campaign_analytics(period: str = "last_30") -> dict:
    """Campaign spend, impressions, clicks, and Lead Gen Form leads.

    Returns:
        {
          "rows":   [{"campaign", "cost", "impressions", "clicks",
                      "ctr", "leads", "cpl", "cpc"}, ...],
          "totals": {...same keys aggregated...},
          "period": str,
          "error":  str | None,
        }
    """
    if not is_configured():
        return {"rows": [], "totals": _zero_totals(), "period": period,
                "error": "not_configured"}

    start, end = _date_range(period)
    account_urn = f"urn:li:sponsoredAccount:{AD_ACCOUNT_ID}"

    try:
        date_range = (
            f"(start:(day:{start.day},month:{start.month},year:{start.year}),"
            f"end:(day:{end.day},month:{end.month},year:{end.year}))"
        )
        params = {
            "q":               "analytics",
            "pivot":           "CAMPAIGN",
            "dateRange":       date_range,
            "timeGranularity": "ALL",
            "accounts":        f"List({account_urn})",
            "fields":          (
                "pivot,pivotValue,impressions,clicks,"
                "costInLocalCurrency,leadGenerationMailContactInfoShares"
            ),
        }
        data     = _get("/adAnalytics", params)
        elements = data.get("elements", [])

        rows = []
        for el in elements:
            pv          = el.get("pivotValue", "")
            cid         = pv.split(":")[-1] if "sponsoredCampaign:" in pv else None
            cost        = float(el.get("costInLocalCurrency", 0))
            impressions = int(el.get("impressions", 0))
            clicks      = int(el.get("clicks", 0))
            leads       = int(el.get("leadGenerationMailContactInfoShares", 0))
            rows.append({
                "campaign":    _campaign_name(cid) if cid else "Unknown",
                "cost":        round(cost, 2),
                "impressions": impressions,
                "clicks":      clicks,
                "ctr":         round(clicks / impressions * 100, 2) if impressions else None,
                "leads":       leads,
                "cpl":         round(cost / leads, 2)  if leads  else None,
                "cpc":         round(cost / clicks, 2) if clicks else None,
            })

        rows = [r for r in rows if r["cost"] > 0 or r["impressions"] > 0]
        rows.sort(key=lambda r: r["cost"], reverse=True)
        return {"rows": rows, "totals": _totals(rows), "period": period, "error": None}

    except Exception as exc:
        log.error("LinkedIn analytics fetch failed: %s", exc)
        return {"rows": [], "totals": _zero_totals(), "period": period, "error": str(exc)}


def _totals(rows: list) -> dict:
    cost        = sum(r["cost"]        for r in rows)
    impressions = sum(r["impressions"] for r in rows)
    clicks      = sum(r["clicks"]      for r in rows)
    leads       = sum(r["leads"]       for r in rows)
    return {
        "cost":        round(cost, 2),
        "impressions": impressions,
        "clicks":      clicks,
        "ctr":         round(clicks / impressions * 100, 2) if impressions else None,
        "leads":       leads,
        "cpl":         round(cost / leads, 2)  if leads  else None,
        "cpc":         round(cost / clicks, 2) if clicks else None,
    }


def _zero_totals() -> dict:
    return {"cost": 0, "impressions": 0, "clicks": 0,
            "ctr": None, "leads": 0, "cpl": None, "cpc": None}
