"""
LinkedIn Marketing API integration for campaign spend and lead gen performance.

Pulls spend, impressions, clicks, and Lead Gen Form leads by campaign
from LinkedIn Campaign Manager via the versioned Marketing Analytics API.

Required env vars:
    LINKEDIN_ACCESS_TOKEN  — OAuth2 bearer token with r_ads + r_ads_reporting scopes
    LINKEDIN_AD_ACCOUNT_ID — numeric Campaign Manager account ID
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
LI_VERSION    = "202604"


def is_configured() -> bool:
    return bool(ACCESS_TOKEN and AD_ACCOUNT_ID)


def _headers() -> dict:
    return {
        "Authorization":             f"Bearer {ACCESS_TOKEN}",
        "Linkedin-Version":          LI_VERSION,
        "X-Restli-Protocol-Version": "2.0.0",
    }


def _get(path: str, params: dict | None = None) -> dict:
    """Build URL with RestLi 2.0-aware encoding.

    dateRange  — parens/colons/commas left unencoded (RestLi complex syntax)
    accounts   — URN colons encoded, List() parens left unencoded
    everything else — standard encoding
    """
    url = f"{BASE_URL}{path}"
    if params:
        parts = []
        for k, v in params.items():
            ek = urllib.parse.quote(str(k))
            if k == "dateRange":
                ev = urllib.parse.quote(str(v), safe="():,")
            elif k == "accounts":
                ev = str(v)  # pre-encoded URN inside List() — passed as-is
            else:
                ev = urllib.parse.quote(str(v), safe=",")
            parts.append(f"{ek}={ev}")
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


def _fetch_campaign_names(campaign_ids: list[str]) -> dict[str, str]:
    """Batch-fetch campaign names for a list of campaign ID strings."""
    if not campaign_ids:
        return {}
    names = {}
    for i in range(0, len(campaign_ids), 20):
        chunk = campaign_ids[i:i + 20]
        encoded_ids = ",".join(
            urllib.parse.quote(f"urn:li:sponsoredCampaign:{cid}", safe="")
            for cid in chunk
        )
        try:
            data = _get("/adCampaigns", {"ids": f"List({encoded_ids})"})
            for urn, detail in data.get("results", {}).items():
                cid = urn.split(":")[-1]
                names[cid] = detail.get("name", cid)
        except Exception as exc:
            log.warning("LinkedIn campaign name batch fetch failed: %s", exc)
            for cid in chunk:
                names[cid] = cid
    return names


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

    # dateRange: RestLi 2.0 complex, year/month/day order per API docs
    date_range = (
        f"(start:(year:{start.year},month:{start.month},day:{start.day}),"
        f"end:(year:{end.year},month:{end.month},day:{end.day}))"
    )
    # accounts: URN colons must be percent-encoded inside List()
    encoded_urn = urllib.parse.quote(
        f"urn:li:sponsoredAccount:{AD_ACCOUNT_ID}", safe=""
    )

    try:
        params = {
            "q":               "analytics",
            "pivot":           "CAMPAIGN",
            "dateRange":       date_range,
            "timeGranularity": "ALL",
            "accounts":        f"List({encoded_urn})",
            "fields":          "pivotValues,impressions,clicks,costInLocalCurrency,oneClickLeads",
        }
        data     = _get("/adAnalytics", params)
        elements = data.get("elements", [])

        # Collect all campaign IDs then batch-fetch names in one request
        campaign_ids = []
        for el in elements:
            pv = (el.get("pivotValues") or [""])[0]
            if "sponsoredCampaign:" in pv:
                campaign_ids.append(pv.split(":")[-1])
        names = _fetch_campaign_names(list(set(campaign_ids)))

        rows = []
        for el in elements:
            pivot_vals  = el.get("pivotValues") or [""]
            pv          = pivot_vals[0]
            cid         = pv.split(":")[-1] if "sponsoredCampaign:" in pv else None
            cost        = float(el.get("costInLocalCurrency", 0))
            impressions = int(el.get("impressions", 0))
            clicks      = int(el.get("clicks", 0))
            leads       = int(el.get("oneClickLeads", 0))
            rows.append({
                "campaign":    names.get(cid, cid or "Unknown"),
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
