import logging
import re
from collections import defaultdict
from datetime import datetime, timezone, timedelta
from statistics import median
from cache_utils import ttl_cache
from hubspot import (
    get_owners, get_deals, get_all_open_deals, get_calls, get_meetings,
    get_contacts_inbound, get_list_contacts, get_date_range, NB_STAGES, DEAL_STAGES,
    get_deal_contact_windows, get_call_to_contact_map, get_team_owner_ids,
    get_scoped_team_owner_ids,
    apply_manual_owner_overrides,
    get_owner_team_map, TEAM_MANAGER,
    get_quotas, get_companies_for_coverage, get_sequence_enrolled_company_ids,
    get_overdue_sequence_tasks, _parse_hs_datetime, get_forecast_submissions,
    get_target_account_companies, _search_all,
    get_calls_enriched,
)

# ── Business model constants ──────────────────────────────────────────────────
ACV                = 12_000   # Average contract value ($)
STAGE1_TO_STAGE2   = 0.60     # % of created deals that advance to stage 2
STAGE2_WIN_RATE    = 0.25     # Win rate for deals that reach stage 2
# Derived: stage 2 pipeline multiple needed to cover quota
S2_COVERAGE_MULT   = round(1 / STAGE2_WIN_RATE)          # 4x
# Derived: created deals needed per $ of quota
DEALS_PER_DOLLAR   = 1 / (ACV * STAGE1_TO_STAGE2 * STAGE2_WIN_RATE)  # 1/1800
# Fixed monthly deals-created target per rep (12 outbound + 3 inbound)
DEALS_CREATED_TARGET_PER_REP = 13

SOURCE_MAP = {
    "PAID_SEARCH": "Paid Search",
    "ORGANIC_SEARCH": "Organic Search",
    "SOCIAL_MEDIA": "Organic Social",
    "PAID_SOCIAL": "Paid Social",
    "DIRECT_TRAFFIC": "Direct Traffic",
    "EMAIL_MARKETING": "Email Marketing",
    "OFFLINE": "Offline Sources",
    "REFERRALS": "Referrals",
    "OTHER_CAMPAIGNS": "Other",
}

DEAL_SOURCE_MAP = {
    "PAID_SEARCH": "Inbound",
    "ORGANIC_SEARCH": "Inbound",
    "SOCIAL_MEDIA": "Inbound",
    "PAID_SOCIAL": "Inbound",
    "DIRECT_TRAFFIC": "Inbound",
    "EMAIL_MARKETING": "Inbound",
    "OFFLINE": "Inbound",
    "REFERRALS": "Referral",
    "COLD_OUTREACH": "Cold outreach",
}

LOST_REASON_MAP = {
    "Cost": "Cost",
    "Never Demo'ed": "Never Demo'ed",
    "Timeline": "Timeline",
    "Stakeholder Issue": "Stakeholder Issue",
    "Competitor": "Competitor",
    "Product": "Product",
    "Other": "Other",
    "Value": "Value",
}

# HubSpot call disposition GUIDs
CALL_CONNECTED_GUIDS = {
    "f240bbac-87c9-4f6e-bf70-924b57d47db7",  # Connected
    "ee078117-c361-4e51-84af-4cfd534fd878",  # Answered - Bad Timing
    "ff4c1e61-46ad-4100-9676-b5d4a0c4f52b",  # Answered - Call Back
    "b43b9c27-ecc9-461f-8b4b-6d2c00ae6f0d",  # Answered - Meeting Set
    "293301fd-5a90-47e9-90a9-b87d59f27cc5",  # Answered - No Longer with Company
    "c6ab5404-53ca-4f44-938d-d4400b589b74",  # Answered - Not Interested
    "a8810b96-f812-4d60-800c-9b0beefa8941",  # Answered - Poor Fit
    "314680f7-ba23-4153-b297-1e3bd1453951",  # Answered - Referral
    "0513d3b2-f7e3-4ae5-81ca-be71e399499b",  # Answered - Wrong Contact
    "bf63f95f-8fa4-4a42-b918-0a8e5ee4ba3e",  # Gatekeeper
    "a4c4c377-d246-4b32-a13b-75a56a4cd0ff",  # Left live message
    "0f54a15c-1cb7-458a-8a2a-5a0e97cd7c13",  # Bad Outcome
}
CALL_CONVERSATION_GUIDS = {
    "f240bbac-87c9-4f6e-bf70-924b57d47db7",  # Connected
    "ee078117-c361-4e51-84af-4cfd534fd878",  # Answered - Bad Timing
    "ff4c1e61-46ad-4100-9676-b5d4a0c4f52b",  # Answered - Call Back
    "b43b9c27-ecc9-461f-8b4b-6d2c00ae6f0d",  # Answered - Meeting Set
    "293301fd-5a90-47e9-90a9-b87d59f27cc5",  # Answered - No Longer with Company
    "c6ab5404-53ca-4f44-938d-d4400b589b74",  # Answered - Not Interested
    "a8810b96-f812-4d60-800c-9b0beefa8941",  # Answered - Poor Fit
    "314680f7-ba23-4153-b297-1e3bd1453951",  # Answered - Referral
    "0513d3b2-f7e3-4ae5-81ca-be71e399499b",  # Answered - Wrong Contact
}

# ── ICP rank: HubSpot internal enum value → display label ────────────────────
_ICP_INTERNAL_TO_LABEL: dict[str, str] = {
    "superior":       "A+",
    "strong":         "A",
    "moderate":       "B",
    "conservative":   "C",
    "least_priority": "D",
    "suppress":       "Least Priority",
}

# ── Disposition GUIDs → human-readable label (module-level for reuse) ────────
DISPOSITION_LABELS: dict[str, str] = {
    "f240bbac-87c9-4f6e-bf70-924b57d47db7": "Connected",
    "bf63f95f-8fa4-4a42-b918-0a8e5ee4ba3e": "Gatekeeper",
    "ee078117-c361-4e51-84af-4cfd534fd878": "Answered - Bad Timing",
    "ff4c1e61-46ad-4100-9676-b5d4a0c4f52b": "Answered - Call Back",
    "b43b9c27-ecc9-461f-8b4b-6d2c00ae6f0d": "Answered - Meeting Set",
    "c6ab5404-53ca-4f44-938d-d4400b589b74": "Answered - Not Interested",
    "293301fd-5a90-47e9-90a9-b87d59f27cc5": "Answered - No Longer with Co.",
    "a8810b96-f812-4d60-800c-9b0beefa8941": "Answered - Poor Fit",
    "314680f7-ba23-4153-b297-1e3bd1453951": "Answered - Referral",
    "0513d3b2-f7e3-4ae5-81ca-be71e399499b": "Answered - Wrong Contact",
    "a4c4c377-d246-4b32-a13b-75a56a4cd0ff": "Left live message",
    "0f54a15c-1cb7-458a-8a2a-5a0e97cd7c13": "Bad Outcome",
    "9d9162e7-6cf3-4944-bf63-4dff82258764": "Busy",
    "b2cf5968-551e-4856-9783-52b3da59a7d0": "Left voicemail",
    "73a0d17f-1163-4015-bdd5-ec830791da20": "No answer",
    "899b0622-cdd2-4c55-8461-1c738dab0b69": "No answer - Poor Fit",
    "17b47fee-58de-441e-a44c-c6300d46f273": "Wrong number",
}

# ── Outcome buckets for grouped display (4 cards) ────────────────────────────
# Maps each disposition GUID to one of four display buckets.
# Note: this is display grouping only — CALL_CONNECTED_GUIDS controls
# connect-rate math and is intentionally separate.
OUTCOME_BUCKET: dict[str, str] = {
    "f240bbac-87c9-4f6e-bf70-924b57d47db7": "Positive connect",  # Connected
    "bf63f95f-8fa4-4a42-b918-0a8e5ee4ba3e": "Positive connect",  # Gatekeeper
    "b43b9c27-ecc9-461f-8b4b-6d2c00ae6f0d": "Positive connect",  # Answered - Meeting Set
    "ff4c1e61-46ad-4100-9676-b5d4a0c4f52b": "Positive connect",  # Answered - Call Back
    "314680f7-ba23-4153-b297-1e3bd1453951": "Positive connect",  # Answered - Referral
    "c6ab5404-53ca-4f44-938d-d4400b589b74": "Negative connect",  # Answered - Not Interested
    "a8810b96-f812-4d60-800c-9b0beefa8941": "Negative connect",  # Answered - Poor Fit
    "ee078117-c361-4e51-84af-4cfd534fd878": "Negative connect",  # Answered - Bad Timing
    "293301fd-5a90-47e9-90a9-b87d59f27cc5": "Negative connect",  # Answered - No Longer with Co.
    "0513d3b2-f7e3-4ae5-81ca-be71e399499b": "Negative connect",  # Answered - Wrong Contact
    "0f54a15c-1cb7-458a-8a2a-5a0e97cd7c13": "Negative connect",  # Bad Outcome
    "73a0d17f-1163-4015-bdd5-ec830791da20": "No answer",          # No answer
    "899b0622-cdd2-4c55-8461-1c738dab0b69": "No answer",          # No answer - Poor Fit
    "9d9162e7-6cf3-4944-bf63-4dff82258764": "No answer",          # Busy
    "17b47fee-58de-441e-a44c-c6300d46f273": "No answer",          # Wrong number
    "a4c4c377-d246-4b32-a13b-75a56a4cd0ff": "Voicemail",          # Left live message
    "b2cf5968-551e-4856-9783-52b3da59a7d0": "Voicemail",          # Left voicemail
}
# No disposition → "No answer" (handled as default in the aggregation loop)
_OUTCOME_BUCKET_ORDER = ["Positive connect", "Negative connect", "No answer", "Voicemail"]

# ── Line type buckets: raw cop_line_type → display bucket ────────────────────
# 3-bucket scheme: Direct line / Mobile / Unknown
_LINE_TYPE_BUCKETS: dict[str, str] = {
    "mobile":               "Mobile",
    "personal_number":      "Mobile",
    "fixed_line":           "Direct line",
    "fixed_line_or_mobile": "Direct line",
    "toll_free":            "Direct line",
    "voip":                 "Direct line",
    "unknown":              "Unknown",
}


def _letter_grade(score: float) -> str:
    # Scale allows >100 when quota is overachieved (quota score capped at 150)
    if score >= 110: return "A+"
    if score >= 88:  return "A"
    if score >= 80:  return "A-"
    if score >= 72:  return "B+"
    if score >= 65:  return "B"
    if score >= 58:  return "B-"
    if score >= 50:  return "C+"
    if score >= 42:  return "C"
    if score >= 35:  return "C-"
    if score >= 27:  return "D+"
    if score >= 18:  return "D"
    return "D-"


def _safe_div(a, b):
    return a / b if b else None


def _pct(a, b):
    return round(a / b * 100, 1) if b else 0.0


def _ts_to_dt(val):
    if not val:
        return None
    try:
        ts = int(val) / 1000
        return datetime.fromtimestamp(ts, tz=timezone.utc)
    except Exception:
        return None


def _parse_amount(val):
    try:
        return float(val or 0)
    except Exception:
        return 0.0


def _owner_allowed(oid: str, as_of=None) -> bool:
    """Return True if this owner is in scope for the specified date.

    Reps in the monthly_store grace list are also allowed so they remain
    visible through their post-departure grace window.
    """
    import monthly_store
    if oid in monthly_store.get_grace_rep_ids():
        return True
    allowed = get_scoped_team_owner_ids(as_of)
    return not allowed or oid in allowed


# Canonical labels — all comparisons use these exact strings
_SOURCE_CANONICAL = {
    "cold outreach": "Cold outreach",
    "inbound":       "Inbound",
    "referral":      "Referral",
    "conference":    "Conference",
}


def _deal_source(deal: dict) -> str:
    # Prefer the custom 'deal_source' property; normalise case so that
    # "Cold Outreach", "cold outreach", "COLD OUTREACH" all resolve correctly.
    custom = (deal.get("properties", {}).get("deal_source") or "").strip()
    if custom:
        return _SOURCE_CANONICAL.get(custom.lower(), custom)
    # Fall back to hs_analytics_source for deals without deal_source set
    src = (deal.get("properties", {}).get("hs_analytics_source") or "").upper()
    return DEAL_SOURCE_MAP.get(src, "Cold outreach")


log = logging.getLogger(__name__)


def _nth_weekday_of_month(year: int, month: int, weekday: int, n: int):
    first = datetime(year, month, 1).date()
    offset = (weekday - first.weekday()) % 7
    return first + timedelta(days=offset + (n - 1) * 7)


def _last_weekday_of_month(year: int, month: int, weekday: int):
    if month == 12:
        next_month = datetime(year + 1, 1, 1).date()
    else:
        next_month = datetime(year, month + 1, 1).date()
    current = next_month - timedelta(days=1)
    while current.weekday() != weekday:
        current -= timedelta(days=1)
    return current


def _observed_fixed_holiday(year: int, month: int, day: int):
    actual = datetime(year, month, day).date()
    if actual.weekday() == 5:
        return actual - timedelta(days=1)
    if actual.weekday() == 6:
        return actual + timedelta(days=1)
    return actual


def _company_holidays_for_year(year: int):
    thanksgiving = _nth_weekday_of_month(year, 11, 3, 4)
    return {
        _observed_fixed_holiday(year, 1, 1): "New Year's Day",
        _nth_weekday_of_month(year, 1, 0, 3): "Martin Luther King Jr. Day",
        _nth_weekday_of_month(year, 2, 0, 3): "Presidents' Day",
        _last_weekday_of_month(year, 5, 0): "Memorial Day",
        _observed_fixed_holiday(year, 6, 19): "Juneteenth",
        _observed_fixed_holiday(year, 7, 4): "Independence Day",
        _nth_weekday_of_month(year, 9, 0, 1): "Labor Day",
        _nth_weekday_of_month(year, 10, 0, 2): "Columbus Day",
        _observed_fixed_holiday(year, 11, 11): "Veterans Day",
        thanksgiving: "Thanksgiving Day",
        thanksgiving + timedelta(days=1): "Day After Thanksgiving",
        _observed_fixed_holiday(year, 12, 24): "Christmas Eve",
        _observed_fixed_holiday(year, 12, 25): "Christmas Day",
    }


def _holiday_map_between(start_date, end_date):
    holiday_map = {}
    for year in range(start_date.year, end_date.year + 1):
        for holiday_date, label in _company_holidays_for_year(year).items():
            if start_date <= holiday_date <= end_date:
                holiday_map[holiday_date] = label
    return holiday_map


def _month_start(date_value):
    return date_value.replace(day=1)


def _month_end(date_value):
    if date_value.month == 12:
        return date_value.replace(year=date_value.year + 1, month=1, day=1) - timedelta(days=1)
    return date_value.replace(month=date_value.month + 1, day=1) - timedelta(days=1)


def _shift_month(date_value, months: int):
    total_month = (date_value.year * 12 + (date_value.month - 1)) + months
    year = total_month // 12
    month = total_month % 12 + 1
    return datetime(year, month, 1).date()


def _is_working_day(date_value, holiday_map):
    return date_value.weekday() < 5 and date_value not in holiday_map


def _working_days_between(start_date, end_date, holiday_map):
    if end_date < start_date:
        return 0
    return sum(
        1
        for i in range((end_date - start_date).days + 1)
        if _is_working_day(start_date + timedelta(days=i), holiday_map)
    )


def _call_daily_series(calls, owners, scope_end, contact_windows, call_to_contact):
    daily_dials = defaultdict(int)
    daily_conversations = defaultdict(int)
    for call in calls:
        oid = call["properties"].get("hubspot_owner_id", "")
        if not oid or not owners.get(oid):
            continue
        if not _owner_allowed(oid, scope_end):
            continue
        if (call["properties"].get("hs_call_direction") or "").upper() == "INBOUND":
            continue
        contact_id = call_to_contact.get(call["id"])
        if contact_id and contact_id in contact_windows:
            ts_raw = call["properties"].get("hs_timestamp") or call["properties"].get("hs_createdate")
            if ts_raw:
                try:
                    call_ts_ms = int(datetime.fromisoformat(str(ts_raw).replace("Z", "+00:00")).timestamp() * 1000)
                    skip = False
                    for (open_start, open_end) in contact_windows[contact_id]:
                        if open_start <= call_ts_ms and (open_end is None or call_ts_ms <= open_end):
                            skip = True
                            break
                    if skip:
                        continue
                except Exception:
                    pass
        ts_raw = call["properties"].get("hs_timestamp") or call["properties"].get("hs_createdate")
        if not ts_raw:
            continue
        try:
            dt = datetime.fromisoformat(str(ts_raw).replace("Z", "+00:00"))
        except Exception:
            continue
        day_key = dt.date().isoformat()
        daily_dials[day_key] += 1
        disposition = (call["properties"].get("hs_call_disposition") or "").strip()
        duration_ms = int(call["properties"].get("hs_call_duration") or 0)
        if disposition in CALL_CONNECTED_GUIDS and duration_ms >= 60000:
            daily_conversations[day_key] += 1
    return daily_dials, daily_conversations


def _deal_daily_series(deals, owners, scope_end):
    daily_cold_outreach = defaultdict(int)
    for deal in deals:
        oid = deal["properties"].get("hubspot_owner_id", "")
        if not oid or not owners.get(oid):
            continue
        if not _owner_allowed(oid, scope_end):
            continue
        if _deal_source(deal) != "Cold outreach":
            continue
        ts_raw = deal["properties"].get("createdate")
        if not ts_raw:
            continue
        try:
            dt = datetime.fromisoformat(str(ts_raw).replace("Z", "+00:00"))
        except Exception:
            continue
        daily_cold_outreach[dt.date().isoformat()] += 1
    return daily_cold_outreach


@ttl_cache
def compute_call_stats(period: str) -> dict:
    start, end = get_date_range(period)
    # Business days (Mon–Fri) elapsed in the period — used as avg/day denominator
    period_bdays = sum(
        1 for i in range((end - start).days + 1)
        if (start + timedelta(days=i)).weekday() < 5
    )
    period_bdays = max(period_bdays, 1)
    owners = apply_manual_owner_overrides(get_owners())
    scope_end = end
    calls = get_calls(start, end)
    log.info("compute_call_stats(%s): range %s → %s, raw calls=%d",
             period, start.isoformat(), end.isoformat(), len(calls))
    deals_created = get_deals(start, end, "createdate")

    # Build time-aware exclusion: {contact_id: [(open_start_ms, open_end_ms_or_None), ...]}
    contact_windows = get_deal_contact_windows()
    call_to_contact = get_call_to_contact_map([c["id"] for c in calls])

    # Map deals to owner
    owner_deals_created = defaultdict(set)
    owner_deals_s2 = defaultdict(set)

    for d in deals_created:
        oid = d["properties"].get("hubspot_owner_id", "")
        if not oid:
            continue
        if not _owner_allowed(oid, scope_end):
            continue
        src = _deal_source(d)
        if src == "Cold outreach":
            owner_deals_created[oid].add(d["id"])
            # Use current dealstage as proxy for advancement (hs_date_entered_* is null on this plan)
            stage = d["properties"].get("dealstage", "")
            if stage in (NB_STAGES["stage2"], NB_STAGES["stage3"], NB_STAGES["stage4"], NB_STAGES["won"]):
                owner_deals_s2[oid].add(d["id"])

    # Count calls per owner
    owner_calls = defaultdict(lambda: {"dials": 0, "connects": 0, "conversations": 0, "days": set()})
    for call in calls:
        oid = call["properties"].get("hubspot_owner_id", "")
        if not oid:
            continue
        if not _owner_allowed(oid, scope_end):
            continue
        if (call["properties"].get("hs_call_direction") or "").upper() == "INBOUND":
            continue
        # Exclude calls where a deal was open for that contact at the time of the call
        contact_id = call_to_contact.get(call["id"])
        if contact_id and contact_id in contact_windows:
            ts_raw = call["properties"].get("hs_timestamp") or call["properties"].get("hs_createdate")
            if ts_raw:
                try:
                    call_ts_ms = int(datetime.fromisoformat(str(ts_raw).replace("Z", "+00:00")).timestamp() * 1000)
                    skip = False
                    for (open_start, open_end) in contact_windows[contact_id]:
                        if open_start <= call_ts_ms and (open_end is None or call_ts_ms <= open_end):
                            skip = True
                            break
                    if skip:
                        continue
                except Exception:
                    pass
        disposition = (call["properties"].get("hs_call_disposition") or "").strip()
        ts_raw = call["properties"].get("hs_timestamp") or call["properties"].get("hs_createdate")
        if ts_raw:
            try:
                from datetime import datetime as _dt
                dt = _dt.fromisoformat(str(ts_raw).replace("Z", "+00:00"))
                owner_calls[oid]["days"].add(dt.strftime("%Y-%m-%d"))
            except Exception:
                pass
        owner_calls[oid]["dials"] += 1
        if disposition in CALL_CONNECTED_GUIDS:
            owner_calls[oid]["connects"] += 1
        # Conversation = connected disposition AND duration >= 60 seconds
        duration_ms = int(call["properties"].get("hs_call_duration") or 0)
        if disposition in CALL_CONNECTED_GUIDS and duration_ms >= 60000:
            owner_calls[oid]["conversations"] += 1

    total_counted = sum(v["dials"] for v in owner_calls.values())
    log.info("compute_call_stats(%s): after filtering, counted dials=%d across %d owners",
             period, total_counted, len(owner_calls))
    rows = []
    active_owners = set(owner_calls.keys()) | set(owner_deals_created.keys())

    for oid in active_owners:
        owner = owners.get(oid)
        if not owner:
            continue
        c = owner_calls[oid]
        dials = c["dials"]
        connects = c["connects"]
        convos = c["conversations"]
        deals_out = len(owner_deals_created[oid])
        deals_s2 = len(owner_deals_s2[oid])
        rows.append({
            "ae": owner["last_name"] or owner["name"],
            "owner_id": oid,
            "dials": dials,
            "avg_dials_per_day": round(dials / period_bdays, 1),
            "pct_connect": _pct(connects, dials),
            "connects": connects,
            "pct_conversation": _pct(convos, connects),
            "conversations": convos,
            "pct_deals": _pct(deals_out, dials),  # dial-to-deal rate (never exceeds 100%)
            "outbound_deals_created": deals_out,
            "outbound_deals_to_s2": deals_s2,
        })

    rows.sort(key=lambda r: r["dials"], reverse=True)

    totals = {
        "ae": "TOTAL",
        "dials": sum(r["dials"] for r in rows),
        "avg_dials_per_day": round(sum(r["dials"] for r in rows) / period_bdays / len(rows), 1) if rows else 0.0,
        "pct_connect": _pct(sum(r["connects"] for r in rows), sum(r["dials"] for r in rows)),
        "connects": sum(r["connects"] for r in rows),
        "pct_conversation": _pct(sum(r["conversations"] for r in rows), sum(r["connects"] for r in rows)),
        "conversations": sum(r["conversations"] for r in rows),
        "pct_deals": _pct(sum(r["outbound_deals_created"] for r in rows), sum(r["dials"] for r in rows)),
        "outbound_deals_created": sum(r["outbound_deals_created"] for r in rows),
        "outbound_deals_to_s2": sum(r["outbound_deals_to_s2"] for r in rows),
    }

    return {"rows": rows, "totals": totals, "period": period, "start": start.isoformat(), "end": end.isoformat()}


# ── ICP rank sort order (unknown/blank sorts last) ───────────────────────────
_ICP_ORDER = ["A+", "A", "B", "C", "D", "Least Priority"]


def _icp_sort_key(rank: str) -> tuple:
    try:
        return (0, _ICP_ORDER.index(rank))
    except ValueError:
        return (1, rank)


def _normalize_icp_rank(raw: str) -> str:
    """Map HubSpot internal icp_rank enum value to display label."""
    v = (raw or "").strip().lower()
    return _ICP_INTERNAL_TO_LABEL.get(v) or (raw.strip() if raw and raw.strip() else "—")


def _normalize_line_type(raw: str) -> str:
    """Map raw cop_line_type value to a display bucket label."""
    v = (raw or "").strip().lower()
    return _LINE_TYPE_BUCKETS.get(v, "Unknown")


def _hour_label(h: int) -> str:
    """Convert 0-23 hour to a short AM/PM label, e.g. 9 → '9 AM', 13 → '1 PM'."""
    if h == 0:
        return "12 AM"
    if h < 12:
        return f"{h} AM"
    if h == 12:
        return "12 PM"
    return f"{h - 12} PM"


@ttl_cache
def compute_connect_diagnostics(period: str) -> dict:
    """Per-rep and team-wide call activity diagnostics.

    Returns:
      rows         — per-rep list sorted by dials desc; each row includes:
                     ae, owner_id, dials, connects, pct_connect,
                     by_hour {h: dial_count}, target_dials
      totals       — {dials, connects, pct_connect} (all permitted reps)
      outcome_dist — {bucket_label: count} for four grouped display buckets
      hourly_stats — [{hour, label, dials, connects, pct|None}] for CT hours 7–20
                     pct is None when dials < 5 (insufficient for a reliable rate)
      target_dials — dials goal for the period (period_bdays × 40)
    """
    from zoneinfo import ZoneInfo
    ET = ZoneInfo("America/Chicago")
    DIALS_PER_DAY_GOAL = 40
    HOURLY_MIN = 5        # minimum dials per hour before showing connect % on chart
    BEST_HOUR_MIN = 25    # minimum dials per hour for Best Connect Hour KPI

    start, end = get_date_range(period)
    period_bdays = max(sum(
        1 for i in range((end - start).days + 1)
        if (start + timedelta(days=i)).weekday() < 5
    ), 1)
    target_dials = period_bdays * DIALS_PER_DAY_GOAL

    owners = apply_manual_owner_overrides(get_owners())
    calls  = get_calls_enriched(start, end)

    # Same exclusion logic as compute_call_stats:
    # - outbound only, permitted reps only
    # - exclude calls where a deal was open for that contact at call time
    contact_windows = get_deal_contact_windows()
    filtered = []
    for c in calls:
        if not c["properties"].get("hubspot_owner_id"):
            continue
        if not _owner_allowed(c["properties"]["hubspot_owner_id"]):
            continue
        if (c["properties"].get("hs_call_direction") or "").upper() == "INBOUND":
            continue
        contact_id = c.get("_contact_id")
        if contact_id and contact_id in contact_windows:
            ts_raw = c["properties"].get("hs_timestamp") or c["properties"].get("hs_createdate")
            if ts_raw:
                try:
                    call_ts_ms = int(datetime.fromisoformat(str(ts_raw).replace("Z", "+00:00")).timestamp() * 1000)
                    skip = False
                    for (open_start, open_end) in contact_windows[contact_id]:
                        if open_start <= call_ts_ms and (open_end is None or call_ts_ms <= open_end):
                            skip = True
                            break
                    if skip:
                        continue
                except Exception:
                    pass
        filtered.append(c)

    _empty_hourly = [
        {"hour": h, "label": _hour_label(h), "dials": 0, "connects": 0, "pct": None}
        for h in range(7, 21)
    ]
    if not filtered:
        return {
            "rows": [], "totals": {"dials": 0, "connects": 0, "pct_connect": 0.0},
            "outcome_dist": {b: 0 for b in _OUTCOME_BUCKET_ORDER},
            "hourly_stats": _empty_hourly,
            "target_dials": target_dials,
        }

    # Single-pass accumulators
    owner_stats:  dict = defaultdict(lambda: {"dials": 0, "connects": 0})
    owner_hourly: dict = defaultdict(lambda: defaultdict(int))   # oid → {hour → dial_count}
    outcome_raw:  dict = defaultdict(int)
    hourly_raw:   dict = defaultdict(lambda: {"dials": 0, "connects": 0})

    for call in filtered:
        oid  = call["properties"]["hubspot_owner_id"]
        disp = (call["properties"].get("hs_call_disposition") or "").strip()
        is_connect = disp in CALL_CONNECTED_GUIDS

        owner_stats[oid]["dials"] += 1
        if is_connect:
            owner_stats[oid]["connects"] += 1

        # Outcome bucket — empty disposition → "No answer" (no disposition recorded)
        outcome_raw[OUTCOME_BUCKET.get(disp, "No answer")] += 1

        # CT hour — used for team hourly chart and per-rep heatmap
        ts_raw = (call["properties"].get("hs_timestamp")
                  or call["properties"].get("hs_createdate") or "")
        try:
            hour_et = _parse_hs_datetime(ts_raw).astimezone(ET).hour
        except (ValueError, AttributeError):
            hour_et = None

        if hour_et is not None:
            hourly_raw[hour_et]["dials"] += 1
            if is_connect:
                hourly_raw[hour_et]["connects"] += 1
            owner_hourly[oid][hour_et] += 1

    # Per-rep rows
    rows = []
    for oid, s in owner_stats.items():
        owner = owners.get(oid, {})
        name  = owner.get("last_name") or owner.get("name") or oid
        d, c  = s["dials"], s["connects"]
        rows.append({
            "ae":          name,
            "owner_id":    oid,
            "dials":       d,
            "connects":    c,
            "pct_connect": _pct(c, d),
            "by_hour":     dict(owner_hourly.get(oid, {})),
            "target_dials": target_dials,
        })
    rows.sort(key=lambda r: r["dials"], reverse=True)

    total_dials    = sum(r["dials"]    for r in rows)
    total_connects = sum(r["connects"] for r in rows)

    # Hourly stats — team-wide, ET, business hours only (7 AM – 8 PM)
    hourly_stats = []
    for h in range(7, 21):
        v = hourly_raw.get(h, {"dials": 0, "connects": 0})
        d, c = v["dials"], v["connects"]
        hourly_stats.append({
            "hour":     h,
            "label":    _hour_label(h),
            "dials":    d,
            "connects": c,
            "pct":      _pct(c, d) if d >= HOURLY_MIN else None,
        })

    best_slot = max(
        (s for s in hourly_stats if s["dials"] >= BEST_HOUR_MIN and s["pct"] is not None),
        key=lambda s: s["pct"],
        default=None,
    )

    return {
        "rows":    rows,
        "totals":  {
            "dials":       total_dials,
            "connects":    total_connects,
            "pct_connect": _pct(total_connects, total_dials),
        },
        "outcome_dist": {b: outcome_raw.get(b, 0) for b in _OUTCOME_BUCKET_ORDER},
        "hourly_stats": hourly_stats,
        "target_dials": target_dials,
        "best_connect_hour": best_slot["label"] if best_slot else "—",
    }


_BUYER_TITLE_PATTERNS = (
    "chief", "vp", "vice president", "head", "director", "manager",
    "founder", "owner", "president", "partner",
)

_PLACEHOLDER_EMAIL_PATTERNS = (
    "example.com", "noemail", "noreply", "invalid", "fake", "test@", "@test.",
)

_CONNECT_DRIVER_SORTS = {
    "worst_delta_vs_team",
    "worst_vs_expected",
    "lowest_gap_explained",
    "highest_connect",
}

_COMPARISON_MODES = {"connect_pct", "delta_vs_team", "actual_vs_expected"}


def _normalize_phone(raw: str) -> str:
    digits = re.sub(r"\D+", "", raw or "")
    if len(digits) >= 10:
        return digits[-10:]
    return ""


def _is_buyer_title(title: str) -> bool:
    t = (title or "").strip().lower()
    return bool(t and any(token in t for token in _BUYER_TITLE_PATTERNS))


_TITLE_SEGMENTS = [
    ("Owner / Executive", (
        re.compile(r"\bceo\b"), re.compile(r"\bcfo\b"), re.compile(r"\bcto\b"),
        re.compile(r"\bcoo\b"), re.compile(r"\bciso\b"),
        "owner", "chief", "founder", "president", "partner", "executive", "principal",
    )),
    ("Operations / General Manager", ("operations", "general manager", "ops manager", "plant manager", "branch manager", "regional manager")),
    ("Field Supervisor / Site Manager", ("supervisor", "site manager", "field manager", "foreman", "superintendent", "crew lead")),
    ("Scheduler / Dispatcher", ("scheduler", "dispatch", "coordinator", "logistics")),
    ("Finance / Payroll", ("finance", "payroll", "accounting", "controller", "comptroller", "bookkeeper", "accounts payable", "accounts receivable", "billing")),
    ("IT / Technical", (re.compile(r"\bit\b"), "information technology", "technical", "technology", "systems admin", "network admin", "helpdesk", "help desk", "sysadmin")),
]

TITLE_SEGMENT_ORDER = [
    "No Title Available",
    "Owner / Executive",
    "Operations / General Manager",
    "Field Supervisor / Site Manager",
    "Scheduler / Dispatcher",
    "Finance / Payroll",
    "IT / Technical",
    "Other",
]


def _title_keyword_match(keyword, text: str) -> bool:
    if isinstance(keyword, re.Pattern):
        return bool(keyword.search(text))
    return keyword in text


def _classify_title(title: str) -> str:
    """Classify a job title into one of the calling title segments."""
    t = (title or "").strip().lower()
    if not t:
        return "No Title Available"
    for segment_label, keywords in _TITLE_SEGMENTS:
        if any(_title_keyword_match(kw, t) for kw in keywords):
            return segment_label
    return "Other"


def _looks_placeholder_email(email: str) -> bool:
    e = (email or "").strip().lower()
    return bool(e and any(token in e for token in _PLACEHOLDER_EMAIL_PATTERNS))


def _fmt_pct_points(value: float, digits: int = 1) -> str:
    return f"{value:.{digits}f}%"


def _fmt_point_delta(value: float, digits: int = 1) -> str:
    if value > 0:
        return f"+{value:.{digits}f} pts"
    return f"{value:.{digits}f} pts"


def _fmt_percent_delta(value: float, digits: int = 1) -> str:
    if value > 0:
        return f"+{value:.{digits}f}%"
    return f"{value:.{digits}f}%"


def _fmt_index_delta(value: float) -> str:
    if value > 0:
        return f"+{value:.0f}"
    return f"{value:.0f}"


def _safe_share_pct(part: float, total: float) -> float:
    return round(part / total * 100, 1) if total else 0.0


def _metric_display(label: str, rep: float, team: float) -> dict:
    delta = round(rep - team, 1)
    if "Index" in label:
        return {
            "rep": f"{rep:.0f}",
            "team": f"{team:.0f}",
            "delta": _fmt_index_delta(delta),
        }
    if "Unique Numbers / 100 Dials" == label:
        return {
            "rep": f"{rep:.1f}",
            "team": f"{team:.1f}",
            "delta": _fmt_percent_delta(delta),
        }
    return {
        "rep": _fmt_pct_points(rep),
        "team": _fmt_pct_points(team),
        "delta": _fmt_point_delta(delta),
    }


def _pct_band(value: float) -> str:
    if value >= 80:
        return "strong"
    if value >= 60:
        return "directional"
    return "incomplete"


def _compute_timing_windows(calls: list[dict]) -> tuple[set[int], set[int], dict[int, dict]]:
    hourly = defaultdict(lambda: {"dials": 0, "connects": 0})
    for call in calls:
        hour = call.get("local_hour")
        if hour is None:
            continue
        hourly[hour]["dials"] += 1
        if call.get("is_connect"):
            hourly[hour]["connects"] += 1
    valid_hours = []
    for hour, stats in hourly.items():
        if stats["dials"] < 5:
            continue
        valid_hours.append((hour, _pct(stats["connects"], stats["dials"])))
    valid_hours.sort(key=lambda item: item[1], reverse=True)
    strong = {hour for hour, _ in valid_hours[:3]}
    weak = {hour for hour, _ in valid_hours[-3:]} if len(valid_hours) >= 3 else set()
    return strong, weak, hourly


def _call_meets_scope(call: dict, team: str, rep: str, owner_team_map: dict) -> bool:
    owner_id = call.get("owner_id")
    if rep != "all":
        return owner_id == rep
    if team != "all":
        return owner_team_map.get(owner_id) == team
    return True


def _build_connect_driver_aggregate(calls: list[dict], strong_hours: set[int], weak_hours: set[int]) -> dict:
    dials = len(calls)
    connects = sum(1 for call in calls if call["is_connect"])
    conversations = sum(1 for call in calls if call.get("is_conversation"))
    unique_numbers = {call["normalized_phone"] for call in calls if call.get("normalized_phone")}
    unique_contacts = {call["contact_id"] for call in calls if call.get("contact_id")}
    direct_count = sum(1 for call in calls if call["is_direct"])
    shared_count = sum(1 for call in calls if call["is_shared_number"])
    high_conf_count = sum(1 for call in calls if call["is_high_conf_phone"])
    buyer_count = sum(1 for call in calls if call["is_buyer_title"])
    icp_ab_count = sum(1 for call in calls if call["is_icp_ab"])
    low_icp_count = sum(1 for call in calls if call.get("is_low_icp"))
    no_icp_count = sum(1 for call in calls if call.get("is_no_icp_data"))
    company_object_count = sum(1 for call in calls if call.get("is_from_company_object"))
    repeat_count = sum(1 for call in calls if call["is_repeat_dial"])
    first_attempt_count = sum(1 for call in calls if call["is_first_attempt"])
    shared_recycle_count = sum(1 for call in calls if call["is_shared_recycle"])
    strong_dials = [call for call in calls if call.get("local_hour") in strong_hours]
    weak_dials = [call for call in calls if call.get("local_hour") in weak_hours]
    covered_scores = [call["coverage_score"] for call in calls]

    direct_rate = _safe_share_pct(direct_count, dials)
    shared_rate = _safe_share_pct(shared_count, dials)
    high_conf_rate = _safe_share_pct(high_conf_count, dials)
    buyer_rate = _safe_share_pct(buyer_count, dials)
    icp_ab_rate = _safe_share_pct(icp_ab_count, dials)
    unique_numbers_per_100 = round(len(unique_numbers) / dials * 100, 1) if dials else 0.0
    repeat_rate = _safe_share_pct(repeat_count, dials)
    first_attempt_rate = _safe_share_pct(first_attempt_count, dials)
    shared_recycle_rate = _safe_share_pct(shared_recycle_count, dials)
    best_window_rate = _safe_share_pct(len(strong_dials), dials)
    weak_window_rate = _safe_share_pct(len(weak_dials), dials)
    strong_connect_rate = _pct(sum(1 for call in strong_dials if call["is_connect"]), len(strong_dials))
    weak_connect_rate = _pct(sum(1 for call in weak_dials if call["is_connect"]), len(weak_dials))

    return {
        "dials": dials,
        "connects": connects,
        "conversations": conversations,
        "connect_pct": _pct(connects, dials),
        "conversation_pct": _pct(conversations, connects),
        "field_coverage_pct": round(sum(covered_scores) / len(covered_scores) * 100, 1) if covered_scores else 0.0,
        "unique_contacts": len(unique_contacts),
        "metrics": {
            "Direct Number Rate": direct_rate,
            "Shared Number Rate": shared_rate,
            "High-Confidence Phone Rate": high_conf_rate,
            "Buyer-Title Rate": buyer_rate,
            "ICP A–C Rate": icp_ab_rate,
            "Low ICP Rate": _safe_share_pct(low_icp_count, dials),
            "No ICP Data Rate": _safe_share_pct(no_icp_count, dials),
            "Company-Object Dial Rate": _safe_share_pct(company_object_count, dials),
            "Unique Numbers / 100 Dials": unique_numbers_per_100,
            "Repeat Dial Rate": repeat_rate,
            "First Attempt Rate": first_attempt_rate,
            "Shared-Number Recycle Rate": shared_recycle_rate,
            "Best-Window Dial Rate": best_window_rate,
            "Weak-Window Dial Rate": weak_window_rate,
            "Connect Rate in Strong Windows": strong_connect_rate,
            "Connect Rate in Weak Windows": weak_connect_rate,
        },
    }


def _rate_for(calls: list[dict], predicate) -> float:
    subset = [call for call in calls if predicate(call)]
    if not subset:
        return 0.0
    return _pct(sum(1 for call in subset if call["is_connect"]), len(subset))


def _metric_lift(current_calls: list[dict], team_calls: list[dict], predicate) -> float:
    team_rate = _rate_for(team_calls, predicate)
    baseline = _pct(sum(1 for call in current_calls if call["is_connect"]), len(current_calls))
    return round(team_rate - baseline, 3)


def _build_driver_contributions(current_calls: list[dict], benchmark_calls: list[dict], strong_hours: set[int], weak_hours: set[int]) -> dict:
    current = _build_connect_driver_aggregate(current_calls, strong_hours, weak_hours)
    benchmark = _build_connect_driver_aggregate(benchmark_calls, strong_hours, weak_hours)

    def metric_delta(label: str) -> float:
        return (current["metrics"][label] - benchmark["metrics"][label]) / 100.0

    dial_mix = sum([
        metric_delta("Direct Number Rate") * _metric_lift(current_calls, benchmark_calls, lambda c: c["is_direct"]),
        metric_delta("Shared Number Rate") * _metric_lift(current_calls, benchmark_calls, lambda c: c["is_shared_number"]),
        metric_delta("High-Confidence Phone Rate") * _metric_lift(current_calls, benchmark_calls, lambda c: c["is_high_conf_phone"]),
        metric_delta("ICP A–C Rate") * _metric_lift(current_calls, benchmark_calls, lambda c: c["is_icp_ab"]),
    ])
    behavior = sum([
        metric_delta("First Attempt Rate") * _metric_lift(current_calls, benchmark_calls, lambda c: c["is_first_attempt"]),
        metric_delta("Repeat Dial Rate") * _metric_lift(current_calls, benchmark_calls, lambda c: c["is_repeat_dial"]),
        metric_delta("Shared-Number Recycle Rate") * _metric_lift(current_calls, benchmark_calls, lambda c: c["is_shared_recycle"]),
    ])
    timing = sum([
        metric_delta("Best-Window Dial Rate") * _metric_lift(current_calls, benchmark_calls, lambda c: c.get("local_hour") in strong_hours),
        metric_delta("Weak-Window Dial Rate") * _metric_lift(current_calls, benchmark_calls, lambda c: c.get("local_hour") in weak_hours),
    ])

    return {
        "Dial Mix": round(dial_mix, 1),
        "Dialing Behavior": round(behavior, 1),
        "Timing": round(timing, 1),
    }


def _build_icp_breakdown(current_calls: list[dict], benchmark_calls: list[dict]) -> list[dict]:
    ranks = ["A+", "A", "B", "C", "D", "Least Priority", "—"]
    current_total = len(current_calls) or 1
    benchmark_total = len(benchmark_calls) or 1
    result = []
    for rank in ranks:
        rep_count = sum(1 for c in current_calls if c["icp_rank"] == rank)
        team_count = sum(1 for c in benchmark_calls if c["icp_rank"] == rank)
        result.append({
            "rank": rank,
            "rep": round(rep_count / current_total * 100, 1),
            "team": round(team_count / benchmark_total * 100, 1),
        })
    return result


def _build_title_breakdown(current_calls: list[dict], benchmark_calls: list[dict]) -> list[dict]:
    current_total = len(current_calls) or 1
    benchmark_total = len(benchmark_calls) or 1
    raw = []
    for bucket in TITLE_SEGMENT_ORDER:
        rep_count = sum(1 for c in current_calls if c["title_segment"] == bucket)
        team_count = sum(1 for c in benchmark_calls if c["title_segment"] == bucket)
        raw.append({
            "bucket": bucket,
            "rep": round(rep_count / current_total * 100, 1),
            "team": round(team_count / benchmark_total * 100, 1),
        })

    # Consolidate segments where both rep and team are < 5% into "Other"
    other = next(r for r in raw if r["bucket"] == "Other")
    result = []
    for row in raw:
        if row["bucket"] != "Other" and row["rep"] < 5 and row["team"] < 5:
            other["rep"] = round(other["rep"] + row["rep"], 1)
            other["team"] = round(other["team"] + row["team"], 1)
        else:
            result.append(row)
    return result


def _build_driver_cards(current_stats: dict, benchmark_stats: dict, current_calls: list[dict] | None = None, benchmark_calls: list[dict] | None = None) -> list[dict]:
    dial_mix_rows = []
    for label in (
        "Direct Number Rate",
        "Shared Number Rate",
        "High-Confidence Phone Rate",
        "ICP A–C Rate",
    ):
        rep = current_stats["metrics"][label]
        team = benchmark_stats["metrics"][label]
        dial_mix_rows.append({
            "label": label,
            "rep": rep,
            "team": team,
            "delta": round(rep - team, 1),
            "display": _metric_display(label, rep, team),
        })

    behavior_rows = []
    for label in (
        "Unique Numbers / 100 Dials",
        "Repeat Dial Rate",
        "First Attempt Rate",
        "Shared-Number Recycle Rate",
    ):
        rep = current_stats["metrics"][label]
        team = benchmark_stats["metrics"][label]
        behavior_rows.append({
            "label": label,
            "rep": rep,
            "team": team,
            "delta": round(rep - team, 1),
            "display": _metric_display(label, rep, team),
        })

    timing_rows = []
    for label in (
        "Best-Window Dial Rate",
        "Weak-Window Dial Rate",
        "Connect Rate in Strong Windows",
        "Connect Rate in Weak Windows",
    ):
        rep = current_stats["metrics"][label]
        team = benchmark_stats["metrics"][label]
        timing_rows.append({
            "label": label,
            "rep": rep,
            "team": team,
            "delta": round(rep - team, 1),
            "display": _metric_display(label, rep, team),
        })

    def index_value(card_rows: list[dict], invert_labels: set[str]) -> float:
        score = 100.0
        for row in card_rows:
            sign = -1 if row["label"] in invert_labels else 1
            score += row["delta"] * sign
        return round(max(40.0, min(160.0, score)))

    return [
        {
            "title": "Dial Mix",
            "question": "Is this team calling stronger or weaker reachable records than average, including the same number across different contacts?",
            "index_label": "Dial Mix Index",
            "index_value": index_value(dial_mix_rows, {"Shared Number Rate"}),
            "index_team_baseline": 100,
            "tip": "Composite read of reachable-record quality versus the selected team baseline. Shared Number Rate tracks the same normalized phone number attached to multiple contacts.",
            "rows": dial_mix_rows,
            "icp_breakdown": _build_icp_breakdown(current_calls, benchmark_calls) if current_calls is not None else [],
            "title_breakdown": _build_title_breakdown(current_calls, benchmark_calls) if current_calls is not None else [],
        },
        {
            "title": "Dialing Behavior",
            "question": "Is this rep creating fresh reach efficiently or wasting volume?",
            "index_label": "Reach Efficiency Index",
            "index_value": index_value(behavior_rows, {"Repeat Dial Rate", "Shared-Number Recycle Rate"}),
            "index_team_baseline": 100,
            "tip": "Composite read of how efficiently the dialing pattern creates fresh reach.",
            "rows": behavior_rows,
        },
        {
            "title": "Timing",
            "question": "Is this rep calling in productive windows?",
            "index_label": "Timing Quality Index",
            "index_value": index_value(timing_rows, {"Weak-Window Dial Rate"}),
            "index_team_baseline": 100,
            "tip": "Composite read of timing quality versus when the selected team tends to connect best.",
            "rows": timing_rows,
        },
    ]


@ttl_cache
def compute_connect_rate_drivers(
    period: str,
    team: str = "all",
    rep: str = "all",
    segment: str = "all",
    comparison_mode: str = "connect_pct",
    table_sort: str = "worst_delta_vs_team",
) -> dict:
    from zoneinfo import ZoneInfo

    if comparison_mode not in _COMPARISON_MODES:
        comparison_mode = "connect_pct"
    if table_sort not in _CONNECT_DRIVER_SORTS:
        table_sort = "worst_delta_vs_team"
    rep = "all"

    ct = ZoneInfo("America/Chicago")
    start, end = get_date_range(period)
    owners = apply_manual_owner_overrides(get_owners())
    owner_team_map = get_owner_team_map()
    contact_windows = get_deal_contact_windows()
    calls = get_calls_enriched(start, end)

    prepared_calls = []
    for call in calls:
        props = call.get("properties", {})
        owner_id = props.get("hubspot_owner_id", "")
        if not owner_id or not _owner_allowed(owner_id, end):
            continue
        if (props.get("hs_call_direction") or "").upper() == "INBOUND":
            continue
        contact_id = call.get("_contact_id")
        ts_raw = props.get("hs_timestamp") or props.get("hs_createdate") or ""
        try:
            ts = _parse_hs_datetime(ts_raw)
        except (ValueError, AttributeError):
            ts = None
        if contact_id and contact_id in contact_windows and ts is not None:
            call_ts_ms = int(ts.timestamp() * 1000)
            skip = False
            for open_start, open_end in contact_windows[contact_id]:
                if open_start <= call_ts_ms and (open_end is None or call_ts_ms <= open_end):
                    skip = True
                    break
            if skip:
                continue

        owner = owners.get(owner_id, {})
        email = call.get("_email") or ""
        phone = call.get("_mobilephone") or call.get("_phone") or ""
        normalized_phone = _normalize_phone(phone)
        jobtitle = call.get("_jobtitle") or ""
        line_type = _normalize_line_type(call.get("_line_type") or "Unknown")
        icp_rank = _normalize_icp_rank(call.get("_icp_rank") or "—")
        is_connect = (props.get("hs_call_disposition") or "").strip() in CALL_CONNECTED_GUIDS
        duration_ms = int(props.get("hs_call_duration") or 0)
        local_hour = ts.astimezone(ct).hour if ts else None
        coverage_fields = [
            1 if normalized_phone else 0,
            1 if line_type != "Unknown" else 0,
            1 if (icp_rank != "—" or jobtitle) else 0,
            1 if local_hour is not None else 0,
        ]
        prepared_calls.append({
            "owner_id": owner_id,
            "rep": owner.get("last_name") or owner.get("name") or owner_id,
            "team": owner_team_map.get(owner_id, "Unassigned"),
            "contact_id": str(contact_id) if contact_id else "",
            "call_id": call.get("id"),
            "is_connect": is_connect,
            "timestamp": ts,
            "local_hour": local_hour,
            "line_type": line_type,
            "icp_rank": icp_rank,
            "email": email,
            "jobtitle": jobtitle,
            "normalized_phone": normalized_phone,
            "is_direct": line_type in {"Direct line", "Mobile"},
            "is_high_conf_phone": bool(normalized_phone and line_type != "Unknown"),
            "is_buyer_title": _is_buyer_title(jobtitle),
            "title_segment": _classify_title(jobtitle),
            "is_icp_ab": icp_rank in {"A+", "A", "B", "C"},
            "is_low_icp": icp_rank in {"D", "Least Priority"},
            "is_no_icp_data": icp_rank == "—",
            "is_from_company_object": call.get("_from_company_object", False),
            "is_conversation": bool(is_connect and duration_ms >= 60000),
            "has_phone_and_email": bool(normalized_phone and email),
            "is_placeholder_email": _looks_placeholder_email(email),
            "coverage_score": sum(coverage_fields) / len(coverage_fields),
        })

    if not prepared_calls:
        return {
            "view": {
                "period": period,
                "period_label": period.replace("_", " ").title(),
                "team": team,
                "rep": "all",
                "rep_label": "All reps",
                "segment": segment,
                "segment_enabled": False,
                "is_rep_view": False,
            },
            "filters": {"teams": [], "reps": [], "segments": []},
            "state": {
                "loading": False,
                "empty": True,
                "partial_explanation": False,
                "sample_too_small": False,
                "field_coverage_weak": False,
                "message": "No eligible call data for selected filters",
            },
            "kpis": [],
            "gap_decomposition": {"title": "What is driving the gap?", "buckets": []},
            "driver_cards": [],
            "team_comparison": {"mode": comparison_mode, "modes": [], "rows": []},
            "diagnostic_table": {"sort": table_sort, "sorts": [], "rows": [], "team_avg_row": None},
            "rep_detail": {"selected_owner_id": None, "available": False},
        }

    phone_to_contacts = defaultdict(set)
    calls_by_owner = defaultdict(list)
    for call in prepared_calls:
        if call["normalized_phone"]:
            if call["contact_id"]:
                phone_to_contacts[call["normalized_phone"]].add(call["contact_id"])
        calls_by_owner[call["owner_id"]].append(call)

    for owner_calls in calls_by_owner.values():
        owner_calls.sort(key=lambda call: call["timestamp"] or datetime.min.replace(tzinfo=timezone.utc))
        seen_phones = set()
        shared_repeat_counts = defaultdict(int)
        for call in owner_calls:
            phone = call["normalized_phone"]
            is_shared = bool(phone and len(phone_to_contacts.get(phone, set())) > 1)
            call["is_shared_number"] = is_shared
            call["is_first_attempt"] = bool(phone) and phone not in seen_phones
            call["is_repeat_dial"] = bool(phone) and phone in seen_phones
            if phone:
                if is_shared and phone in seen_phones:
                    shared_repeat_counts[phone] += 1
                seen_phones.add(phone)
            call["is_shared_recycle"] = bool(phone and is_shared and shared_repeat_counts[phone] > 0)

    strong_hours, weak_hours, _ = _compute_timing_windows(prepared_calls)
    global_stats = _build_connect_driver_aggregate(prepared_calls, strong_hours, weak_hours)
    team_options = [{"value": "all", "label": "All"}]
    for team_name in sorted({call["team"] for call in prepared_calls if call["team"] in {"Veterans", "Rising"}}):
        team_options.append({"value": team_name, "label": team_name})

    visible_calls = [call for call in prepared_calls if _call_meets_scope(call, team, "all", owner_team_map)]
    if not visible_calls:
        return {
            "view": {
                "period": period,
                "period_label": period.replace("_", " ").title(),
                "team": team,
                "rep": "all",
                "rep_label": "All reps",
                "segment": segment,
                "segment_enabled": False,
                "is_rep_view": False,
            },
            "filters": {
                "teams": team_options,
                "reps": [{"value": "all", "label": "All reps"}],
                "segments": [],
            },
            "state": {
                "loading": False,
                "empty": True,
                "partial_explanation": False,
                "sample_too_small": False,
                "field_coverage_weak": False,
                "message": "No eligible call data for selected filters",
            },
            "kpis": [],
            "gap_decomposition": {"title": "What is driving the gap?", "buckets": []},
            "driver_cards": [],
            "team_comparison": {"mode": comparison_mode, "modes": [], "rows": []},
            "diagnostic_table": {"sort": table_sort, "sorts": [], "rows": [], "team_avg_row": None},
            "rep_detail": {"selected_owner_id": None, "available": False},
        }
    benchmark_calls = prepared_calls if team != "all" else visible_calls
    benchmark_stats = _build_connect_driver_aggregate(benchmark_calls, strong_hours, weak_hours)
    current_team_stats = _build_connect_driver_aggregate(visible_calls, strong_hours, weak_hours)

    rep_rows = []
    visible_owner_ids = sorted({call["owner_id"] for call in visible_calls}, key=lambda oid: owners.get(oid, {}).get("last_name") or owners.get(oid, {}).get("name") or oid)
    for owner_id in visible_owner_ids:
        rep_label = owners.get(owner_id, {}).get("last_name") or owners.get(owner_id, {}).get("name") or owner_id
        owner_calls = [call for call in visible_calls if call["owner_id"] == owner_id]
        owner_stats = _build_connect_driver_aggregate(owner_calls, strong_hours, weak_hours)
        contributions = _build_driver_contributions(owner_calls, visible_calls, strong_hours, weak_hours)
        explained_points = round(sum(contributions.values()), 1)
        delta_vs_team = round(owner_stats["connect_pct"] - current_team_stats["connect_pct"], 1)
        unexplained = round(delta_vs_team - explained_points, 1)
        expected = round(current_team_stats["connect_pct"] + explained_points, 1)
        actual_vs_expected = round(owner_stats["connect_pct"] - expected, 1)
        gap_explained = 100.0 if abs(delta_vs_team) < 0.1 else round(min(100.0, abs(explained_points) / abs(delta_vs_team) * 100), 1)
        row = {
            "owner_id": owner_id,
            "rep": rep_label,
            "actual_connect_pct": owner_stats["connect_pct"],
            "expected_connect_pct": expected,
            "delta_vs_team_avg": delta_vs_team,
            "actual_vs_expected": actual_vs_expected,
            "gap_explained_pct": gap_explained,
            "field_coverage_pct": owner_stats["field_coverage_pct"],
            "shared_number_rate": owner_stats["metrics"]["Shared Number Rate"],
            "conversation_pct": owner_stats["conversation_pct"],
            "low_icp_rate": owner_stats["metrics"]["Low ICP Rate"],
            "no_icp_data_rate": owner_stats["metrics"]["No ICP Data Rate"],
            "company_object_rate": owner_stats["metrics"].get("Company-Object Dial Rate", 0.0),
            "primary_driver": max(contributions, key=lambda label: abs(contributions[label])) if contributions else "Unexplained",
            "secondary_driver": sorted(contributions, key=lambda label: abs(contributions[label]), reverse=True)[1] if len(contributions) > 1 else "Unexplained",
            "partial_explanation": gap_explained < 80 or owner_stats["field_coverage_pct"] < 70 or owner_stats["dials"] < 25,
            "sample_too_small": owner_stats["dials"] < 25,
            "driver_points": {
                "Dial Mix": contributions["Dial Mix"],
                "Dialing Behavior": contributions["Dialing Behavior"],
                "Timing": contributions["Timing"],
                "Unexplained": unexplained,
            },
            "driver_cards": _build_driver_cards(owner_stats, current_team_stats, owner_calls, visible_calls),
            "stats": owner_stats,
        }
        rep_rows.append(row)

    selected_calls = [call for call in prepared_calls if _call_meets_scope(call, team, "all", owner_team_map)]
    selected_stats = _build_connect_driver_aggregate(selected_calls, strong_hours, weak_hours) if selected_calls else current_team_stats
    selected_benchmark_calls = benchmark_calls
    selected_benchmark_stats = _build_connect_driver_aggregate(selected_benchmark_calls, strong_hours, weak_hours)
    selected_contributions = _build_driver_contributions(selected_calls, selected_benchmark_calls, strong_hours, weak_hours) if selected_calls else {"Dial Mix": 0.0, "Dialing Behavior": 0.0, "Timing": 0.0}
    selected_delta = round(selected_stats["connect_pct"] - selected_benchmark_stats["connect_pct"], 1)
    selected_explained = round(sum(selected_contributions.values()), 1)
    selected_unexplained = round(selected_delta - selected_explained, 1)
    if team == "all" and rep_rows:
        _n = len(rep_rows)
        selected_contributions = {
            "Dial Mix":         round(sum(r["driver_points"]["Dial Mix"]         for r in rep_rows) / _n, 1),
            "Dialing Behavior": round(sum(r["driver_points"]["Dialing Behavior"] for r in rep_rows) / _n, 1),
            "Timing":           round(sum(r["driver_points"]["Timing"]           for r in rep_rows) / _n, 1),
        }
        selected_explained   = round(sum(selected_contributions.values()), 1)
        selected_unexplained = round(sum(r["driver_points"]["Unexplained"] for r in rep_rows) / _n, 1)
    selected_expected = round(selected_benchmark_stats["connect_pct"] + selected_explained, 1)
    selected_actual_vs_expected = round(selected_stats["connect_pct"] - selected_expected, 1)
    selected_gap_explained = 100.0 if abs(selected_delta) < 0.1 else round(min(100.0, abs(selected_explained) / abs(selected_delta) * 100), 1)
    selected_partial = selected_gap_explained < 80 or selected_stats["field_coverage_pct"] < 70 or selected_stats["dials"] < 25

    comparison_rows = []
    for row in rep_rows:
        comparison_rows.append({
            "owner_id": row["owner_id"],
            "rep": row["rep"],
            "actual_connect_pct": row["actual_connect_pct"],
            "expected_connect_pct": row["expected_connect_pct"],
            "delta_vs_team_avg": row["delta_vs_team_avg"],
            "actual_vs_expected": row["actual_vs_expected"],
            "selected": False,
        })

    if table_sort == "worst_delta_vs_team":
        rep_rows.sort(key=lambda row: (row["delta_vs_team_avg"], row["rep"]))
    elif table_sort == "worst_vs_expected":
        rep_rows.sort(key=lambda row: (row["actual_vs_expected"], row["rep"]))
    elif table_sort == "lowest_gap_explained":
        rep_rows.sort(key=lambda row: (row["gap_explained_pct"], row["rep"]))
    else:
        rep_rows.sort(key=lambda row: (-row["actual_connect_pct"], row["rep"]))

    period_label = {
        "today": "Today",
        "this_week": "This Week",
        "last_week": "Last Week",
        "this_month": "This Month",
        "last_month": "Last Month",
        "last_30": "Last 30 Days",
        "last_90": "Last 90 Days",
        "this_quarter": "This Quarter",
        "last_quarter": "Last Quarter",
        "ytd": "Year to Date",
    }.get(period, period.replace("_", " ").title())
    selected_label = "Selected Team Connect %"

    kpis = [
        {
            "label": selected_label,
            "value": selected_stats["connect_pct"],
            "display": _fmt_pct_points(selected_stats["connect_pct"]),
            "delta_points": None,
            "tip": None,
        },
        {
            "label": "Team Avg Connect %",
            "value": selected_benchmark_stats["connect_pct"],
            "display": _fmt_pct_points(selected_benchmark_stats["connect_pct"]),
            "delta_points": None,
            "tip": None,
        },
        {
            "label": "Delta vs Team Avg",
            "value": selected_delta,
            "display": _fmt_point_delta(selected_delta),
            "delta_points": selected_delta,
            "tip": None,
        },
        {
            "label": "Expected Connect %",
            "value": selected_expected,
            "display": _fmt_pct_points(selected_expected),
            "delta_points": None,
            "tip": "Estimated connect rate based on dial mix, dialing behavior, and timing only.",
        },
        {
            "label": "Actual vs Expected",
            "value": selected_actual_vs_expected,
            "display": _fmt_point_delta(selected_actual_vs_expected),
            "delta_points": selected_actual_vs_expected,
            "tip": "Shows whether actual connect rate landed above or below the measured-condition benchmark.",
        },
        {
            "label": "Gap Explained %",
            "value": selected_gap_explained,
            "display": f"{selected_gap_explained:.0f}%",
            "delta_points": None,
            "band": _pct_band(selected_gap_explained),
            "tip": "Shows how much of the gap versus team average is explained by the tracked drivers.",
        },
        {
            "label": "Field Coverage %",
            "value": selected_stats["field_coverage_pct"],
            "display": f"{selected_stats['field_coverage_pct']:.0f}%",
            "delta_points": None,
            "tip": "Shows how much of the analyzed dialing volume has the fields needed to explain the read confidently.",
        },
    ]

    driver_cards = _build_driver_cards(selected_stats, selected_benchmark_stats, selected_calls, selected_benchmark_calls)
    team_avg_row = {
        "rep": "Team Avg",
        "actual_connect_pct": current_team_stats["connect_pct"],
        "delta_vs_team_avg": 0.0,
        "expected_connect_pct": current_team_stats["connect_pct"],
        "actual_vs_expected": 0.0,
        "gap_explained_pct": 100.0,
        "shared_number_rate": current_team_stats["metrics"]["Shared Number Rate"],
        "conversation_pct": current_team_stats["conversation_pct"],
        "low_icp_rate": current_team_stats["metrics"]["Low ICP Rate"],
        "no_icp_data_rate": current_team_stats["metrics"]["No ICP Data Rate"],
        "company_object_rate": current_team_stats["metrics"].get("Company-Object Dial Rate", 0.0),
    }

    return {
        "view": {
            "period": period,
            "period_label": period_label,
            "team": team,
            "rep": "all",
            "rep_label": "All reps",
            "segment": segment,
            "segment_enabled": False,
            "is_rep_view": False,
            "is_aggregate_view": team == "all",
        },
        "filters": {
            "teams": team_options,
            "reps": [{"value": "all", "label": "All reps"}],
            "segments": [],
        },
        "state": {
            "loading": False,
            "empty": False,
            "partial_explanation": selected_partial,
            "sample_too_small": selected_stats["dials"] < 25,
            "field_coverage_weak": selected_stats["field_coverage_pct"] < 70,
            "message": "Partial explanation" if selected_partial else "Strong explanation",
        },
        "kpis": kpis,
        "notes": {
            "shared_number_definition": "Shared Number Rate flags the same normalized phone number appearing across multiple contact records, which is the closest read on reps calling the same number through different people.",
            "conversation_rate_definition": "Conversation rate uses the same definition as Call Stats: connected outbound calls with 60+ seconds duration divided by live connects.",
            "clearout_phone_source": "Phone type (mobile vs. direct line) comes from the contact record in HubSpot. When the primary line-type field is blank, a secondary enrichment field is used as a fallback. A phone is considered high-confidence when a normalized number is present and the line type is known.",
        },
        "gap_decomposition": {
            "title": "What is driving the gap?",
            "team_avg_connect_pct": selected_benchmark_stats["connect_pct"],
            "rep_connect_pct": selected_stats["connect_pct"],
            "expected_connect_pct": selected_expected,
            "buckets": [
                {"label": "Dial Mix", "points": selected_contributions["Dial Mix"]},
                {"label": "Dialing Behavior", "points": selected_contributions["Dialing Behavior"]},
                {"label": "Timing", "points": selected_contributions["Timing"]},
                {"label": "Unexplained", "points": selected_unexplained},
            ],
        },
        "driver_cards": driver_cards,
        "team_comparison": {
            "mode": comparison_mode,
            "modes": [
                {"value": "connect_pct", "label": "Connect %"},
                {"value": "delta_vs_team", "label": "Delta vs Team"},
                {"value": "actual_vs_expected", "label": "Actual vs Expected"},
            ],
            "team_avg_connect_pct": current_team_stats["connect_pct"],
            "rows": comparison_rows,
        },
        "diagnostic_table": {
            "sort": table_sort,
            "sorts": [
                {"value": "worst_delta_vs_team", "label": "worst Delta vs Team"},
                {"value": "worst_vs_expected", "label": "worst Vs Expected"},
                {"value": "lowest_gap_explained", "label": "lowest Gap Explained %"},
                {"value": "highest_connect", "label": "highest Connect %"},
            ],
            "rows": rep_rows,
            "team_avg_row": team_avg_row,
        },
        "rep_detail": {"selected_owner_id": None, "available": False},
    }


@ttl_cache
def compute_dial_pipeline(period: str) -> dict:
    """Show how dialing volume relates to outbound deals created."""
    data = compute_call_stats(period)
    start_dt = datetime.fromisoformat(data["start"])
    end_dt = datetime.fromisoformat(data["end"])
    start = start_dt.date()
    end = end_dt.date()
    holiday_map = _holiday_map_between(_shift_month(_month_start(start), -6), _month_end(end))
    business_days = max(_working_days_between(start, end, holiday_map), 1)
    target_avg_dials_per_day = 40
    target_dials_per_rep = business_days * target_avg_dials_per_day

    def _next_month_start(d):
        return (d.replace(day=28) + timedelta(days=4)).replace(day=1)

    def _business_days_in_range(range_start, range_end):
        return max(_working_days_between(range_start, range_end, holiday_map), 0)

    def _period_cold_outreach_goal_per_rep():
        cursor = start.replace(day=1)
        goal = 0.0
        while cursor <= end:
            month_end = _next_month_start(cursor) - timedelta(days=1)
            overlap_start = max(start, cursor)
            overlap_end = min(end, month_end)
            overlap_bdays = _business_days_in_range(overlap_start, overlap_end)
            month_bdays = max(_business_days_in_range(cursor, month_end), 1)
            goal += DEALS_CREATED_TARGET_PER_REP * (overlap_bdays / month_bdays)
            cursor = _next_month_start(cursor)
        return round(goal, 1)

    rows = []
    for row in data["rows"]:
        actual_deals = row["outbound_deals_created"]
        dial_to_deal_rate = (row["pct_deals"] or 0) / 100.0
        attainment_pct = round((row["avg_dials_per_day"] / target_avg_dials_per_day) * 100, 1)
        dial_gap_to_target = round(target_avg_dials_per_day - row["avg_dials_per_day"], 1)
        meets_projection_threshold = row["dials"] >= 300 and actual_deals >= 2
        projected_deals = (
            round(target_dials_per_rep * dial_to_deal_rate, 1)
            if meets_projection_threshold and dial_to_deal_rate > 0
            else None
        )
        additional_deals = round(max((projected_deals or 0) - actual_deals, 0), 1) if projected_deals is not None else None
        if row["dials"] >= 600 and actual_deals >= 4:
            projection_confidence = "High confidence"
        elif meets_projection_threshold:
            projection_confidence = "Medium confidence"
        else:
            projection_confidence = "Insufficient sample"
        rows.append({
            **row,
            "attainment_pct": attainment_pct,
            "dial_gap_to_target": dial_gap_to_target,
            "meets_projection_threshold": meets_projection_threshold,
            "projection_confidence": projection_confidence,
            "projected_deals_at_goal": projected_deals,
            "additional_deals_at_goal": additional_deals,
        })

    rows.sort(key=lambda row: (-row["dial_gap_to_target"], row["ae"]))

    totals = data["totals"]
    team_avg_dials_per_day = totals["avg_dials_per_day"]
    team_attainment_pct = round((team_avg_dials_per_day / target_avg_dials_per_day) * 100, 1) if rows else 0.0
    active_rep_count = len(rows)
    team_target_dials_per_day = target_avg_dials_per_day * active_rep_count
    team_actual_dials_per_day_total = round((totals["dials"] / business_days), 1) if rows else 0.0
    goal_end = end
    if period == "this_month":
        goal_end = _next_month_start(start) - timedelta(days=1)
    goal_business_days = max(_business_days_in_range(start, goal_end), 1)
    team_target_dials_for_period = goal_business_days * target_avg_dials_per_day * active_rep_count
    period_cold_outreach_goal_per_rep = (
        round(DEALS_CREATED_TARGET_PER_REP, 1)
        if period in {"this_month", "last_month"}
        else _period_cold_outreach_goal_per_rep()
    )
    team_cold_outreach_goal_for_period = round(period_cold_outreach_goal_per_rep * active_rep_count, 1)

    team_dial_to_conversation_rate = (totals["conversations"] / totals["dials"]) if totals["dials"] else 0.0
    conversation_to_cold_outreach_rate = (
        totals["outbound_deals_created"] / totals["conversations"]
        if totals["conversations"] else 0.0
    )
    estimated_conversations_at_target = round(team_target_dials_for_period * team_dial_to_conversation_rate, 1)
    estimated_cold_outreach_at_target = round(
        estimated_conversations_at_target * conversation_to_cold_outreach_rate, 1
    )
    total_projected_deals = estimated_cold_outreach_at_target
    total_additional_deals = round(
        max(estimated_cold_outreach_at_target - totals["outbound_deals_created"], 0),
        1,
    )
    team_dials_gap = max(team_target_dials_for_period - totals["dials"], 0)
    team_dials_attainment_pct = round((totals["dials"] / team_target_dials_for_period) * 100, 1) if team_target_dials_for_period else 0.0
    team_cold_outreach_attainment_pct = round(
        (totals["outbound_deals_created"] / team_cold_outreach_goal_for_period) * 100, 1
    ) if team_cold_outreach_goal_for_period else 0.0
    today = datetime.now(timezone.utc).date()
    pace_end = min(end, today)
    elapsed_business_days = _business_days_in_range(start, pace_end) if pace_end >= start else 0
    expected_dials_to_date = team_target_dials_per_day * elapsed_business_days
    expected_conversations_to_date = round(expected_dials_to_date * team_dial_to_conversation_rate)
    expected_cold_outreach_to_date = round(
        expected_conversations_to_date * conversation_to_cold_outreach_rate
    )
    dials_gap_to_expected = totals["dials"] - expected_dials_to_date
    conversations_gap_to_expected = totals["conversations"] - expected_conversations_to_date
    cold_outreach_gap_to_expected = totals["outbound_deals_created"] - expected_cold_outreach_to_date

    owners = apply_manual_owner_overrides(get_owners())
    scope_end = end_dt
    calls = get_calls(start_dt, end_dt)
    deals_created = get_deals(start_dt, end_dt, "createdate")
    contact_windows = get_deal_contact_windows()
    call_to_contact = get_call_to_contact_map([c["id"] for c in calls])
    daily_dials, daily_conversations = _call_daily_series(
        calls, owners, scope_end, contact_windows, call_to_contact
    )
    daily_cold_outreach = _deal_daily_series(deals_created, owners, scope_end)

    historical_month_start = _shift_month(_month_start(start), -6)
    historical_month_end = _month_end(_shift_month(_month_start(start), -1))
    historical_dial_share_by_index = {}
    historical_cold_share_by_index = {}
    historical_dial_share_by_pattern = {}
    historical_cold_share_by_pattern = {}
    if historical_month_end >= historical_month_start:
        historical_start_dt = datetime.combine(historical_month_start, datetime.min.time(), tzinfo=timezone.utc)
        historical_end_dt = datetime.combine(historical_month_end, datetime.max.time(), tzinfo=timezone.utc)
        historical_calls = get_calls(historical_start_dt, historical_end_dt)
        historical_deals = get_deals(historical_start_dt, historical_end_dt, "createdate")
        historical_call_to_contact = get_call_to_contact_map([c["id"] for c in historical_calls])
        hist_dials, hist_conversations = _call_daily_series(
            historical_calls, owners, historical_end_dt, contact_windows, historical_call_to_contact
        )
        hist_cold = _deal_daily_series(historical_deals, owners, historical_end_dt)

        dial_share_samples_by_index = defaultdict(list)
        cold_share_samples_by_index = defaultdict(list)
        dial_share_samples_by_pattern = defaultdict(list)
        cold_share_samples_by_pattern = defaultdict(list)
        cursor = historical_month_start
        while cursor <= historical_month_end:
            month_start = _month_start(cursor)
            month_end = _month_end(cursor)
            month_holidays = _holiday_map_between(month_start, month_end)
            month_workdays = [
                month_start + timedelta(days=i)
                for i in range((month_end - month_start).days + 1)
                if _is_working_day(month_start + timedelta(days=i), month_holidays)
            ]
            month_dial_total = sum(hist_dials.get(day.isoformat(), 0) for day in month_workdays)
            month_cold_total = sum(hist_cold.get(day.isoformat(), 0) for day in month_workdays)
            for workday_index, current_workday in enumerate(month_workdays, start=1):
                day_key = current_workday.isoformat()
                pattern_key = (((current_workday.day - 1) // 7) + 1, current_workday.weekday())
                if month_dial_total:
                    dial_share = hist_dials.get(day_key, 0) / month_dial_total
                    dial_share_samples_by_index[workday_index].append(dial_share)
                    dial_share_samples_by_pattern[pattern_key].append(dial_share)
                if month_cold_total:
                    cold_share = hist_cold.get(day_key, 0) / month_cold_total
                    cold_share_samples_by_index[workday_index].append(cold_share)
                    cold_share_samples_by_pattern[pattern_key].append(cold_share)
            cursor = _shift_month(cursor, 1)

        historical_dial_share_by_index = {
            index: median(values) for index, values in dial_share_samples_by_index.items() if values
        }
        historical_cold_share_by_index = {
            index: median(values) for index, values in cold_share_samples_by_index.items() if values
        }
        historical_dial_share_by_pattern = {
            key: median(values) for key, values in dial_share_samples_by_pattern.items() if values
        }
        historical_cold_share_by_pattern = {
            key: median(values) for key, values in cold_share_samples_by_pattern.items() if values
        }

    current_period_workdays = []
    cursor_day = start
    trend_end = goal_end if period == "this_month" else end
    while cursor_day <= trend_end:
        if _is_working_day(cursor_day, holiday_map):
            current_period_workdays.append(cursor_day)
        cursor_day += timedelta(days=1)

    dial_typical_daily_shares = []
    cold_typical_daily_shares = []
    for workday_index, current_workday in enumerate(current_period_workdays, start=1):
        pattern_key = (((current_workday.day - 1) // 7) + 1, current_workday.weekday())
        dial_typical_daily_shares.append(
            historical_dial_share_by_pattern.get(
                pattern_key,
                historical_dial_share_by_index.get(workday_index, 0.0),
            )
        )
        cold_typical_daily_shares.append(
            historical_cold_share_by_pattern.get(
                pattern_key,
                historical_cold_share_by_index.get(workday_index, 0.0),
            )
        )

    dial_share_total = sum(dial_typical_daily_shares)
    if dial_share_total > 0:
        dial_typical_daily_shares = [share / dial_share_total for share in dial_typical_daily_shares]
    cold_share_total = sum(cold_typical_daily_shares)
    if cold_share_total > 0:
        cold_typical_daily_shares = [share / cold_share_total for share in cold_typical_daily_shares]

    trend_points = []
    cumulative_dials = 0
    cumulative_target_dials = 0
    cumulative_cold_outreach = 0
    cumulative_goal_cold_outreach = 0.0
    cold_outreach_goal_per_business_day = (
        team_cold_outreach_goal_for_period / goal_business_days if goal_business_days else 0.0
    )
    current_day = start
    trend_end = goal_end if period == "this_month" else end
    working_day_index = 0
    cumulative_typical_dials = 0.0
    cumulative_typical_cold = 0.0
    while current_day <= trend_end:
        day_key = current_day.isoformat()
        label = f"{current_day.month}/{current_day.day}"
        holiday_label = holiday_map.get(current_day)
        is_business_day = _is_working_day(current_day, holiday_map)
        is_future = period == "this_month" and current_day > today
        cumulative_dials += daily_dials.get(day_key, 0)
        cumulative_cold_outreach += daily_cold_outreach.get(day_key, 0)
        if is_business_day:
            working_day_index += 1
            cumulative_target_dials += team_target_dials_per_day
            cumulative_goal_cold_outreach += cold_outreach_goal_per_business_day
            if working_day_index <= len(dial_typical_daily_shares):
                cumulative_typical_dials += dial_typical_daily_shares[working_day_index - 1] * team_target_dials_for_period
            if working_day_index <= len(cold_typical_daily_shares):
                cumulative_typical_cold += cold_typical_daily_shares[working_day_index - 1] * team_cold_outreach_goal_for_period
        dial_goal_pct = round((cumulative_target_dials / team_target_dials_for_period) * 100, 1) if team_target_dials_for_period else 0.0
        dial_actual_pct = round((cumulative_dials / team_target_dials_for_period) * 100, 1) if team_target_dials_for_period else 0.0
        cold_outreach_goal_pct = round((cumulative_goal_cold_outreach / team_cold_outreach_goal_for_period) * 100, 1) if team_cold_outreach_goal_for_period else 0.0
        cold_outreach_actual_pct = round((cumulative_cold_outreach / team_cold_outreach_goal_for_period) * 100, 1) if team_cold_outreach_goal_for_period else 0.0
        dial_typical_pct = round((cumulative_typical_dials / team_target_dials_for_period) * 100, 1) if team_target_dials_for_period else None
        cold_outreach_typical_pct = round((cumulative_typical_cold / team_cold_outreach_goal_for_period) * 100, 1) if team_cold_outreach_goal_for_period else None
        trend_points.append({
            "label": label,
            "date": day_key,
            "holiday_name": holiday_label,
            "is_holiday": bool(holiday_label),
            "is_future": is_future,
            "dial_goal_raw": cumulative_target_dials,
            "dial_actual_raw": cumulative_dials,
            "dial_typical_raw": round(cumulative_typical_dials),
            "cold_outreach_goal_raw": round(cumulative_goal_cold_outreach, 1),
            "cold_outreach_actual_raw": cumulative_cold_outreach,
            "cold_outreach_typical_raw": round(cumulative_typical_cold),
            "dial_goal_pct": dial_goal_pct,
            "dial_actual_pct": dial_actual_pct,
            "dial_typical_pct": dial_typical_pct,
            "cold_outreach_goal_pct": cold_outreach_goal_pct,
            "cold_outreach_actual_pct": cold_outreach_actual_pct,
            "cold_outreach_typical_pct": cold_outreach_typical_pct,
        })
        current_day += timedelta(days=1)

    return {
        "rows": rows,
        "period": period,
        "start": data["start"],
        "end": data["end"],
        "business_days": business_days,
        "target_avg_dials_per_day": target_avg_dials_per_day,
        "target_dials_per_rep": target_dials_per_rep,
        "trend_points": trend_points,
        "totals": {
            **totals,
            "team_avg_dials_per_day": team_avg_dials_per_day,
            "team_attainment_pct": team_attainment_pct,
            "active_rep_count": active_rep_count,
            "team_target_dials_per_day": team_target_dials_per_day,
            "team_target_dials_for_period": team_target_dials_for_period,
            "team_actual_dials_per_day_total": team_actual_dials_per_day_total,
            "team_dials_gap": team_dials_gap,
            "team_dials_attainment_pct": team_dials_attainment_pct,
            "period_cold_outreach_goal_per_rep": period_cold_outreach_goal_per_rep,
            "team_cold_outreach_goal_for_period": team_cold_outreach_goal_for_period,
            "team_cold_outreach_attainment_pct": team_cold_outreach_attainment_pct,
            "team_dial_to_conversation_rate": round(team_dial_to_conversation_rate * 100, 1),
            "conversation_to_cold_outreach_rate": round(conversation_to_cold_outreach_rate * 100, 1),
            "elapsed_business_days": elapsed_business_days,
            "expected_dials_to_date": expected_dials_to_date,
            "expected_conversations_to_date": expected_conversations_to_date,
            "expected_cold_outreach_to_date": expected_cold_outreach_to_date,
            "dials_gap_to_expected": dials_gap_to_expected,
            "conversations_gap_to_expected": conversations_gap_to_expected,
            "cold_outreach_gap_to_expected": cold_outreach_gap_to_expected,
            "estimated_conversations_at_target": estimated_conversations_at_target,
            "estimated_cold_outreach_at_target": estimated_cold_outreach_at_target,
            "total_projected_deals": total_projected_deals,
            "total_additional_deals": total_additional_deals,
        },
    }



@ttl_cache
def compute_pipeline_generated(period: str) -> dict:
    start, end = get_date_range(period)
    owners = apply_manual_owner_overrides(get_owners())
    scope_end = end
    deals = get_deals(start, end, "createdate")

    owner_data = defaultdict(lambda: {
        "cold_outreach_amt": 0.0, "cold_outreach_n": 0,
        "inbound_amt": 0.0, "inbound_n": 0,
        "conference_amt": 0.0, "conference_n": 0,
        "referral_amt": 0.0, "referral_n": 0,
    })

    for d in deals:
        oid = d["properties"].get("hubspot_owner_id", "")
        if not oid:
            continue
        if not _owner_allowed(oid, scope_end):
            continue
        amount = _parse_amount(d["properties"].get("amount"))
        src = _deal_source(d)
        raw_src = (d["properties"].get("hs_analytics_source") or "").upper()
        if src == "Cold outreach":
            owner_data[oid]["cold_outreach_amt"] += amount
            owner_data[oid]["cold_outreach_n"] += 1
        elif src == "Inbound" or raw_src in ("PAID_SEARCH", "ORGANIC_SEARCH", "SOCIAL_MEDIA", "PAID_SOCIAL", "DIRECT_TRAFFIC", "EMAIL_MARKETING", "OFFLINE"):
            owner_data[oid]["inbound_amt"] += amount
            owner_data[oid]["inbound_n"] += 1
        elif src == "Referral" or raw_src == "REFERRALS":
            owner_data[oid]["referral_amt"] += amount
            owner_data[oid]["referral_n"] += 1
        elif src == "Conference":
            owner_data[oid]["conference_amt"] += amount
            owner_data[oid]["conference_n"] += 1
        else:
            owner_data[oid]["cold_outreach_amt"] += amount
            owner_data[oid]["cold_outreach_n"] += 1

    rows = []
    for oid, data in owner_data.items():
        owner = owners.get(oid)
        if not owner:
            continue
        total_n = data["cold_outreach_n"] + data["inbound_n"] + data["conference_n"] + data["referral_n"]
        total_amt = data["cold_outreach_amt"] + data["inbound_amt"] + data["conference_amt"] + data["referral_amt"]
        total_acv = total_amt / total_n if total_n else 0
        rows.append({
            "ae": owner["last_name"] or owner["name"],
            "owner_id": oid,
            "cold_outreach_amt": data["cold_outreach_amt"],
            "cold_outreach_n": data["cold_outreach_n"],
            "cold_outreach_acv": data["cold_outreach_amt"] / data["cold_outreach_n"] if data["cold_outreach_n"] else 0,
            "inbound_amt": data["inbound_amt"],
            "inbound_n": data["inbound_n"],
            "inbound_acv": data["inbound_amt"] / data["inbound_n"] if data["inbound_n"] else 0,
            "conference_amt": data["conference_amt"],
            "conference_n": data["conference_n"],
            "referral_amt": data["referral_amt"],
            "referral_n": data["referral_n"],
            "total_acv": total_acv,
            "total_amt": total_amt,
            "total_n": total_n,
        })

    rows.sort(key=lambda r: r["total_amt"], reverse=True)

    def _sum(key):
        return sum(r[key] for r in rows)

    tot_n = _sum("total_n")
    totals = {
        "ae": "TOTAL",
        "cold_outreach_amt": _sum("cold_outreach_amt"),
        "cold_outreach_n": _sum("cold_outreach_n"),
        "inbound_amt": _sum("inbound_amt"),
        "inbound_n": _sum("inbound_n"),
        "conference_amt": _sum("conference_amt"),
        "conference_n": _sum("conference_n"),
        "referral_amt": _sum("referral_amt"),
        "referral_n": _sum("referral_n"),
        "total_acv": _sum("total_amt") / tot_n if tot_n else 0,
        "total_amt": _sum("total_amt"),
        "total_n": tot_n,
    }

    return {"rows": rows, "totals": totals, "period": period}


def _coverage_end(period: str, start: datetime, end: datetime) -> datetime:
    """Return the true upper boundary of a period for the open-deals coverage query.

    For current-period views (this_month, this_quarter, ytd), get_date_range
    returns end=now, which silently drops open deals with expected close dates
    later in the period (e.g. March 18-31 when today is March 17).
    This helper extends end to the actual period boundary so the full
    pipeline is visible. Historical periods already have a correct end date.
    """
    if period == "this_month":
        if start.month == 12:
            return start.replace(year=start.year + 1, month=1, day=1,
                                 hour=0, minute=0, second=0, microsecond=0) - timedelta(seconds=1)
        return start.replace(month=start.month + 1, day=1,
                             hour=0, minute=0, second=0, microsecond=0) - timedelta(seconds=1)
    if period == "this_quarter":
        q_month = ((start.month - 1) // 3) * 3 + 1
        next_q_month = q_month + 3
        if next_q_month > 12:
            return start.replace(year=start.year + 1, month=next_q_month - 12, day=1,
                                 hour=0, minute=0, second=0, microsecond=0) - timedelta(seconds=1)
        return start.replace(month=next_q_month, day=1,
                             hour=0, minute=0, second=0, microsecond=0) - timedelta(seconds=1)
    if period == "ytd":
        return start.replace(month=12, day=31, hour=23, minute=59, second=59, microsecond=0)
    return end  # historical / next_month periods already have the correct boundary


def _quota_window(period: str, start: datetime, end: datetime) -> tuple[datetime, datetime]:
    """Return the correct quota target window for a selected period.

    Current-period views should compare performance against the full target
    period, not just the elapsed slice through "now". Example: on April 1,
    "This Month" should still use the full April quota, not one day's prorated
    fraction of it.
    """
    if period in ("this_month", "this_quarter", "ytd"):
        return start, _coverage_end(period, start, end)
    return start, end


@ttl_cache
def compute_pipeline_coverage(period: str = None) -> dict:
    owners = apply_manual_owner_overrides(get_owners())
    if period:
        start, end = get_date_range(period)
        scope_end = _coverage_end(period, start, end)
        # Use the true period boundary for the open-deals query so deals with
        # expected close dates later in the period (e.g. March 18-31) are included.
        open_deals = get_all_open_deals(start, scope_end)
        # Won deals are excluded by get_all_open_deals — fetch separately via closedate.
        # get_deals(…, "closedate") is already cached so no extra API call.
        closed_deals = get_deals(start, end, "closedate")
    else:
        scope_end = datetime.now(timezone.utc)
        open_deals = get_all_open_deals()
        closed_deals = []

    STAGE_ORDER = [NB_STAGES["stage1"], NB_STAGES["stage2"], NB_STAGES["stage3"], NB_STAGES["stage4"]]

    owner_data = defaultdict(lambda: {s: {"n": 0, "amt": 0.0} for s in STAGE_ORDER})
    owner_won = defaultdict(lambda: {"n": 0, "amt": 0.0})

    for d in open_deals:
        oid = d["properties"].get("hubspot_owner_id", "")
        stage = d["properties"].get("dealstage", "")
        amount = _parse_amount(d["properties"].get("amount"))
        if not oid:
            continue
        if not _owner_allowed(oid, scope_end):
            continue
        if stage in owner_data[oid]:
            owner_data[oid][stage]["n"] += 1
            owner_data[oid][stage]["amt"] += amount

    for d in closed_deals:
        if d["properties"].get("hs_is_closed_won") != "true":
            continue
        oid = d["properties"].get("hubspot_owner_id", "")
        amount = _parse_amount(d["properties"].get("amount"))
        if not oid:
            continue
        if not _owner_allowed(oid, scope_end):
            continue
        owner_won[oid]["n"] += 1
        owner_won[oid]["amt"] += amount

    rows = []
    active = set(owner_data.keys()) | set(owner_won.keys())
    for oid in active:
        owner = owners.get(oid)
        if not owner:
            continue
        rows.append({
            "ae": owner["last_name"] or owner["name"],
            "owner_id": oid,
            "s1_n": owner_data[oid][NB_STAGES["stage1"]]["n"],
            "s1_amt": owner_data[oid][NB_STAGES["stage1"]]["amt"],
            "s2_n": owner_data[oid][NB_STAGES["stage2"]]["n"],
            "s2_amt": owner_data[oid][NB_STAGES["stage2"]]["amt"],
            "s3_n": owner_data[oid][NB_STAGES["stage3"]]["n"],
            "s3_amt": owner_data[oid][NB_STAGES["stage3"]]["amt"],
            "s4_n": owner_data[oid][NB_STAGES["stage4"]]["n"],
            "s4_amt": owner_data[oid][NB_STAGES["stage4"]]["amt"],
            "won_n": owner_won[oid]["n"],
            "won_amt": owner_won[oid]["amt"],
        })

    rows.sort(key=lambda r: r["s1_n"] + r["s2_n"] + r["s3_n"] + r["s4_n"], reverse=True)

    def _sum(key):
        return sum(r[key] for r in rows)

    totals = {k: _sum(k) for k in ["s1_n", "s1_amt", "s2_n", "s2_amt", "s3_n", "s3_amt", "s4_n", "s4_amt", "won_n", "won_amt"]}
    totals["ae"] = "TOTAL"

    return {"rows": rows, "totals": totals, "period": period}


@ttl_cache
def compute_deal_advancement(period: str, source: str = "All") -> dict:
    start, end = get_date_range(period)
    owners = apply_manual_owner_overrides(get_owners())
    scope_end = end

    # Cohort view: all deals created in the period, showing their CURRENT stage.
    # Uses dealstage (always populated) so this works on any HubSpot plan —
    # no dependency on hs_date_entered_* which requires Professional/Enterprise.
    # For meaningful advancement numbers select a period ≥ 90 days so deals
    # have had time to progress beyond Stage 1.
    deals = get_deals(start, end, "createdate")
    if source != "All":
        deals = [d for d in deals if _deal_source(d) == source]

    _s2   = NB_STAGES["stage2"]
    _s3   = NB_STAGES["stage3"]
    _s4   = NB_STAGES["stage4"]
    _won  = NB_STAGES["won"]
    _lost = NB_STAGES["lost"]

    owner_data = defaultdict(lambda: {
        "created": 0,
        "to_s2": 0, "to_s3": 0, "to_s4": 0, "won": 0, "lost": 0,
    })

    for d in deals:
        oid   = d["properties"].get("hubspot_owner_id", "")
        stage = d["properties"].get("dealstage", "")
        if not oid:
            continue
        if not _owner_allowed(oid, scope_end):
            continue
        owner_data[oid]["created"] += 1
        # A deal at stage X has progressed through all earlier stages.
        # Won deals count for every stage column; lost deals only count as lost
        # (we don't know which stage they were lost from).
        if stage in (_s2, _s3, _s4, _won):
            owner_data[oid]["to_s2"] += 1
        if stage in (_s3, _s4, _won):
            owner_data[oid]["to_s3"] += 1
        if stage in (_s4, _won):
            owner_data[oid]["to_s4"] += 1
        if stage == _won:
            owner_data[oid]["won"] += 1
        if stage == _lost:
            owner_data[oid]["lost"] += 1

    rows = []
    for oid, data in owner_data.items():
        owner = owners.get(oid)
        if not owner or data["created"] == 0:
            continue
        rows.append({
            "ae": owner["last_name"] or owner["name"],
            "owner_id": oid,
            "created": data["created"],
            "to_s2":   data["to_s2"],
            "to_s3":   data["to_s3"],
            "to_s4":   data["to_s4"],
            "won":     data["won"],
            "lost":    data["lost"],
        })

    rows.sort(key=lambda r: r["created"], reverse=True)

    def _sum(key):
        return sum(r[key] for r in rows)

    totals = {
        "ae": "TOTAL",
        "created": _sum("created"),
        "to_s2":   _sum("to_s2"),
        "to_s3":   _sum("to_s3"),
        "to_s4":   _sum("to_s4"),
        "won":     _sum("won"),
        "lost":    _sum("lost"),
    }

    return {"rows": rows, "totals": totals, "period": period, "source": source}


@ttl_cache
def compute_deals_won(period: str, source: str = "All") -> dict:
    start, end = get_date_range(period)
    quota_start, quota_end = _quota_window(period, start, end)
    owners = apply_manual_owner_overrides(get_owners())
    scope_end = end
    quotas = get_quotas(quota_start, quota_end)  # full target window for current-period views

    won_deals = get_deals(start, end, "closedate")
    won_deals = [d for d in won_deals if d["properties"].get("hs_is_closed_won") == "true"]

    lost_deals = get_deals(start, end, "closedate")
    lost_deals = [d for d in lost_deals if d["properties"].get("hs_is_closed_lost") == "true"]

    if source != "All":
        won_deals = [d for d in won_deals if _deal_source(d) == source]
        lost_deals = [d for d in lost_deals if _deal_source(d) == source]

    owner_won = defaultdict(lambda: {"cold_amt": 0.0, "cold_n": 0, "inbound_amt": 0.0, "inbound_n": 0, "conf_amt": 0.0, "conf_n": 0, "ref_amt": 0.0, "ref_n": 0, "total_amt": 0.0, "total_n": 0, "days_to_close_sum": 0.0, "days_to_close_n": 0})
    owner_lost = defaultdict(int)

    for d in won_deals:
        oid = d["properties"].get("hubspot_owner_id", "")
        if not oid:
            continue
        if not _owner_allowed(oid, scope_end):
            continue
        amount = _parse_amount(d["properties"].get("amount"))
        src = _deal_source(d)
        owner_won[oid]["total_amt"] += amount
        owner_won[oid]["total_n"] += 1
        dtc = d["properties"].get("days_to_close")
        if dtc is not None:
            try:
                owner_won[oid]["days_to_close_sum"] += float(dtc)
                owner_won[oid]["days_to_close_n"] += 1
            except (ValueError, TypeError):
                pass
        if src == "Cold outreach":
            owner_won[oid]["cold_amt"] += amount
            owner_won[oid]["cold_n"] += 1
        elif src == "Inbound":
            owner_won[oid]["inbound_amt"] += amount
            owner_won[oid]["inbound_n"] += 1
        elif src == "Conference":
            owner_won[oid]["conf_amt"] += amount
            owner_won[oid]["conf_n"] += 1
        elif src == "Referral":
            owner_won[oid]["ref_amt"] += amount
            owner_won[oid]["ref_n"] += 1

    for d in lost_deals:
        oid = d["properties"].get("hubspot_owner_id", "")
        if oid and _owner_allowed(oid, scope_end):
            owner_lost[oid] += 1

    all_owners = set(owner_won.keys()) | set(owner_lost.keys())
    rows = []
    for oid in all_owners:
        owner = owners.get(oid)
        if not owner:
            continue
        won_n = owner_won[oid]["total_n"]
        lost_n = owner_lost[oid]
        total = won_n + lost_n
        quota_amt = quotas.get(oid, 0.0)
        total_won = owner_won[oid]["total_amt"]
        rows.append({
            "ae": owner["last_name"] or owner["name"],
            "owner_id": oid,
            "cold_amt": owner_won[oid]["cold_amt"],
            "cold_n": owner_won[oid]["cold_n"],
            "inbound_amt": owner_won[oid]["inbound_amt"],
            "inbound_n": owner_won[oid]["inbound_n"],
            "conf_amt": owner_won[oid]["conf_amt"],
            "conf_n": owner_won[oid]["conf_n"],
            "ref_amt": owner_won[oid]["ref_amt"],
            "ref_n": owner_won[oid]["ref_n"],
            "total_won_amt": total_won,
            "total_won_n": won_n,
            "total_lost_n": lost_n,
            "acv": total_won / won_n if won_n else 0,
            "win_rate": _pct(won_n, total),
            "quota_amt":  quota_amt,
            "delta_amt":  total_won - quota_amt,
            "attain_pct": round(total_won / quota_amt * 100, 1) if quota_amt else None,
            "days_to_close_sum": owner_won[oid]["days_to_close_sum"],
            "days_to_close_n":   owner_won[oid]["days_to_close_n"],
            "avg_days_to_close": round(owner_won[oid]["days_to_close_sum"] / owner_won[oid]["days_to_close_n"]) if owner_won[oid]["days_to_close_n"] else None,
        })

    rows.sort(key=lambda r: r["total_won_amt"], reverse=True)

    def _sum(key):
        return sum(r[key] for r in rows)

    tw = _sum("total_won_n")
    tl = _sum("total_lost_n")
    total_won_rev = _sum("total_won_amt")
    total_quota   = _sum("quota_amt")
    tot_dtc_sum = _sum("days_to_close_sum")
    tot_dtc_n   = _sum("days_to_close_n")
    totals = {
        "ae": "TOTAL",
        "cold_amt": _sum("cold_amt"), "cold_n": _sum("cold_n"),
        "inbound_amt": _sum("inbound_amt"), "inbound_n": _sum("inbound_n"),
        "conf_amt": _sum("conf_amt"), "conf_n": _sum("conf_n"),
        "ref_amt": _sum("ref_amt"), "ref_n": _sum("ref_n"),
        "total_won_amt": total_won_rev,
        "total_won_n": tw, "total_lost_n": tl,
        "acv": total_won_rev / tw if tw else 0,
        "win_rate": _pct(tw, tw + tl),
        "quota_amt":  total_quota,
        "delta_amt":  total_won_rev - total_quota,
        "attain_pct": round(total_won_rev / total_quota * 100, 1) if total_quota else None,
        "days_to_close_sum": tot_dtc_sum,
        "days_to_close_n":   tot_dtc_n,
        "avg_days_to_close": round(tot_dtc_sum / tot_dtc_n) if tot_dtc_n else None,
    }

    return {"rows": rows, "totals": totals, "period": period, "source": source}


@ttl_cache
def compute_forecast(period: str) -> dict:
    start, end = get_date_range(period)
    quota_start, quota_end = _quota_window(period, start, end)
    owners = apply_manual_owner_overrides(get_owners())
    quotas = get_quotas(quota_start, quota_end)

    won_deals = get_deals(start, end, "closedate")
    won_deals = [d for d in won_deals if d["properties"].get("hs_is_closed_won") == "true"]

    open_deals = get_all_open_deals(start, end)

    # Map HubSpot user_id → owner_id (needed to resolve forecast submissions)
    user_id_to_owner_id = {
        v["user_id"]: v["id"] for v in owners.values() if v.get("user_id")
    }

    # Fetch HubSpot forecast submissions and pick the most-recent one per rep.
    # The beta API returns an object per submission; we take the latest by
    # hs_createdate for each user.  Amount field name is not yet confirmed in
    # public docs, so we try the most likely candidates in order.
    _AMOUNT_FIELDS = (
        "hs_forecasted_amount",
        "hs_amount",
        "hs_submission_amount",
        "hs_target_amount",
    )
    owner_submitted: dict[str, float] = {}
    raw_submissions = get_forecast_submissions()
    # Group by user, keep latest
    latest_by_user: dict[str, dict] = {}
    for sub in raw_submissions:
        props = sub.get("properties", {})
        uid = props.get("hs_created_by_user_id") or ""
        if not uid:
            continue
        created = props.get("hs_createdate") or ""
        prev = latest_by_user.get(uid)
        if prev is None or created > (prev.get("properties", {}).get("hs_createdate") or ""):
            latest_by_user[uid] = sub
    for uid, sub in latest_by_user.items():
        props = sub.get("properties", {})
        amt = 0.0
        for field in _AMOUNT_FIELDS:
            raw = props.get(field)
            if raw not in (None, "", "0", 0):
                try:
                    amt = float(raw)
                    break
                except (ValueError, TypeError):
                    pass
        oid = user_id_to_owner_id.get(uid)
        if oid:
            owner_submitted[oid] = amt

    # Index won by owner
    owner_won = defaultdict(float)
    for d in won_deals:
        oid = d["properties"].get("hubspot_owner_id") or ""
        owner_won[oid] += float(d["properties"].get("amount") or 0)

    # Index open deals by owner — commit, best case, weighted
    owner_commit   = defaultdict(float)
    owner_bestcase = defaultdict(float)
    owner_weighted = defaultdict(float)
    owner_commit_n   = defaultdict(int)
    owner_bestcase_n = defaultdict(int)

    for d in open_deals:
        props = d["properties"]
        oid = props.get("hubspot_owner_id") or ""
        amt  = float(props.get("amount") or 0)
        prob = float(props.get("hs_deal_stage_probability") or 0)
        cat  = (props.get("hs_manual_forecast_category") or "").lower()

        owner_bestcase[oid] += amt
        owner_bestcase_n[oid] += 1
        owner_weighted[oid] += amt * prob

        if cat == "commit":
            owner_commit[oid] += amt
            owner_commit_n[oid] += 1

    owner_team = get_owner_team_map()  # {owner_id: "Rising" | "Veterans"}

    rows = []
    for oid, owner in owners.items():
        if not _owner_allowed(oid):
            continue
        won_amt      = owner_won.get(oid, 0.0)
        commit_amt   = owner_commit.get(oid, 0.0)
        commit_n     = owner_commit_n.get(oid, 0)
        bestcase_amt = owner_bestcase.get(oid, 0.0)
        bestcase_n   = owner_bestcase_n.get(oid, 0)
        weighted_amt = owner_weighted.get(oid, 0.0)
        submitted_amt = owner_submitted.get(oid)   # None = not submitted
        quota_amt    = quotas.get(oid, 0.0)
        gap_amt      = (quota_amt - won_amt) if quota_amt else None
        attain_pct   = round(won_amt / quota_amt * 100, 1) if quota_amt else None

        rows.append({
            "ae":             owner["last_name"] or owner["name"],
            "team":           owner_team.get(oid, ""),
            "won_amt":        won_amt,
            "commit_amt":     commit_amt,
            "commit_n":       commit_n,
            "submitted_amt":  submitted_amt,
            "bestcase_amt":   bestcase_amt,
            "bestcase_n":     bestcase_n,
            "weighted_amt":   weighted_amt,
            "quota_amt":      quota_amt,
            "gap_amt":        gap_amt,
            "attain_pct":     attain_pct,
        })

    rows.sort(key=lambda r: (r["team"], -(r["submitted_amt"] or 0)))

    def _s(k, src): return sum(r[k] for r in src if r[k] is not None)

    def _subtotal(label, src):
        sub_submitted = _s("submitted_amt", src)
        sub_quota     = _s("quota_amt", src)
        return {
            "ae":             label,
            "won_amt":        _s("won_amt", src),
            "commit_amt":     _s("commit_amt", src),
            "commit_n":       _s("commit_n", src),
            "submitted_amt":  sub_submitted,
            "bestcase_amt":   _s("bestcase_amt", src),
            "bestcase_n":     _s("bestcase_n", src),
            "weighted_amt":   _s("weighted_amt", src),
            "quota_amt":      sub_quota,
            "gap_amt":        (sub_quota - _s("won_amt", src)) if sub_quota else None,
            "attain_pct":     round(_s("won_amt", src) / sub_quota * 100, 1) if sub_quota else None,
        }

    # Build team groups (preserve TEAM_FILTER order)
    from hubspot import TEAM_FILTER
    groups = []
    for team_name in TEAM_FILTER:
        team_rows = [r for r in rows if r["team"] == team_name]
        if not team_rows:
            continue
        groups.append({
            "team":    team_name,
            "manager": TEAM_MANAGER.get(team_name, team_name),
            "rows":    team_rows,
            "subtotal": _subtotal(f"{team_name} Total", team_rows),
        })

    total_submitted = _s("submitted_amt", rows)
    total_quota     = _s("quota_amt", rows)
    totals = {
        "ae":             "TOTAL",
        "won_amt":        _s("won_amt", rows),
        "commit_amt":     _s("commit_amt", rows),
        "commit_n":       _s("commit_n", rows),
        "submitted_amt":  total_submitted,
        "bestcase_amt":   _s("bestcase_amt", rows),
        "bestcase_n":     _s("bestcase_n", rows),
        "weighted_amt":   _s("weighted_amt", rows),
        "quota_amt":      total_quota,
        "gap_amt":        (total_quota - _s("won_amt", rows)) if total_quota else None,
        "attain_pct":     round(_s("won_amt", rows) / total_quota * 100, 1) if total_quota else None,
    }

    return {"rows": rows, "groups": groups, "totals": totals, "period": period}


@ttl_cache
def compute_deals_lost(period: str) -> dict:
    start, end = get_date_range(period)
    owners = apply_manual_owner_overrides(get_owners())
    scope_end = end

    lost_deals = get_deals(start, end, "hs_v2_date_entered_71300363")

    REASONS = ["Cost", "Never Demo'ed", "Timeline", "Stakeholder Issue", "Competitor", "Product", "Other", "Value"]

    owner_data = defaultdict(lambda: {r: 0 for r in REASONS} | {"total": 0})

    for d in lost_deals:
        oid = d["properties"].get("hubspot_owner_id", "")
        if not oid:
            continue
        if not _owner_allowed(oid, scope_end):
            continue
        reason = d["properties"].get("closed_lost_reason") or "Other"
        matched = next((r for r in REASONS if r.lower() in reason.lower()), "Other")
        owner_data[oid][matched] += 1
        owner_data[oid]["total"] += 1

    rows = []
    for oid, data in owner_data.items():
        owner = owners.get(oid)
        if not owner or data["total"] == 0:
            continue
        row = {"ae": owner["last_name"] or owner["name"], "owner_id": oid, "total": data["total"]}
        for r in REASONS:
            row[r.lower().replace(" ", "_").replace("'", "")] = data[r]
        rows.append(row)

    rows.sort(key=lambda r: r["total"], reverse=True)

    keys = [r.lower().replace(" ", "_").replace("'", "") for r in REASONS]
    totals = {"ae": "TOTAL", "total": sum(r["total"] for r in rows)}
    for k in keys:
        totals[k] = sum(r.get(k, 0) for r in rows)

    return {"rows": rows, "totals": totals, "period": period, "reasons": REASONS}


@ttl_cache
def compute_inbound_funnel(period: str, size: str = "All Sizes") -> dict:
    start, end = get_date_range(period)

    # Leads = Demo Requests from list 1082 only
    contacts = get_list_contacts(1082, start, end)

    # Deals = Inbound deal source only, grouped by last_touch_channel
    all_deals = get_deals(start, end, "createdate")
    inbound_deals = [d for d in all_deals if (d["properties"].get("deal_source") or "").lower() == "inbound"]
    won_deals = [d for d in inbound_deals if d["properties"].get("hs_is_closed_won") == "true"]
    lost_deals = [d for d in inbound_deals if d["properties"].get("hs_is_closed_lost") == "true"]

    src_data = defaultdict(lambda: {
        "leads_created": 0,
        "leads_disqualified": 0,
        "leads_contacted": 0,
        "deals_created": 0,
        "deals_lost": 0,
        "deals_won": 0,
        "pg_amt": 0.0,
        "won_amt": 0.0,
        "lost_amt": 0.0,
    })

    CHANNEL_LABELS = {
        "PAID_SEARCH": "Paid Search",
        "ORGANIC_SEARCH": "Organic Search",
        "SOCIAL_MEDIA": "Organic Social",
        "PAID_SOCIAL": "Paid Social",
        "DIRECT_TRAFFIC": "Direct Traffic",
        "EMAIL_MARKETING": "Email Marketing",
        "OFFLINE": "Offline Sources",
        "REFERRALS": "Referrals",
    }

    def _contact_channel(props):
        raw = (props.get("utm_source") or "").strip().upper()
        return CHANNEL_LABELS.get(raw, raw) if raw else "Unknown"

    def _deal_channel(props):
        raw = (props.get("last_touch_channel") or "").strip().upper()
        return CHANNEL_LABELS.get(raw, raw) if raw else "Unknown"

    for c in contacts:
        src = _contact_channel(c["properties"])
        src_data[src]["leads_created"] += 1
        is_dq = (c["properties"].get("lifecyclestage") or "") == "184059525"
        if is_dq:
            src_data[src]["leads_disqualified"] += 1
        elif c["properties"].get("first_sales_activity_after_demo_request"):
            src_data[src]["leads_contacted"] += 1

    for d in inbound_deals:
        src = _deal_channel(d["properties"])
        amount = _parse_amount(d["properties"].get("amount"))
        src_data[src]["deals_created"] += 1
        src_data[src]["pg_amt"] += amount

    for d in won_deals:
        src = _deal_channel(d["properties"])
        amount = _parse_amount(d["properties"].get("amount"))
        src_data[src]["deals_won"] += 1
        src_data[src]["won_amt"] += amount

    for d in lost_deals:
        src = _deal_channel(d["properties"])
        amount = _parse_amount(d["properties"].get("amount"))
        src_data[src]["deals_lost"] += 1
        src_data[src]["lost_amt"] += amount

    rows = []
    for src, data in src_data.items():
        lc = data["leads_created"]
        dc = data["deals_created"]
        dw = data["deals_won"]
        qualified = lc - data["leads_disqualified"]
        rows.append({
            "source": src,
            **data,
            "acv_won": data["won_amt"] / dw if dw else 0,
            "dq_pct": _pct(data["leads_disqualified"], lc),
            "follow_up_pct": _pct(data["leads_contacted"], qualified),
            "deal_creation_pct": _pct(dc, qualified),
            "win_rate_pct": _pct(dw, dc),
        })

    rows.sort(key=lambda r: (r["leads_created"], r["deals_created"]), reverse=True)

    def _sum(key):
        return sum(r[key] for r in rows)

    tot_lc = _sum("leads_created")
    tot_dc = _sum("deals_created")
    tot_dw = _sum("deals_won")
    totals = {
        "source": "TOTAL",
        "leads_created": tot_lc,
        "leads_disqualified": _sum("leads_disqualified"),
        "leads_contacted": _sum("leads_contacted"),
        "deals_created": tot_dc,
        "deals_lost": _sum("deals_lost"),
        "deals_won": tot_dw,
        "pg_amt": _sum("pg_amt"),
        "won_amt": _sum("won_amt"),
        "lost_amt": _sum("lost_amt"),
        "acv_won": _sum("won_amt") / tot_dw if tot_dw else 0,
        "dq_pct": _pct(_sum("leads_disqualified"), tot_lc),
        "follow_up_pct": _pct(_sum("leads_contacted"), tot_lc - _sum("leads_disqualified")),
        "deal_creation_pct": _pct(tot_dc, tot_lc - _sum("leads_disqualified")),
        "win_rate_pct": _pct(tot_dw, tot_dc),
    }

    return {"rows": rows, "totals": totals, "period": period}


@ttl_cache
def compute_book_coverage() -> dict:
    """Compute point-in-time book coverage metrics per AE at the account (company) level.

    Metrics are calculated against each AE's A+ to C tier companies:
      - Total Named Accounts : all companies owned by the AE
      - A+ to C Accounts     : companies with icp_rank in {A+, A, B, C}
      - % within ROE         : A-C companies where active_since_transfer is TRUE
      - % in sequence        : A-C companies with at least one contact in a sequence
      - Overdue tasks        : past-due not-started tasks owned by the AE
    """
    now = datetime.now(timezone.utc)

    owners = apply_manual_owner_overrides(get_owners())
    companies = get_companies_for_coverage()
    seq_company_ids = get_sequence_enrolled_company_ids()
    tasks = get_overdue_sequence_tasks()

    AC_TIERS = {"superior", "strong", "moderate", "conservative"}

    owner_data = defaultdict(lambda: {
        "total": 0,
        "ac_accounts": 0,
        "within_roe": 0,
        "in_sequence": 0,
        "overdue_tasks": 0,
    })

    def _is_truthy(val) -> bool:
        return str(val).strip().lower() in ("true", "yes", "1")

    for company in companies:
        props = company["properties"]
        oid = props.get("hubspot_owner_id", "")
        if not oid or not _owner_allowed(oid, now):
            continue

        owner_data[oid]["total"] += 1

        tier = (props.get("icp_rank") or "").strip().lower()
        is_ac = tier in AC_TIERS
        if is_ac:
            owner_data[oid]["ac_accounts"] += 1

            # Within ROE: active_since_transfer == TRUE
            active_since = props.get("active_since_transfer")
            if active_since is not None and _is_truthy(active_since):
                owner_data[oid]["within_roe"] += 1

            # In active sequence
            custom_seq = props.get("in_active_sequence")
            if custom_seq is not None and custom_seq != "":
                if _is_truthy(custom_seq):
                    owner_data[oid]["in_sequence"] += 1
            else:
                if company["id"] in seq_company_ids:
                    owner_data[oid]["in_sequence"] += 1

    for task in tasks:
        oid = task["properties"].get("hubspot_owner_id", "")
        if oid and _owner_allowed(oid, now):
            owner_data[oid]["overdue_tasks"] += 1

    rows = []
    for oid, data in owner_data.items():
        owner = owners.get(oid)
        if not owner:
            continue
        ac = data["ac_accounts"]
        rows.append({
            "ae": owner["last_name"] or owner["name"],
            "owner_id": oid,
            "total_accounts": data["total"],
            "ac_accounts": ac,
            "within_roe": data["within_roe"],
            "in_sequence": data["in_sequence"],
            "pct_within_roe": _pct(data["within_roe"], ac),
            "pct_in_sequence": _pct(data["in_sequence"], ac),
            "overdue_tasks": data["overdue_tasks"],
        })

    rows.sort(key=lambda r: r["ae"])

    def _sum(key):
        return sum(r[key] for r in rows)

    total_ac = _sum("ac_accounts")
    totals = {
        "ae": "TOTAL",
        "total_accounts": _sum("total_accounts"),
        "ac_accounts": total_ac,
        "within_roe": _sum("within_roe"),
        "in_sequence": _sum("in_sequence"),
        "pct_within_roe": _pct(_sum("within_roe"), total_ac),
        "pct_in_sequence": _pct(_sum("in_sequence"), total_ac),
        "overdue_tasks": _sum("overdue_tasks"),
    }

    return {"rows": rows, "totals": totals}


@ttl_cache
def _rep_trailing_deal_stats(lookback_days: int = 90) -> dict:
    """Compute per-rep win rate, stage-2 advancement rate, and ACV from closed deals.

    Uses trailing `lookback_days` of won+lost deals. Falls back to global constants
    for reps with fewer than MIN_SAMPLE concluded deals.

    Returns {owner_id: {"win_rate_s2": float, "s1_to_s2": float, "acv": float, "sample": int}}
    """
    MIN_SAMPLE = 5
    _s2 = NB_STAGES["stage2"]

    end   = datetime.now(timezone.utc)
    start = end - timedelta(days=lookback_days)
    deals = get_deals(start, end, "closedate")
    closed = [d for d in deals
              if d["properties"].get("hs_is_closed_won") == "true"
              or d["properties"].get("hs_is_closed_lost") == "true"]

    owner_stats = defaultdict(lambda: {
        "total": 0, "won": 0, "s2_total": 0, "s2_won": 0, "won_amt": 0.0
    })
    for d in closed:
        oid  = d["properties"].get("hubspot_owner_id", "")
        if not oid or not _owner_allowed(oid, end):
            continue
        props  = d["properties"]
        is_won = props.get("hs_is_closed_won") == "true"
        amt    = float(props.get("amount") or 0)
        # Deal reached stage 2 if the hs_date_entered field for that stage is populated
        hit_s2 = bool(props.get(f"hs_date_entered_{_s2}"))

        owner_stats[oid]["total"] += 1
        if is_won:
            owner_stats[oid]["won"]     += 1
            owner_stats[oid]["won_amt"] += amt
        if hit_s2:
            owner_stats[oid]["s2_total"] += 1
            if is_won:
                owner_stats[oid]["s2_won"] += 1

    result = {}
    for oid, s in owner_stats.items():
        if s["total"] >= MIN_SAMPLE:
            win_rate_s2 = (s["s2_won"] / s["s2_total"]) if s["s2_total"] else STAGE2_WIN_RATE
            s1_to_s2    = (s["s2_total"] / s["total"])  if s["total"]    else STAGE1_TO_STAGE2
            acv         = (s["won_amt"]  / s["won"])     if s["won"]      else ACV
        else:
            win_rate_s2 = STAGE2_WIN_RATE
            s1_to_s2    = STAGE1_TO_STAGE2
            acv         = ACV
        result[oid] = {
            "win_rate_s2": max(win_rate_s2, 0.05),  # floor at 5% to avoid runaway targets
            "s1_to_s2":    max(s1_to_s2,    0.10),
            "acv":         max(acv,          1_000),
            "sample":      s["total"],
        }
    return result


@ttl_cache
def compute_scorecard(period: str = "this_month") -> dict:
    """Scorecard: per-rep weighted grade across 8 KPIs for the given period."""
    start, end = get_date_range(period)
    quota_start, quota_end = _quota_window(period, start, end)
    scope_end = end
    period_bdays = max(
        sum(1 for i in range((end - start).days + 1)
            if (start + timedelta(days=i)).weekday() < 5),
        1,
    )

    owners = apply_manual_owner_overrides(get_owners())
    quotas = get_quotas(quota_start, quota_end)

    won_deals     = [d for d in get_deals(start, end, "closedate")
                     if d["properties"].get("hs_is_closed_won") == "true"]
    created_deals = get_deals(start, end, "createdate")

    # Open deals for Next Steps % metric
    open_deals = get_all_open_deals()
    owner_open_total      = defaultdict(int)
    owner_open_with_steps = defaultdict(int)
    for d in open_deals:
        oid = d["properties"].get("hubspot_owner_id", "")
        if not oid or not _owner_allowed(oid):
            continue
        owner_open_total[oid] += 1
        ns = d["properties"].get("hs_next_step") or ""
        if ns.strip():
            owner_open_with_steps[oid] += 1

    book             = compute_book_coverage()
    book_by_owner    = {row["owner_id"]: row for row in book["rows"]}
    rep_deal_stats   = _rep_trailing_deal_stats()
    call_stats       = compute_call_stats(period)
    call_stats_by_owner = {r["owner_id"]: r for r in call_stats["rows"]}
    # ── per-owner aggregations ────────────────────────────────────────────────
    owner_won   = defaultdict(float)
    for d in won_deals:
        oid = d["properties"].get("hubspot_owner_id", "")
        if oid and _owner_allowed(oid, scope_end):
            owner_won[oid] += _parse_amount(d["properties"].get("amount"))

    # Deals created this month (all sources)
    owner_created = defaultdict(int)
    for d in created_deals:
        oid = d["properties"].get("hubspot_owner_id", "")
        if not oid or not _owner_allowed(oid, scope_end):
            continue
        owner_created[oid] += 1

    # $ advanced to Stage 2 this period: deals whose hs_v2_date_entered Stage 2
    # field falls within [start, end].  This matches HubSpot's own "Date entered
    # Stage 2" report and correctly includes deals created in prior periods that
    # advanced to Stage 2 this month.
    s2_deals = get_deals(start, end, "hs_v2_date_entered_71300358")
    owner_s2_amt = defaultdict(float)
    for d in s2_deals:
        oid = d["properties"].get("hubspot_owner_id", "")
        if not oid or not _owner_allowed(oid, scope_end):
            continue
        owner_s2_amt[oid] += _parse_amount(d["properties"].get("amount"))

    # ── grade weights ─────────────────────────────────────────────────────────
    WEIGHTS = {
        "quota_attainment": 0.50,
        "stage2":           0.15,
        "deals_created":    0.12,
        "stale_accounts":   0.10,
        "avg_dials":        0.08,
        "connect_rate":     0.05,
    }

    def _score(actual, target):
        return min(actual / target * 100, 100.0) if target else 0.0

    GRADE_ORDER = ["A+", "A", "A-", "B+", "B", "B-", "C+", "C", "C-", "D+", "D", "D-"]

    all_oids = {oid for oid in (set(owners) | set(quotas) | set(owner_won) | set(call_stats_by_owner))
                if _owner_allowed(oid, scope_end) and owners.get(oid)}

    rows = []
    for oid in all_oids:
        quota     = quotas.get(oid, 0.0)
        won       = owner_won.get(oid, 0.0)
        created   = owner_created.get(oid, 0)
        s2_amt    = owner_s2_amt.get(oid, 0.0)
        cs            = call_stats_by_owner.get(oid, {})
        dials         = cs.get("dials", 0)
        connects      = cs.get("connects", 0)
        avg_dials     = cs.get("avg_dials_per_day", 0.0)
        connect_rate  = cs.get("pct_connect", 0.0)
        attain_pct    = round(won / quota * 100, 1) if quota else 0.0
        rs            = rep_deal_stats.get(oid, {})
        rep_win_s2    = rs.get("win_rate_s2", STAGE2_WIN_RATE)
        rep_s1_to_s2  = rs.get("s1_to_s2",   STAGE1_TO_STAGE2)
        rep_acv       = rs.get("acv",         ACV)
        s2_target     = quota / rep_win_s2
        deals_target  = DEALS_CREATED_TARGET_PER_REP

        book_row    = book_by_owner.get(oid, {})
        ac_accounts = book_row.get("ac_accounts", 0)
        stale_count = ac_accounts - book_row.get("active_30", 0)
        # stale score: 0 stale = 100, all stale = 0; target ≤10% stale (90% active)
        stale_pct   = (stale_count / ac_accounts * 100) if ac_accounts else 0.0
        # Gradual decay: 0% stale=100, 50% stale=50, 100% stale=0 (was cliff at 10%)
        stale_score = max(0.0, 100.0 - stale_pct) if ac_accounts else 100.0

        scores = {
            # Uncapped at 150: 300% quota → score 150, rewarding overperformance
            "quota_attainment": min(attain_pct, 150.0),
            "stage2":           _score(s2_amt, s2_target),
            "deals_created":    _score(created, deals_target),
            "stale_accounts":   stale_score,
            "avg_dials":        _score(avg_dials, 40),
            "connect_rate":     _score(connect_rate, 10),
        }
        weighted = sum(scores[k] * WEIGHTS[k] for k in WEIGHTS)
        # Quota attainment floor: hitting quota guarantees a minimum grade
        # regardless of how other metrics score (stale accounts, dials, etc.)
        if attain_pct >= 100:
            weighted = max(weighted, 88.0)   # floor at A
        elif attain_pct >= 80:
            weighted = max(weighted, 65.0)   # floor at B
        elif attain_pct >= 60:
            weighted = max(weighted, 42.0)   # floor at C
        elif won > 0:
            weighted = max(weighted, 27.0)   # floor at D+: closed something vs zero
        grade    = _letter_grade(weighted)

        open_total      = owner_open_total.get(oid, 0)
        open_with_steps = owner_open_with_steps.get(oid, 0)
        next_steps_pct  = round(open_with_steps / open_total * 100) if open_total else None

        rows.append({
            "ae":              owners[oid]["last_name"] or owners[oid]["name"],
            "owner_id":        oid,
            "grade":           grade,
            "grade_sort":      GRADE_ORDER.index(grade),
            "quota_amt":       quota,
            "won_amt":         won,
            "attain_pct":      attain_pct,
            "deals_created":   created,
            "deals_target":    deals_target,
            "s2_amt":          s2_amt,
            "s2_target":       round(s2_target),
            "rep_win_rate":    round(rep_win_s2 * 100, 1),
            "rep_acv":         round(rep_acv),
            "avg_dials":       avg_dials,
            "connect_rate":    connect_rate,
            "stale_count":     stale_count,
            "ac_accounts":     ac_accounts,
            "next_steps_pct":  next_steps_pct,
        })

    rows.sort(key=lambda r: r["grade_sort"])

    # ── team totals ───────────────────────────────────────────────────────────
    t_quota    = sum(r["quota_amt"] for r in rows)
    t_won      = sum(r["won_amt"] for r in rows)
    t_dials    = sum(call_stats_by_owner.get(r["owner_id"], {}).get("dials", 0) for r in rows)
    t_connects = sum(call_stats_by_owner.get(r["owner_id"], {}).get("connects", 0) for r in rows)

    n_reps = len(rows)
    # Only count reps with call data for avg_dials denominator so inactive/new
    # owners added via set(owners) don't dilute the team average.
    n_reps_with_calls = sum(1 for r in rows if call_stats_by_owner.get(r["owner_id"], {}).get("dials", 0) > 0)
    team = {
        "attain_pct":    round(t_won / t_quota * 100, 1) if t_quota else 0.0,
        "won_amt":       t_won,
        "quota_amt":     t_quota,
        "deals_created": sum(r["deals_created"] for r in rows),
        "deals_target":  sum(r["deals_target"] for r in rows),
        "s2_amt":        sum(r["s2_amt"] for r in rows),
        "s2_target":     sum(r["s2_target"] for r in rows),
        "avg_dials":     round(t_dials / period_bdays / n_reps_with_calls, 1) if n_reps_with_calls else 0.0,
        "connect_rate":  _pct(t_connects, t_dials),
        "stale_count":   sum(r["stale_count"] for r in rows),
        "ac_accounts":   sum(r["ac_accounts"] for r in rows),
    }

    return {"rows": rows, "team": team}


@ttl_cache
def compute_win_rate_by_source(period: str) -> dict:
    start, end = get_date_range(period)

    all_deals = get_deals(start, end, "closedate")
    closed = [d for d in all_deals if
              d["properties"].get("hs_is_closed_won") == "true" or
              d["properties"].get("hs_is_closed_lost") == "true"]

    SOURCES = ["Cold outreach", "Inbound", "Conference", "Referral"]
    src_data = defaultdict(lambda: {"n": 0, "won": 0, "revenue": 0.0})

    for d in closed:
        oid = d["properties"].get("hubspot_owner_id", "")
        if not _owner_allowed(oid):
            continue
        src = _deal_source(d)
        amount = _parse_amount(d["properties"].get("amount"))
        src_data[src]["n"] += 1
        if d["properties"].get("hs_is_closed_won") == "true":
            src_data[src]["won"] += 1
            src_data[src]["revenue"] += amount

    rows = []
    for src in SOURCES:
        data = src_data[src]
        rows.append({
            "source": src,
            "created_closed": data["n"],
            "won": data["won"],
            "win_rate": _pct(data["won"], data["n"]),
            "revenue": data["revenue"],
            "acv": data["revenue"] / data["won"] if data["won"] else 0,
        })

    tot_n = sum(r["created_closed"] for r in rows)
    tot_won = sum(r["won"] for r in rows)
    totals = {
        "source": "TOTAL",
        "created_closed": tot_n,
        "won": tot_won,
        "win_rate": _pct(tot_won, tot_n),
        "revenue": sum(r["revenue"] for r in rows),
        "acv": sum(r["revenue"] for r in rows) / tot_won if tot_won else 0,
    }


@ttl_cache
def compute_abm_coverage(period: str = "this_month") -> dict:
    """ABM account coverage: target accounts per AE with activity and deal signals.

    Period drives the created/won deal windows. Activity (30d) is always point-in-time.
    """
    now = datetime.now(timezone.utc)
    thirty_days_ago = now - timedelta(days=30)

    start, end = get_date_range(period)
    start_dt = datetime(start.year, start.month, start.day, tzinfo=timezone.utc)
    end_dt   = datetime(end.year, end.month, end.day, 23, 59, 59, tzinfo=timezone.utc)
    start_ts = int(start_dt.timestamp() * 1000)
    end_ts   = int(end_dt.timestamp() * 1000)

    owners    = apply_manual_owner_overrides(get_owners())
    companies = get_target_account_companies()
    allowed_oids = get_team_owner_ids()

    def _deal_query(filters: list, properties: list) -> list:
        results = _search_all("deals", {
            "filterGroups": [{"filters": [
                {"propertyName": "pipeline",       "operator": "EQ", "value": "31544320"},
                {"propertyName": "target_account", "operator": "EQ", "value": "true"},
            ] + filters}],
            "properties": properties,
        })
        if allowed_oids:
            results = [d for d in results
                       if d.get("properties", {}).get("hubspot_owner_id") in allowed_oids]
        return results

    created_deals = _deal_query(
        [{"propertyName": "createdate", "operator": "GTE", "value": str(start_ts)},
         {"propertyName": "createdate", "operator": "LTE", "value": str(end_ts)}],
        ["createdate", "hubspot_owner_id", "amount"],
    )
    won_deals = _deal_query(
        [{"propertyName": "closedate",        "operator": "GTE", "value": str(start_ts)},
         {"propertyName": "closedate",        "operator": "LTE", "value": str(end_ts)},
         {"propertyName": "hs_is_closed_won", "operator": "EQ",  "value": "true"}],
        ["closedate", "hubspot_owner_id", "amount"],
    )

    owner_created_n: dict   = defaultdict(int)
    owner_created_amt: dict = defaultdict(float)
    for deal in created_deals:
        p   = deal.get("properties") or {}
        oid = p.get("hubspot_owner_id", "")
        if not oid:
            continue
        owner_created_n[oid]   += 1
        owner_created_amt[oid] += float(p.get("amount") or 0)

    owner_won_n: dict   = defaultdict(int)
    owner_won_amt: dict = defaultdict(float)
    for deal in won_deals:
        p   = deal.get("properties") or {}
        oid = p.get("hubspot_owner_id", "")
        if not oid:
            continue
        owner_won_n[oid]   += 1
        owner_won_amt[oid] += float(p.get("amount") or 0)

    owner_data = defaultdict(lambda: {"total": 0, "active_30": 0})

    for company in companies:
        props = company.get("properties") or {}
        oid   = props.get("hubspot_owner_id")
        if not oid or oid not in owners:
            continue
        owner_data[oid]["total"] += 1
        last_act_raw = props.get("notes_last_activity_date") or props.get("notes_last_contacted")
        if last_act_raw:
            try:
                if _parse_hs_datetime(last_act_raw) >= thirty_days_ago:
                    owner_data[oid]["active_30"] += 1
            except Exception:
                pass

    all_oids = set(list(owner_created_n) + list(owner_won_n) + list(owner_data))
    rows = []
    for oid in all_oids:
        o = owners.get(oid)
        if not o:
            continue
        d      = owner_data[oid]
        total  = d["total"]
        active = d["active_30"]
        rows.append({
            "ae":          f"{o['first_name']} {o['last_name']}".strip() or o["name"],
            "total":       total,
            "active_30":   active,
            "active_pct":  round(active / total * 100) if total else 0,
            "created_n":   owner_created_n[oid],
            "created_amt": owner_created_amt[oid],
            "won_n":       owner_won_n[oid],
            "won_amt":     owner_won_amt[oid],
        })

    rows.sort(key=lambda r: (-r["total"], r["ae"]))

    tot_total  = sum(r["total"] for r in rows)
    tot_active = sum(r["active_30"] for r in rows)
    totals = {
        "total":       tot_total,
        "active_30":   tot_active,
        "active_pct":  round(tot_active / tot_total * 100) if tot_total else 0,
        "created_n":   sum(r["created_n"]   for r in rows),
        "created_amt": sum(r["created_amt"] for r in rows),
        "won_n":       sum(r["won_n"]       for r in rows),
        "won_amt":     sum(r["won_amt"]     for r in rows),
    }

    return {"rows": rows, "totals": totals, "period": period}
