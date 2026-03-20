from collections import defaultdict
from datetime import datetime, timezone, timedelta
from cache_utils import ttl_cache
from hubspot import (
    get_owners, get_deals, get_all_open_deals, get_calls, get_meetings,
    get_contacts_inbound, get_list_contacts, get_date_range, NB_STAGES, DEAL_STAGES,
    get_deal_contact_windows, get_call_to_contact_map, get_team_owner_ids,
    get_owner_team_map, TEAM_MANAGER,
    get_quotas, get_companies_for_coverage, get_sequence_enrolled_company_ids,
    get_overdue_sequence_tasks, _parse_hs_datetime, get_forecast_submissions,
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
DEALS_CREATED_TARGET_PER_REP = 15

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


def _owner_allowed(oid: str) -> bool:
    """Return True if this owner is on a TEAM_FILTER team (or if no teams are configured)."""
    allowed = get_team_owner_ids()
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


@ttl_cache
def compute_call_stats(period: str) -> dict:
    start, end = get_date_range(period)
    # Business days (Mon–Fri) elapsed in the period — used as avg/day denominator
    period_bdays = sum(
        1 for i in range((end - start).days + 1)
        if (start + timedelta(days=i)).weekday() < 5
    )
    period_bdays = max(period_bdays, 1)
    owners = get_owners()
    calls = get_calls(start, end)
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
        if not _owner_allowed(oid):
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
        if not _owner_allowed(oid):
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
        "avg_dials_per_day": round(sum(r["dials"] for r in rows) / period_bdays, 1),
        "pct_connect": _pct(sum(r["connects"] for r in rows), sum(r["dials"] for r in rows)),
        "connects": sum(r["connects"] for r in rows),
        "pct_conversation": _pct(sum(r["conversations"] for r in rows), sum(r["connects"] for r in rows)),
        "conversations": sum(r["conversations"] for r in rows),
        "pct_deals": _pct(sum(r["outbound_deals_created"] for r in rows), sum(r["dials"] for r in rows)),
        "outbound_deals_created": sum(r["outbound_deals_created"] for r in rows),
        "outbound_deals_to_s2": sum(r["outbound_deals_to_s2"] for r in rows),
    }

    return {"rows": rows, "totals": totals, "period": period, "start": start.isoformat(), "end": end.isoformat()}


@ttl_cache
def compute_pipeline_generated(period: str) -> dict:
    start, end = get_date_range(period)
    owners = get_owners()
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
        if not _owner_allowed(oid):
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


@ttl_cache
def compute_pipeline_coverage(period: str = None) -> dict:
    owners = get_owners()
    if period:
        start, end = get_date_range(period)
        # Use the true period boundary for the open-deals query so deals with
        # expected close dates later in the period (e.g. March 18-31) are included.
        open_deals = get_all_open_deals(start, _coverage_end(period, start, end))
        # Won deals are excluded by get_all_open_deals — fetch separately via closedate.
        # get_deals(…, "closedate") is already cached so no extra API call.
        closed_deals = get_deals(start, end, "closedate")
    else:
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
        if not _owner_allowed(oid):
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
        if not _owner_allowed(oid):
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
    owners = get_owners()

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
        if not _owner_allowed(oid):
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
    owners = get_owners()
    quotas = get_quotas(start, end)  # {owner_id: quota_amount} — {} if scope missing

    won_deals = get_deals(start, end, "closedate")
    won_deals = [d for d in won_deals if d["properties"].get("hs_is_closed_won") == "true"]

    lost_deals = get_deals(start, end, "closedate")
    lost_deals = [d for d in lost_deals if d["properties"].get("hs_is_closed_lost") == "true"]

    if source != "All":
        won_deals = [d for d in won_deals if _deal_source(d) == source]
        lost_deals = [d for d in lost_deals if _deal_source(d) == source]

    owner_won = defaultdict(lambda: {"cold_amt": 0.0, "cold_n": 0, "inbound_amt": 0.0, "inbound_n": 0, "conf_amt": 0.0, "conf_n": 0, "ref_amt": 0.0, "ref_n": 0, "total_amt": 0.0, "total_n": 0})
    owner_lost = defaultdict(int)

    for d in won_deals:
        oid = d["properties"].get("hubspot_owner_id", "")
        if not oid:
            continue
        if not _owner_allowed(oid):
            continue
        amount = _parse_amount(d["properties"].get("amount"))
        src = _deal_source(d)
        owner_won[oid]["total_amt"] += amount
        owner_won[oid]["total_n"] += 1
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
        if oid and _owner_allowed(oid):
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
        })

    rows.sort(key=lambda r: r["total_won_amt"], reverse=True)

    def _sum(key):
        return sum(r[key] for r in rows)

    tw = _sum("total_won_n")
    tl = _sum("total_lost_n")
    total_won_rev = _sum("total_won_amt")
    total_quota   = _sum("quota_amt")
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
    }

    return {"rows": rows, "totals": totals, "period": period, "source": source}


@ttl_cache
def compute_forecast(period: str) -> dict:
    start, end = get_date_range(period)
    owners = get_owners()
    quotas = get_quotas(start, end)

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
    owners = get_owners()

    lost_deals = get_deals(start, end, "closedate")
    lost_deals = [d for d in lost_deals if d["properties"].get("hs_is_closed_lost") == "true"]

    REASONS = ["Cost", "Never Demo'ed", "Timeline", "Stakeholder Issue", "Competitor", "Product", "Other", "Value"]

    owner_data = defaultdict(lambda: {r: 0 for r in REASONS} | {"total": 0})

    for d in lost_deals:
        oid = d["properties"].get("hubspot_owner_id", "")
        if not oid:
            continue
        if not _owner_allowed(oid):
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
      - % activity (30d)     : A-C companies with any activity in last 30 days
      - % contacted (120d)   : A-C companies with notes_last_contacted in last 120 days
      - % in sequence        : A-C companies with at least one contact in a sequence
      - Overdue tasks        : past-due not-started tasks owned by the AE
    """
    now = datetime.now(timezone.utc)
    thirty_days_ago = now - timedelta(days=30)
    onetwenty_days_ago = now - timedelta(days=120)

    owners = get_owners()
    companies = get_companies_for_coverage()
    seq_company_ids = get_sequence_enrolled_company_ids()
    tasks = get_overdue_sequence_tasks()

    AC_TIERS = {"superior", "strong", "moderate", "conservative"}

    owner_data = defaultdict(lambda: {
        "total": 0,
        "ac_accounts": 0,
        "active_30": 0,
        "called_120": 0,
        "in_sequence": 0,
        "overdue_tasks": 0,
    })

    def _is_truthy(val) -> bool:
        return str(val).strip().lower() in ("true", "yes", "1")

    for company in companies:
        props = company["properties"]
        oid = props.get("hubspot_owner_id", "")
        if not oid or not _owner_allowed(oid):
            continue

        owner_data[oid]["total"] += 1

        tier = (props.get("icp_rank") or "").strip().lower()
        is_ac = tier in AC_TIERS
        if is_ac:
            owner_data[oid]["ac_accounts"] += 1

            # Any sales activity in last 30 days — prefer notes_last_activity_date
            # (broadest activity signal); fall back to notes_last_contacted
            last_act_raw = (props.get("notes_last_activity_date")
                            or props.get("notes_last_contacted"))
            if last_act_raw:
                try:
                    if _parse_hs_datetime(last_act_raw) >= thirty_days_ago:
                        owner_data[oid]["active_30"] += 1
                except Exception:
                    pass

            # Called within 120 days — use hs_last_call_date (call-specific rollup);
            # fall back to notes_last_contacted if not populated
            last_call_raw = (props.get("hs_last_call_date")
                             or props.get("notes_last_contacted"))
            if last_call_raw:
                try:
                    if _parse_hs_datetime(last_call_raw) >= onetwenty_days_ago:
                        owner_data[oid]["called_120"] += 1
                except Exception:
                    pass

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
        if oid and _owner_allowed(oid):
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
            "active_30": data["active_30"],
            "called_120": data["called_120"],
            "in_sequence": data["in_sequence"],
            "pct_active_30": _pct(data["active_30"], ac),
            "pct_called_120": _pct(data["called_120"], ac),
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
        "active_30": _sum("active_30"),
        "called_120": _sum("called_120"),
        "in_sequence": _sum("in_sequence"),
        "pct_active_30": _pct(_sum("active_30"), total_ac),
        "pct_called_120": _pct(_sum("called_120"), total_ac),
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
        if not oid or not _owner_allowed(oid):
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
    period_bdays = max(
        sum(1 for i in range((end - start).days + 1)
            if (start + timedelta(days=i)).weekday() < 5),
        1,
    )

    owners = get_owners()
    quotas = get_quotas(start, end)

    won_deals     = [d for d in get_deals(start, end, "closedate")
                     if d["properties"].get("hs_is_closed_won") == "true"]
    created_deals = get_deals(start, end, "createdate")
    book             = compute_book_coverage()
    book_by_owner    = {row["owner_id"]: row for row in book["rows"]}
    rep_deal_stats   = _rep_trailing_deal_stats()
    call_stats       = compute_call_stats(period)
    call_stats_by_owner = {r["owner_id"]: r for r in call_stats["rows"]}
    # ── per-owner aggregations ────────────────────────────────────────────────
    owner_won   = defaultdict(float)
    for d in won_deals:
        oid = d["properties"].get("hubspot_owner_id", "")
        if oid and _owner_allowed(oid):
            owner_won[oid] += _parse_amount(d["properties"].get("amount"))

    # Deals created this month (all sources)
    owner_created = defaultdict(int)
    for d in created_deals:
        oid = d["properties"].get("hubspot_owner_id", "")
        if not oid or not _owner_allowed(oid):
            continue
        owner_created[oid] += 1

    # $ advanced to Stage 2 this period: use hs_v2_date_entered_71300358 (exact
    # date the deal entered Stage 2) across open deals + won deals this period.
    open_deals   = get_all_open_deals()
    # Combine open + won + created (covers lost deals); dedup by deal id
    _seen_s2 = set()
    all_deals_for_s2 = []
    for d in list(open_deals) + list(won_deals) + list(created_deals):
        did = d.get("id")
        if did not in _seen_s2:
            _seen_s2.add(did)
            all_deals_for_s2.append(d)
    owner_s2_amt = defaultdict(float)
    start_ms = int(start.timestamp() * 1000)
    end_ms   = int(end.timestamp() * 1000)
    for d in all_deals_for_s2:
        oid = d["properties"].get("hubspot_owner_id", "")
        if not oid or not _owner_allowed(oid):
            continue
        raw = (d["properties"].get("hs_v2_date_entered_71300358")
               or d["properties"].get("hs_date_entered_71300358"))
        if not raw:
            continue
        try:
            ts = int(float(raw))
        except (ValueError, TypeError):
            continue
        if start_ms <= ts <= end_ms:
            owner_s2_amt[oid] += _parse_amount(d["properties"].get("amount"))

    # ── grade weights ─────────────────────────────────────────────────────────
    WEIGHTS = {
        "quota_attainment": 0.75,
        "stage2":           0.08,
        "deals_created":    0.06,
        "stale_accounts":   0.08,
        "avg_dials":        0.02,
        "connect_rate":     0.01,
    }

    def _score(actual, target):
        return min(actual / target * 100, 100.0) if target else 0.0

    GRADE_ORDER = ["A+", "A", "A-", "B+", "B", "B-", "C+", "C", "C-", "D+", "D", "D-"]

    all_oids = {oid for oid in (set(quotas) | set(owner_won) | set(call_stats_by_owner))
                if _owner_allowed(oid) and owners.get(oid)}

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
        grade    = _letter_grade(weighted)

        rows.append({
            "ae":            owners[oid]["last_name"] or owners[oid]["name"],
            "owner_id":      oid,
            "grade":         grade,
            "grade_sort":    GRADE_ORDER.index(grade),
            "quota_amt":     quota,
            "won_amt":       won,
            "attain_pct":    attain_pct,
            "deals_created": created,
            "deals_target":  deals_target,
            "s2_amt":        s2_amt,
            "s2_target":     round(s2_target),
            "rep_win_rate":  round(rep_win_s2 * 100, 1),
            "rep_acv":       round(rep_acv),
            "avg_dials":     avg_dials,
            "connect_rate":  connect_rate,
            "stale_count":   stale_count,
            "ac_accounts":   ac_accounts,
        })

    rows.sort(key=lambda r: r["grade_sort"])

    # ── team totals ───────────────────────────────────────────────────────────
    t_quota    = sum(r["quota_amt"] for r in rows)
    t_won      = sum(r["won_amt"] for r in rows)
    t_dials    = sum(call_stats_by_owner.get(r["owner_id"], {}).get("dials", 0) for r in rows)
    t_connects = sum(call_stats_by_owner.get(r["owner_id"], {}).get("connects", 0) for r in rows)

    n_reps = len(rows)
    team = {
        "attain_pct":    round(t_won / t_quota * 100, 1) if t_quota else 0.0,
        "won_amt":       t_won,
        "quota_amt":     t_quota,
        "deals_created": sum(r["deals_created"] for r in rows),
        "deals_target":  sum(r["deals_target"] for r in rows),
        "s2_amt":        sum(r["s2_amt"] for r in rows),
        "s2_target":     sum(r["s2_target"] for r in rows),
        "avg_dials":     round(t_dials / period_bdays / n_reps, 1) if n_reps else 0.0,
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

    return {"rows": rows, "totals": totals, "period": period}
