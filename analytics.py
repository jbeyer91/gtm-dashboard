from collections import defaultdict
from datetime import datetime, timezone
from cache_utils import ttl_cache
from hubspot import (
    get_owners, get_deals, get_all_open_deals, get_calls, get_meetings,
    get_contacts_inbound, get_date_range, NB_STAGES, DEAL_STAGES,
    get_deal_contact_windows, get_call_to_contact_map, get_team_owner_ids,
)

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


def _deal_source(deal: dict) -> str:
    # Prefer the custom 'deal_source' property (Cold outreach / Inbound / Referral / Conference)
    custom = (deal.get("properties", {}).get("deal_source") or "").strip()
    if custom:
        return custom
    # Fall back to hs_analytics_source for deals without deal_source set
    src = (deal.get("properties", {}).get("hs_analytics_source") or "").upper()
    return DEAL_SOURCE_MAP.get(src, "Cold outreach")


@ttl_cache
def compute_call_stats(period: str) -> dict:
    start, end = get_date_range(period)
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
        if (call["properties"].get("hs_call_direction") or "").upper() != "OUTBOUND":
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
        days = len(c["days"]) or 1
        deals_out = len(owner_deals_created[oid])
        deals_s2 = len(owner_deals_s2[oid])
        rows.append({
            "ae": owner["last_name"] or owner["name"],
            "owner_id": oid,
            "dials": dials,
            "avg_dials_per_day": round(dials / days, 1),
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
        "avg_dials_per_day": round(sum(r["dials"] for r in rows) / max(sum(len(owner_calls[o]["days"]) for o in active_owners if owner_calls[o]["days"]), 1), 1),
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
        elif src == "Referral" or raw_src == "REFERRALS":
            owner_data[oid]["referral_amt"] += amount
            owner_data[oid]["referral_n"] += 1
        elif raw_src in ("PAID_SEARCH", "ORGANIC_SEARCH", "SOCIAL_MEDIA", "PAID_SOCIAL", "DIRECT_TRAFFIC", "EMAIL_MARKETING", "OFFLINE"):
            owner_data[oid]["inbound_amt"] += amount
            owner_data[oid]["inbound_n"] += 1
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


@ttl_cache
def compute_pipeline_coverage(period: str = None) -> dict:
    owners = get_owners()
    if period:
        start, end = get_date_range(period)
        open_deals = get_all_open_deals(start, end)
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
            "total_won_amt": owner_won[oid]["total_amt"],
            "total_won_n": won_n,
            "total_lost_n": lost_n,
            "acv": owner_won[oid]["total_amt"] / won_n if won_n else 0,
            "win_rate": _pct(won_n, total),
        })

    rows.sort(key=lambda r: r["total_won_amt"], reverse=True)

    def _sum(key):
        return sum(r[key] for r in rows)

    tw = _sum("total_won_n")
    tl = _sum("total_lost_n")
    totals = {
        "ae": "TOTAL",
        "cold_amt": _sum("cold_amt"), "cold_n": _sum("cold_n"),
        "inbound_amt": _sum("inbound_amt"), "inbound_n": _sum("inbound_n"),
        "conf_amt": _sum("conf_amt"), "conf_n": _sum("conf_n"),
        "ref_amt": _sum("ref_amt"), "ref_n": _sum("ref_n"),
        "total_won_amt": _sum("total_won_amt"),
        "total_won_n": tw, "total_lost_n": tl,
        "acv": _sum("total_won_amt") / tw if tw else 0,
        "win_rate": _pct(tw, tw + tl),
    }

    return {"rows": rows, "totals": totals, "period": period, "source": source}


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
        reason = d["properties"].get("hs_closed_lost_reason") or "Other"
        # normalize
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

    SOURCES = ["Paid Search", "Organic Search", "Organic Social", "Paid Social",
               "Direct Traffic", "Email Marketing", "Offline Sources", "Referrals", "AI Referrals"]

    contacts = get_contacts_inbound(start, end)
    deals = get_deals(start, end, "createdate")
    won_deals = [d for d in deals if d["properties"].get("hs_is_closed_won") == "true"]
    lost_deals = [d for d in deals if d["properties"].get("hs_is_closed_lost") == "true"]

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

    hs_to_display = {
        "PAID_SEARCH": "Paid Search",
        "ORGANIC_SEARCH": "Organic Search",
        "SOCIAL_MEDIA": "Organic Social",
        "PAID_SOCIAL": "Paid Social",
        "DIRECT_TRAFFIC": "Direct Traffic",
        "EMAIL_MARKETING": "Email Marketing",
        "OFFLINE": "Offline Sources",
        "REFERRALS": "Referrals",
    }

    for c in contacts:
        raw = (c["properties"].get("hs_analytics_source") or "").upper()
        src = hs_to_display.get(raw, "Direct Traffic")
        src_data[src]["leads_created"] += 1
        status = (c["properties"].get("hs_lead_status") or "").upper()
        lifecycle = (c["properties"].get("lifecyclestage") or "").lower()
        if status in ("UNQUALIFIED", "BAD TIMING", "DISQUALIFIED"):
            src_data[src]["leads_disqualified"] += 1
        if c["properties"].get("num_associated_deals", 0):
            src_data[src]["leads_contacted"] += 1

    for d in deals:
        raw = (d["properties"].get("hs_analytics_source") or "").upper()
        src = hs_to_display.get(raw, "Direct Traffic")
        amount = _parse_amount(d["properties"].get("amount"))
        src_data[src]["deals_created"] += 1
        src_data[src]["pg_amt"] += amount

    for d in won_deals:
        raw = (d["properties"].get("hs_analytics_source") or "").upper()
        src = hs_to_display.get(raw, "Direct Traffic")
        amount = _parse_amount(d["properties"].get("amount"))
        src_data[src]["deals_won"] += 1
        src_data[src]["won_amt"] += amount

    for d in lost_deals:
        raw = (d["properties"].get("hs_analytics_source") or "").upper()
        src = hs_to_display.get(raw, "Direct Traffic")
        amount = _parse_amount(d["properties"].get("amount"))
        src_data[src]["deals_lost"] += 1
        src_data[src]["lost_amt"] += amount

    rows = []
    for src in SOURCES:
        data = src_data[src]
        lc = data["leads_created"]
        dc = data["deals_created"]
        dw = data["deals_won"]
        rows.append({
            "source": src,
            **data,
            "acv_won": data["won_amt"] / dw if dw else 0,
            "dq_pct": _pct(data["leads_disqualified"], lc),
            "follow_up_pct": _pct(data["leads_contacted"], lc),
            "deal_creation_pct": _pct(dc, lc),
            "win_rate_pct": _pct(dw, dc),
        })

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
        "follow_up_pct": _pct(_sum("leads_contacted"), tot_lc),
        "deal_creation_pct": _pct(tot_dc, tot_lc),
        "win_rate_pct": _pct(tot_dw, tot_dc),
    }

    return {"rows": rows, "totals": totals, "period": period}


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
