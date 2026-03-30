"""
Rules-based monthly summary generation for rep and team scorecards.

Generation always targets the completed prior calendar month
(last_completed_month() from monthly_store).  Summaries are structured
following Barbara Minto: answer first (main takeaway), then grouped
support (why), then forward implication (next_focus).

No LLM or external service — all text is produced by deterministic rules
operating on the same metrics the dashboard already displays.

Public API
----------
collect_rep_snapshot(owner_id)      → metrics dict
collect_team_snapshot()              → metrics dict
generate_rep_summary(m, name, label) → {main_takeaway, why, next_focus}
generate_team_summary(m, label)      → {main_takeaway, why, next_focus}
generate_and_save_rep(owner_id, label, year, month) → bool
generate_and_save_team(year, month)                  → bool
generate_all_for_month(year, month)                  → {team: bool, reps: {}}
"""

from datetime import datetime

import analytics
import monthly_store as store
from hubspot import get_owners, get_team_owner_ids, get_calls, get_date_range
from analytics import CALL_CONNECTED_GUIDS

# ── Thresholds (match template colour breakpoints where they exist) ────────────
_WIN_RATE_WARN      = 20.0   # pct — below this flags a close-quality issue
_CONNECT_RATE_WARN  = 10.0   # pct — below this flags connect quality
_ATTAIN_ON_TRACK    = 100.0  # pct — at or above → quota met
_ATTAIN_WARN        = 75.0   # pct — below this → materially missed
_MONTHLY_DIALS_MIN  = 250    # rough monthly floor: ~13/day × 20 bdays
_DEALS_CREATED_MIN  = 4      # below this per rep → creation bottleneck
_ADV_RATE_MIN       = 0.20   # S1→S3 rate; below this → progression stall


# ── Formatting helpers ────────────────────────────────────────────────────────

def _m(v):
    """$12,345"""
    try:
        return f"${float(v or 0):,.0f}"
    except (TypeError, ValueError):
        return "$0"


def _p(v):
    """34.5%"""
    try:
        return f"{float(v or 0):.1f}%"
    except (TypeError, ValueError):
        return "0.0%"


def _n(v, singular, plural=None):
    """'1 deal' / '3 deals'"""
    plural = plural or (singular + "s")
    return f"{int(v or 0)} {singular if int(v or 0) == 1 else plural}"


def _month_name(month_int):
    return datetime(2000, int(month_int), 1).strftime("%B")


# ── Grade ─────────────────────────────────────────────────────────────────────

def _grade(attain_pct):
    """Letter grade from attainment %. Returns '' if no quota."""
    if attain_pct is None:
        return ""
    a = float(attain_pct)
    if a >= 100: return "A"
    if a >= 85:  return "B+"
    if a >= 70:  return "B"
    if a >= 55:  return "C+"
    if a >= 40:  return "C"
    return "D"


# ── Row lookup helper ─────────────────────────────────────────────────────────

def _row(rows, owner_id):
    """Find a row dict by owner_id, or return {}."""
    for r in rows:
        if r.get("owner_id") == owner_id:
            return r
    return {}


def _top_lost_reason(lost_row):
    """Return (label, count) for the most common loss reason in a deals_lost row."""
    REASON_KEYS = [
        ("cost",              "Cost"),
        ("competitor",        "Competitor"),
        ("timeline",          "Timeline"),
        ("stakeholder_issue", "Stakeholder Issue"),
        ("product",           "Product"),
        ("value",             "Value"),
        ("never_demoed",      "Never Demo'd"),
        ("other",             "Other"),
    ]
    best_label, best_n = "Other", 0
    for key, label in REASON_KEYS:
        n = int(lost_row.get(key, 0) or 0)
        if n > best_n:
            best_label, best_n = label, n
    return best_label, best_n


# ── Raw call counting ─────────────────────────────────────────────────────────

def _raw_call_counts(period: str, owner_id: str = None) -> dict:
    """Count all outbound calls for the period without the contact-window filter.

    compute_call_stats() excludes calls to contacts at companies with any deal
    in the last 12 months, which is correct for the Call Stats dashboard (cold
    prospecting view) but produces non-deterministic and highly deflated numbers
    for snapshot storage (the filter result varies by cache state at generation
    time).

    This function counts every outbound call logged by the team (or a single rep
    if owner_id is given), giving a stable and user-legible activity figure.

    Returns {"dials": int, "connects": int, "connect_rate": float,
             "conversations": int}.
    """
    start, end = get_date_range(period)
    calls = get_calls(start, end)

    allowed = get_team_owner_ids()

    per_owner: dict = {}  # owner_id → {"dials", "connects", "conversations"}

    for call in calls:
        oid = call["properties"].get("hubspot_owner_id", "")
        if not oid:
            continue
        if allowed and oid not in allowed:
            continue
        if (call["properties"].get("hs_call_direction") or "").upper() != "OUTBOUND":
            continue

        if oid not in per_owner:
            per_owner[oid] = {"dials": 0, "connects": 0, "conversations": 0}

        per_owner[oid]["dials"] += 1
        disposition = (call["properties"].get("hs_call_disposition") or "").strip()
        if disposition in CALL_CONNECTED_GUIDS:
            per_owner[oid]["connects"] += 1
        duration_ms = int(call["properties"].get("hs_call_duration") or 0)
        if disposition in CALL_CONNECTED_GUIDS and duration_ms >= 60000:
            per_owner[oid]["conversations"] += 1

    if owner_id is not None:
        c = per_owner.get(owner_id, {"dials": 0, "connects": 0, "conversations": 0})
    else:
        # Aggregate across all allowed owners
        c = {"dials": 0, "connects": 0, "conversations": 0}
        for v in per_owner.values():
            c["dials"]         += v["dials"]
            c["connects"]      += v["connects"]
            c["conversations"] += v["conversations"]

    dials = c["dials"]
    connects = c["connects"]
    connect_rate = round(connects / dials * 100, 1) if dials else 0.0
    return {
        "dials":        dials,
        "connects":     connects,
        "connect_rate": connect_rate,
        "conversations": c["conversations"],
    }


# ── Snapshot collection ───────────────────────────────────────────────────────

def collect_rep_snapshot(owner_id):
    """Pull last-month metrics for one rep across all analytics surfaces.

    Returns a flat dict of numbers.  All values default to 0 / 0.0 / None
    so the caller never needs to guard against missing keys.
    """
    won  = analytics.compute_deals_won("last_month")
    call = analytics.compute_call_stats("last_month")
    pg   = analytics.compute_pipeline_generated("last_month")
    lost = analytics.compute_deals_lost("last_month")
    adv  = analytics.compute_deal_advancement("last_month")
    cov  = analytics.compute_pipeline_coverage("this_month")
    sc   = analytics.compute_scorecard("last_month")
    raw  = _raw_call_counts("last_month", owner_id)

    wr = _row(won["rows"],  owner_id)
    cr = _row(call["rows"], owner_id)
    pr = _row(pg["rows"],   owner_id)
    lr = _row(lost["rows"], owner_id)
    ar = _row(adv["rows"],  owner_id)
    vr = _row(cov["rows"],  owner_id)
    sr = _row(sc["rows"],   owner_id)

    top_reason, top_reason_n = _top_lost_reason(lr)

    return {
        # Outcome
        "won_amt":          wr.get("total_won_amt",  0.0),
        "won_n":            wr.get("total_won_n",    0),
        "quota_amt":        wr.get("quota_amt",      0.0),
        "attain_pct":       wr.get("attain_pct"),            # None if no quota
        "delta_amt":        wr.get("delta_amt",      0.0),
        "win_rate":         wr.get("win_rate",       0.0),
        "acv":              wr.get("acv",            0.0),
        # Source split — won revenue
        "cold_won_amt":     wr.get("cold_amt",       0.0),
        "cold_won_n":       wr.get("cold_n",         0),
        "inbound_won_amt":  wr.get("inbound_amt",    0.0),
        "inbound_won_n":    wr.get("inbound_n",      0),
        # Losses
        "lost_n":           wr.get("total_lost_n",   0),
        "top_lost_reason":  top_reason,
        "top_lost_n":       top_reason_n,
        # Activity — all outbound calls (no contact-window exclusion; see _raw_call_counts)
        "dials":            raw["dials"],
        "connects":         raw["connects"],
        "connect_rate":     raw["connect_rate"],
        "conversations":    raw["conversations"],
        "co_deals_created": cr.get("outbound_deals_created", 0),
        "co_deals_to_s2":   cr.get("outbound_deals_to_s2",   0),
        "dial_to_deal_pct": cr.get("pct_deals",      0.0),
        # Pipeline generated (deal creation by source)
        "pg_cold_n":        pr.get("cold_outreach_n",   0),
        "pg_cold_amt":      pr.get("cold_outreach_amt", 0.0),
        "pg_inbound_n":     pr.get("inbound_n",         0),
        "pg_inbound_amt":   pr.get("inbound_amt",       0.0),
        # Deal advancement — cohort created in last_month, current stage
        "adv_created":      ar.get("created", 0),
        "adv_to_s2":        ar.get("to_s2",   0),
        "adv_to_s3":        ar.get("to_s3",   0),
        "adv_to_s4":        ar.get("to_s4",   0),
        "adv_won":          ar.get("won",     0),
        "adv_lost":         ar.get("lost",    0),
        # Scorecard snapshot metrics
        "score_deals_created": sr.get("deals_created", 0),
        "score_s2_amt":        sr.get("s2_amt",        0.0),
        "score_avg_dials":     sr.get("avg_dials",     0.0),
        "score_connect_rate":  sr.get("connect_rate",  0.0),
        "score_stale_count":   sr.get("stale_count",   0),
        "score_ac_accounts":   sr.get("ac_accounts",   0),
        # Forward coverage — open pipeline entering this month
        "cov_s1_n":         vr.get("s1_n",   0),  "cov_s1_amt": vr.get("s1_amt", 0.0),
        "cov_s2_n":         vr.get("s2_n",   0),  "cov_s2_amt": vr.get("s2_amt", 0.0),
        "cov_s3_n":         vr.get("s3_n",   0),  "cov_s3_amt": vr.get("s3_amt", 0.0),
        "cov_s4_n":         vr.get("s4_n",   0),  "cov_s4_amt": vr.get("s4_amt", 0.0),
    }


def collect_team_snapshot():
    """Pull last-month metrics at the team level.

    Includes aggregated totals and a per-rep attainment breakdown so the
    team summary can identify concentration, outliers, and systemic patterns.
    """
    won  = analytics.compute_deals_won("last_month")
    call = analytics.compute_call_stats("last_month")
    pg   = analytics.compute_pipeline_generated("last_month")
    adv  = analytics.compute_deal_advancement("last_month")
    cov  = analytics.compute_pipeline_coverage("this_month")
    sc   = analytics.compute_scorecard("last_month")
    raw  = _raw_call_counts("last_month")

    wt = won["totals"]
    ct = call["totals"]
    pt = pg["totals"]
    at = adv["totals"]
    vt = cov["totals"]
    st = sc["team"]

    # Per-rep attainment for concentration analysis
    rep_att = []
    for r in won["rows"]:
        rep_att.append({
            "ae":         r["ae"],
            "owner_id":   r["owner_id"],
            "won_amt":    r["total_won_amt"],
            "quota_amt":  r.get("quota_amt", 0.0),
            "attain_pct": r.get("attain_pct"),
        })
    rep_att.sort(key=lambda x: x["won_amt"], reverse=True)

    n_reps     = len(rep_att)
    n_on_quota = sum(1 for r in rep_att if r["attain_pct"] is not None and r["attain_pct"] >= _ATTAIN_ON_TRACK)
    n_at_risk  = sum(1 for r in rep_att if r["attain_pct"] is not None and r["attain_pct"] < _ATTAIN_WARN)

    cov_total_amt = vt.get("s1_amt", 0) + vt.get("s2_amt", 0) + vt.get("s3_amt", 0) + vt.get("s4_amt", 0)
    cov_total_n   = vt.get("s1_n",   0) + vt.get("s2_n",   0) + vt.get("s3_n",   0) + vt.get("s4_n",   0)

    return {
        # Outcome
        "won_amt":          wt.get("total_won_amt",  0.0),
        "won_n":            wt.get("total_won_n",    0),
        "quota_amt":        wt.get("quota_amt",      0.0),
        "attain_pct":       wt.get("attain_pct"),
        "delta_amt":        wt.get("delta_amt",      0.0),
        "win_rate":         wt.get("win_rate",       0.0),
        "acv":              wt.get("acv",            0.0),
        "lost_n":           wt.get("total_lost_n",   0),
        # Source split — won
        "cold_won_amt":     wt.get("cold_amt",       0.0),
        "cold_won_n":       wt.get("cold_n",         0),
        "inbound_won_amt":  wt.get("inbound_amt",    0.0),
        "inbound_won_n":    wt.get("inbound_n",      0),
        # Activity — all outbound calls (no contact-window exclusion; see _raw_call_counts)
        "dials":            raw["dials"],
        "connect_rate":     raw["connect_rate"],
        "connects":         raw["connects"],
        "conversations":    raw["conversations"],
        "co_deals_created": ct.get("outbound_deals_created", 0),
        # Pipeline generated
        "pg_cold_n":        pt.get("cold_outreach_n", 0),
        "pg_cold_amt":      pt.get("cold_outreach_amt", 0.0),
        "pg_inbound_n":     pt.get("inbound_n",      0),
        "pg_inbound_amt":   pt.get("inbound_amt",    0.0),
        # Advancement
        "adv_created":      at.get("created", 0),
        "adv_to_s3":        at.get("to_s3",   0),
        # Scorecard snapshot metrics
        "score_deals_created": st.get("deals_created", 0),
        "score_s2_amt":        st.get("s2_amt",        0.0),
        "score_avg_dials":     st.get("avg_dials",     0.0),
        "score_connect_rate":  st.get("connect_rate",  0.0),
        "score_stale_count":   st.get("stale_count",   0),
        "score_ac_accounts":   st.get("ac_accounts",   0),
        # Coverage
        "cov_total_amt":    cov_total_amt,
        "cov_total_n":      cov_total_n,
        # Rep breakdown
        "n_reps":           n_reps,
        "n_on_quota":       n_on_quota,
        "n_at_risk":        n_at_risk,
        "rep_attainment":   rep_att,
    }


# ── Bottleneck detection ──────────────────────────────────────────────────────

def _bottleneck(m):
    """Identify the primary performance bottleneck.

    Evaluated from closest-to-outcome backward through the funnel so the
    most proximate cause is named, not an upstream symptom.

    Returns one of:
      "on_track"    — quota met (or no quota + wins present)
      "inbound_dep" — quota met but zero cold outreach wins
      "close"       — created adequate pipeline but win rate is low
      "progression" — created deals but most stalled before S3
      "activity"    — not enough dials to generate deal creation volume
      "dial_conv"   — dials adequate but poor conversion to deals
      "coverage"    — forward-looking coverage concern / default
    """
    attain     = m.get("attain_pct")
    won_n      = int(m.get("won_n",      0) or 0)
    lost_n     = int(m.get("lost_n",     0) or 0)
    closed_n   = won_n + lost_n
    win_rate   = float(m.get("win_rate", 0.0) or 0)
    dials      = int(m.get("dials",      0) or 0)
    pg_cold_n  = int(m.get("pg_cold_n",  0) or 0)
    pg_inb_n   = int(m.get("pg_inbound_n", 0) or 0)
    adv_create = int(m.get("adv_created", 0) or 0)
    adv_s3     = int(m.get("adv_to_s3",   0) or 0)
    cold_won_n = int(m.get("cold_won_n",  0) or 0)

    # Outcome met
    if attain is not None and attain >= _ATTAIN_ON_TRACK:
        if cold_won_n == 0 and won_n >= 2:
            return "inbound_dep"
        return "on_track"

    # Layer 1 — close quality (pipeline existed but deals were lost)
    if closed_n >= 2 and win_rate < _WIN_RATE_WARN:
        return "close"

    # Layer 2 — progression (created deals but most stalled before S3)
    if adv_create >= 3 and (adv_s3 / adv_create) < _ADV_RATE_MIN:
        return "progression"

    # Layer 3 — deal creation volume
    if (pg_cold_n + pg_inb_n) < _DEALS_CREATED_MIN:
        if dials < _MONTHLY_DIALS_MIN:
            return "activity"
        return "dial_conv"

    # Layer 4 — inbound dependency with no quota context
    if cold_won_n == 0 and won_n >= 1:
        return "inbound_dep"

    return "coverage"


# ── Rep summary text generation ───────────────────────────────────────────────

def generate_rep_summary(m, name, month_label):
    """Produce {main_takeaway, why, next_focus} for a rep.

    m           — metrics dict from collect_rep_snapshot()
    name        — display name (last name preferred)
    month_label — e.g. "February 2026"
    """
    won_amt      = float(m.get("won_amt",      0) or 0)
    quota_amt    = float(m.get("quota_amt",    0) or 0)
    attain       = m.get("attain_pct")
    won_n        = int(m.get("won_n",          0) or 0)
    lost_n       = int(m.get("lost_n",         0) or 0)
    win_rate     = float(m.get("win_rate",     0) or 0)
    dials        = int(m.get("dials",          0) or 0)
    connects     = int(m.get("connects",       0) or 0)
    connect_rate = float(m.get("connect_rate", 0) or 0)
    conversations = int(m.get("conversations", 0) or 0)
    co_deals     = int(m.get("co_deals_created", 0) or 0)
    dial_to_deal = float(m.get("dial_to_deal_pct", 0) or 0)
    pg_cold_n    = int(m.get("pg_cold_n",      0) or 0)
    pg_inb_n     = int(m.get("pg_inbound_n",   0) or 0)
    cold_won_n   = int(m.get("cold_won_n",     0) or 0)
    cold_won_amt = float(m.get("cold_won_amt", 0) or 0)
    inb_won_n    = int(m.get("inbound_won_n",  0) or 0)
    inb_won_amt  = float(m.get("inbound_won_amt", 0) or 0)
    adv_create   = int(m.get("adv_created",    0) or 0)
    adv_to_s3    = int(m.get("adv_to_s3",      0) or 0)
    top_reason   = m.get("top_lost_reason", "Other")
    top_reason_n = int(m.get("top_lost_n",     0) or 0)
    acv          = float(m.get("acv",          0) or 0)
    cov_amt      = sum(float(m.get(k, 0) or 0) for k in ("cov_s1_amt","cov_s2_amt","cov_s3_amt","cov_s4_amt"))
    cov_n        = sum(int(m.get(k,   0) or 0) for k in ("cov_s1_n",  "cov_s2_n",  "cov_s3_n",  "cov_s4_n"))
    delta        = float(m.get("delta_amt", 0) or 0)

    bn = _bottleneck(m)

    # ── Main takeaway — answer first ──────────────────────────────────────────
    if won_n == 0 and not quota_amt:
        takeaway = f"{name} closed 0 deals in {month_label} with no quota on record."
    elif quota_amt:
        gap = abs(delta)
        if attain is not None and attain >= _ATTAIN_ON_TRACK:
            takeaway = (
                f"{name} exceeded quota in {month_label}, closing {_m(won_amt)} "
                f"across {_n(won_n, 'deal')} at {_p(attain)} attainment."
            )
        elif attain is not None and attain >= 75:
            takeaway = (
                f"{name} reached {_p(attain)} of quota in {month_label}, closing "
                f"{_m(won_amt)} and falling {_m(gap)} short of the {_m(quota_amt)} target."
            )
        else:
            attain_str = _p(attain) if attain is not None else "an untracked %"
            takeaway = (
                f"{name} closed {_m(won_amt)} in {month_label}, reaching {attain_str} of quota "
                f"and missing target by {_m(gap)}."
            )
    else:
        takeaway = (
            f"{name} closed {_n(won_n, 'deal')} totalling {_m(won_amt)} in "
            f"{month_label} (no quota on record)."
        )

    # ── Why — 2–3 numbered reasons with proof bullets ─────────────────────────
    why = []

    if bn == "on_track":
        src_parts = []
        if cold_won_n > 0:
            src_parts.append(f"cold outreach: {_n(cold_won_n, 'deal')} ({_m(cold_won_amt)})")
        if inb_won_n > 0:
            src_parts.append(f"inbound: {_n(inb_won_n, 'deal')} ({_m(inb_won_amt)})")
        src_str = " and ".join(src_parts) if src_parts else _n(won_n, "deal")
        closed_n = won_n + lost_n
        win_line = f"\n   - Win rate: {_p(win_rate)} ({won_n} won, {lost_n} lost of {closed_n} closed)." if closed_n >= 2 else ""
        why.append(
            f"1. Revenue came from {src_str}, with average ACV of {_m(acv)}.{win_line}"
        )
        if adv_create >= 2:
            adv_rate = round(adv_to_s3 / adv_create * 100) if adv_create else 0
            cov_line = f"\n   - Open coverage entering next month: {cov_n} open deals totalling {_m(cov_amt)}." if cov_n > 0 else ""
            why.append(
                f"2. Deal advancement: {adv_rate:.0f}% of deals created in {month_label} "
                f"reached S3 or beyond ({adv_to_s3} of {adv_create}).{cov_line}"
            )
        if dials > 0:
            why.append(
                f"3. Outbound activity: {dials:,} dials at {_p(connect_rate)} connect rate "
                f"({connects} connects) generated {_n(co_deals, 'cold deal')} created."
            )

    elif bn == "inbound_dep":
        why.append(
            f"1. All closed revenue came from inbound; cold outreach produced 0 won deals."
            f"\n   - Inbound: {_n(inb_won_n, 'deal')} closed ({_m(inb_won_amt)})."
            f"\n   - Cold outreach: {_n(pg_cold_n, 'deal')} created but none closed."
        )
        if dials > 0:
            why.append(
                f"2. Outbound activity: {dials:,} dials produced {_n(co_deals, 'new cold deal')} "
                f"({_p(dial_to_deal)} conversion)."
                f"\n   - Connect rate: {_p(connect_rate)} ({connects} of {dials:,} dials)."
            )
        if cov_n > 0:
            why.append(
                f"3. Forward pipeline: {cov_n} open deals totalling {_m(cov_amt)} — "
                f"source mix determines whether cold outreach exposure improves next month."
            )

    elif bn == "close":
        closed_n = won_n + lost_n
        reason_line = f"\n   - Top loss reason: {top_reason} ({top_reason_n} of {lost_n} losses)." if top_reason_n > 0 else ""
        why.append(
            f"1. Win rate of {_p(win_rate)} was below the {_WIN_RATE_WARN:.0f}% threshold "
            f"({won_n} won vs {lost_n} lost of {closed_n} closed deals).{reason_line}"
        )
        total_pg = pg_cold_n + pg_inb_n
        if total_pg > 0:
            why.append(
                f"2. Deal creation was adequate: {total_pg} new deals entered the funnel "
                f"({pg_cold_n} cold, {pg_inb_n} inbound)."
                f"\n   - The gap came from losing deals already in the pipeline, not from low creation."
            )
        if dials > 0:
            constraint = "adequate" if connect_rate >= _CONNECT_RATE_WARN else "also low"
            why.append(
                f"3. Activity: {dials:,} dials at {_p(connect_rate)} connect rate — "
                f"{constraint}; late-stage conversion is the lever to close the gap."
            )

    elif bn == "progression":
        adv_rate = round(adv_to_s3 / adv_create * 100, 1) if adv_create else 0.0
        why.append(
            f"1. Only {_p(adv_rate)} of deals created in {month_label} progressed past S2 "
            f"({adv_to_s3} of {adv_create})."
            f"\n   - Most deals stalled at S1 with no confirmed next step."
        )
        total_pg = pg_cold_n + pg_inb_n
        if total_pg >= _DEALS_CREATED_MIN:
            why.append(
                f"2. Deal creation was not the constraint: {total_pg} new deals entered the funnel "
                f"({pg_cold_n} cold, {pg_inb_n} inbound)."
                f"\n   - The bottleneck is in moving deals forward, not generating them."
            )
        if cov_n > 0:
            why.append(
                f"3. Open pipeline: {cov_n} deals ({_m(cov_amt)}) — stalled S1 deals are inflating "
                f"this number without representing real near-term revenue."
            )

    elif bn == "activity":
        why.append(
            f"1. Only {dials:,} outbound dials logged in {month_label}, "
            f"below the ~{_MONTHLY_DIALS_MIN:,} monthly minimum."
            f"\n   - At this pace, deal creation targets cannot be consistently reached."
        )
        total_pg = pg_cold_n + pg_inb_n
        if total_pg < _DEALS_CREATED_MIN:
            why.append(
                f"2. New deal creation was {_n(total_pg, 'deal')} "
                f"({pg_cold_n} cold, {pg_inb_n} inbound) — well below the {_DEALS_CREATED_MIN}-deal target."
                f"\n   - Insufficient pipeline creation now will compress won revenue in 60–90 days."
            )
        qual = "within range" if connect_rate >= _CONNECT_RATE_WARN else "also low"
        if dials > 0:
            why.append(
                f"3. Connect rate of {_p(connect_rate)} ({connects} connects) is {qual} — "
                f"volume, not conversion quality, is the primary constraint."
            )

    elif bn == "dial_conv":
        why.append(
            f"1. Dial volume was {dials:,} — above the {_MONTHLY_DIALS_MIN:,} floor — but produced "
            f"only {_n(co_deals, 'cold outreach deal')} ({_p(dial_to_deal)} dial-to-deal rate)."
            f"\n   - Conversion from dial to qualified deal, not volume, is the constraint."
        )
        if connect_rate < _CONNECT_RATE_WARN:
            why.append(
                f"2. Connect rate of {_p(connect_rate)} is below the {_CONNECT_RATE_WARN:.0f}% threshold "
                f"({connects} connects from {dials:,} dials)."
                f"\n   - Low connect quality reduces the number of real conversations available to convert."
            )
        else:
            why.append(
                f"2. Connect rate of {_p(connect_rate)} is within range ({connects} connects from {dials:,} dials) — "
                f"conversations are happening but not converting to booked S1 meetings."
                f"\n   - Review pitch and ICP qualification on connected calls."
            )
        if pg_inb_n > 0:
            total_pg = pg_cold_n + pg_inb_n
            why.append(
                f"3. Total funnel entry: {_n(total_pg, 'deal')} ({pg_cold_n} cold, {pg_inb_n} inbound) "
                f"against a target of {_DEALS_CREATED_MIN}+."
            )

    else:  # coverage / default
        why.append(
            f"1. Closed {_n(won_n, 'deal')} ({_m(won_amt)}) against "
            f"{'quota of ' + _m(quota_amt) if quota_amt else 'no recorded quota'}."
        )
        if cov_n > 0:
            why.append(
                f"2. Forward pipeline: {cov_n} open deals totalling {_m(cov_amt)} across all stages."
            )
        if dials > 0:
            why.append(
                f"3. Outbound: {dials:,} dials, {_p(connect_rate)} connect rate, "
                f"{_n(co_deals, 'new cold deal')} created."
            )

    # ── Next focus — 2–3 forward-looking actionable bullets ──────────────────
    next_focus = []

    if bn == "on_track":
        if cov_n < 3:
            next_focus.append(
                f"Pipeline coverage entering next month is thin ({_n(cov_n, 'deal')}); "
                f"prioritise S1 creation in the first two weeks to protect the following month."
            )
        else:
            next_focus.append(
                f"Maintain current cadence — {cov_n} open deals ({_m(cov_amt)}) provides "
                f"adequate near-term coverage."
            )
        if cold_won_n == 0:
            next_focus.append(
                "Build cold outreach wins into the pipeline — all current wins are inbound, "
                "which creates quota risk if inbound volume softens."
            )
        else:
            next_focus.append(
                f"Sustain outbound cadence: {dials:,} dials generated {_n(co_deals, 'cold deal')} — "
                f"replicate the approach to protect next month's attainment."
            )

    elif bn == "inbound_dep":
        target_new = max(2, _DEALS_CREATED_MIN - pg_cold_n)
        next_focus.append(
            f"Build {target_new}+ cold outreach deals into active pipeline this month — "
            f"inbound-only attainment is fragile if lead volume fluctuates."
        )
        next_focus.append(
            f"Review the {_n(pg_cold_n, 'cold deal')} already created: identify which have a "
            f"confirmed next step and push each to S2 this week."
        )
        if cov_n > 0:
            next_focus.append(
                f"Use the {cov_n}-deal open pipeline ({_m(cov_amt)}) to forecast realistically; "
                f"disqualify any deals without a committed next step."
            )

    elif bn == "close":
        loss_line = (
            f"{top_reason_n} losses to {top_reason} — establish a specific counter-play "
            f"before the next late-stage conversation."
            if top_reason_n > 0
            else "identify the common pattern across losses."
        )
        next_focus.append(
            f"Review the {_n(lost_n, 'lost deal')} with manager: {loss_line}"
        )
        next_focus.append(
            f"Tighten S3 exit criteria — a {_p(win_rate)} win rate suggests deals are "
            f"advancing to contract stage without sufficient buying commitment."
        )
        if cov_n > 0:
            next_focus.append(
                f"Audit the {cov_n} deals in the open pipeline for the same qualification "
                f"gaps before they reach late stage."
            )

    elif bn == "progression":
        next_focus.append(
            "Review all S1 deals this week: assign each a concrete next-step date or "
            "disqualify — stalled S1 deals inflate pipeline coverage without representing real revenue."
        )
        next_focus.append(
            "Focus discovery calls on establishing a clear problem and getting an explicit "
            "S2 commitment before closing the call."
        )
        if cov_n > 0:
            next_focus.append(
                f"Of the {cov_n} open deals ({_m(cov_amt)}), flag any without stage movement "
                f"in the last 14 days — recycle or drop rather than let them age."
            )

    elif bn == "activity":
        target_daily = round(_MONTHLY_DIALS_MIN / 20)
        next_focus.append(
            f"Restore dial volume to {target_daily}+/day — block a dedicated 90-minute "
            f"prospecting window each morning before any other work."
        )
        next_focus.append(
            f"With only {_n(pg_cold_n + pg_inb_n, 'deal')} created this month, forward pipeline "
            f"is at risk; prioritise deal creation over advancement work this week."
        )
        if cov_n > 0:
            next_focus.append(
                f"Continue advancing the {cov_n} open deals ({_m(cov_amt)}) already in the funnel — "
                f"do not let advancement work displace prospecting time."
            )

    elif bn == "dial_conv":
        next_focus.append(
            "Review pitch and ICP qualification — dials are adequate but not converting to S1: "
            "the problem is on the connected call, not the dial volume."
        )
        if connect_rate < _CONNECT_RATE_WARN:
            next_focus.append(
                f"Improve list quality to lift connect rate above {_CONNECT_RATE_WARN:.0f}% — "
                f"current {_p(connect_rate)} limits the number of real conversations available."
            )
        else:
            next_focus.append(
                f"{_n(conversations, 'conversation')} happened but only {_n(co_deals, 'deal')} "
                f"was created — listen to 3–5 recorded calls with manager to find the drop-off point."
            )
        next_focus.append(
            f"Set a deal creation target of {_DEALS_CREATED_MIN}+ new S1 deals next month "
            f"and track it weekly rather than waiting for month-end."
        )

    else:  # coverage
        next_focus.append(
            f"Establish a weekly pipeline review — identify which of the {cov_n} open deals "
            f"need active next steps."
        )
        next_focus.append(
            f"Target {_DEALS_CREATED_MIN}+ new deals per month from a mix of cold and inbound "
            f"to build consistent attainment."
        )

    return {
        "main_takeaway": takeaway,
        "why":           why,
        "next_focus":    next_focus,
    }


# ── Team summary text generation ──────────────────────────────────────────────

def generate_team_summary(m, month_label):
    """Produce {main_takeaway, why, next_focus} for the whole team."""
    won_amt      = float(m.get("won_amt",      0) or 0)
    quota_amt    = float(m.get("quota_amt",    0) or 0)
    attain       = m.get("attain_pct")
    won_n        = int(m.get("won_n",          0) or 0)
    lost_n       = int(m.get("lost_n",         0) or 0)
    win_rate     = float(m.get("win_rate",     0) or 0)
    dials        = int(m.get("dials",          0) or 0)
    connect_rate = float(m.get("connect_rate", 0) or 0)
    co_deals     = int(m.get("co_deals_created", 0) or 0)
    pg_cold_n    = int(m.get("pg_cold_n",      0) or 0)
    pg_inb_n     = int(m.get("pg_inbound_n",   0) or 0)
    cold_won_n   = int(m.get("cold_won_n",     0) or 0)
    cold_won_amt = float(m.get("cold_won_amt", 0) or 0)
    inb_won_n    = int(m.get("inbound_won_n",  0) or 0)
    inb_won_amt  = float(m.get("inbound_won_amt", 0) or 0)
    adv_create   = int(m.get("adv_created",    0) or 0)
    adv_to_s3    = int(m.get("adv_to_s3",      0) or 0)
    cov_amt      = float(m.get("cov_total_amt", 0) or 0)
    cov_n        = int(m.get("cov_total_n",     0) or 0)
    n_reps       = int(m.get("n_reps",          0) or 0)
    n_on_quota   = int(m.get("n_on_quota",      0) or 0)
    n_at_risk    = int(m.get("n_at_risk",       0) or 0)
    rep_att      = m.get("rep_attainment", [])
    delta        = float(m.get("delta_amt", 0) or 0)

    bn = _bottleneck(m)

    # ── Main takeaway ─────────────────────────────────────────────────────────
    if quota_amt:
        gap = abs(delta)
        if attain is not None and attain >= _ATTAIN_ON_TRACK:
            takeaway = (
                f"The team closed {_m(won_amt)} in {month_label} at {_p(attain)} of quota, "
                f"with {n_on_quota} of {n_reps} reps at or above target."
            )
        else:
            attain_str = _p(attain) if attain is not None else "an untracked %"
            takeaway = (
                f"The team reached {attain_str} of quota in {month_label}, closing "
                f"{_m(won_amt)} and missing the {_m(quota_amt)} target by {_m(gap)}."
            )
    else:
        takeaway = (
            f"The team closed {_n(won_n, 'deal')} totalling {_m(won_amt)} in {month_label}."
        )

    # ── Why ───────────────────────────────────────────────────────────────────
    why = []

    # Revenue concentration
    if len(rep_att) >= 2 and won_amt > 0:
        top2_amt = sum(r["won_amt"] for r in rep_att[:2])
        top2_pct = round(top2_amt / won_amt * 100)
        top_names = " and ".join(r["ae"] for r in rep_att[:2])
        concentration = "concentrated" if top2_pct >= 60 else "distributed"
        risk_line = f"\n   - {n_at_risk} rep{'s' if n_at_risk != 1 else ''} below {_ATTAIN_WARN:.0f}% attainment." if n_at_risk > 0 else ""
        why.append(
            f"1. Revenue was {concentration}: {top_names} drove {_p(top2_pct)} of total won "
            f"revenue ({_m(top2_amt)} of {_m(won_amt)}).{risk_line}"
        )
    elif len(rep_att) == 1:
        why.append(
            f"1. All won revenue came from a single rep ({rep_att[0]['ae']}, {_m(rep_att[0]['won_amt'])})."
        )

    # Team bottleneck
    if bn == "close":
        closed_n = won_n + lost_n
        why.append(
            f"2. Win rate of {_p(win_rate)} was below the {_WIN_RATE_WARN:.0f}% threshold "
            f"({won_n} won vs {lost_n} lost of {closed_n} closed)."
            f"\n   - Deal creation was not the constraint; late-stage conversion was the gap."
        )
    elif bn in ("activity", "dial_conv"):
        total_target = n_reps * _DEALS_CREATED_MIN
        why.append(
            f"2. Team generated {_n(pg_cold_n + pg_inb_n, 'new deal')} "
            f"({pg_cold_n} cold, {pg_inb_n} inbound) against a target of {total_target}+ across {n_reps} reps."
            + (f"\n   - Activity: {dials:,} total dials, {_p(connect_rate)} connect rate." if dials > 0 else "")
        )
    elif bn == "progression":
        adv_rate = round(adv_to_s3 / adv_create * 100, 1) if adv_create else 0
        why.append(
            f"2. Deal progression was weak: {_p(adv_rate)} of deals created in {month_label} "
            f"reached S3 ({adv_to_s3} of {adv_create})."
            f"\n   - Pipeline exists but is not advancing — indicates qualification or urgency gaps."
        )
    elif bn == "inbound_dep":
        why.append(
            f"2. Outbound produced {_n(cold_won_n, 'won deal')} ({_m(cold_won_amt)}); "
            f"inbound accounted for {_n(inb_won_n, 'won deal')} ({_m(inb_won_amt)})."
            f"\n   - Quota attainment is inbound-dependent, increasing exposure to marketing-volume risk."
        )
    else:
        total_pg = pg_cold_n + pg_inb_n
        why.append(
            f"2. Team created {_n(total_pg, 'new deal')} in {month_label} "
            f"({pg_cold_n} cold, {pg_inb_n} inbound) with {_p(win_rate)} win rate."
        )

    # Forward coverage
    if cov_n > 0:
        cov_ratio_line = (
            f"\n   - Coverage vs quota: {_p(cov_amt / quota_amt * 100)}."
            if quota_amt else ""
        )
        why.append(
            f"3. Open pipeline entering next period: {cov_n} deals totalling {_m(cov_amt)}.{cov_ratio_line}"
        )
    else:
        why.append(
            "3. No open pipeline recorded for the next period — forward coverage is at risk."
        )

    # ── Next focus ────────────────────────────────────────────────────────────
    next_focus = []

    if bn in ("on_track", "coverage"):
        next_focus.append(
            f"Team enters next month with {cov_n} open deals ({_m(cov_amt)}) — "
            f"{'maintain cadence' if cov_n >= n_reps * 2 else 'prioritise S1 creation in the first two weeks'}."
        )
        if n_at_risk > 0:
            next_focus.append(
                f"Coach the {n_at_risk} rep{'s' if n_at_risk != 1 else ''} below {_ATTAIN_WARN:.0f}% — "
                f"team attainment can mask individual risk."
            )
        next_focus.append(
            "Review deal source mix: if inbound is carrying the majority of quota, confirm marketing "
            "pipeline is on track for next month before relying on it."
        )

    elif bn == "inbound_dep":
        next_focus.append(
            "Increase team cold outreach output — set a minimum of 2 CO wins per rep per month "
            "to reduce structural inbound dependency."
        )
        next_focus.append(
            f"Review each rep's cold deal creation: team created {_n(pg_cold_n, 'cold deal')} "
            f"but closed only {_n(cold_won_n, 'cold deal')} — identify where cold deals are stalling."
        )

    elif bn == "close":
        next_focus.append(
            f"Run a loss review with the full team — {_n(lost_n, 'deal')} lost in {month_label} "
            f"represents a pattern to diagnose before similar deals enter the funnel next month."
        )
        next_focus.append(
            f"Review S3 and S4 qualification criteria team-wide; a {_p(win_rate)} win rate "
            f"at close suggests deals are advancing without sufficient buying commitment."
        )

    elif bn in ("activity", "dial_conv"):
        next_focus.append(
            f"Team dial target for next month: {n_reps * _MONTHLY_DIALS_MIN:,}+ "
            f"({_MONTHLY_DIALS_MIN:,} per rep) — manager to review weekly activity reports."
        )
        next_focus.append(
            f"Pipeline creation target: {n_reps * _DEALS_CREATED_MIN}+ new deals across the team — "
            f"track deal creation weekly, not at month-end."
        )

    elif bn == "progression":
        next_focus.append(
            "Conduct a deal review on all S1 deals created last month: each must have a committed "
            "next-step date; disqualify any without one."
        )
        next_focus.append(
            "Determine whether the S1 stall is an ICP problem (wrong companies) or a follow-through "
            "problem (no next step set) — each requires a different coaching response."
        )

    return {
        "main_takeaway": takeaway,
        "why":           why,
        "next_focus":    next_focus,
    }


# ── Public entry points ───────────────────────────────────────────────────────

def generate_and_save_rep(owner_id, label, year, month):
    """Generate and persist a locked monthly summary for one rep.

    Skips silently if a summary for this owner/month already exists.
    Returns True if a new record was saved, False if already locked.
    """
    for rec in store.get_rep_history(owner_id):
        if rec["year"] == year and rec["month"] == month:
            return False

    metrics     = collect_rep_snapshot(owner_id)
    month_label = f"{_month_name(month)} {year}"
    summary     = generate_rep_summary(metrics, label, month_label)

    return store.save_summary({
        "year":          year,
        "month":         month,
        "entity_type":   "rep",
        "entity_id":     owner_id,
        "entity_label":  label,
        "final_grade":   _grade(metrics.get("attain_pct")),
        "metrics":       metrics,
        "main_takeaway": summary["main_takeaway"],
        "why":           summary["why"],
        "next_focus":    summary["next_focus"],
    })


def generate_and_save_team(year, month):
    """Generate and persist a locked monthly summary for the whole team.

    Returns True if saved, False if already locked for this month.
    """
    for rec in store.get_team_history():
        if rec["year"] == year and rec["month"] == month:
            return False

    metrics     = collect_team_snapshot()
    month_label = f"{_month_name(month)} {year}"
    summary     = generate_team_summary(metrics, month_label)

    return store.save_summary({
        "year":          year,
        "month":         month,
        "entity_type":   "team",
        "entity_id":     "team",
        "entity_label":  "Team",
        "final_grade":   _grade(metrics.get("attain_pct")),
        "metrics":       metrics,
        "main_takeaway": summary["main_takeaway"],
        "why":           summary["why"],
        "next_focus":    summary["next_focus"],
    })


def generate_all_for_month(year, month):
    """Generate and save summaries for every active rep and the team.

    Safe to call multiple times — already-locked records are skipped.
    Intended to be called from the Flask app on first page load of a new
    month, or from an admin endpoint.

    Returns {"team": bool, "reps": {owner_id: bool}}.
    """
    owners  = get_owners()
    allowed = get_team_owner_ids()

    rep_results = {}
    for oid, info in owners.items():
        if allowed and oid not in allowed:
            continue
        label = info.get("last_name") or info.get("name", oid)
        try:
            rep_results[oid] = generate_and_save_rep(oid, label, year, month)
        except Exception:
            rep_results[oid] = False

    team_result = False
    try:
        team_result = generate_and_save_team(year, month)
    except Exception:
        pass

    return {"team": team_result, "reps": rep_results}


# ── On-read retrieval (prefer locked record, generate if absent) ──────────────

def get_or_generate_rep_summary(owner_id):
    """Return the locked summary for last_completed_month(), generating if absent.

    This is the safe retrieval path for the UI.  It guarantees:
      - Locked records are never recomputed.
      - The first call after month rollover generates and locks the record.
      - Subsequent calls return the locked record instantly (no HubSpot calls).

    Returns the stored record dict, or None if generation fails.
    """
    year, month = store.last_completed_month()

    # Fast path — locked record already exists
    for rec in store.get_rep_history(owner_id):
        if rec["year"] == year and rec["month"] == month:
            return rec

    # Slow path — generate once, then lock
    owners = get_owners()
    info   = owners.get(owner_id, {})
    label  = info.get("last_name") or info.get("name") or owner_id
    try:
        generate_and_save_rep(owner_id, label, year, month)
    except Exception:
        return None

    return store.get_latest_rep_summary(owner_id)


def get_or_generate_team_summary():
    """Return the locked team summary for last_completed_month(), generating if absent.

    Same guarantee as get_or_generate_rep_summary — locked once per month.
    Returns the stored record dict, or None if generation fails.
    """
    year, month = store.last_completed_month()

    # Fast path
    for rec in store.get_team_history():
        if rec["year"] == year and rec["month"] == month:
            return rec

    # Slow path
    try:
        generate_and_save_team(year, month)
    except Exception:
        return None

    return store.get_latest_team_summary()
