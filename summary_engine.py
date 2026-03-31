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

from datetime import datetime, timedelta

import analytics
import monthly_store as store
from hubspot import get_date_range, get_owners, get_scoped_team_owner_ids

# ── Thresholds (match template colour breakpoints where they exist) ────────────
_WIN_RATE_WARN      = 20.0   # pct — below this flags a close-quality issue
_CONNECT_RATE_WARN  = 10.0   # pct — below this flags connect quality
_ATTAIN_ON_TRACK    = 100.0  # pct — at or above → quota met
_ATTAIN_WARN        = 75.0   # pct — below this → materially missed
_MONTHLY_DIALS_MIN  = 250    # rough monthly floor: ~13/day × 20 bdays
_DEALS_CREATED_MIN  = 4      # below this per rep → creation bottleneck
_ADV_RATE_MIN        = 0.20   # S1→S3 rate; below this → progression stall
_ADV_S1_TO_S2_MIN    = 0.40   # S1→S2 rate; below this → demo-conversion gap
_NEVER_DEMOED_THRESH = 0.35   # ≥35 % of losses never demo'd → early-funnel bottleneck
_COV_QUOTA_FLOOR_PCT = 0.50   # forward coverage < 50 % of quota → flag coverage risk


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


def _month_period(year: int, month: int) -> str:
    return f"month:{int(year):04d}-{int(month):02d}"


def _next_month_period(year: int, month: int) -> str:
    year = int(year)
    month = int(month)
    if month == 12:
        return _month_period(year + 1, 1)
    return _month_period(year, month + 1)


def _month_scope_end(year: int, month: int) -> datetime:
    period = _next_month_period(year, month)
    start, _ = get_date_range(period)
    return start - timedelta(seconds=1)


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


# ── Snapshot collection ───────────────────────────────────────────────────────

def _prefetch_analytics(period, coverage_period):
    """Fetch all analytics datasets needed for snapshot generation.

    Call this once per (period, coverage_period) pair when generating summaries
    for multiple reps so HubSpot API calls are not duplicated per rep.
    """
    return {
        "won":  analytics.compute_deals_won(period),
        "call": analytics.compute_call_stats(period),
        "pg":   analytics.compute_pipeline_generated(period),
        "lost": analytics.compute_deals_lost(period),
        "adv":  analytics.compute_deal_advancement(period),
        "cov":  analytics.compute_pipeline_coverage(coverage_period),
        "sc":   analytics.compute_scorecard(period),
    }


def collect_rep_snapshot(owner_id, period="last_month", coverage_period="this_month",
                         prefetched=None):
    """Pull monthly metrics for one rep across all analytics surfaces.

    period          — data period for deals/calls/pipeline (default "last_month";
                      pass "month:YYYY-MM" for historical backfill)
    coverage_period — period used for forward pipeline coverage snapshot
                      (default "this_month"; pass "month:YYYY-MM" for backfill)
    prefetched      — optional dict from _prefetch_analytics(); when supplied the
                      analytics API calls are skipped (used by generate_all_for_month
                      to avoid N×7 HubSpot round-trips for a team-wide backfill)

    Returns a flat dict of numbers.  All values default to 0 / 0.0 / None
    so the caller never needs to guard against missing keys.
    """
    if prefetched is not None:
        won  = prefetched["won"]
        call = prefetched["call"]
        pg   = prefetched["pg"]
        lost = prefetched["lost"]
        adv  = prefetched["adv"]
        cov  = prefetched["cov"]
        sc   = prefetched["sc"]
    else:
        won  = analytics.compute_deals_won(period)
        call = analytics.compute_call_stats(period)
        pg   = analytics.compute_pipeline_generated(period)
        lost = analytics.compute_deals_lost(period)
        adv  = analytics.compute_deal_advancement(period)
        cov  = analytics.compute_pipeline_coverage(coverage_period)
        sc   = analytics.compute_scorecard(period)

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
        "never_demoed_n":   int(lr.get("never_demoed", 0) or 0),
        # Activity — use the same filtered call metrics as the Call Stats page
        "dials":            cr.get("dials",              0),
        "connects":         cr.get("connects",           0),
        "connect_rate":     cr.get("pct_connect",        0.0),
        "conversations":    cr.get("conversations",      0),
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

def collect_team_snapshot(period="last_month", coverage_period="this_month",
                          prefetched=None):
    """Pull monthly metrics at the team level.

    period          — data period for deals/calls/pipeline (default "last_month";
                      pass "month:YYYY-MM" for historical backfill)
    coverage_period — period used for forward pipeline coverage snapshot
    prefetched      — optional dict from _prefetch_analytics(); skips API calls

    Includes aggregated totals and a per-rep attainment breakdown so the
    team summary can identify concentration, outliers, and systemic patterns.
    """
    if prefetched is not None:
        won  = prefetched["won"]
        call = prefetched["call"]
        pg   = prefetched["pg"]
        lost = prefetched["lost"]
        adv  = prefetched["adv"]
        cov  = prefetched["cov"]
        sc   = prefetched["sc"]
    else:
        won  = analytics.compute_deals_won(period)
        call = analytics.compute_call_stats(period)
        pg   = analytics.compute_pipeline_generated(period)
        lost = analytics.compute_deals_lost(period)
        adv  = analytics.compute_deal_advancement(period)
        cov  = analytics.compute_pipeline_coverage(coverage_period)
        sc   = analytics.compute_scorecard(period)

    wt = won["totals"]
    ct = call["totals"]
    pt = pg["totals"]
    lt = lost["totals"]
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
        "top_lost_reason":  _top_lost_reason(lt)[0],
        "top_lost_n":       _top_lost_reason(lt)[1],
        "never_demoed_n":   int(lt.get("never_demoed", 0) or 0),
        # Source split — won
        "cold_won_amt":     wt.get("cold_amt",       0.0),
        "cold_won_n":       wt.get("cold_n",         0),
        "inbound_won_amt":  wt.get("inbound_amt",    0.0),
        "inbound_won_n":    wt.get("inbound_n",      0),
        # Activity — use the same filtered call metrics as the Call Stats page
        "dials":            ct.get("dials",         0),
        "connect_rate":     ct.get("pct_connect",   0.0),
        "connects":         ct.get("connects",      0),
        "conversations":    ct.get("conversations", 0),
        "co_deals_created": ct.get("outbound_deals_created", 0),
        # Pipeline generated
        "pg_cold_n":        pt.get("cold_outreach_n", 0),
        "pg_cold_amt":      pt.get("cold_outreach_amt", 0.0),
        "pg_inbound_n":     pt.get("inbound_n",      0),
        "pg_inbound_amt":   pt.get("inbound_amt",    0.0),
        # Advancement
        "adv_created":      at.get("created", 0),
        "adv_to_s2":        at.get("to_s2",   0),
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
      "on_track"       — quota met with adequate forward coverage
      "coverage_risk"  — quota met but forward pipeline is thin (attainment masking pipeline risk)
      "inbound_dep"    — quota met but zero cold outreach wins
      "never_demoed"   — significant share of losses never reached demo (early-funnel failure)
      "close"          — created adequate pipeline but win rate is low
      "s1_stall"       — deals created but most never advanced to demo stage
      "progression"    — deals reached demo but stalled before S3
      "activity"       — not enough dials to generate deal creation volume
      "dial_conv"      — dials adequate but poor conversion to deals
      "coverage"       — forward-looking coverage concern / default
    """
    attain      = m.get("attain_pct")
    won_n       = int(m.get("won_n",          0) or 0)
    lost_n      = int(m.get("lost_n",         0) or 0)
    closed_n    = won_n + lost_n
    win_rate    = float(m.get("win_rate",     0.0) or 0)
    dials       = int(m.get("dials",          0) or 0)
    pg_cold_n   = int(m.get("pg_cold_n",      0) or 0)
    pg_inb_n    = int(m.get("pg_inbound_n",   0) or 0)
    adv_create  = int(m.get("adv_created",    0) or 0)
    adv_s2      = int(m.get("adv_to_s2",      0) or 0)
    adv_s3      = int(m.get("adv_to_s3",      0) or 0)
    cold_won_n  = int(m.get("cold_won_n",     0) or 0)
    never_dem   = int(m.get("never_demoed_n", 0) or 0)
    quota_amt   = float(m.get("quota_amt",    0) or 0)
    # Support both rep (individual stage keys) and team (cov_total_amt) snapshots
    cov_amt     = float(m.get("cov_total_amt") or 0) or sum(
        float(m.get(k, 0) or 0) for k in ("cov_s1_amt", "cov_s2_amt", "cov_s3_amt", "cov_s4_amt")
    )

    # Outcome met
    if attain is not None and attain >= _ATTAIN_ON_TRACK:
        # Forward coverage risk: quota met but thin pipeline entering next month
        if quota_amt > 0 and cov_amt < quota_amt * _COV_QUOTA_FLOOR_PCT:
            return "coverage_risk"
        if cold_won_n == 0 and won_n >= 2:
            return "inbound_dep"
        return "on_track"

    # Layer 1 — close quality: enough closed deals but low win rate
    if closed_n >= 3 and win_rate < _WIN_RATE_WARN:
        # Sub-distinguish: early-funnel failure (never demo'd) vs late-stage loss
        if never_dem >= 2 and lost_n > 0 and (never_dem / lost_n) >= _NEVER_DEMOED_THRESH:
            return "never_demoed"
        return "close"

    # Layer 2 — S1 stall: deals created but most never reached demo stage
    if adv_create >= 3 and (adv_s2 / adv_create) < _ADV_S1_TO_S2_MIN:
        return "s1_stall"

    # Layer 3 — post-demo progression: deals reached demo but stalled before S3
    if adv_create >= 3 and (adv_s3 / adv_create) < _ADV_RATE_MIN:
        return "progression"

    # Layer 4 — deal creation volume
    if (pg_cold_n + pg_inb_n) < _DEALS_CREATED_MIN:
        if dials < _MONTHLY_DIALS_MIN:
            return "activity"
        return "dial_conv"

    # Layer 5 — inbound dependency with no quota context
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
    adv_to_s2    = int(m.get("adv_to_s2",      0) or 0)
    adv_to_s3    = int(m.get("adv_to_s3",      0) or 0)
    top_reason   = m.get("top_lost_reason", "Other")
    top_reason_n = int(m.get("top_lost_n",     0) or 0)
    never_dem_n  = int(m.get("never_demoed_n", 0) or 0)
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
        cov_pct_of_quota = cov_amt / quota_amt * 100 if quota_amt else 0
        if bn == "coverage_risk":
            takeaway = (
                f"{name} exceeded quota in {month_label}, closing {_m(won_amt)} "
                f"({_p(attain)} attainment) — but enters next month with only "
                f"{_m(cov_amt)} in open pipeline ({_p(cov_pct_of_quota)} of quota)."
            )
        elif attain is not None and attain >= _ATTAIN_ON_TRACK:
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

    if bn == "coverage_risk":
        closed_n = won_n + lost_n
        win_line = (
            f"\n   - Win rate: {_p(win_rate)} ({won_n} won, {lost_n} lost of {closed_n} closed)."
            if closed_n >= 2 else ""
        )
        why.append(
            f"1. Quota achieved: {_n(won_n, 'deal')} closed, {_m(won_amt)} ({_p(attain)} attainment).{win_line}"
        )
        cov_pct_of_quota = cov_amt / quota_amt * 100 if quota_amt else 0
        why.append(
            f"2. Forward pipeline entering next month: {_n(cov_n, 'deal')} totalling {_m(cov_amt)} — "
            f"covering only {_p(cov_pct_of_quota)} of the {_m(quota_amt)} quota."
            f"\n   - Below the {_p(_COV_QUOTA_FLOOR_PCT * 100)} coverage floor needed for consistent attainment."
        )
        if dials > 0:
            src_note = ""
            if cold_won_n == 0 and won_n >= 2:
                src_note = f"\n   - All closed revenue came from inbound — cold outreach pipeline needs to be rebuilt."
            why.append(
                f"3. Outbound: {dials:,} dials at {_p(connect_rate)} connect rate "
                f"generated {_n(co_deals, 'cold deal')} created.{src_note}"
            )

    elif bn == "on_track":
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

    elif bn == "never_demoed":
        closed_n = won_n + lost_n
        why.append(
            f"1. {never_dem_n} of {lost_n} losses never reached demo stage — "
            f"the primary gap is early-funnel conversion, not late-stage close quality."
            f"\n   - Win rate: {_p(win_rate)} ({won_n} won vs {lost_n} lost of {closed_n} closed)."
        )
        total_pg = pg_cold_n + pg_inb_n
        if total_pg > 0:
            why.append(
                f"2. Deal creation: {total_pg} new deals entered the funnel "
                f"({pg_cold_n} cold, {pg_inb_n} inbound)."
                f"\n   - Most created deals are not reaching demo — the bottleneck is before the meeting, not in the close."
            )
        if dials > 0:
            why.append(
                f"3. Activity: {dials:,} dials at {_p(connect_rate)} connect rate ({connects} connects)."
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

    elif bn == "s1_stall":
        s1_to_s2_rate = round(adv_to_s2 / adv_create * 100, 1) if adv_create else 0.0
        why.append(
            f"1. Only {_p(s1_to_s2_rate)} of deals created in {month_label} advanced to demo stage "
            f"({adv_to_s2} of {adv_create}) — first conversations are not converting to committed next steps."
        )
        total_pg = pg_cold_n + pg_inb_n
        if total_pg >= _DEALS_CREATED_MIN:
            why.append(
                f"2. Deal creation was not the constraint: {total_pg} new deals entered the funnel "
                f"({pg_cold_n} cold, {pg_inb_n} inbound)."
                f"\n   - The gap is between opening conversations and booking demos, not generating them."
            )
        if dials > 0:
            constraint = "adequate" if connect_rate >= _CONNECT_RATE_WARN else "also below target"
            why.append(
                f"3. Activity: {dials:,} dials at {_p(connect_rate)} connect rate — "
                f"{constraint}; conversion from conversation to demo is the primary lever."
            )

    elif bn == "progression":
        adv_rate = round(adv_to_s3 / adv_create * 100, 1) if adv_create else 0.0
        s1_to_s2_rate = round(adv_to_s2 / adv_create * 100, 1) if adv_create else 0.0
        why.append(
            f"1. {_p(s1_to_s2_rate)} of new deals reached demo stage ({adv_to_s2} of {adv_create}), "
            f"but only {_p(adv_rate)} advanced past demo ({adv_to_s3} of {adv_create})."
            f"\n   - Demos are being held but not converting to committed next steps."
        )
        total_pg = pg_cold_n + pg_inb_n
        if total_pg >= _DEALS_CREATED_MIN:
            why.append(
                f"2. Deal creation was not the constraint: {total_pg} new deals entered the funnel "
                f"({pg_cold_n} cold, {pg_inb_n} inbound)."
                f"\n   - The bottleneck is post-demo progression, not pipeline generation."
            )
        if cov_n > 0:
            why.append(
                f"3. Open pipeline: {cov_n} deals ({_m(cov_amt)}) — S2 deals without forward stage movement "
                f"are inflating this number without representing confirmed near-term revenue."
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

    if bn == "coverage_risk":
        cov_pct_of_quota = cov_amt / quota_amt * 100 if quota_amt else 0
        next_focus.append(
            f"Forward coverage is only {_p(cov_pct_of_quota)} of quota entering next month — "
            f"prioritise new S1 creation in the first two weeks before deal advancement consumes the calendar."
        )
        next_focus.append(
            "Strong close months create pipeline gaps the following month — protect next month's "
            "attainment by running the same prospecting cadence regardless of this month's result."
        )
        if cold_won_n == 0 and won_n >= 2:
            next_focus.append(
                "All current wins came from inbound; build cold outreach pipeline early in the month "
                "to reduce reliance on inbound lead volume."
            )
        elif dials > 0:
            next_focus.append(
                f"The {dials:,}-dial cadence generated {_n(co_deals, 'cold deal')} this month — "
                f"sustain that prospecting volume into next month to avoid a coverage gap."
            )

    elif bn == "on_track":
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

    elif bn == "never_demoed":
        next_focus.append(
            f"Audit the {never_dem_n} never-demo'd losses: identify whether the gap is the demo ask, "
            f"urgency, or prospect engagement — each requires a different fix."
        )
        next_focus.append(
            "On every live S1 conversation, make a direct demo ask before ending the call — "
            "if the prospect won't commit to a demo, qualify hard on urgency and need before continuing outreach."
        )
        if cov_n > 0:
            next_focus.append(
                f"Review the {cov_n} open deals ({_m(cov_amt)}) to confirm each has a scheduled demo — "
                f"any without one should be treated as unconfirmed S1 until the meeting is booked."
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

    elif bn == "s1_stall":
        s1_to_s2_rate = round(adv_to_s2 / adv_create * 100, 1) if adv_create else 0.0
        next_focus.append(
            "The clearest gap is between conversations started and demos booked — review S1 call quality: "
            "are you creating urgency and making a direct demo ask before ending the call?"
        )
        next_focus.append(
            f"Of the {adv_create} new deals created, only {adv_to_s2} reached demo — "
            f"run a call review with your manager to identify exactly where conversations are dropping off."
        )
        if cov_n > 0:
            next_focus.append(
                f"Advance the {cov_n} open S1 deals ({_m(cov_amt)}) this week: "
                f"each needs a confirmed next-step date or should be disqualified."
            )

    elif bn == "progression":
        next_focus.append(
            "Review all S2 deals: after each demo, has a clear next step been committed "
            "(buying committee meeting, trial, or ROI review)? If not, re-engage or disqualify."
        )
        next_focus.append(
            "Focus post-demo follow-up on stakeholder alignment and a defined next step within "
            "48 hours of each demo — this is the highest-leverage gap based on the data."
        )
        if cov_n > 0:
            next_focus.append(
                f"Of the {cov_n} open deals ({_m(cov_amt)}), flag any in S2 without stage movement "
                f"in the last 14 days — advance or disqualify rather than let them age."
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
    adv_to_s2    = int(m.get("adv_to_s2",      0) or 0)
    adv_to_s3    = int(m.get("adv_to_s3",      0) or 0)
    top_reason   = m.get("top_lost_reason", "Other")
    top_reason_n = int(m.get("top_lost_n",     0) or 0)
    never_dem_n  = int(m.get("never_demoed_n", 0) or 0)
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
        cov_pct_of_quota = cov_amt / quota_amt * 100 if quota_amt else 0
        if bn == "coverage_risk":
            takeaway = (
                f"The team exceeded quota in {month_label} at {_p(attain)} attainment, "
                f"closing {_m(won_amt)} — but enters next month with only {_m(cov_amt)} "
                f"in open pipeline ({_p(cov_pct_of_quota)} of quota)."
            )
        elif attain is not None and attain >= _ATTAIN_ON_TRACK:
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
    if bn == "coverage_risk":
        cov_pct_of_quota = cov_amt / quota_amt * 100 if quota_amt else 0
        why.append(
            f"2. Forward pipeline entering next month: {_n(cov_n, 'deal')} totalling {_m(cov_amt)} — "
            f"covering only {_p(cov_pct_of_quota)} of the {_m(quota_amt)} team quota."
            f"\n   - Below the {_p(_COV_QUOTA_FLOOR_PCT * 100)} coverage floor needed for consistent attainment."
        )
        if dials > 0:
            why.append(
                f"3. Outbound: {dials:,} total dials at {_p(connect_rate)} connect rate, "
                f"{_n(co_deals, 'cold deal')} created team-wide."
            )
    elif bn == "never_demoed":
        closed_n = won_n + lost_n
        reason_line = f"\n   - Top loss reason: {top_reason} ({top_reason_n} of {lost_n} losses)." if top_reason_n > 0 else ""
        why.append(
            f"2. {never_dem_n} of {lost_n} team losses never reached demo stage — "
            f"the primary gap is early-funnel conversion, not close quality."
            f"\n   - Win rate: {_p(win_rate)} ({won_n} won vs {lost_n} lost of {closed_n} closed).{reason_line}"
        )
        total_pg = pg_cold_n + pg_inb_n
        if cov_n > 0:
            cov_ratio_line = (
                f"\n   - Coverage vs quota: {_p(cov_amt / quota_amt * 100)}."
                if quota_amt else ""
            )
            why.append(
                f"3. Open pipeline entering next period: {cov_n} deals totalling {_m(cov_amt)}.{cov_ratio_line}"
            )
    elif bn == "close":
        closed_n = won_n + lost_n
        reason_line = f"\n   - Top loss reason: {top_reason} ({top_reason_n} of {lost_n} losses)." if top_reason_n > 0 else ""
        why.append(
            f"2. Win rate of {_p(win_rate)} was below the {_WIN_RATE_WARN:.0f}% threshold "
            f"({won_n} won vs {lost_n} lost of {closed_n} closed).{reason_line}"
            f"\n   - Deal creation was not the constraint; late-stage conversion was the gap."
        )
        if cov_n > 0:
            cov_ratio_line = (
                f"\n   - Coverage vs quota: {_p(cov_amt / quota_amt * 100)}."
                if quota_amt else ""
            )
            why.append(
                f"3. Open pipeline entering next period: {cov_n} deals totalling {_m(cov_amt)}.{cov_ratio_line}"
            )
    elif bn == "s1_stall":
        s1_to_s2_rate = round(adv_to_s2 / adv_create * 100, 1) if adv_create else 0.0
        adv_rate = round(adv_to_s3 / adv_create * 100, 1) if adv_create else 0.0
        why.append(
            f"2. Only {_p(s1_to_s2_rate)} of deals created in {month_label} advanced to demo stage "
            f"({adv_to_s2} of {adv_create}) — first conversations are not converting to committed meetings."
            f"\n   - This is a system-level pattern, not isolated to one rep."
        )
        if cov_n > 0:
            why.append(
                f"3. Open pipeline: {cov_n} deals ({_m(cov_amt)}) — stalled S1 deals may be "
                f"inflating this count without representing confirmed near-term revenue."
            )
    elif bn in ("activity", "dial_conv"):
        total_target = n_reps * _DEALS_CREATED_MIN
        why.append(
            f"2. Team generated {_n(pg_cold_n + pg_inb_n, 'new deal')} "
            f"({pg_cold_n} cold, {pg_inb_n} inbound) against a target of {total_target}+ across {n_reps} reps."
            + (f"\n   - Activity: {dials:,} total dials, {_p(connect_rate)} connect rate." if dials > 0 else "")
        )
        if cov_n > 0:
            why.append(
                f"3. Open pipeline entering next period: {cov_n} deals totalling {_m(cov_amt)}."
            )
    elif bn == "progression":
        adv_rate = round(adv_to_s3 / adv_create * 100, 1) if adv_create else 0.0
        s1_to_s2_rate = round(adv_to_s2 / adv_create * 100, 1) if adv_create else 0.0
        why.append(
            f"2. {_p(s1_to_s2_rate)} of new deals reached demo ({adv_to_s2} of {adv_create}), "
            f"but only {_p(adv_rate)} advanced past demo stage ({adv_to_s3} of {adv_create})."
            f"\n   - Demos are being held but not converting to committed next steps team-wide."
        )
        if cov_n > 0:
            cov_ratio_line = (
                f"\n   - Coverage vs quota: {_p(cov_amt / quota_amt * 100)}."
                if quota_amt else ""
            )
            why.append(
                f"3. Open pipeline entering next period: {cov_n} deals totalling {_m(cov_amt)}.{cov_ratio_line}"
            )
    elif bn == "inbound_dep":
        why.append(
            f"2. Outbound produced {_n(cold_won_n, 'won deal')} ({_m(cold_won_amt)}); "
            f"inbound accounted for {_n(inb_won_n, 'won deal')} ({_m(inb_won_amt)})."
            f"\n   - Quota attainment is inbound-dependent, increasing exposure to marketing-volume risk."
        )
        if cov_n > 0:
            cov_ratio_line = (
                f"\n   - Coverage vs quota: {_p(cov_amt / quota_amt * 100)}."
                if quota_amt else ""
            )
            why.append(
                f"3. Open pipeline entering next period: {cov_n} deals totalling {_m(cov_amt)}.{cov_ratio_line}"
            )
    else:
        total_pg = pg_cold_n + pg_inb_n
        why.append(
            f"2. Team created {_n(total_pg, 'new deal')} in {month_label} "
            f"({pg_cold_n} cold, {pg_inb_n} inbound) with {_p(win_rate)} win rate."
        )
        if cov_n > 0:
            cov_ratio_line = (
                f"\n   - Coverage vs quota: {_p(cov_amt / quota_amt * 100)}."
                if quota_amt else ""
            )
            why.append(
                f"3. Open pipeline entering next period: {cov_n} deals totalling {_m(cov_amt)}.{cov_ratio_line}"
            )
        elif not cov_n:
            why.append(
                "3. No open pipeline recorded for the next period — forward coverage is at risk."
            )

    # ── Next focus ────────────────────────────────────────────────────────────
    next_focus = []

    if bn == "coverage_risk":
        cov_pct_of_quota = cov_amt / quota_amt * 100 if quota_amt else 0
        next_focus.append(
            f"Team pipeline entering next month covers only {_p(cov_pct_of_quota)} of quota — "
            f"prioritise new S1 creation in the first two weeks before advancement work consumes the calendar."
        )
        next_focus.append(
            "Strong close months create coverage gaps the following month — set a team prospecting "
            "target for the first week and track it daily rather than waiting for mid-month."
        )
        if n_at_risk > 0:
            next_focus.append(
                f"Coach the {n_at_risk} rep{'s' if n_at_risk != 1 else ''} below {_ATTAIN_WARN:.0f}% — "
                f"their pipeline gap is likely larger and needs early-month attention."
            )

    elif bn in ("on_track", "coverage"):
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

    elif bn == "never_demoed":
        next_focus.append(
            f"Run a loss review focused on the {never_dem_n} never-demo'd deals — "
            f"identify whether the gap is the demo ask, urgency creation, or follow-up cadence, "
            f"then establish a team-wide counter-play."
        )
        next_focus.append(
            "Review S1 conversation quality team-wide: are reps making a direct demo ask on every live call? "
            "Coach reps to set a next step before ending any S1 conversation."
        )
        if n_at_risk > 0:
            next_focus.append(
                f"The {n_at_risk} rep{'s' if n_at_risk != 1 else ''} below {_ATTAIN_WARN:.0f}% likely "
                f"have the most stalled S1 deals — prioritise individual call reviews for these reps."
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
        reason_focus = (
            f"Run a loss review with the full team — {top_reason_n} of {lost_n} losses came from "
            f"{top_reason}; establish a specific counter-play before similar deals enter the funnel."
            if top_reason_n > 0
            else f"Run a loss review with the full team — {_n(lost_n, 'deal')} lost in {month_label} "
                 f"represents a pattern to diagnose before similar deals enter the funnel next month."
        )
        next_focus.append(reason_focus)
        next_focus.append(
            f"Review S3 and S4 qualification criteria team-wide; a {_p(win_rate)} win rate "
            f"at close suggests deals are advancing without sufficient buying commitment."
        )

    elif bn == "s1_stall":
        s1_to_s2_rate = round(adv_to_s2 / adv_create * 100, 1) if adv_create else 0.0
        next_focus.append(
            "Conduct a team call review focused on S1 conversations: are reps creating urgency and "
            "making a direct demo ask? Identify the drop-off point before the meeting is booked."
        )
        next_focus.append(
            f"Only {_p(s1_to_s2_rate)} of new deals advanced to demo — set a team S2 meeting "
            f"target for next month and track it weekly, not at month-end."
        )
        if n_at_risk > 0:
            next_focus.append(
                f"Coach the {n_at_risk} rep{'s' if n_at_risk != 1 else ''} below {_ATTAIN_WARN:.0f}% "
                f"first — their S1 conversion gap is likely driving most of the team shortfall."
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
            "Conduct a deal review on all S2 deals created last month: each must have a committed "
            "next step (trial, stakeholder meeting, or ROI review); disqualify any without one."
        )
        next_focus.append(
            "Determine whether the post-demo stall is a discovery gap (problem not clearly established) "
            "or a follow-through gap (no next step set) — each requires a different coaching response."
        )

    return {
        "main_takeaway": takeaway,
        "why":           why,
        "next_focus":    next_focus,
    }


# ── Public entry points ───────────────────────────────────────────────────────

def _periods_for(year, month):
    """Return (data_period, coverage_period) for a given year/month.

    For the most-recently completed calendar month the named periods are used
    so the TTL cache keys stay stable.  For any other month the explicit
    "month:YYYY-MM" form is used.
    """
    cur_year, cur_month = store.last_completed_month()
    if year == cur_year and month == cur_month:
        return "last_month", "this_month"
    cov_year  = year + (month // 12)
    cov_month = (month % 12) + 1
    return f"month:{year:04d}-{month:02d}", f"month:{cov_year:04d}-{cov_month:02d}"


def generate_and_save_rep(owner_id, label, year, month, prefetched=None):
    """Generate and persist a locked monthly summary for one rep.

    Skips silently if a summary for this owner/month already exists.
    Returns True if a new record was saved, False if already locked.
    prefetched — optional dict from _prefetch_analytics(); avoids redundant API calls.
    """
    for rec in store.get_rep_history(owner_id):
        if rec["year"] == year and rec["month"] == month:
            return False

    data_period, coverage_period = _periods_for(year, month)
    metrics     = collect_rep_snapshot(owner_id, data_period, coverage_period,
                                       prefetched=prefetched)
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


def generate_and_save_team(year, month, prefetched=None):
    """Generate and persist a locked monthly summary for the whole team.

    Returns True if saved, False if already locked for this month.
    prefetched — optional dict from _prefetch_analytics(); avoids redundant API calls.
    """
    for rec in store.get_team_history():
        if rec["year"] == year and rec["month"] == month:
            return False

    data_period, coverage_period = _periods_for(year, month)
    metrics     = collect_team_snapshot(data_period, coverage_period,
                                        prefetched=prefetched)
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
    Fetches all HubSpot analytics data once and distributes to each rep/team
    snapshot to avoid N×7 redundant API calls during a backfill.

    Returns {"team": bool, "reps": {owner_id: bool}}.
    """
    owners  = get_owners()
    allowed = get_scoped_team_owner_ids(_month_scope_end(year, month))
    # Grace reps (departed but still in analytics through month-end) must be
    # included even though they are no longer in the HubSpot team filter.
    grace_ids = store.get_grace_rep_ids()
    effective_allowed = allowed | grace_ids if allowed else allowed

    data_period, coverage_period = _periods_for(year, month)
    prefetched = _prefetch_analytics(data_period, coverage_period)

    rep_results = {}
    for oid, info in owners.items():
        if effective_allowed and oid not in effective_allowed:
            continue
        label = info.get("last_name") or info.get("name", oid)
        try:
            rep_results[oid] = generate_and_save_rep(oid, label, year, month,
                                                     prefetched=prefetched)
        except Exception:
            rep_results[oid] = False

    team_result = False
    try:
        team_result = generate_and_save_team(year, month, prefetched=prefetched)
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
