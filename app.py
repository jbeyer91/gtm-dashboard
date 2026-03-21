import os
from functools import wraps
from dotenv import load_dotenv

load_dotenv()

from flask import (
    Flask, render_template, request, redirect, url_for,
    session, jsonify, abort, Response
)
import csv, io
import analytics
from cache_utils import clear_cache, last_refreshed_str, last_refreshed_ts
from hubspot import get_prior_range

app = Flask(__name__)
app.secret_key = os.environ.get("SECRET_KEY", "dev-secret-key")

DASHBOARD_PASSWORD = os.environ.get("DASHBOARD_PASSWORD", "belfry2026")

PERIODS = [
    ("this_month", "This Month"),
    ("last_month", "Last Month"),
    ("last_30", "Last 30 Days"),
    ("last_90", "Last 90 Days"),
    ("this_quarter", "This Quarter"),
    ("last_quarter", "Last Quarter"),
    ("ytd", "Year to Date"),
]

FORECAST_PERIODS = [
    ("this_month", "This Month"),
    ("last_month", "Last Month"),
    ("this_quarter", "This Quarter"),
    ("last_quarter", "Last Quarter"),
    ("ytd", "Year to Date"),
]

CALL_STATS_PERIODS = [
    ("today", "Today"),
    ("this_week", "This Week"),
    ("this_month", "This Month"),
    ("last_month", "Last Month"),
    ("last_30", "Last 30 Days"),
    ("last_90", "Last 90 Days"),
    ("this_quarter", "This Quarter"),
    ("last_quarter", "Last Quarter"),
    ("ytd", "Year to Date"),
]

COVERAGE_PERIODS = [
    ("this_month", "This Month"),
    ("next_month", "Next Month"),
]

SOURCES = ["All", "Cold outreach", "Inbound", "Referral", "Conference"]

NAV = [
    {"type": "link",  "endpoint": "scorecard",         "label": "Scorecard"},
    {"type": "group", "label": "Deals", "children": [
        {"endpoint": "deals_won",       "label": "Won"},
        {"endpoint": "deals_lost",      "label": "Lost"},
        {"endpoint": "deal_advancement","label": "Stage Advancement"},
        {"endpoint": "forecast",        "label": "Forecast"},
    ]},
    {"type": "group", "label": "Pipeline", "children": [
        {"endpoint": "pipeline_generated", "label": "Pipeline Generated"},
        {"endpoint": "pipeline_coverage",  "label": "Pipeline Coverage"},
    ]},
    {"type": "link",  "endpoint": "book_coverage",     "label": "Account Coverage"},
    {"type": "link",  "endpoint": "call_stats",        "label": "Calls"},
    {"type": "link",  "endpoint": "inbound_funnel",    "label": "Inbound Funnel"},
]


@app.context_processor
def inject_cache_info():
    """Make last_refreshed available in every template automatically."""
    import time
    ts = last_refreshed_ts()
    stale = ts > 0 and (time.time() - ts) > 7200  # >2 hours = stale
    return {"last_refreshed": last_refreshed_str(), "cache_stale": stale}


def login_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        if not session.get("authenticated"):
            return redirect(url_for("login", next=request.url))
        return f(*args, **kwargs)
    return decorated


@app.route("/login", methods=["GET", "POST"])
def login():
    error = None
    if request.method == "POST":
        if request.form.get("password") == DASHBOARD_PASSWORD:
            session["authenticated"] = True
            next_url = request.args.get("next") or url_for("index")
            return redirect(next_url)
        error = "Incorrect password."
    return render_template("login.html", error=error)


@app.route("/logout")
def logout():
    session.clear()
    return redirect(url_for("login"))


@app.route("/refresh-cache", methods=["POST"])
@login_required
def refresh_cache():
    """Bust the server-side cache so the next page load fetches fresh HubSpot data."""
    clear_cache()
    return redirect(request.referrer or url_for("index"))


@app.route("/")
@login_required
def index():
    return redirect(url_for("scorecard"))


def _prior(period, fn, *args):
    """Call analytics fn with 'prior_<period>' and return (prior_data, prior_label).
    Returns (None, '') for periods without a meaningful prior (e.g. next_month).
    """
    if period in ("next_month",):
        return None, ""
    try:
        prior_data = fn("prior_" + period, *args)
        _, _, label = get_prior_range(period)
        return prior_data, label
    except Exception:
        return None, ""


def _d(cur, pri, key, scale=1):
    """Delta between cur and prior totals for a key; 0 if prior is missing."""
    if pri is None:
        return None
    return round((cur.get(key, 0) or 0) - (pri.get(key, 0) or 0), 1) * scale


@app.route("/scorecard")
@login_required
def scorecard():
    from datetime import datetime, timezone
    try:
        data  = analytics.compute_scorecard("this_month")
        t     = data["team"]
        _, _, prior_label = get_prior_range("this_month")
        prior_data, _ = _prior("this_month", analytics.compute_scorecard)
        lt = (prior_data or {}).get("team") or {}
        def _delta(key): return round((t.get(key) or 0) - (lt.get(key) or 0), 1)
        deltas = {
            "attain_pct":    _delta("attain_pct"),
            "deals_created": _delta("deals_created"),
            "avg_dials":     _delta("avg_dials"),
            "connect_rate":  _delta("connect_rate"),
            "stale_count":   _delta("stale_count"),
        }
    except Exception as e:
        return render_template("error.html", message=str(e), nav=NAV, active="scorecard")
    month_label = datetime.now(timezone.utc).strftime("%B %Y")
    return render_template("scorecard.html", data=data, month_label=month_label,
                           deltas=deltas, prior_label=prior_label,
                           active="scorecard", nav=NAV)


@app.route("/call-stats")
@login_required
def call_stats():
    period = request.args.get("period", "last_90")
    try:
        data = analytics.compute_call_stats(period)
        prior_data, prior_label = _prior(period, analytics.compute_call_stats)
        t  = data["totals"]
        pt = (prior_data or {}).get("totals")
        deltas = {
            "dials":            _d(t, pt, "dials"),
            "pct_connect":      _d(t, pt, "pct_connect"),
            "pct_conversation": _d(t, pt, "pct_conversation"),
            "ob_deals":         _d(t, pt, "outbound_deals_created"),
            "deals_s2":         _d(t, pt, "outbound_deals_to_s2"),
        }
    except Exception as e:
        return render_template("error.html", message=str(e), nav=NAV, active="call_stats")
    return render_template("call_stats.html", data=data, periods=CALL_STATS_PERIODS,
                           period=period, deltas=deltas, prior_label=prior_label,
                           nav=NAV, active="call_stats")


@app.route("/pipeline-generated")
@login_required
def pipeline_generated():
    period = request.args.get("period", "this_month")
    try:
        data = analytics.compute_pipeline_generated(period)
        prior_data, prior_label = _prior(period, analytics.compute_pipeline_generated)
        t  = data["totals"]
        pt = (prior_data or {}).get("totals")
        deltas = {
            "total_amt":         _d(t, pt, "total_amt"),
            "total_n":           _d(t, pt, "total_n"),
            "total_acv":         _d(t, pt, "total_acv"),
            "cold_outreach_amt": _d(t, pt, "cold_outreach_amt"),
            "inbound_amt":       _d(t, pt, "inbound_amt"),
        }
    except Exception as e:
        return render_template("error.html", message=str(e), nav=NAV, active="pipeline_generated")
    return render_template("pipeline_generated.html", data=data, periods=PERIODS,
                           period=period, deltas=deltas, prior_label=prior_label,
                           nav=NAV, active="pipeline_generated")


@app.route("/pipeline-coverage")
@login_required
def pipeline_coverage():
    period = request.args.get("period", "this_month")
    try:
        data = analytics.compute_pipeline_coverage(period)
        prior_data, prior_label = _prior(period, analytics.compute_pipeline_coverage)
        t  = data["totals"]
        pt = (prior_data or {}).get("totals")
        deltas = {
            "s1_amt":  _d(t, pt, "s1_amt"),
            "s2_amt":  _d(t, pt, "s2_amt"),
            "s3_amt":  _d(t, pt, "s3_amt"),
            "s4_amt":  _d(t, pt, "s4_amt"),
            "won_amt": _d(t, pt, "won_amt"),
        }
    except Exception as e:
        return render_template("error.html", message=str(e), nav=NAV, active="pipeline_coverage")
    return render_template("pipeline_coverage.html", data=data, periods=COVERAGE_PERIODS,
                           period=period, deltas=deltas, prior_label=prior_label,
                           nav=NAV, active="pipeline_coverage")


@app.route("/deal-advancement")
@login_required
def deal_advancement():
    period = request.args.get("period", "last_90")
    source = request.args.get("source", "All")
    try:
        data = analytics.compute_deal_advancement(period, source)
        prior_data, prior_label = _prior(period, analytics.compute_deal_advancement, source)
        t  = data["totals"]
        pt = (prior_data or {}).get("totals")
        deltas = {
            "created": _d(t, pt, "created"),
            "to_s2":   _d(t, pt, "to_s2"),
            "to_s3":   _d(t, pt, "to_s3"),
            "to_s4":   _d(t, pt, "to_s4"),
            "won":     _d(t, pt, "won"),
            "lost":    _d(t, pt, "lost"),
        }
    except Exception as e:
        return render_template("error.html", message=str(e), nav=NAV, active="deal_advancement")
    return render_template("deal_advancement.html", data=data, periods=PERIODS,
                           period=period, sources=SOURCES, source=source,
                           deltas=deltas, prior_label=prior_label,
                           nav=NAV, active="deal_advancement")


@app.route("/deals-won")
@login_required
def deals_won():
    period = request.args.get("period", "this_month")
    source = request.args.get("source", "All")
    try:
        data = analytics.compute_deals_won(period, source)
        prior_data, prior_label = _prior(period, analytics.compute_deals_won, source)
        t  = data["totals"]
        pt = (prior_data or {}).get("totals")
        deltas = {
            "total_won_amt": _d(t, pt, "total_won_amt"),
            "total_won_n":   _d(t, pt, "total_won_n"),
            "acv":           _d(t, pt, "acv"),
            "win_rate":      _d(t, pt, "win_rate"),
        }
    except Exception as e:
        return render_template("error.html", message=str(e), nav=NAV, active="deals_won")
    return render_template("deals_won.html", data=data, periods=PERIODS,
                           period=period, sources=SOURCES, source=source,
                           deltas=deltas, prior_label=prior_label,
                           nav=NAV, active="deals_won")


@app.route("/deals-lost")
@login_required
def deals_lost():
    period = request.args.get("period", "this_month")
    try:
        data = analytics.compute_deals_lost(period)
        prior_data, prior_label = _prior(period, analytics.compute_deals_lost)
        t  = data["totals"]
        pt = (prior_data or {}).get("totals")
        deltas = {
            "total":        _d(t, pt, "total"),
            "cost":         _d(t, pt, "cost"),
            "never_demoed": _d(t, pt, "never_demoed"),
            "timeline":     _d(t, pt, "timeline"),
        }
    except Exception as e:
        return render_template("error.html", message=str(e), nav=NAV, active="deals_lost")
    return render_template("deals_lost.html", data=data, periods=PERIODS,
                           period=period, deltas=deltas, prior_label=prior_label,
                           nav=NAV, active="deals_lost")


@app.route("/forecast")
@login_required
def forecast():
    period = request.args.get("period", "this_quarter")
    try:
        data = analytics.compute_forecast(period)
        prior_data, prior_label = _prior(period, analytics.compute_forecast)
        t  = data["totals"]
        pt = (prior_data or {}).get("totals")
        deltas = {
            "won_amt":       _d(t, pt, "won_amt"),
            "attain_pct":    _d(t, pt, "attain_pct"),
            "submitted_amt": _d(t, pt, "submitted_amt"),
        }
    except Exception as e:
        return render_template("error.html", message=str(e), nav=NAV, active="forecast")
    return render_template("forecast.html", data=data, periods=FORECAST_PERIODS,
                           period=period, deltas=deltas, prior_label=prior_label,
                           nav=NAV, active="forecast")


@app.route("/inbound-funnel")
@login_required
def inbound_funnel():
    period = request.args.get("period", "this_month")
    try:
        data = analytics.compute_inbound_funnel(period)
        prior_data, prior_label = _prior(period, analytics.compute_inbound_funnel)
        t  = data["totals"]
        pt = (prior_data or {}).get("totals")
        deltas = {
            "leads_created":     _d(t, pt, "leads_created"),
            "deal_creation_pct": _d(t, pt, "deal_creation_pct"),
            "deals_created":     _d(t, pt, "deals_created"),
            "win_rate_pct":      _d(t, pt, "win_rate_pct"),
            "won_amt":           _d(t, pt, "won_amt"),
        }
    except Exception as e:
        return render_template("error.html", message=str(e), nav=NAV, active="inbound_funnel")
    return render_template("inbound_funnel.html", data=data, periods=PERIODS,
                           period=period, deltas=deltas, prior_label=prior_label,
                           nav=NAV, active="inbound_funnel")


@app.route("/book-coverage")
@login_required
def book_coverage():
    try:
        data = analytics.compute_book_coverage()
    except Exception as e:
        return render_template("error.html", message=str(e), nav=NAV, active="book_coverage")
    return render_template("book_coverage.html", data=data, nav=NAV, active="book_coverage")


@app.route("/api/cache/clear", methods=["POST"])
@login_required
def api_cache_clear():
    """Bust the entire in-memory + disk cache so the next request fetches fresh data."""
    clear_cache()
    return jsonify({"status": "ok", "message": "Cache cleared. Fresh data will be fetched on next page load."})


@app.route("/api/debug/deals-won")
@login_required
def debug_deals_won():
    """Show the raw date range and deal counts used by the Deals Won view."""
    from hubspot import get_date_range, get_deals
    period = request.args.get("period", "last_quarter")
    start, end = get_date_range(period, _force=True)
    raw_deals = get_deals(start, end, "closedate", _force=True)
    won = [d for d in raw_deals if d["properties"].get("hs_is_closed_won") == "true"]
    lost = [d for d in raw_deals if d["properties"].get("hs_is_closed_lost") == "true"]
    return jsonify({
        "period": period,
        "window_start": start.isoformat(),
        "window_end": end.isoformat(),
        "total_deals_in_range": len(raw_deals),
        "won_deals": len(won),
        "lost_deals": len(lost),
        "sample_won": [
            {"id": d["id"], "closedate": d["properties"].get("closedate"), "amount": d["properties"].get("amount")}
            for d in won[:5]
        ],
    })


@app.route("/api/debug/lost-reasons")
@login_required
def debug_lost_reasons():
    """Find the correct HubSpot property name for closed lost reasons."""
    import requests as req
    from hubspot import BASE_URL, HEADERS, get_date_range
    period = request.args.get("period", "this_month")
    start, end = get_date_range(period)

    # Step 1: find all deal properties whose name or label contains "reason" or "lost"
    props_resp = req.get(f"{BASE_URL}/crm/v3/properties/deals?limit=500", headers=HEADERS)
    candidate_props = []
    if props_resp.ok:
        for p in props_resp.json().get("results", []):
            name = p.get("name", "")
            label = p.get("label", "")
            if any(kw in name.lower() or kw in label.lower() for kw in ("reason", "lost", "loss")):
                candidate_props.append({"name": name, "label": label, "type": p.get("type")})

    # Step 2: fetch 3 sample lost deals with those candidate property names
    prop_names = [p["name"] for p in candidate_props]
    payload = {
        "filterGroups": [{"filters": [
            {"propertyName": "pipeline",         "operator": "EQ",  "value": "31544320"},
            {"propertyName": "hs_is_closed_lost", "operator": "EQ",  "value": "true"},
            {"propertyName": "closedate",         "operator": "GTE", "value": str(int(start.timestamp() * 1000))},
            {"propertyName": "closedate",         "operator": "LTE", "value": str(int(end.timestamp() * 1000))},
        ]}],
        "properties": prop_names,
        "limit": 3,
    }
    deals_resp = req.post(f"{BASE_URL}/crm/v3/objects/deals/search", headers=HEADERS, json=payload)
    samples = []
    if deals_resp.ok:
        for deal in deals_resp.json().get("results", []):
            populated = {k: v for k, v in deal.get("properties", {}).items() if v and k in prop_names}
            samples.append({"id": deal["id"], "populated_reason_props": populated})

    return jsonify({"candidate_properties": candidate_props, "sample_deals": samples})


@app.route("/api/debug/company-properties")
@login_required
def debug_company_properties():
    """List company properties whose label or name matches coverage-related keywords."""
    import requests
    from hubspot import BASE_URL, HEADERS
    resp = requests.get(f"{BASE_URL}/crm/v3/properties/companies?limit=500", headers=HEADERS)
    if not resp.ok:
        return jsonify({"error": resp.status_code, "body": resp.text}), resp.status_code
    keywords = {"sequence", "activity", "called", "overdue", "task", "icp", "rank"}
    matches = []
    for p in resp.json().get("results", []):
        label = p.get("label", "").lower()
        name  = p.get("name", "").lower()
        if any(k in label or k in name for k in keywords):
            matches.append({"name": p["name"], "label": p["label"], "type": p.get("type")})
    matches.sort(key=lambda x: x["label"])
    return jsonify(matches)


@app.route("/api/debug/inbound-funnel")
@login_required
def debug_inbound_funnel():
    """Show all non-null properties on sample list-1082 contacts to find field names."""
    import requests as req
    from hubspot import BASE_URL, HEADERS

    # Fetch a few members from list 1082
    list_resp = req.get(
        f"{BASE_URL}/crm/v3/lists/1082/memberships?limit=5",
        headers=HEADERS,
    )
    if not list_resp.ok:
        return jsonify({"error": list_resp.text})

    member_ids = [str(r["recordId"]) for r in list_resp.json().get("results", [])]
    if not member_ids:
        return jsonify({"error": "no members in list"})

    # Read ALL properties so we can find the demo request date + first sales activity fields
    props_resp = req.get(
        f"{BASE_URL}/crm/v3/properties/contacts?limit=1000",
        headers=HEADERS,
    )
    all_prop_names = [p["name"] for p in props_resp.json().get("results", [])] if props_resp.ok else []

    batch_resp = req.post(
        f"{BASE_URL}/crm/v3/objects/contacts/batch/read",
        headers=HEADERS,
        json={"inputs": [{"id": cid} for cid in member_ids[:3]], "properties": all_prop_names},
    )
    samples = []
    if batch_resp.ok:
        for r in batch_resp.json().get("results", []):
            samples.append({k: v for k, v in r["properties"].items() if v and v != "false"})

    # Also surface any property whose label contains "demo" or "sales activity"
    matching_props = [
        {"name": p["name"], "label": p["label"]}
        for p in (props_resp.json().get("results", []) if props_resp.ok else [])
        if any(kw in (p.get("label") or "").lower() for kw in ["demo", "sales activity", "first sales"])
    ]

    return jsonify({"matching_props": matching_props, "sample_contact_props": samples})


@app.route("/api/debug/lifecyclestage-values")
@login_required
def debug_lifecyclestage_values():
    """Show lifecyclestage values from actual list-1082 contacts for last 30 days."""
    from hubspot import get_list_contacts, get_date_range
    from collections import Counter
    start, end = get_date_range("last_30")
    contacts = get_list_contacts(1082, start, end)
    values = Counter(c["properties"].get("lifecyclestage") or "NULL" for c in contacts)
    return jsonify({"total": len(contacts), "lifecyclestage_counts": dict(values)})


@app.route("/api/debug/deal-sources")
@login_required
def debug_deal_sources():
    """Show the raw deal_source values coming from HubSpot for this month."""
    from hubspot import get_date_range, get_deals
    from collections import Counter
    start, end = get_date_range("last_90")
    deals = get_deals(start, end, "createdate")
    counts = Counter(
        (d["properties"].get("deal_source") or "").strip() or "(blank)"
        for d in deals
    )
    return jsonify({"total_deals": len(deals), "deal_source_values": dict(counts.most_common())})


@app.route("/api/debug/teams")
@login_required
def debug_teams():
    """Diagnostic endpoint — shows owner team membership and resolved filter."""
    import requests
    from hubspot import BASE_URL, HEADERS, TEAM_FILTER, get_team_owner_ids

    resp = requests.get(
        f"{BASE_URL}/crm/v3/owners?limit=200&includeTeams=true",
        headers=HEADERS,
    )

    owners_with_teams = []
    if resp.ok:
        for o in resp.json().get("results", []):
            name = f"{o.get('firstName','')} {o.get('lastName','')}".strip()
            team_names = [t.get("name") for t in o.get("teams", [])]
            owners_with_teams.append({
                "owner_id":      str(o["id"]),
                "name":          name,
                "teams":         team_names,
                "in_filter":     any(t in TEAM_FILTER for t in team_names),
            })

    allowed = get_team_owner_ids()
    return jsonify({
        "team_filter":        list(TEAM_FILTER),
        "resolved_count":     len(allowed),
        "resolved_owner_ids": sorted(allowed),
        "owners_api_ok":      resp.ok,
        "owners_api_status":  resp.status_code,
        "owners_with_teams":  owners_with_teams,
    })


@app.route("/api/debug/quotas")
@login_required
def debug_quotas():
    """Show the raw goal_target records returned by HubSpot so we can inspect
    datetime field formats and amounts before any pro-rating is applied."""
    import requests
    from datetime import timezone
    from hubspot import BASE_URL, HEADERS, get_owners, get_date_range, _parse_hs_datetime

    period = request.args.get("period", "this_month")
    start, end = get_date_range(period)

    start_ts = str(int(start.timestamp() * 1000))
    end_ts   = str(int(end.timestamp() * 1000))

    payload = {
        "filterGroups": [{"filters": [
            {"propertyName": "hs_end_datetime",   "operator": "GTE", "value": start_ts},
            {"propertyName": "hs_start_datetime", "operator": "LTE", "value": end_ts},
        ]}],
        "properties": [
            "hs_goal_name", "hs_target_amount",
            "hs_start_datetime", "hs_end_datetime", "hs_assignee_user_id",
        ],
        "limit": 200,
    }

    resp = requests.post(
        f"{BASE_URL}/crm/v3/objects/goal_targets/search",
        headers=HEADERS,
        json=payload,
    )

    owners = get_owners()
    user_id_to_name = {
        v["user_id"]: v.get("name", v["user_id"])
        for v in owners.values() if v.get("user_id")
    }

    window_secs = (end - start).total_seconds()
    raw_goals = []
    for goal in (resp.json().get("results", []) if resp.ok else []):
        props = goal.get("properties", {})
        uid   = str(props.get("hs_assignee_user_id") or "")
        start_raw = props.get("hs_start_datetime")
        end_raw   = props.get("hs_end_datetime")
        amount_raw = props.get("hs_target_amount")

        # Attempt to parse and show pro-rate calculation
        calc = "parse_error"
        try:
            goal_start_dt = _parse_hs_datetime(start_raw)
            goal_end_dt   = _parse_hs_datetime(end_raw)
            goal_secs = max((goal_end_dt - goal_start_dt).total_seconds(), 1)
            overlap_secs = max((min(end, goal_end_dt) - max(start, goal_start_dt)).total_seconds(), 0)
            amount = float(amount_raw or 0)
            if goal_secs > window_secs * 2:
                prorated = amount * (overlap_secs / goal_secs)
                calc = f"PRO-RATED: {amount} × ({overlap_secs/86400:.1f}d / {goal_secs/86400:.1f}d) = {prorated:.2f}"
            else:
                calc = f"FULL: {amount} (goal {goal_secs/86400:.1f}d ≤ 2× window {window_secs/86400:.1f}d)"
        except Exception as ex:
            calc = f"parse_error: {ex}"

        raw_goals.append({
            "owner_name":           user_id_to_name.get(uid, f"uid:{uid}"),
            "hs_goal_name":         props.get("hs_goal_name"),
            "hs_target_amount":     amount_raw,
            "hs_start_datetime_raw": start_raw,
            "hs_end_datetime_raw":   end_raw,
            "calculation":          calc,
        })

    return jsonify({
        "period":          period,
        "window_start":    start.isoformat(),
        "window_end":      end.isoformat(),
        "window_days":     round(window_secs / 86400, 1),
        "api_status":      resp.status_code,
        "goal_count":      len(raw_goals),
        "goals":           raw_goals,
    })


@app.route("/api/debug/forecast-submissions")
@login_required
def debug_forecast_submissions():
    """Show raw forecast submission objects to help identify the correct amount property."""
    from hubspot import get_forecast_submissions
    subs = get_forecast_submissions()
    # Return all properties from the first few records so we can see what's available
    sample = [s.get("properties", {}) for s in subs[:20]]
    return jsonify({"total": len(subs), "sample": sample})


@app.route("/api/debug/icp-rank-values")
@login_required
def debug_icp_rank_values():
    """Show distinct icp_rank values on a sample of companies to diagnose blank A+ to C counts."""
    from hubspot import get_companies_for_coverage
    companies = get_companies_for_coverage()
    from collections import Counter
    rank_counts = Counter()
    for c in companies:
        rank = (c["properties"].get("icp_rank") or "").strip()
        rank_counts[rank if rank else "(blank)"] += 1
    return jsonify({"total_companies": len(companies), "icp_rank_distribution": dict(rank_counts)})


# ── CSV export routes ────────────────────────────────────────────────────────

@app.route("/scorecard/export.csv")
@login_required
def scorecard_csv():
    from datetime import datetime, timezone
    data = analytics.compute_scorecard("this_month")
    month = datetime.now(timezone.utc).strftime("%Y-%m")
    out = io.StringIO()
    w = csv.writer(out)
    w.writerow(["Rep", "Grade", "Quota %", "Won $", "Quota $", "Deals Created",
                "$ to Stage 2", "Stage 2 Target $", "Avg Dials/Day", "Connect %", "Stale Accounts", "Total Accounts"])
    for r in data["rows"]:
        w.writerow([
            r["ae"], r["grade"],
            f"{r['attain_pct']:.1f}%",
            r["won_amt"], r["quota_amt"],
            r["deals_created"],
            r["s2_amt"], r["s2_target"],
            r["avg_dials"],
            f"{r['connect_rate']:.1f}%",
            r["stale_count"], r["ac_accounts"],
        ])
    t = data["team"]
    w.writerow([
        "TOTAL", "",
        f"{t['attain_pct']:.1f}%",
        t["won_amt"], t["quota_amt"],
        t["deals_created"],
        t["s2_amt"], t["s2_target"],
        t["avg_dials"],
        f"{t['connect_rate']:.1f}%",
        t["stale_count"], t["ac_accounts"],
    ])
    return Response(
        out.getvalue(),
        mimetype="text/csv",
        headers={"Content-Disposition": f"attachment; filename=scorecard-{month}.csv"},
    )


@app.route("/call-stats/export.csv")
@login_required
def call_stats_csv():
    period = request.args.get("period", "last_90")
    data = analytics.compute_call_stats(period)
    out = io.StringIO()
    w = csv.writer(out)
    w.writerow(["Rep", "Dials", "Avg Dials/Day", "Connects", "Connect %",
                "Conversations", "Convo %", "OB Deals Created", "Dial→Deal %", "OB Deals to Stage 2"])
    for r in data["rows"]:
        w.writerow([
            r["ae"], r["dials"], r["avg_dials_per_day"],
            r["connects"], f"{r['pct_connect']:.1f}%",
            r["conversations"], f"{r['pct_conversation']:.1f}%",
            r["outbound_deals_created"], f"{r['pct_deals']:.1f}%",
            r["outbound_deals_to_s2"],
        ])
    t = data["totals"]
    w.writerow([
        "TOTAL", t["dials"], t["avg_dials_per_day"],
        t["connects"], f"{t['pct_connect']:.1f}%",
        t["conversations"], f"{t['pct_conversation']:.1f}%",
        t["outbound_deals_created"], f"{t['pct_deals']:.1f}%",
        t["outbound_deals_to_s2"],
    ])
    return Response(
        out.getvalue(),
        mimetype="text/csv",
        headers={"Content-Disposition": f"attachment; filename=call-stats-{period}.csv"},
    )


@app.route("/deals-won/export.csv")
@login_required
def deals_won_csv():
    period = request.args.get("period", "this_month")
    source = request.args.get("source", "All")
    data = analytics.compute_deals_won(period, source)
    out = io.StringIO()
    w = csv.writer(out)
    w.writerow(["Rep", "Won $", "Won Deals", "Lost Deals", "Win Rate %", "ACV $",
                "Quota $", "Attain %", "Cold Outbound $", "Cold Outbound #",
                "Inbound $", "Inbound #", "Conference $", "Conference #", "Referral $", "Referral #"])
    for r in data["rows"]:
        w.writerow([
            r["ae"], r["total_won_amt"], r["total_won_n"], r["total_lost_n"],
            f"{r['win_rate']:.1f}%", round(r["acv"]),
            r["quota_amt"],
            f"{r['attain_pct']:.1f}%" if r["attain_pct"] is not None else "",
            r["cold_amt"], r["cold_n"],
            r["inbound_amt"], r["inbound_n"],
            r["conf_amt"], r["conf_n"],
            r["ref_amt"], r["ref_n"],
        ])
    t = data["totals"]
    w.writerow([
        "TOTAL", t["total_won_amt"], t["total_won_n"], t["total_lost_n"],
        f"{t['win_rate']:.1f}%", round(t["acv"]),
        t["quota_amt"],
        f"{t['attain_pct']:.1f}%" if t["attain_pct"] is not None else "",
        t["cold_amt"], t["cold_n"],
        t["inbound_amt"], t["inbound_n"],
        t["conf_amt"], t["conf_n"],
        t["ref_amt"], t["ref_n"],
    ])
    return Response(
        out.getvalue(),
        mimetype="text/csv",
        headers={"Content-Disposition": f"attachment; filename=deals-won-{period}.csv"},
    )


@app.route("/pipeline-generated/export.csv")
@login_required
def pipeline_generated_csv():
    period = request.args.get("period", "this_month")
    data = analytics.compute_pipeline_generated(period)
    out = io.StringIO()
    w = csv.writer(out)
    w.writerow(["Rep", "Total Pipeline $", "Total Deals", "Total ACV $",
                "Cold Outreach $", "Cold Outreach #",
                "Inbound $", "Inbound #",
                "Conference $", "Conference #",
                "Referral $", "Referral #"])
    for r in data["rows"]:
        w.writerow([r["ae"], r["total_amt"], r["total_n"], round(r["total_acv"]),
                    r["cold_outreach_amt"], r["cold_outreach_n"],
                    r["inbound_amt"], r["inbound_n"],
                    r["conference_amt"], r["conference_n"],
                    r["referral_amt"], r["referral_n"]])
    t = data["totals"]
    w.writerow(["TOTAL", t["total_amt"], t["total_n"], round(t["total_acv"]),
                t["cold_outreach_amt"], t["cold_outreach_n"],
                t["inbound_amt"], t["inbound_n"],
                t["conference_amt"], t["conference_n"],
                t["referral_amt"], t["referral_n"]])
    return Response(out.getvalue(), mimetype="text/csv",
                    headers={"Content-Disposition": f"attachment; filename=pipeline-generated-{period}.csv"})


@app.route("/pipeline-coverage/export.csv")
@login_required
def pipeline_coverage_csv():
    period = request.args.get("period", "this_month")
    data = analytics.compute_pipeline_coverage(period)
    out = io.StringIO()
    w = csv.writer(out)
    w.writerow(["Rep", "S1 #", "S1 $", "S2 #", "S2 $", "S3 #", "S3 $", "S4 #", "S4 $", "Won #", "Won $"])
    for r in data["rows"]:
        w.writerow([r["ae"], r["s1_n"], r["s1_amt"], r["s2_n"], r["s2_amt"],
                    r["s3_n"], r["s3_amt"], r["s4_n"], r["s4_amt"], r["won_n"], r["won_amt"]])
    t = data["totals"]
    w.writerow(["TOTAL", t["s1_n"], t["s1_amt"], t["s2_n"], t["s2_amt"],
                t["s3_n"], t["s3_amt"], t["s4_n"], t["s4_amt"], t["won_n"], t["won_amt"]])
    return Response(out.getvalue(), mimetype="text/csv",
                    headers={"Content-Disposition": f"attachment; filename=pipeline-coverage-{period}.csv"})


@app.route("/deal-advancement/export.csv")
@login_required
def deal_advancement_csv():
    period = request.args.get("period", "last_90")
    source = request.args.get("source", "All")
    data = analytics.compute_deal_advancement(period, source)
    out = io.StringIO()
    w = csv.writer(out)
    w.writerow(["Rep", "Deals Created", "Advanced to S2", "Advanced to S3", "Advanced to S4", "Won", "Lost"])
    for r in data["rows"]:
        w.writerow([r["ae"], r["created"], r["to_s2"], r["to_s3"], r["to_s4"], r["won"], r["lost"]])
    t = data["totals"]
    w.writerow(["TOTAL", t["created"], t["to_s2"], t["to_s3"], t["to_s4"], t["won"], t["lost"]])
    return Response(out.getvalue(), mimetype="text/csv",
                    headers={"Content-Disposition": f"attachment; filename=deal-advancement-{period}.csv"})


@app.route("/deals-lost/export.csv")
@login_required
def deals_lost_csv():
    period = request.args.get("period", "this_month")
    data = analytics.compute_deals_lost(period)
    out = io.StringIO()
    w = csv.writer(out)
    w.writerow(["Rep", "Total Lost", "Cost/Price", "Never Demoed", "Timeline",
                "Stakeholder Issue", "Competitor", "Product Gap", "Other", "Value/ROI"])
    for r in data["rows"]:
        w.writerow([r["ae"], r["total"], r["cost"], r["never_demoed"], r["timeline"],
                    r["stakeholder_issue"], r["competitor"], r["product"], r["other"], r["value"]])
    t = data["totals"]
    w.writerow(["TOTAL", t["total"], t["cost"], t["never_demoed"], t["timeline"],
                t["stakeholder_issue"], t["competitor"], t["product"], t["other"], t["value"]])
    return Response(out.getvalue(), mimetype="text/csv",
                    headers={"Content-Disposition": f"attachment; filename=deals-lost-{period}.csv"})


@app.route("/forecast/export.csv")
@login_required
def forecast_csv():
    period = request.args.get("period", "this_quarter")
    data = analytics.compute_forecast(period)
    out = io.StringIO()
    w = csv.writer(out)
    w.writerow(["Rep", "Won $", "Commit $", "Commit #", "Submitted Forecast $",
                "Best Case $", "Best Case #", "Weighted $", "Quota $", "Gap $", "Attain %"])
    for r in data["rows"]:
        w.writerow([r["ae"], r["won_amt"], r["commit_amt"], r["commit_n"],
                    r["submitted_amt"] or "", r["bestcase_amt"], r["bestcase_n"],
                    r["weighted_amt"], r["quota_amt"], r["gap_amt"],
                    f"{r['attain_pct']:.1f}%" if r["attain_pct"] is not None else ""])
    t = data["totals"]
    w.writerow(["TOTAL", t["won_amt"], t["commit_amt"], t["commit_n"],
                t["submitted_amt"] or "", t["bestcase_amt"], t["bestcase_n"],
                t["weighted_amt"], t["quota_amt"], t["gap_amt"],
                f"{t['attain_pct']:.1f}%" if t["attain_pct"] is not None else ""])
    return Response(out.getvalue(), mimetype="text/csv",
                    headers={"Content-Disposition": f"attachment; filename=forecast-{period}.csv"})


@app.route("/book-coverage/export.csv")
@login_required
def book_coverage_csv():
    data = analytics.compute_book_coverage()
    out = io.StringIO()
    w = csv.writer(out)
    w.writerow(["Rep", "Total Accounts", "A+-C Accounts", "Active (30d)", "Called (120d)",
                "In Sequence", "Active %", "Called %", "In Seq %", "Overdue Tasks"])
    for r in data["rows"]:
        w.writerow([r["ae"], r["total_accounts"], r["ac_accounts"], r["active_30"],
                    r["called_120"], r["in_sequence"],
                    f"{r['pct_active_30']:.1f}%", f"{r['pct_called_120']:.1f}%",
                    f"{r['pct_in_sequence']:.1f}%", r["overdue_tasks"]])
    t = data["totals"]
    w.writerow(["TOTAL", t["total_accounts"], t["ac_accounts"], t["active_30"],
                t["called_120"], t["in_sequence"],
                f"{t['pct_active_30']:.1f}%", f"{t['pct_called_120']:.1f}%",
                f"{t['pct_in_sequence']:.1f}%", t["overdue_tasks"]])
    return Response(out.getvalue(), mimetype="text/csv",
                    headers={"Content-Disposition": "attachment; filename=book-coverage.csv"})


@app.route("/inbound-funnel/export.csv")
@login_required
def inbound_funnel_csv():
    period = request.args.get("period", "this_month")
    data = analytics.compute_inbound_funnel(period)
    out = io.StringIO()
    w = csv.writer(out)
    w.writerow(["Source", "Leads Created", "Disqualified", "Contacted", "DQ %",
                "Follow-up %", "Deals Created", "Deal Creation %", "Deals Lost",
                "Deals Won", "Win Rate %", "Pipeline $", "Won $", "Lost $", "ACV $"])
    for r in data["rows"]:
        w.writerow([r["source"], r["leads_created"], r["leads_disqualified"],
                    r["leads_contacted"], f"{r['dq_pct']:.1f}%", f"{r['follow_up_pct']:.1f}%",
                    r["deals_created"], f"{r['deal_creation_pct']:.1f}%",
                    r["deals_lost"], r["deals_won"], f"{r['win_rate_pct']:.1f}%",
                    r["pg_amt"], r["won_amt"], r["lost_amt"], round(r["acv_won"])])
    t = data["totals"]
    w.writerow(["TOTAL", t["leads_created"], t["leads_disqualified"],
                t["leads_contacted"], f"{t['dq_pct']:.1f}%", f"{t['follow_up_pct']:.1f}%",
                t["deals_created"], f"{t['deal_creation_pct']:.1f}%",
                t["deals_lost"], t["deals_won"], f"{t['win_rate_pct']:.1f}%",
                t["pg_amt"], t["won_amt"], t["lost_amt"], round(t["acv_won"])])
    return Response(out.getvalue(), mimetype="text/csv",
                    headers={"Content-Disposition": f"attachment; filename=inbound-funnel-{period}.csv"})


# ── Background cache scheduler ───────────────────────────────────────────────
# Guard against Flask's dev-reloader starting the thread twice.
# In production (Gunicorn) this block always runs once per worker.
import cache_scheduler
if not app.debug or os.environ.get("WERKZEUG_RUN_MAIN") == "true":
    cache_scheduler.start(initial_delay_s=0)  # warm cache immediately on boot


if __name__ == "__main__":
    app.run(debug=True, port=5001)
