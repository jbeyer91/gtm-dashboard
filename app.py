import os
from functools import wraps
from dotenv import load_dotenv

load_dotenv()

from flask import (
    Flask, render_template, request, redirect, url_for,
    session, jsonify, abort
)
import analytics
from cache_utils import clear_cache, last_refreshed_str

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

COVERAGE_PERIODS = [
    ("this_month", "This Month"),
    ("next_month", "Next Month"),
]

SOURCES = ["All", "Cold outreach", "Inbound", "Referral", "Conference"]

NAV = [
    {"type": "link",  "endpoint": "call_stats",       "label": "Call Stats"},
    {"type": "group", "label": "Pipeline", "children": [
        {"endpoint": "pipeline_generated", "label": "Pipeline Generated"},
        {"endpoint": "pipeline_coverage",  "label": "Pipeline Coverage"},
    ]},
    {"type": "group", "label": "Deals", "children": [
        {"endpoint": "deals_won",       "label": "Won"},
        {"endpoint": "deals_lost",      "label": "Lost"},
        {"endpoint": "deal_advancement","label": "Stage Advancement"},
    ]},
    {"type": "link",  "endpoint": "inbound_funnel",    "label": "Inbound Funnel"},
    {"type": "link",  "endpoint": "book_coverage",     "label": "Book Coverage"},
]


@app.context_processor
def inject_cache_info():
    """Make last_refreshed available in every template automatically."""
    return {"last_refreshed": last_refreshed_str()}


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
    return redirect(url_for("call_stats"))


@app.route("/call-stats")
@login_required
def call_stats():
    period = request.args.get("period", "last_90")
    try:
        data = analytics.compute_call_stats(period)
    except Exception as e:
        return render_template("error.html", message=str(e), nav=NAV, active="call_stats")
    return render_template("call_stats.html", data=data, periods=PERIODS, period=period, nav=NAV, active="call_stats")


@app.route("/pipeline-generated")
@login_required
def pipeline_generated():
    period = request.args.get("period", "this_month")
    try:
        data = analytics.compute_pipeline_generated(period)
    except Exception as e:
        return render_template("error.html", message=str(e), nav=NAV, active="pipeline_generated")
    return render_template("pipeline_generated.html", data=data, periods=PERIODS, period=period, nav=NAV, active="pipeline_generated")


@app.route("/pipeline-coverage")
@login_required
def pipeline_coverage():
    period = request.args.get("period", "this_month")
    try:
        data = analytics.compute_pipeline_coverage(period)
    except Exception as e:
        return render_template("error.html", message=str(e), nav=NAV, active="pipeline_coverage")
    return render_template("pipeline_coverage.html", data=data, periods=COVERAGE_PERIODS, period=period, nav=NAV, active="pipeline_coverage")


@app.route("/deal-advancement")
@login_required
def deal_advancement():
    period = request.args.get("period", "last_90")
    source = request.args.get("source", "All")
    try:
        data = analytics.compute_deal_advancement(period, source)
    except Exception as e:
        return render_template("error.html", message=str(e), nav=NAV, active="deal_advancement")
    return render_template("deal_advancement.html", data=data, periods=PERIODS, period=period, sources=SOURCES, source=source, nav=NAV, active="deal_advancement")


@app.route("/deals-won")
@login_required
def deals_won():
    period = request.args.get("period", "this_month")
    source = request.args.get("source", "All")
    try:
        data = analytics.compute_deals_won(period, source)
    except Exception as e:
        return render_template("error.html", message=str(e), nav=NAV, active="deals_won")
    return render_template("deals_won.html", data=data, periods=PERIODS, period=period, sources=SOURCES, source=source, nav=NAV, active="deals_won")


@app.route("/deals-lost")
@login_required
def deals_lost():
    period = request.args.get("period", "this_month")
    try:
        data = analytics.compute_deals_lost(period)
    except Exception as e:
        return render_template("error.html", message=str(e), nav=NAV, active="deals_lost")
    return render_template("deals_lost.html", data=data, periods=PERIODS, period=period, nav=NAV, active="deals_lost")


@app.route("/inbound-funnel")
@login_required
def inbound_funnel():
    period = request.args.get("period", "this_month")
    try:
        data = analytics.compute_inbound_funnel(period)
    except Exception as e:
        return render_template("error.html", message=str(e), nav=NAV, active="inbound_funnel")
    return render_template("inbound_funnel.html", data=data, periods=PERIODS, period=period, nav=NAV, active="inbound_funnel")


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


# ── Background cache scheduler ───────────────────────────────────────────────
# Guard against Flask's dev-reloader starting the thread twice.
# In production (Gunicorn) this block always runs once per worker.
import cache_scheduler
if not app.debug or os.environ.get("WERKZEUG_RUN_MAIN") == "true":
    cache_scheduler.start(initial_delay_s=0)  # warm cache immediately on boot


if __name__ == "__main__":
    app.run(debug=True, port=5001)
