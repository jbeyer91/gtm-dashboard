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
    ("last_60", "Last 60 Days"),
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
    ("call_stats", "Call Stats"),
    ("pipeline_generated", "Pipeline Generated"),
    ("pipeline_coverage", "Pipeline Coverage"),
    ("deal_advancement", "Deal Advancement"),
    ("deals_won", "Deals Won"),
    ("deals_lost", "Deals Lost"),
    ("inbound_funnel", "Inbound Funnel"),
    ("win_rate_by_source", "Win-Rate by Source"),
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


@app.route("/win-rate-by-source")
@login_required
def win_rate_by_source():
    period = request.args.get("period", "this_quarter")
    try:
        data = analytics.compute_win_rate_by_source(period)
    except Exception as e:
        return render_template("error.html", message=str(e), nav=NAV, active="win_rate_by_source")
    return render_template("win_rate_by_source.html", data=data, periods=PERIODS, period=period, nav=NAV, active="win_rate_by_source")


@app.route("/api/debug/teams")
@login_required
def debug_teams():
    """Diagnostic endpoint — shows raw HubSpot team data and resolved owner IDs."""
    import requests
    from hubspot import BASE_URL, HEADERS, TEAM_FILTER, get_team_owner_ids

    resp_owners = requests.get(f"{BASE_URL}/crm/v3/owners?limit=200", headers=HEADERS)
    resp_teams  = requests.get(f"{BASE_URL}/settings/v3/users/teams", headers=HEADERS)

    user_to_owner = {}
    if resp_owners.ok:
        for o in resp_owners.json().get("results", []):
            uid = o.get("userId")
            if uid:
                user_to_owner[str(uid)] = {"owner_id": str(o["id"]), "name": f"{o.get('firstName','')} {o.get('lastName','')}".strip()}

    # Summarise teams in a readable way
    teams_summary = []
    if resp_teams.ok:
        for t in resp_teams.json().get("results", []):
            teams_summary.append({
                "id":                t.get("id"),
                "name":              t.get("name"),
                "parentTeamId":      t.get("parentTeamId"),
                "userIds":           t.get("userIds", []),
                "secondaryUserIds":  t.get("secondaryUserIds", []),
                "name_in_filter":    t.get("name") in TEAM_FILTER,
            })

    return jsonify({
        "team_filter":        list(TEAM_FILTER),
        "resolved_owner_ids": sorted(get_team_owner_ids()),
        "resolved_count":     len(get_team_owner_ids()),
        "owners_api_ok":      resp_owners.ok,
        "teams_api_ok":       resp_teams.ok,
        "teams_api_status":   resp_teams.status_code,
        "teams_summary":      teams_summary,
        "user_to_owner_map":  user_to_owner,
    })


# ── Background cache scheduler ───────────────────────────────────────────────
# Guard against Flask's dev-reloader starting the thread twice.
# In production (Gunicorn) this block always runs once per worker.
import cache_scheduler
if not app.debug or os.environ.get("WERKZEUG_RUN_MAIN") == "true":
    cache_scheduler.start(initial_delay_s=0)  # warm cache immediately on boot


if __name__ == "__main__":
    app.run(debug=True, port=5001)
