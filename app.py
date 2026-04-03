import logging
import os
import time
from datetime import date
from functools import wraps
from dotenv import load_dotenv

load_dotenv()

log = logging.getLogger(__name__)

from flask import (
    Flask, render_template, request, redirect, url_for,
    session, jsonify, abort, Response, g
)
from authlib.integrations.flask_client import OAuth
import csv, io
import analytics
import calls_drilldown as calls_drilldown_bp
import monthly_store
from cache_utils import clear_cache, get_cached, last_refreshed_str, last_refreshed_ts, is_cached
from hubspot import get_prior_range, get_owners, OWNER_EXCLUDE, get_team_owner_ids, get_owner_team_map

ALLOWED_DOMAIN = "belfrysoftware.com"
ADMIN_EMAIL_ALLOWLIST = frozenset(
    email.strip().lower()
    for email in os.environ.get("ADMIN_EMAILS", "").split(",")
    if email.strip()
)
ADMIN_OWNER_ALLOWLIST = frozenset(
    owner_id.strip()
    for owner_id in os.environ.get("ADMIN_OWNER_IDS", "").split(",")
    if owner_id.strip()
)

app = Flask(__name__)
app.secret_key = os.environ.get("SECRET_KEY", "dev-secret-key")
app.register_blueprint(calls_drilldown_bp.bp)

oauth = OAuth(app)
oauth.register(
    name="google",
    client_id=os.environ.get("GOOGLE_CLIENT_ID"),
    client_secret=os.environ.get("GOOGLE_CLIENT_SECRET"),
    server_metadata_url="https://accounts.google.com/.well-known/openid-configuration",
    # default_timeout caps every HTTP call made by the OAuth2Session (discovery
    # fetch, token exchange, userinfo) at 10 seconds instead of blocking
    # indefinitely.  Note: Authlib 1.x uses "default_timeout", not "timeout" —
    # the latter is not a recognised OAuth2Session constructor parameter and
    # would be silently ignored.
    client_kwargs={"scope": "openid email profile", "default_timeout": 10},
)

# Pre-warm the OIDC discovery document so the first /login after a worker
# restart is a fast local cache lookup, not a 10-30s blocking HTTP call.
# load_server_metadata() sets server_metadata['_loaded_at']; all subsequent
# calls inside authorize_redirect() and fetch_access_token() return instantly.
# Wrapped in try/except: a network failure here is non-fatal — the app still
# starts and Authlib will re-attempt the fetch on the first real login request.
try:
    with app.app_context():
        oauth.google.load_server_metadata()
    log.info("OAuth OIDC metadata pre-warmed successfully")
except Exception as exc:
    log.warning("OAuth OIDC metadata pre-warm failed — first login after this "
                "restart may be slow: %s", exc)

PERIODS = [
    ("this_month", "This Month"),
    ("last_month", "Last Month"),
    ("last_30", "Last 30 Days"),
    ("last_90", "Last 90 Days"),
    ("this_quarter", "This Quarter"),
    ("last_quarter", "Last Quarter"),
    ("ytd", "Year to Date"),
]

# PERIODS + weekly options for pages where week-level granularity makes sense
DEAL_PERIODS = [
    ("this_week",  "This Week"),
    ("last_week",  "Last Week"),
    ("this_month", "This Month"),
    ("last_month", "Last Month"),
    ("last_30",    "Last 30 Days"),
    ("last_90",    "Last 90 Days"),
    ("this_quarter", "This Quarter"),
    ("last_quarter", "Last Quarter"),
    ("ytd", "Year to Date"),
]

FORECAST_PERIODS = [
    ("this_month", "This Month"),
]

CALL_STATS_PERIODS = [
    ("today",      "Today"),
    ("this_week",  "This Week"),
    ("last_week",  "Last Week"),
    ("this_month", "This Month"),
    ("last_month", "Last Month"),
    ("last_30",    "Last 30 Days"),
    ("last_90",    "Last 90 Days"),
    ("this_quarter", "This Quarter"),
    ("last_quarter", "Last Quarter"),
    ("ytd", "Year to Date"),
]

TEAMS = [("all", "All"), ("Veterans", "Veterans"), ("Rising", "Rising")]

COVERAGE_PERIODS = [
    ("this_month", "This Month"),
    ("next_month", "Next Month"),
]

SOURCES = ["All", "Cold outreach", "Inbound", "Referral", "Conference"]

NAV = [
    {"type": "link",  "endpoint": "home",               "label": "Home"},
    {"type": "group", "label": "Scorecard", "children": [
        {"endpoint": "scorecard",         "label": "This Month"},
        {"endpoint": "scorecard_history", "label": "Scorecard History"},
    ]},
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
    {"type": "group", "label": "Calls", "children": [
        {"endpoint": "call_stats",                    "label": "Summary"},
        {"endpoint": "calls_drilldown.calls_drilldown", "label": "Connect Analysis"},
        {"endpoint": "calls_drilldown.dial_pipeline", "label": "Dial Pipeline"},
    ]},
    {"type": "group", "label": "Marketing", "children": [
        {"endpoint": "inbound_funnel",  "label": "Inbound Funnel"},
        {"endpoint": "abm",             "label": "ABM"},
    ]},
]


@app.context_processor
def inject_cache_info():
    """Make last_refreshed available in every template automatically."""
    import cache_scheduler
    ts = last_refreshed_ts()
    stale = ts > 0 and (time.time() - ts) > 7200  # >2 hours = stale
    return {
        "last_refreshed": last_refreshed_str(),
        "cache_stale": stale,
        "cache_syncing": cache_scheduler.is_syncing(),
        "can_manage_settings": bool(session.get("authenticated")) and _current_user_is_admin(),
    }


@app.before_request
def _record_request_start():
    g.t0 = time.monotonic()


@app.after_request
def _log_request_duration(response):
    duration_ms = (time.monotonic() - g.t0) * 1000
    endpoint = request.endpoint or request.path
    log.info("%-30s  %s  %.0fms", endpoint, response.status_code, duration_ms)
    return response


def login_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        if not session.get("authenticated"):
            return redirect(url_for("login", next=request.url))
        return f(*args, **kwargs)
    return decorated


def _is_admin_user(owner_id: str, email: str = "") -> bool:
    persisted = monthly_store.get_admin_settings()
    persisted_emails = frozenset(persisted.get("admin_emails", []))
    if email and email.lower() in ADMIN_EMAIL_ALLOWLIST:
        return True
    if email and email.lower() in persisted_emails:
        return True
    if owner_id and owner_id in ADMIN_OWNER_ALLOWLIST:
        return True
    team_oids = get_team_owner_ids()
    return owner_id in OWNER_EXCLUDE or bool(team_oids and owner_id not in team_oids)


@app.route("/login")
def login():
    redirect_uri = url_for("auth_callback", _external=True)
    return oauth.google.authorize_redirect(redirect_uri)


@app.route("/auth/callback")
def auth_callback():
    token = oauth.google.authorize_access_token()
    user_info = token.get("userinfo") or oauth.google.userinfo()
    email = (user_info.get("email") or "").lower()

    if not email.endswith(f"@{ALLOWED_DOMAIN}"):
        return render_template("login_error.html",
                               message="Sign-in requires a @belfrysoftware.com account.")

    # Map email → HubSpot owner; determine admin status
    owners = get_owners()
    owner = next((o for o in owners.values() if (o.get("email") or "").lower() == email), None)

    if owner is None:
        return render_template("login_error.html",
                               message="Your account isn't linked to a HubSpot owner. Contact your admin.")

    session["authenticated"] = True
    session["email"] = email
    session["owner_id"] = owner["id"]
    session["is_admin"] = _is_admin_user(owner["id"], email)

    next_url = request.args.get("next") or url_for("home")
    return redirect(next_url)


@app.route("/logout")
def logout():
    session.clear()
    return redirect(url_for("login"))


def _current_user_is_admin() -> bool:
    """Prefer the login-time admin flag; only recompute if the session lacks it."""
    session_flag = session.get("is_admin")
    if session_flag is not None:
        return bool(session_flag)

    owner_id = session.get("owner_id", "")
    email = (session.get("email") or "").lower()
    return _is_admin_user(owner_id, email)


def _parse_settings_list(raw: str, lowercase: bool = False) -> list[str]:
    values = []
    for chunk in (raw or "").replace(",", "\n").splitlines():
        value = chunk.strip()
        if not value:
            continue
        values.append(value.lower() if lowercase else value)
    return values


@app.route("/refresh-cache", methods=["POST"])
@login_required
def refresh_cache():
    """Kick off a background refresh while continuing to serve the current cache."""
    cache_scheduler.trigger()
    return redirect(request.referrer or url_for("home"))


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


def _business_days_in_month(year: int, month: int) -> int:
    import calendar

    last_day = calendar.monthrange(year, month)[1]
    return sum(1 for day in range(1, last_day + 1) if date(year, month, day).weekday() < 5)


def _summary_meta(record) -> dict:
    import calendar

    if not record:
        return {}

    year = int(record["year"])
    month = int(record["month"])
    last_day = calendar.monthrange(year, month)[1]

    return {
        "key": f"{year:04d}-{month:02d}",
        "month_label": date(year, month, 1).strftime("%B %Y"),
        "cutoff_label": date(year, month, last_day).strftime("%B %-d, %Y"),
    }


def _annotate_live_row(row: dict) -> dict:
    attain = float(row.get("attain_pct", 0) or 0)
    deals_created = int(row.get("deals_created", 0) or 0)
    connect_rate = float(row.get("connect_rate", 0) or 0)
    s2_amt = float(row.get("s2_amt", 0) or 0)
    s2_target = float(row.get("s2_target", 0) or 0)
    ac_accounts = int(row.get("ac_accounts", 0) or 0)
    stale_count = int(row.get("stale_count", 0) or 0)
    stale_pct = (stale_count / ac_accounts * 100) if ac_accounts else 0.0

    note = "Current pace looks healthy."
    rank = 0
    label = "On track"

    if attain < 60:
        label = "Needs attention"
        rank = 2
        note = "Below 60% to quota."
    elif deals_created < 5:
        label = "Needs attention"
        rank = 2
        note = "Pipeline creation is behind."
    elif s2_target and (s2_amt / s2_target) < 0.4:
        label = "Needs attention"
        rank = 2
        note = "Stage 2 progression is behind."
    elif attain < 80 or connect_rate < 8 or stale_pct > 40:
        label = "Watch"
        rank = 1
        if attain < 80:
            note = "Pace is behind target."
        elif connect_rate < 8:
            note = "Connect rate is low."
        else:
            note = "Book discipline needs review."

    return {
        **row,
        "attention_label": label,
        "attention_rank": rank,
        "attention_note": note,
    }


def _grade_sort_value(grade: str) -> int:
    order = {
        "A+": 0,
        "A": 1,
        "A-": 2,
        "B+": 3,
        "B": 4,
        "B-": 5,
        "C+": 6,
        "C": 7,
        "C-": 8,
        "D+": 9,
        "D": 10,
    }
    return order.get((grade or "").strip(), 999)


def _grade_summary(rows: list[dict]) -> dict:
    summary = {"strong": 0, "mixed": 0, "at_risk": 0}
    for row in rows:
        grade = (row.get("grade") or "").strip()
        if grade.startswith("A") or grade.startswith("B"):
            summary["strong"] += 1
        elif grade.startswith("C"):
            summary["mixed"] += 1
        else:
            summary["at_risk"] += 1
    return summary


def _filter_by_team(data: dict, team: str) -> dict:
    """Filter rows to a specific team and recompute totals from the filtered set."""
    if team == "all":
        return data
    team_map = get_owner_team_map()
    rows = [r for r in data.get("rows", []) if team_map.get(r.get("owner_id")) == team]
    orig = data.get("totals", {})

    if not rows:
        totals = {k: (0 if isinstance(v, (int, float)) else v) for k, v in orig.items()}
        return {**data, "rows": rows, "totals": totals}

    # Sum every raw numeric field that appears in row dicts
    skip = {"acv", "win_rate", "attain_pct", "delta_amt", "pct_connect",
            "pct_conversation", "pct_deals", "pct_active_30", "pct_called_120",
            "pct_in_sequence", "cold_outreach_acv", "inbound_acv",
            "referral_acv", "conference_acv", "total_acv", "avg_days_to_close"}
    totals = dict(orig)
    for k, v in orig.items():
        if isinstance(v, (int, float)) and k not in skip:
            totals[k] = sum(r.get(k, 0) for r in rows)

    # Recompute derived ratios
    def _pct(a, b): return round(a / b * 100, 1) if b else 0.0
    if "connects" in totals:
        totals["pct_connect"]      = _pct(totals["connects"], totals.get("dials", 0))
    if "conversations" in totals:
        totals["pct_conversation"] = _pct(totals["conversations"], totals.get("connects", 0))
    if "outbound_deals_created" in totals:
        totals["pct_deals"]        = _pct(totals["outbound_deals_created"], totals.get("dials", 0))
    if "total_won_amt" in totals and "total_won_n" in totals:
        totals["acv"]       = round(totals["total_won_amt"] / totals["total_won_n"]) if totals["total_won_n"] else 0
        closed              = totals["total_won_n"] + totals.get("total_lost_n", 0)
        totals["win_rate"]  = _pct(totals["total_won_n"], closed)
        totals["delta_amt"] = totals["total_won_amt"] - totals.get("quota_amt", 0)
        totals["attain_pct"]= _pct(totals["total_won_amt"], totals.get("quota_amt", 0))
        dtc_n = totals.get("days_to_close_n", 0)
        totals["avg_days_to_close"] = round(totals.get("days_to_close_sum", 0) / dtc_n) if dtc_n else None
    if "active_30" in totals and "ac_accounts" in totals:
        totals["pct_active_30"]   = _pct(totals["active_30"],   totals["ac_accounts"])
        totals["pct_called_120"]  = _pct(totals.get("called_120", 0),  totals["ac_accounts"])
        totals["pct_in_sequence"] = _pct(totals.get("in_sequence", 0), totals["ac_accounts"])
    for src in ("cold_outreach", "inbound", "referral", "conference"):
        n = totals.get(f"{src}_n", 0)
        totals[f"{src}_acv"] = round(totals.get(f"{src}_amt", 0) / n) if n else 0
    if "total_n" in totals:
        totals["total_acv"] = round(totals.get("total_amt", 0) / totals["total_n"]) if totals["total_n"] else 0

    # Also filter sub-groups (e.g. pipeline_generated "groups" key)
    if "groups" in data:
        filtered_groups = []
        for g in data["groups"]:
            g_rows = [r for r in g.get("rows", []) if team_map.get(r.get("owner_id")) == team]
            if g_rows:
                filtered_groups.append({**g, "rows": g_rows})
        return {**data, "rows": rows, "totals": totals, "groups": filtered_groups}

    return {**data, "rows": rows, "totals": totals}


def _filter_by_owner(data: dict, owner_id: str) -> dict:
    """Filter rows to a single owner and recompute totals from the filtered set."""
    rows = [r for r in data.get("rows", []) if r.get("owner_id") == owner_id]
    orig = data.get("totals", {})

    if not rows:
        totals = {k: (0 if isinstance(v, (int, float)) else v) for k, v in orig.items()}
        return {**data, "rows": rows, "totals": totals}

    totals = dict(orig)
    for k, v in orig.items():
        if isinstance(v, (int, float)):
            totals[k] = sum(r.get(k, 0) for r in rows)

    def _pct(a, b): return round(a / b * 100, 1) if b else 0.0
    if "active_30" in totals and "ac_accounts" in totals:
        totals["pct_active_30"] = _pct(totals["active_30"], totals["ac_accounts"])
        totals["pct_called_120"] = _pct(totals.get("called_120", 0), totals["ac_accounts"])
        totals["pct_in_sequence"] = _pct(totals.get("in_sequence", 0), totals["ac_accounts"])

    return {**data, "rows": rows, "totals": totals}


@app.route("/")
@login_required
def home():
    from datetime import datetime, timezone, date, timedelta
    import calendar
    team = request.args.get("team", "all")
    data = get_cached(analytics.compute_scorecard, "this_month")
    if data is None:
        return render_template("loading.html", nav=NAV, active="home"), 202
    try:
        data = _filter_by_team(data, team)
        # Recompute team-level KPIs from filtered rows so the KPI cards reflect
        # the selected team. _filter_by_team only filters data["rows"]; the
        # pre-aggregated data["team"] is unaware of the team param otherwise.
        if team != "all":
            rows = data["rows"]
            if rows:
                t_quota = sum(r["quota_amt"] for r in rows)
                t_won   = sum(r["won_amt"]   for r in rows)
                active  = [r for r in rows if r.get("avg_dials", 0) > 0]
                dial_sum = sum(r["avg_dials"] for r in active)
                data = dict(data)
                data["team"] = {
                    **data["team"],
                    "attain_pct":    round(t_won / t_quota * 100, 1) if t_quota else 0.0,
                    "won_amt":       t_won,
                    "quota_amt":     t_quota,
                    "deals_created": sum(r["deals_created"] for r in rows),
                    "s2_amt":        sum(r["s2_amt"]        for r in rows),
                    "s2_target":     sum(r["s2_target"]     for r in rows),
                    "avg_dials":     round(dial_sum / len(active), 1) if active else 0.0,
                    "connect_rate":  round(
                        sum(r["connect_rate"] * r["avg_dials"] for r in active) / dial_sum, 1
                    ) if dial_sum else 0.0,
                    "stale_count":   sum(r["stale_count"]   for r in rows),
                    "ac_accounts":   sum(r["ac_accounts"]   for r in rows),
                }
            else:
                data = dict(data)
                data["team"] = {k: (0 if isinstance(v, (int, float)) else v)
                                for k, v in data["team"].items()}
        t      = data["team"]
        n_reps = len(data["rows"])   # reflects filtered team for deals_target
    except Exception as e:
        return render_template("error.html", message=str(e), nav=NAV, active="home")

    home_metrics_warming = False
    win_rate = None
    acv = None
    try:
        won_data = get_cached(analytics.compute_deals_won, "this_month")
        if won_data is None:
            raise RuntimeError("deals_won cache warming")
        won_data = _filter_by_team(won_data, team)
        wt = won_data["totals"]
        win_rate = wt["win_rate"]
        acv = wt["acv"]
    except Exception:
        # Deals-won data is secondary on the landing page. If HubSpot is slow
        # or rate-limiting, render Home with scorecard data and let these KPIs
        # fill in on a later request.
        home_metrics_warming = True

    month_label = datetime.now(timezone.utc).strftime("%B %Y")

    # Business-day pace indicator (same logic as scorecard route — not refactored per scope)
    today          = date.today()
    first_of_month = today.replace(day=1)
    last_day       = calendar.monthrange(today.year, today.month)[1]
    last_of_month  = today.replace(day=last_day)

    def _count_bdays(start, end):
        n, cur = 0, start
        while cur <= end:
            if cur.weekday() < 5:
                n += 1
            cur += timedelta(days=1)
        return n

    bdays_total   = _count_bdays(first_of_month, last_of_month)
    bdays_elapsed = _count_bdays(first_of_month, today)
    pace_pct      = round(bdays_elapsed / bdays_total * 100, 1) if bdays_total else 0

    return render_template("home.html", data=data, t=t, month_label=month_label,
                           pace_pct=pace_pct, bdays_elapsed=bdays_elapsed, bdays_total=bdays_total,
                           n_reps=n_reps, team=team, teams=TEAMS,
                           win_rate=win_rate, acv=acv, home_metrics_warming=home_metrics_warming,
                           active="home", nav=NAV)


@app.route("/scorecard")
@login_required
def scorecard():
    from datetime import datetime, timezone, date, timedelta
    import calendar
    data = get_cached(analytics.compute_scorecard, "this_month")
    if data is None:
        return render_template("loading.html", nav=NAV, active="scorecard"), 202
    try:
        t     = data["team"]
        owner_id = session.get("owner_id", "")
        is_admin = _current_user_is_admin()
        visible_rows = data["rows"]
        if not is_admin:
            visible_rows = [r for r in visible_rows if r.get("owner_id") == owner_id]

        visible_rows = [_annotate_live_row(r) for r in visible_rows]
        visible_rows.sort(
            key=lambda r: (
                _grade_sort_value(r.get("grade", "")),
                float(r.get("attain_pct", 0) or 0),
                r.get("ae", ""),
            )
        )

        data = dict(data)
        data["rows"] = visible_rows

        import monthly_store
        import summary_engine

        team_summary = None
        team_summary_meta = None
        team_history_count = 0
        if is_admin:
            team_history = monthly_store.get_team_history()
            if not team_history:
                generated = summary_engine.get_or_generate_team_summary()
                team_history = [generated] if generated else []
            team_history_count = len(team_history)
            if team_history:
                team_summary = team_history[0]
                team_summary_meta = _summary_meta(team_history[0])

        rep_data = {}
        rep_history_available = False
        for row in data["rows"]:
            oid = row["owner_id"]
            history = monthly_store.get_rep_history(oid)
            if not history:
                rec = summary_engine.get_or_generate_rep_summary(oid)
                history = [rec] if rec else []
            if len(history) > 1:
                rep_history_available = True
            rep_data[oid] = {
                "summary": history[0] if history else None,
                "meta": _summary_meta(history[0]) if history else {},
            }
        live_grade_summary = _grade_summary(data["rows"])
        has_history = team_history_count > 1 if is_admin else rep_history_available
    except Exception as e:
        return render_template("error.html", message=str(e), nav=NAV, active="scorecard")
    month_label = datetime.now(timezone.utc).strftime("%B %Y")
    live_cutoff_label = date.today().strftime("%B %-d, %Y")

    # Business-day pace indicator
    today = date.today()
    first_of_month = today.replace(day=1)
    last_day = calendar.monthrange(today.year, today.month)[1]
    last_of_month = today.replace(day=last_day)

    def _count_bdays(start, end):
        n, cur = 0, start
        while cur <= end:
            if cur.weekday() < 5:
                n += 1
            cur += timedelta(days=1)
        return n

    bdays_total   = _count_bdays(first_of_month, last_of_month)
    bdays_elapsed = _count_bdays(first_of_month, today)
    pace_pct      = round(bdays_elapsed / bdays_total * 100, 1) if bdays_total else 0

    return render_template("scorecard.html", data=data, month_label=month_label,
                           team_summary=team_summary, team_summary_meta=team_summary_meta,
                           rep_data=rep_data,
                           is_admin=is_admin,
                           live_grade_summary=live_grade_summary,
                           has_history=has_history,
                           live_cutoff_label=live_cutoff_label,
                           pace_pct=pace_pct, bdays_elapsed=bdays_elapsed, bdays_total=bdays_total,
                           active="scorecard", nav=NAV)


@app.route("/scorecard/history")
@login_required
def scorecard_history():
    from datetime import date
    data = get_cached(analytics.compute_scorecard, "this_month")
    if data is None:
        return render_template("loading.html", nav=NAV, active="scorecard_history"), 202
    try:
        owner_id = session.get("owner_id", "")
        is_admin = _current_user_is_admin()
        visible_rows = data["rows"]
        if not is_admin:
            visible_rows = [r for r in visible_rows if r.get("owner_id") == owner_id]
        visible_rows = [_annotate_live_row(r) for r in visible_rows]
        visible_rows.sort(key=lambda r: (r.get("ae", ""),))
        data = dict(data)
        data["rows"] = visible_rows

        import monthly_store
        import summary_engine

        team_history_entries = []
        if is_admin:
            team_history = monthly_store.get_team_history()
            if not team_history:
                generated = summary_engine.get_or_generate_team_summary()
                team_history = [generated] if generated else []
            team_history_entries = [
                {"record": record, "meta": _summary_meta(record)}
                for record in team_history
            ]
        team_locked_months_by_key = {
            entry["meta"]["key"]: entry["meta"]
            for entry in team_history_entries
        }

        rep_data = {}
        rep_locked_months_by_key = {}
        active_oids = {row["owner_id"] for row in data["rows"]}

        # Include departed reps who have locked history records but are no
        # longer on the active team — but only for admins. Reps should never
        # see other people's historical rows.
        if is_admin:
            all_history_reps = monthly_store.get_all_rep_ids_with_history()
            departed_oids = {oid for oid in all_history_reps if oid not in active_oids}
            for oid in departed_oids:
                data["rows"].append({
                    "owner_id": oid,
                    "ae": all_history_reps[oid],
                    "_departed": True,
                })

        for row in data["rows"]:
            oid = row["owner_id"]
            history = monthly_store.get_rep_history(oid)
            if not history and not row.get("_departed"):
                rec = summary_engine.get_or_generate_rep_summary(oid)
                history = [rec] if rec else []
            history_entries = [
                {"record": hist, "meta": _summary_meta(hist)}
                for hist in history
            ]
            for entry in history_entries:
                rep_locked_months_by_key.setdefault(entry["meta"]["key"], entry["meta"])
            rep_data[oid] = {
                "summary": history[0] if history else None,
                "meta": _summary_meta(history[0]) if history else {},
                "history_entries": history_entries,
            }

        rep_locked_months = sorted(
            rep_locked_months_by_key.values(),
            key=lambda meta: meta["key"],
            reverse=True,
        )
        if is_admin:
            locked_months = sorted(
                {**rep_locked_months_by_key, **team_locked_months_by_key}.values(),
                key=lambda meta: meta["key"],
                reverse=True,
            )
        else:
            locked_months = rep_locked_months
        if not locked_months:
            selected_locked_key = ""
        else:
            requested_key = request.args.get("month", "").strip()
            valid_keys = {meta["key"] for meta in locked_months}
            selected_locked_key = requested_key if requested_key in valid_keys else locked_months[0]["key"]

        selected_team_entry = next(
            (entry for entry in team_history_entries if entry["meta"]["key"] == selected_locked_key),
            team_history_entries[0] if team_history_entries else None,
        )
    except Exception as e:
        return render_template("error.html", message=str(e), nav=NAV, active="scorecard_history")

    return render_template(
        "scorecard_history.html",
        data=data,
        rep_data=rep_data,
        is_admin=is_admin,
        locked_months=locked_months,
        selected_locked_key=selected_locked_key,
        selected_team_entry=selected_team_entry,
        today_label=date.today().strftime("%B %-d, %Y"),
        active="scorecard_history",
        nav=NAV,
    )


@app.route("/call-stats")
@login_required
def call_stats():
    period = request.args.get("period", "this_week")
    team   = request.args.get("team", "all")
    try:
        data = analytics.compute_call_stats(period)
        prior_data, prior_label = _prior(period, analytics.compute_call_stats)
        data       = _filter_by_team(data, team)
        prior_data = _filter_by_team(prior_data, team) if prior_data else prior_data
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
                           period=period, team=team, teams=TEAMS,
                           deltas=deltas, prior_label=prior_label,
                           nav=NAV, active="call_stats")


@app.route("/pipeline-generated")
@login_required
def pipeline_generated():
    period = request.args.get("period", "this_week")
    team   = request.args.get("team", "all")
    try:
        data = analytics.compute_pipeline_generated(period)
        prior_data, prior_label = _prior(period, analytics.compute_pipeline_generated)
        data       = _filter_by_team(data, team)
        prior_data = _filter_by_team(prior_data, team) if prior_data else prior_data
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
    return render_template("pipeline_generated.html", data=data, periods=DEAL_PERIODS,
                           period=period, team=team, teams=TEAMS,
                           deltas=deltas, prior_label=prior_label,
                           nav=NAV, active="pipeline_generated")


@app.route("/pipeline-coverage")
@login_required
def pipeline_coverage():
    period = request.args.get("period", "this_month")
    team   = request.args.get("team", "all")
    try:
        data = analytics.compute_pipeline_coverage(period)
        prior_data, prior_label = _prior(period, analytics.compute_pipeline_coverage)
        data       = _filter_by_team(data, team)
        prior_data = _filter_by_team(prior_data, team) if prior_data else prior_data
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
                           period=period, team=team, teams=TEAMS,
                           deltas=deltas, prior_label=prior_label,
                           nav=NAV, active="pipeline_coverage")


@app.route("/deal-advancement")
@login_required
def deal_advancement():
    period = request.args.get("period", "this_week")
    source = request.args.get("source", "All")
    team   = request.args.get("team", "all")
    try:
        data = analytics.compute_deal_advancement(period, source)
        prior_data, prior_label = _prior(period, analytics.compute_deal_advancement, source)
        data       = _filter_by_team(data, team)
        prior_data = _filter_by_team(prior_data, team) if prior_data else prior_data
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
    return render_template("deal_advancement.html", data=data, periods=DEAL_PERIODS,
                           period=period, team=team, teams=TEAMS,
                           sources=SOURCES, source=source,
                           deltas=deltas, prior_label=prior_label,
                           nav=NAV, active="deal_advancement")


@app.route("/deals-won")
@login_required
def deals_won():
    period = request.args.get("period", "this_week")
    source = request.args.get("source", "All")
    team   = request.args.get("team", "all")
    try:
        data = analytics.compute_deals_won(period, source)
        prior_data, prior_label = _prior(period, analytics.compute_deals_won, source)
        data       = _filter_by_team(data, team)
        prior_data = _filter_by_team(prior_data, team) if prior_data else prior_data
        t  = data["totals"]
        pt = (prior_data or {}).get("totals")
        deltas = {
            "total_won_amt":    _d(t, pt, "total_won_amt"),
            "total_won_n":      _d(t, pt, "total_won_n"),
            "acv":              _d(t, pt, "acv"),
            "win_rate":         _d(t, pt, "win_rate"),
            "avg_days_to_close": _d(t, pt, "avg_days_to_close"),
        }
    except Exception as e:
        return render_template("error.html", message=str(e), nav=NAV, active="deals_won")
    return render_template("deals_won.html", data=data, periods=DEAL_PERIODS,
                           period=period, team=team, teams=TEAMS,
                           sources=SOURCES, source=source,
                           deltas=deltas, prior_label=prior_label,
                           nav=NAV, active="deals_won")


@app.route("/deals-lost")
@login_required
def deals_lost():
    period = request.args.get("period", "this_week")
    team   = request.args.get("team", "all")
    try:
        data = analytics.compute_deals_lost(period)
        prior_data, prior_label = _prior(period, analytics.compute_deals_lost)
        data       = _filter_by_team(data, team)
        prior_data = _filter_by_team(prior_data, team) if prior_data else prior_data
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
    return render_template("deals_lost.html", data=data, periods=DEAL_PERIODS,
                           period=period, team=team, teams=TEAMS,
                           deltas=deltas, prior_label=prior_label,
                           nav=NAV, active="deals_lost")


@app.route("/forecast")
@login_required
def forecast():
    period = request.args.get("period", "this_month")
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
    this_month_label = date.today().strftime("%B %Y")
    return render_template("forecast.html", data=data, periods=FORECAST_PERIODS,
                           period=period, deltas=deltas, prior_label=prior_label,
                           this_month_label=this_month_label,
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


@app.route("/abm")
@login_required
def abm():
    try:
        data = analytics.compute_abm_coverage()
    except Exception as e:
        return render_template("error.html", message=str(e), nav=NAV, active="abm")
    return render_template("abm.html", data=data, nav=NAV, active="abm")


@app.route("/book-coverage")
@login_required
def book_coverage():
    owner_id = session.get("owner_id", "")
    is_admin = _current_user_is_admin()
    team = request.args.get("team", "all") if is_admin else "all"
    try:
        data = analytics.compute_book_coverage()
        data = _filter_by_team(data, team) if is_admin else _filter_by_owner(data, owner_id)
    except Exception as e:
        return render_template("error.html", message=str(e), nav=NAV, active="book_coverage")
    return render_template("book_coverage.html", data=data, team=team, teams=TEAMS,
                           is_admin=is_admin, nav=NAV, active="book_coverage")


@app.route("/settings", methods=["GET", "POST"])
@login_required
def settings():
    if not _current_user_is_admin():
        abort(403)

    saved_message = ""
    if request.method == "POST":
        persisted = monthly_store.get_admin_settings()
        admin_emails = list(persisted.get("admin_emails", []))
        action = (request.form.get("action") or "").strip()

        if action == "add":
            new_email = (request.form.get("admin_email") or "").strip().lower()
            if new_email:
                admin_emails.append(new_email)
                monthly_store.update_admin_settings(admin_emails)
                saved_message = f"Added {new_email}."
        elif action == "remove":
            remove_email = (request.form.get("remove_email") or "").strip().lower()
            admin_emails = [email for email in admin_emails if email != remove_email]
            monthly_store.update_admin_settings(admin_emails)
            saved_message = f"Removed {remove_email}."

    persisted = monthly_store.get_admin_settings()
    return render_template(
        "settings.html",
        nav=NAV,
        active="settings",
        saved_message=saved_message,
        admin_emails=sorted(persisted.get("admin_emails", [])),
    )


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
    start, end = get_date_range(period)
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
    period = request.args.get("period", "this_month")
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
    owner_id = session.get("owner_id", "")
    is_admin = _current_user_is_admin()
    team = request.args.get("team", "all") if is_admin else "all"
    data = analytics.compute_book_coverage()
    data = _filter_by_team(data, team) if is_admin else _filter_by_owner(data, owner_id)
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


@app.route("/api/export/abm-deal-backfill.csv")
@login_required
def abm_deal_backfill_csv():
    """Export all NB pipeline deals with their target_account flag and create date.

    Use this CSV to backfill the target account deal property in HubSpot:
      - Import column 'Record ID' as the deal identifier
      - Import column 'Target Account' as the property value to set
    """
    from hubspot import _search_all, _parse_hs_datetime, get_owners

    owners = get_owners()
    owner_map = {oid: o["name"] for oid, o in owners.items()}

    payload = {
        "filterGroups": [{"filters": [
            {"propertyName": "pipeline", "operator": "EQ", "value": "31544320"},
        ]}],
        "properties": ["dealname", "createdate", "hubspot_owner_id", "target_account"],
        "sorts": [{"propertyName": "createdate", "direction": "ASCENDING"}],
    }
    deals = _search_all("deals", payload)
    if not deals:
        return Response("No deals found", mimetype="text/plain")

    out = io.StringIO()
    w = csv.writer(out)
    w.writerow(["Record ID", "Target Account", "Deal Name", "Create Date", "AE"])

    for deal in deals:
        props = deal.get("properties") or {}
        try:
            cd_str = _parse_hs_datetime(props.get("createdate", "")).strftime("%Y-%m-%d")
        except Exception:
            cd_str = props.get("createdate", "")
        ta = props.get("target_account") or "false"
        ae = owner_map.get(props.get("hubspot_owner_id", ""), "")
        w.writerow([deal["id"], ta, props.get("dealname", ""), cd_str, ae])

    return Response(
        out.getvalue(),
        mimetype="text/csv",
        headers={"Content-Disposition": "attachment; filename=abm-deal-backfill.csv"},
    )


# ── Monthly summary routes ────────────────────────────────────────────────────

@app.route("/api/monthly-summary/team")
@login_required
def monthly_summary_team():
    import summary_engine
    rec = summary_engine.get_or_generate_team_summary()
    if rec is None:
        return ("", 204)
    return jsonify(rec)


@app.route("/api/monthly-summary/rep/<owner_id>")
@login_required
def monthly_summary_rep(owner_id):
    import summary_engine
    rec = summary_engine.get_or_generate_rep_summary(owner_id)
    if rec is None:
        return ("", 204)
    return jsonify(rec)


@app.route("/api/monthly-summary/rep/<owner_id>/history")
@login_required
def monthly_summary_rep_history(owner_id):
    import monthly_store
    return jsonify(monthly_store.get_rep_history(owner_id))


@app.route("/api/monthly-summary/backfill", methods=["POST"])
@login_required
def monthly_summary_backfill():
    """Generate any missing summary records for a specific historical month."""
    import summary_engine

    payload = request.get_json(silent=True) or {}
    try:
        year = int(payload["year"])
        month = int(payload["month"])
    except (KeyError, TypeError, ValueError):
        return jsonify({"error": "year and month are required integers"}), 400

    if month < 1 or month > 12:
        return jsonify({"error": "month must be between 1 and 12"}), 400

    result = summary_engine.generate_all_for_month(year, month)
    n_saved = sum(1 for v in result["reps"].values() if v) + (1 if result["team"] else 0)
    return jsonify({"year": year, "month": month, "records_saved": n_saved})


@app.route("/api/monthly-summary/generate", methods=["POST"])
@login_required
def monthly_summary_generate():
    """Trigger summary generation for all active reps and the team.

    Always force-refreshes the underlying HubSpot analytics cache before
    generating, so summaries are never built from stale or partially-populated
    data regardless of cache state at call time.

    Idempotent — already-locked records are skipped.
    Returns a count of newly saved records.
    """
    import summary_engine
    import monthly_store
    import cache_scheduler

    cache_scheduler._refresh_base_data()
    year, month = monthly_store.last_completed_month()
    cache_scheduler._refresh_period_data("last_month")
    cache_scheduler._refresh_period_data("this_month")

    result  = summary_engine.generate_all_for_month(year, month)
    n_saved = sum(1 for v in result["reps"].values() if v) + (1 if result["team"] else 0)
    return jsonify({"year": year, "month": month, "records_saved": n_saved})


@app.route("/api/monthly-summary/regenerate", methods=["POST"])
@login_required
def monthly_summary_regenerate():
    """Delete and re-generate summaries for last_completed_month.

    Use this to correct summaries that were locked against stale data.
    """
    import summary_engine
    import monthly_store

    year, month = monthly_store.last_completed_month()
    n_deleted = monthly_store.delete_month(year, month)

    result  = summary_engine.generate_all_for_month(year, month)
    n_saved = sum(1 for v in result["reps"].values() if v) + (1 if result["team"] else 0)
    return jsonify({"year": year, "month": month, "deleted": n_deleted, "records_saved": n_saved})




@app.route("/api/scorecard/mark-departed", methods=["POST"])
@login_required
def scorecard_mark_departed():
    """Add a rep to the grace period so they stay in analytics through month-end.

    Body: {"owner_id": "12345", "label": "Clayton"}

    After their month-end summary is locked, call this endpoint again with
    {"owner_id": "12345", "remove": true} to clear them from the grace list.
    """
    import monthly_store
    data   = request.get_json() or {}
    oid    = str(data.get("owner_id", "")).strip()
    label  = str(data.get("label", "")).strip()
    remove = bool(data.get("remove", False))
    if not oid:
        return jsonify({"error": "owner_id required"}), 400
    if remove:
        monthly_store.remove_grace_rep(oid)
        return jsonify({"owner_id": oid, "status": "removed from grace list"})
    if not label:
        return jsonify({"error": "label required when adding"}), 400
    monthly_store.add_grace_rep(oid, label)
    return jsonify({"owner_id": oid, "label": label,
                    "status": "added to grace list — will remain in analytics through month-end"})


@app.route("/api/monthly-summary/delete", methods=["POST"])
@login_required
def monthly_summary_delete():
    """Delete locked summaries for last_completed_month without regenerating.

    Records are regenerated lazily on next page load using the existing cache,
    avoiding the timeout risk of a full HubSpot cache refresh.
    """
    import monthly_store
    year, month = monthly_store.last_completed_month()
    n_deleted = monthly_store.delete_month(year, month)
    return jsonify({"year": year, "month": month, "deleted": n_deleted,
                    "next_step": "Load /scorecard/history to regenerate"})


# ── Background cache scheduler ───────────────────────────────────────────────
# Guard against Flask's dev-reloader starting the thread twice.
# In production (Gunicorn) this block always runs once per worker.
import cache_scheduler
if not app.debug or os.environ.get("WERKZEUG_RUN_MAIN") == "true":
    cache_scheduler.start(initial_delay_s=0)  # warm cache immediately on boot


if __name__ == "__main__":
    app.run(debug=True, port=5001)
