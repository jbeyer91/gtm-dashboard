"""Blueprint: /calls/connect-analysis — connect-rate diagnostics drill-down."""
import logging
from functools import wraps

from flask import Blueprint, render_template, request, redirect, url_for, session

import analytics
from cache_utils import is_cached

log = logging.getLogger(__name__)

bp = Blueprint("calls_drilldown", __name__)

CALL_STATS_PERIODS = [
    ("today",        "Today"),
    ("this_week",    "This Week"),
    ("last_week",    "Last Week"),
    ("this_month",   "This Month"),
    ("last_month",   "Last Month"),
    ("last_30",      "Last 30 Days"),
    ("last_90",      "Last 90 Days"),
    ("this_quarter", "This Quarter"),
    ("last_quarter", "Last Quarter"),
    ("ytd",          "Year to Date"),
]

DIAL_PIPELINE_PERIODS = [
    ("this_month", "This Month"),
    ("last_month", "Last Month"),
]


def _login_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        if not session.get("authenticated"):
            return redirect(url_for("login", next=request.url))
        return f(*args, **kwargs)
    return decorated


@bp.route("/calls/connect-analysis")
@_login_required
def calls_drilldown():
    period = request.args.get("period", "this_month")
    if period not in {p for p, _ in CALL_STATS_PERIODS}:
        period = "this_month"

    if not is_cached(analytics.compute_connect_diagnostics, period):
        from app import NAV
        return render_template(
            "calls_drilldown.html",
            loading=True, period=period,
            periods=CALL_STATS_PERIODS,
            nav=NAV, active="calls_drilldown.calls_drilldown",
        ), 202

    try:
        diag = analytics.compute_connect_diagnostics(period)
    except Exception as e:
        log.exception("calls_drilldown error")
        from app import NAV
        return render_template("error.html", message=str(e), nav=NAV, active="calls_drilldown.calls_drilldown")

    from app import NAV
    return render_template(
        "calls_drilldown.html",
        diag=diag,
        period=period,
        periods=CALL_STATS_PERIODS,
        nav=NAV,
        active="calls_drilldown.calls_drilldown",
    )


@bp.route("/calls/connect-rate-drivers")
@_login_required
def connect_rate_drivers():
    period = request.args.get("period", "this_month")
    if period not in {p for p, _ in CALL_STATS_PERIODS}:
        period = "this_month"

    team = request.args.get("team", "all")
    rep = request.args.get("rep", "all")
    segment = request.args.get("segment", "all")

    if not is_cached(analytics.compute_connect_rate_drivers, period):
        from app import NAV
        return render_template(
            "connect_rate_drivers.html",
            loading=True, period=period,
            periods=CALL_STATS_PERIODS,
            nav=NAV, active="calls_drilldown.connect_rate_drivers",
        ), 202

    try:
        data = analytics.compute_connect_rate_drivers(period, team=team, rep=rep, segment=segment)
    except Exception as e:
        log.exception("connect_rate_drivers error")
        from app import NAV
        return render_template("error.html", message=str(e), nav=NAV, active="calls_drilldown.connect_rate_drivers")

    # Resolve period label
    period_label = next((lbl for val, lbl in CALL_STATS_PERIODS if val == period), period)

    # Rep filter — drill into a single rep vs team
    rep_id = request.args.get("rep", "all")
    selected_row = next((r for r in data["rows"] if r["owner_id"] == rep_id), None)

    # Gap waterfall: attribute the explained gap to each driver bucket (in pp)
    waterfall = None
    if selected_row:
        t_rate = data["team"]["connect_rate"]
        waterfall = {
            "dmi_contribution": round(t_rate * (selected_row["dmi"] - 100) / 100, 2),
            "rei_contribution": round(t_rate * (selected_row["rei"] - 100) / 100, 2),
            "tqi_contribution": round(t_rate * (selected_row["tqi"] - 100) / 100, 2),
            "unexplained":      selected_row["actual_vs_expected"],
            "total_gap":        selected_row["vs_team"],
        }

    from app import NAV
    from app import TEAMS
    rep_options = [{"value": "all", "label": "All reps"}] + [
        {"value": r["owner_id"], "label": r["ae"]}
        for r in data.get("rows", [])
    ]
    return render_template(
        "connect_rate_drivers.html",
        data=data,
        period=period,
        team=team,
        rep=rep,
        segment=segment,
        teams=TEAMS,
        rep_options=rep_options,
        period_label=period_label,
        periods=CALL_STATS_PERIODS,
        rep_id=rep_id,
        selected_row=selected_row,
        waterfall=waterfall,
        nav=NAV,
        active="calls_drilldown.connect_rate_drivers",
    )


@bp.route("/calls/dial-pipeline")
@_login_required
def dial_pipeline():
    period = request.args.get("period", "last_month")
    if period not in {p for p, _ in DIAL_PIPELINE_PERIODS}:
        period = "last_month"

    if not is_cached(analytics.compute_dial_pipeline, period):
        from app import NAV
        return render_template(
            "dial_pipeline.html",
            loading=True,
            period=period,
            periods=DIAL_PIPELINE_PERIODS,
            nav=NAV,
            active="calls_drilldown.dial_pipeline",
        ), 202

    try:
        data = analytics.compute_dial_pipeline(period)
    except Exception as e:
        log.exception("dial_pipeline error")
        from app import NAV
        return render_template("error.html", message=str(e), nav=NAV, active="calls_drilldown.dial_pipeline")

    from app import NAV
    return render_template(
        "dial_pipeline.html",
        data=data,
        period=period,
        periods=DIAL_PIPELINE_PERIODS,
        nav=NAV,
        active="calls_drilldown.dial_pipeline",
    )
