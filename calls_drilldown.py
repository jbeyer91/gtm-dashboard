"""Blueprint: /calls/connect-analysis — connect-rate diagnostics drill-down."""
import logging
from functools import wraps

from flask import Blueprint, render_template, request, redirect, url_for, session

import analytics

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

TEAMS = [("all", "All"), ("Veterans", "Veterans"), ("Rising", "Rising")]


def _login_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        if not session.get("authenticated"):
            return redirect(url_for("login", next=request.url))
        return f(*args, **kwargs)
    return decorated


def _filter_rows_by_team(data: dict, team: str) -> dict:
    """Filter rows to a specific team and recompute totals."""
    if team == "all":
        return data
    from hubspot import get_owner_team_map
    team_map = get_owner_team_map()
    rows = [r for r in data.get("rows", []) if team_map.get(r.get("owner_id")) == team]
    if not rows:
        totals = {k: (0 if isinstance(v, (int, float)) else v) for k, v in data.get("totals", {}).items()}
        return {**data, "rows": rows, "totals": totals}
    from analytics import _pct
    total_dials    = sum(r["dials"]    for r in rows)
    total_connects = sum(r["connects"] for r in rows)
    totals = {
        **data.get("totals", {}),
        "dials":       total_dials,
        "connects":    total_connects,
        "pct_connect": _pct(total_connects, total_dials),
    }
    if "companies_called" in data.get("totals", {}):
        totals["companies_called"] = len(rows)
    return {**data, "rows": rows, "totals": totals}


@bp.route("/calls/connect-analysis")
@_login_required
def calls_drilldown():
    period = request.args.get("period", "this_month")
    team   = request.args.get("team", "all")
    try:
        diag = analytics.compute_connect_diagnostics(period)
        cov  = analytics.compute_account_coverage(period)
        diag = _filter_rows_by_team(diag, team)
        cov  = _filter_rows_by_team(cov, team)
    except Exception as e:
        log.exception("calls_drilldown error")
        from app import NAV
        return render_template("error.html", message=str(e), nav=NAV, active="calls_drilldown.calls_drilldown")

    from app import NAV
    return render_template(
        "calls_drilldown.html",
        diag=diag,
        cov=cov,
        period=period,
        team=team,
        periods=CALL_STATS_PERIODS,
        teams=TEAMS,
        nav=NAV,
        active="calls_drilldown.calls_drilldown",
    )
