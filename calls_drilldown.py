"""Blueprint: /calls/connect-analysis — connect-rate diagnostics drill-down."""
import logging
from functools import wraps

from flask import Blueprint, render_template, request, redirect, url_for, session

import analytics
from cache_utils import is_cached

log = logging.getLogger(__name__)

bp = Blueprint("calls_drilldown", __name__)

CALL_STATS_PERIODS = [
    ("this_month",   "This Month"),
    ("last_month",   "Last Month"),
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
