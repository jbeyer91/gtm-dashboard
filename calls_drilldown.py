"""Blueprint: /calls/connect-analysis — connect-rate diagnostics drill-down."""
import logging
import threading
from functools import wraps

from flask import Blueprint, render_template, request, redirect, url_for, session

import analytics
import day_of_week as _dow
from cache_utils import is_cached

log = logging.getLogger(__name__)

bp = Blueprint("calls_drilldown", __name__)

# Tracks (period, team, rep, segment) tuples for which a background
# compute_connect_rate_drivers call is already in-flight, to avoid spawning
# duplicate threads when the 12-second meta-refresh fires repeatedly.
_crd_warming: set = set()
_crd_warming_lock = threading.Lock()

# Dedup set for compute_connect_diagnostics background warming.
_cd_warming: set = set()
_cd_warming_lock = threading.Lock()

# Dedup set for compute_dial_pipeline background warming.
_dp_warming: set = set()
_dp_warming_lock = threading.Lock()

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

CONNECT_RATE_DRIVER_PERIODS = CALL_STATS_PERIODS


def _normalize_connect_rate_driver_payload(
    raw: dict,
    period: str,
    team: str,
    rep: str,
    segment: str,
    comparison_mode: str,
    table_sort: str,
) -> dict:
    """Adapt the older analytics payload shape to the newer team-only template."""
    if raw is None:
        return raw
    if "team_comparison" in raw and "diagnostic_table" in raw:
        return raw

    state_flags = raw.get("state_flags", {})
    kpi_strip = raw.get("kpi_strip", {})
    team = raw.get("team", {})
    totals = raw.get("totals", {})
    driver_cards = raw.get("driver_cards", {})

    def _fmt_card_rows(rows):
        formatted = []
        for row in rows or []:
            rep_val = row.get("rep")
            team_val = row.get("team")
            if rep_val is None or team_val is None:
                continue
            delta = round(rep_val - team_val, 1)
            formatted.append({
                "label": row.get("metric", "Metric"),
                "rep": rep_val,
                "team": team_val,
                "delta": delta,
                "display": analytics._metric_display(row.get("metric", "Metric"), rep_val, team_val),
            })
        return formatted

    card_map = [
        ("dial_mix", "Dial Mix", "Dial Mix Index", "Composite read of reachable-record quality versus the selected team baseline."),
        ("dialing_behavior", "Dialing Behavior", "Reach Efficiency Index", "Composite read of how efficiently the dialing pattern creates fresh reach."),
        ("timing", "Timing", "Timing Quality Index", "Composite read of timing quality versus when the selected team tends to connect best."),
    ]

    normalized_cards = []
    for key, title, index_label, tip in card_map:
        card = driver_cards.get(key, {})
        card_entry = {
            "title": title,
            "question": "",
            "index_label": index_label,
            "index_value": round(card.get("index_value", 100)),
            "index_team_baseline": 100,
            "tip": tip,
            "rows": _fmt_card_rows(card.get("rows")),
        }
        if key == "dial_mix":
            card_entry["icp_breakdown"] = card.get("icp_breakdown", [])
            card_entry["title_breakdown"] = card.get("title_breakdown", [])
            card_entry["phone_type_breakdown"] = card.get("phone_type_breakdown", [])
        normalized_cards.append(card_entry)

    rows = raw.get("rows", [])
    comparison_rows = []
    diagnostic_rows = []
    for row in rows:
        comparison_rows.append({
            "owner_id": row.get("owner_id"),
            "rep": row.get("ae"),
            "actual_connect_pct": row.get("connect_rate", 0.0),
            "expected_connect_pct": row.get("expected_connect_rate", 0.0),
            "delta_vs_team_avg": row.get("vs_team", 0.0),
            "actual_vs_expected": row.get("actual_vs_expected", 0.0),
            "selected": False,
        })
        diagnostic_rows.append({
            "owner_id": row.get("owner_id"),
            "rep": row.get("ae"),
            "actual_connect_pct": row.get("connect_rate", 0.0),
            "expected_connect_pct": row.get("expected_connect_rate", 0.0),
            "delta_vs_team_avg": row.get("vs_team", 0.0),
            "actual_vs_expected": row.get("actual_vs_expected", 0.0),
            "gap_explained_pct": row.get("gap_explained_pct") if row.get("gap_explained_pct") is not None else 0.0,
            "shared_number_rate": row.get("unknown_line_pct", 0.0),
            "conversation_pct": row.get("conversation_rate", 0.0),
            "low_icp_rate": row.get("icp_low_pct", 0.0),
            "no_icp_data_rate": row.get("icp_unknown_pct", 0.0),
            "company_object_rate": row.get("company_object_rate", 0.0),
            "primary_driver": next((d.get("primary_driver") for d in raw.get("diagnostic_rows", []) if d.get("owner_id") == row.get("owner_id")), "Dial Mix"),
            "secondary_driver": next((d.get("secondary_driver") for d in raw.get("diagnostic_rows", []) if d.get("owner_id") == row.get("owner_id")), "Timing"),
        })

    gap_buckets = raw.get("gap_decomposition", {}).get("buckets", [])
    normalized_gap_buckets = [
        {"label": bucket.get("label", "Bucket"), "points": bucket.get("pts", 0.0)}
        for bucket in gap_buckets
    ]

    period_label = next((label for value, label in CONNECT_RATE_DRIVER_PERIODS if value == period), period.replace("_", " ").title())
    team_avg_connect = team.get("connect_rate", 0.0)

    return {
        "view": {
            "period": period,
            "period_label": period_label,
            "team": team,
            "rep": "all",
            "rep_label": "All reps",
            "segment": segment,
            "segment_enabled": False,
            "is_rep_view": False,
        },
        "filters": {
            "teams": [{"value": "all", "label": "All"}],
            "reps": [{"value": "all", "label": "All reps"}],
            "segments": [],
        },
        "state": {
            "loading": False,
            "empty": state_flags.get("is_empty", False),
            "partial_explanation": state_flags.get("partial_explanation", False),
            "sample_too_small": state_flags.get("small_sample", False),
            "field_coverage_weak": state_flags.get("low_coverage", False),
            "message": "Partial explanation" if state_flags.get("partial_explanation", False) else "Strong explanation",
        },
        "kpis": [
            {"label": "Selected Team Connect %", "value": kpi_strip.get("Rep Connect %", team_avg_connect), "display": f"{kpi_strip.get('Rep Connect %', team_avg_connect):.1f}%", "delta_points": None, "tip": None},
            {"label": "Team Avg Connect %", "value": kpi_strip.get("Team Avg Connect %", team_avg_connect), "display": f"{kpi_strip.get('Team Avg Connect %', team_avg_connect):.1f}%", "delta_points": None, "tip": None},
            {"label": "Delta vs Team Avg", "value": kpi_strip.get("Delta vs Team Avg", 0.0), "display": analytics._fmt_point_delta(kpi_strip.get("Delta vs Team Avg", 0.0)), "delta_points": kpi_strip.get("Delta vs Team Avg", 0.0), "tip": None},
            {"label": "Expected Connect %", "value": kpi_strip.get("Expected Connect %", team_avg_connect), "display": f"{kpi_strip.get('Expected Connect %', team_avg_connect):.1f}%", "delta_points": None, "tip": "Estimated connect rate based on dial mix, dialing behavior, and timing only."},
            {"label": "Actual vs Expected", "value": kpi_strip.get("Actual vs Expected", 0.0), "display": analytics._fmt_point_delta(kpi_strip.get("Actual vs Expected", 0.0)), "delta_points": kpi_strip.get("Actual vs Expected", 0.0), "tip": "Shows whether actual connect rate landed above or below the measured-condition benchmark."},
            {"label": "Gap Explained %", "value": kpi_strip.get("Gap Explained %") or 0.0, "display": f"{(kpi_strip.get('Gap Explained %') or 0.0):.0f}%", "delta_points": None, "band": analytics._pct_band((kpi_strip.get('Gap Explained %') or 0.0)), "tip": "Shows how much of the gap versus team average is explained by the tracked drivers."},
            {"label": "Field Coverage %", "value": kpi_strip.get("Field Coverage %", team.get("field_coverage_pct", 0.0)), "display": f"{kpi_strip.get('Field Coverage %', team.get('field_coverage_pct', 0.0)):.0f}%", "delta_points": None, "tip": "Shows how much of the analyzed dialing volume has the fields needed to explain the read confidently."},
        ],
        "notes": {
            "shared_number_definition": "Shared Number Rate flags the same normalized phone number appearing across multiple contact records, which is the closest read on reps calling the same number through different people.",
            "conversation_rate_definition": "Conversation rate uses the same definition as Call Stats: connected outbound calls with 60+ seconds duration divided by live connects.",
            "clearout_phone_source": "Phone type (mobile vs. direct line) comes from the contact record in HubSpot. When the primary line-type field is blank, a secondary enrichment field is used as a fallback. A phone is considered high-confidence when a normalized number is present and the line type is known.",
        },
        "gap_decomposition": {
            "title": "What is driving the gap?",
            "team_avg_connect_pct": raw.get("gap_decomposition", {}).get("start", team_avg_connect),
            "rep_connect_pct": raw.get("gap_decomposition", {}).get("end", team_avg_connect),
            "expected_connect_pct": kpi_strip.get("Expected Connect %", team_avg_connect),
            "buckets": normalized_gap_buckets,
        },
        "driver_cards": normalized_cards,
        "team_comparison": {
            "mode": comparison_mode,
            "modes": [
                {"value": "connect_pct", "label": "Connect %"},
                {"value": "delta_vs_team", "label": "Delta vs Team"},
                {"value": "actual_vs_expected", "label": "Actual vs Expected"},
            ],
            "team_avg_connect_pct": team_avg_connect,
            "rows": comparison_rows,
        },
        "diagnostic_table": {
            "sort": table_sort,
            "sorts": [
                {"value": "worst_delta_vs_team", "label": "worst Delta vs Team"},
                {"value": "worst_vs_expected", "label": "worst Vs Expected"},
                {"value": "lowest_gap_explained", "label": "lowest Gap Explained %"},
                {"value": "highest_connect", "label": "highest Connect %"},
            ],
            "rows": diagnostic_rows,
            "team_avg_row": {
                "rep": "Team Avg",
                "actual_connect_pct": totals.get("connect_rate", team_avg_connect),
                "delta_vs_team_avg": 0.0,
                "expected_connect_pct": totals.get("connect_rate", team_avg_connect),
                "actual_vs_expected": 0.0,
                "gap_explained_pct": 100.0,
                "shared_number_rate": totals.get("unknown_line_pct", 0.0),
                "conversation_pct": totals.get("conversation_rate", team.get("convo_rate", 0.0)),
                "low_icp_rate": totals.get("icp_low_pct", 0.0),
                "no_icp_data_rate": totals.get("icp_unknown_pct", 0.0),
                "company_object_rate": totals.get("company_object_rate", 0.0),
            },
        },
        "rep_detail": {"selected_owner_id": None, "available": False},
    }


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
        _warm_key = (period,)
        with _cd_warming_lock:
            _already = _warm_key in _cd_warming
            if not _already:
                _cd_warming.add(_warm_key)
        if not _already:
            def _bg(p=period, k=_warm_key):
                try:
                    analytics.compute_connect_diagnostics(p)
                    log.info("bg compute_connect_diagnostics(%s) complete", p)
                except Exception as exc:
                    log.warning("bg compute_connect_diagnostics(%s) failed: %s", p, exc)
                finally:
                    with _cd_warming_lock:
                        _cd_warming.discard(k)
            threading.Thread(target=_bg, daemon=True).start()
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


@bp.route("/calls/dial-pipeline")
@_login_required
def dial_pipeline():
    period = request.args.get("period", "last_month")
    if period not in {p for p, _ in DIAL_PIPELINE_PERIODS}:
        period = "last_month"

    if not is_cached(analytics.compute_dial_pipeline, period):
        _warm_key = (period,)
        with _dp_warming_lock:
            _already = _warm_key in _dp_warming
            if not _already:
                _dp_warming.add(_warm_key)
        if not _already:
            def _bg(p=period, k=_warm_key):
                try:
                    analytics.compute_dial_pipeline(p)
                    log.info("bg compute_dial_pipeline(%s) complete", p)
                except Exception as exc:
                    log.warning("bg compute_dial_pipeline(%s) failed: %s", p, exc)
                finally:
                    with _dp_warming_lock:
                        _dp_warming.discard(k)
            threading.Thread(target=_bg, daemon=True).start()
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


@bp.route("/calls/connect-rate-drivers")
@_login_required
def connect_rate_drivers():
    period = request.args.get("period", "this_month")
    if period not in {p for p, _ in CONNECT_RATE_DRIVER_PERIODS}:
        period = "this_month"

    team = request.args.get("team", "all")
    rep = "all"
    segment = request.args.get("segment", "all")
    comparison_mode = request.args.get("comparison_mode", "connect_pct")
    table_sort = request.args.get("table_sort", "worst_delta_vs_team")

    dow_team = request.args.get("dow_team", "all")
    if dow_team not in {"all", "Veterans", "Rising"}:
        dow_team = "all"

    dow_period = request.args.get("dow_period", "ytd")
    _valid_periods = {p for p, _ in CALL_STATS_PERIODS}
    if dow_period not in _valid_periods:
        dow_period = "ytd"

    if not is_cached(analytics.compute_connect_rate_drivers, period, team, rep, segment):
        _warm_key = (period, team, rep, segment)
        with _crd_warming_lock:
            _already = _warm_key in _crd_warming
            if not _already:
                _crd_warming.add(_warm_key)
        if not _already:
            def _bg(p=period, t=team, r=rep, s=segment, k=_warm_key):
                try:
                    analytics.compute_connect_rate_drivers(p, t, r, s)
                    log.info("bg connect_rate_drivers(%s, %s) complete", p, t)
                except Exception as exc:
                    log.warning("bg connect_rate_drivers(%s, %s) failed: %s", p, t, exc)
                finally:
                    with _crd_warming_lock:
                        _crd_warming.discard(k)
            threading.Thread(target=_bg, daemon=True).start()
        # Also warm DOW tables in background so they're ready when the page loads.
        threading.Thread(
            target=lambda dt=dow_team, dp=dow_period: _dow.build_dow_tables(dt, dp),
            daemon=True,
        ).start()
        from app import NAV
        return render_template(
            "connect_rate_drivers.html",
            loading=True,
            period=period,
            team=team,
            rep=rep,
            segment=segment,
            comparison_mode=comparison_mode,
            table_sort=table_sort,
            dow_team=dow_team,
            dow_team_options=_dow.DOW_TEAM_OPTIONS,
            dow_period=dow_period,
            dow_period_options=CALL_STATS_PERIODS,
            periods=CONNECT_RATE_DRIVER_PERIODS,
            nav=NAV,
            active="calls_drilldown.connect_rate_drivers",
        ), 202

    try:
        data = analytics.compute_connect_rate_drivers(period, team, rep, segment)

        # Apply presentation parameters post-hoc — these don't affect the cached
        # data computation so they're handled here rather than in the function.
        data["team_comparison"]["mode"] = comparison_mode

        if table_sort != "worst_delta_vs_team":
            rows = data["diagnostic_table"]["rows"]
            if table_sort == "worst_vs_expected":
                rows.sort(key=lambda r: (r["actual_vs_expected"], r["rep"]))
            elif table_sort == "lowest_gap_explained":
                rows.sort(key=lambda r: (r["gap_explained_pct"], r["rep"]))
            elif table_sort == "highest_connect":
                rows.sort(key=lambda r: (-r["actual_connect_pct"], r["rep"]))
        data["diagnostic_table"]["sort"] = table_sort
    except Exception as e:
        log.exception("connect_rate_drivers error")
        from app import NAV
        return render_template("error.html", message=str(e), nav=NAV, active="calls_drilldown.connect_rate_drivers")

    # Fetch DOW tables from cache; trigger background build if not yet ready.
    if is_cached(_dow.build_dow_tables, dow_team, dow_period):
        dow_data = _dow.build_dow_tables(dow_team, dow_period)
    else:
        threading.Thread(
            target=lambda dt=dow_team, dp=dow_period: _dow.build_dow_tables(dt, dp),
            daemon=True,
        ).start()
        dow_data = None

    from app import NAV
    return render_template(
        "connect_rate_drivers.html",
        data=data,
        period=period,
        team=team,
        rep=rep,
        segment=segment,
        comparison_mode=comparison_mode,
        table_sort=table_sort,
        dow_data=dow_data,
        dow_team=dow_team,
        dow_team_options=_dow.DOW_TEAM_OPTIONS,
        dow_period=dow_period,
        dow_period_options=CALL_STATS_PERIODS,
        periods=CONNECT_RATE_DRIVER_PERIODS,
        nav=NAV,
        active="calls_drilldown.connect_rate_drivers",
    )
