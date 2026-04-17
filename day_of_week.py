"""Day-of-week activity tables for the Connect Rate Diagnostics page."""
import logging
from collections import defaultdict
from datetime import datetime, timezone

from cache_utils import ttl_cache
from hubspot import (
    get_owners,
    get_deals,
    get_calls,
    get_owner_team_map,
    apply_manual_owner_overrides,
)

log = logging.getLogger(__name__)

DAYS = ["Mon", "Tue", "Wed", "Thu", "Fri"]

DOW_TEAM_OPTIONS = [
    {"value": "all",      "label": "All"},
    {"value": "Veterans", "label": "Veterans"},
    {"value": "Rising",   "label": "Rising"},
]


@ttl_cache
def build_dow_tables(team: str) -> dict:
    """Return day-of-week activity data for the three DOW tables.

    team: "all" | "Veterans" | "Rising"

    Returns:
      {
        "dials":        {"rows": [...], "team_avg": {...}},
        "connect_rate": {"rows": [...], "team_avg": {...}},
        "deals":        {"rows": [...], "team_avg": {...}},
      }
    Each row is {"rep": str, "Mon": val, "Tue": val, ..., "Fri": val}.
    Rep rows are sorted alphabetically. team_avg uses the same column keys.
    """
    # Imported here to avoid a top-level circular import (analytics imports hubspot)
    from analytics import CALL_CONNECTED_GUIDS

    now = datetime.now(timezone.utc)
    start = now.replace(month=1, day=1, hour=0, minute=0, second=0, microsecond=0)
    end = now

    team_map = get_owner_team_map()  # {owner_id: "Veterans"|"Rising"}
    owners = apply_manual_owner_overrides(get_owners())

    if team == "all":
        scope_ids = set(team_map.keys())
    else:
        scope_ids = {oid for oid, t in team_map.items() if t == team}

    rep_names: dict[str, str] = {}
    for oid in scope_ids:
        owner = owners.get(oid)
        if not owner:
            continue
        name = owner.get("name") or (
            f"{owner.get('first_name', '')} {owner.get('last_name', '')}".strip()
        )
        if name:
            rep_names[oid] = name

    sorted_reps = sorted(rep_names.items(), key=lambda x: x[1])  # [(oid, name), ...]

    dials: dict = defaultdict(lambda: defaultdict(int))
    connects: dict = defaultdict(lambda: defaultdict(int))
    deal_counts: dict = defaultdict(lambda: defaultdict(int))

    for call in get_calls(start, end):
        props = call.get("properties", {})
        oid = props.get("hubspot_owner_id", "")
        if not oid or oid not in scope_ids:
            continue
        if (props.get("hs_call_direction") or "").upper() != "OUTBOUND":
            continue
        ts_raw = props.get("hs_timestamp") or props.get("hs_createdate")
        if not ts_raw:
            continue
        try:
            dt = datetime.fromisoformat(str(ts_raw).replace("Z", "+00:00"))
        except Exception:
            continue
        weekday = dt.weekday()  # 0=Mon … 6=Sun
        if weekday > 4:
            continue
        day_abbr = DAYS[weekday]
        dials[oid][day_abbr] += 1
        disposition = (props.get("hs_call_disposition") or "").strip()
        if disposition in CALL_CONNECTED_GUIDS:
            connects[oid][day_abbr] += 1

    for deal in get_deals(start, end, "createdate"):
        props = deal.get("properties", {})
        oid = props.get("hubspot_owner_id", "")
        if not oid or oid not in scope_ids:
            continue
        createdate = props.get("createdate")
        if not createdate:
            continue
        try:
            dt = datetime.fromisoformat(str(createdate).replace("Z", "+00:00"))
        except Exception:
            continue
        weekday = dt.weekday()
        if weekday > 4:
            continue
        deal_counts[oid][DAYS[weekday]] += 1

    dials_rows = []
    connect_rows = []
    deals_rows = []
    for oid, name in sorted_reps:
        d_row: dict = {"rep": name}
        c_row: dict = {"rep": name}
        de_row: dict = {"rep": name}
        for day in DAYS:
            d = dials[oid][day]
            c = connects[oid][day]
            d_row[day] = d
            c_row[day] = f"{100 * c / d:.1f}%" if d > 0 else "0.0%"
            de_row[day] = deal_counts[oid][day]
        dials_rows.append(d_row)
        connect_rows.append(c_row)
        deals_rows.append(de_row)

    n = len(sorted_reps)

    dials_avg: dict = {"rep": "Team Avg"}
    connect_avg: dict = {"rep": "Team Avg"}
    deals_avg: dict = {"rep": "Team Avg"}
    for day in DAYS:
        total_d = sum(dials[oid][day] for oid, _ in sorted_reps)
        total_c = sum(connects[oid][day] for oid, _ in sorted_reps)
        total_de = sum(deal_counts[oid][day] for oid, _ in sorted_reps)
        dials_avg[day] = round(total_d / n, 1) if n > 0 else 0
        connect_avg[day] = (
            f"{100 * total_c / total_d:.1f}%" if total_d > 0 else "0.0%"
        )
        deals_avg[day] = round(total_de / n, 1) if n > 0 else 0

    log.info(
        "build_dow_tables(%s): %d reps, %d calls, %d deals",
        team,
        n,
        sum(sum(v.values()) for v in dials.values()),
        sum(sum(v.values()) for v in deal_counts.values()),
    )

    return {
        "dials":        {"rows": dials_rows,    "team_avg": dials_avg},
        "connect_rate": {"rows": connect_rows,  "team_avg": connect_avg},
        "deals":        {"rows": deals_rows,    "team_avg": deals_avg},
    }
