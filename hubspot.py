import logging
import os
import requests
from datetime import datetime, timedelta, timezone
from functools import lru_cache
from cache_utils import ttl_cache

logger = logging.getLogger(__name__)


def _parse_hs_datetime(raw: str) -> datetime:
    """Parse a HubSpot datetime property value into an aware UTC datetime.

    HubSpot CRM objects v3 may return datetime properties as either:
      - ISO 8601 string:        "2026-01-01T00:00:00.000Z"
      - Millisecond epoch str:  "1735689600000"

    Raises ValueError if the string can't be parsed in either format.
    """
    s = (raw or "").strip()
    if not s:
        raise ValueError("empty datetime string")
    # Millisecond epoch: all digits, length 10–14
    if s.lstrip("-").isdigit() and 10 <= len(s) <= 14:
        return datetime.fromtimestamp(int(s) / 1000, tz=timezone.utc)
    # ISO 8601 — replace trailing Z with explicit UTC offset
    dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
    # Date-only strings (e.g. "2026-03-01") produce naive datetimes; assume UTC
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt

HUBSPOT_TOKEN = os.environ.get("HUBSPOT_TOKEN", "")
BASE_URL = "https://api.hubapi.com"

HEADERS = {
    "Authorization": f"Bearer {HUBSPOT_TOKEN}",
    "Content-Type": "application/json",
}

PIPELINES = {
    "31544320": "New Business Pipeline",
    "32114559": "Renewal Pipeline",
    "32094831": "Upsell / Expansion Pipeline",
    "752708198": "Enterprise - New Business Pipeline",
}

DEAL_STAGES = {
    "71300357": "Stage 1 - Initial Demo",
    "71300358": "Stage 2 - Solution Review",
    "1294419353": "Stage 3 - Stake Holder Alignment",
    "71300359": "Stage 4 - Contract Sent",
    "71300362": "Closed won",
    "71300363": "Closed lost",
    "72373864": "Pending Renewal",
    "72373867": "Outreach",
    "72373868": "Negotiation",
    "245262602": "Verbal Commit Received",
    "72373869": "Closed won",
    "72373870": "Closed lost",
    "72343439": "Demoing",
    "72343440": "Validating",
    "72343442": "Proposing",
    "72343444": "Closed won",
    "72343445": "Closed lost",
    "1217069237": "Stage 0 - Discovery",
    "1095006368": "Stage 1 - Getting Buy-In",
    "1095006369": "Stage 2 - Solution Validation",
    "1217020559": "Stage 2.5 - Advisory Board",
    "1095006370": "Stage 3 - Executive Alignment",
    "1217040475": "Stage 4 - Proposing",
    "1095006373": "Closed won",
    "1095006374": "Closed lost",
}

NB_STAGES = {
    "stage1": "71300357",
    "stage2": "71300358",
    "stage3": "1294419353",
    "stage4": "71300359",
    "won": "71300362",
    "lost": "71300363",
}

CALL_OUTCOMES = {
    "CONNECTED": ["Connected", "Answered - Call Back", "Gatekeeper"],
    "NOT_CONNECTED": ["No answer", "Left voicemail", "Busy", "Wrong number"],
    "CONVERSATION": ["Connected"],
}

# Only show data for owners on these HubSpot teams.
TEAM_FILTER = ("Veterans", "Rising")

# Manager / non-AE owner IDs to exclude even if they appear on a filtered team.
OWNER_EXCLUDE = frozenset({
    "79795769",   # Joe Mathews
    "88798218",   # Jordan Wallach (CEO)
    "371621550",  # Jordan Wallach (duplicate)
    "403559039",  # Taylor Tempel
})

# Which manager owns each team (for forecast roll-up labels).
TEAM_MANAGER = {
    "Rising":   "Joe",
    "Veterans": "Jordan",
}


@lru_cache(maxsize=1)
def get_lifecyclestage_value(label: str) -> str:
    """Return the internal API value for a lifecycle stage label (e.g. 'Disqualified').

    HubSpot stores custom lifecycle stages with numeric internal IDs. This
    function calls the properties API once (result is process-lifetime cached)
    and maps the human-readable label to its internal value string.
    Falls back to label.lower() with spaces stripped if the API call fails or
    the label isn't found.
    """
    fallback = label.lower().replace(" ", "")
    try:
        resp = requests.get(
            f"{BASE_URL}/crm/v3/properties/contacts/lifecyclestage",
            headers=HEADERS,
        )
        if not resp.ok:
            logger.warning("lifecyclestage property fetch failed: %s", resp.text)
            return fallback
        for opt in resp.json().get("options", []):
            if opt.get("label", "").strip().lower() == label.strip().lower():
                return opt["value"]
        logger.warning("lifecyclestage label %r not found in options", label)
        return fallback
    except Exception as exc:
        logger.warning("lifecyclestage lookup error: %s", exc)
        return fallback


@ttl_cache
def get_team_owner_ids() -> frozenset:
    """Return CRM owner IDs for all members of the TEAM_FILTER teams.

    Uses ?includeTeams=true on the owners endpoint so each owner object
    carries its own team membership — no separate teams API call needed,
    and no permission issues with the settings endpoint.

    Each owner has a 'teams' array; each entry has 'name' and optionally
    'secondaryTeam: true' for owners who are secondary members of a team.
    We include both primary and secondary team members.

    Falls back to an empty frozenset (= no restriction) if the API call
    fails so the dashboard degrades gracefully rather than going blank.
    """
    resp = requests.get(
        f"{BASE_URL}/crm/v3/owners?limit=200&includeTeams=true",
        headers=HEADERS,
    )
    if not resp.ok:
        return frozenset()

    allowed: set = set()
    for o in resp.json().get("results", []):
        owner_id = str(o["id"])
        if owner_id in OWNER_EXCLUDE:
            continue
        for team in o.get("teams", []):
            if team.get("name") in TEAM_FILTER:
                allowed.add(owner_id)
                break  # no need to check other teams for this owner

    return frozenset(allowed)


@ttl_cache
def get_owner_team_map() -> dict:
    """Return {owner_id: team_name} for every allowed (non-manager) rep."""
    resp = requests.get(
        f"{BASE_URL}/crm/v3/owners?limit=200&includeTeams=true",
        headers=HEADERS,
    )
    if not resp.ok:
        return {}
    result = {}
    for o in resp.json().get("results", []):
        owner_id = str(o["id"])
        if owner_id in OWNER_EXCLUDE:
            continue
        for team in o.get("teams", []):
            if team.get("name") in TEAM_FILTER:
                result[owner_id] = team["name"]
                break
    return result


@ttl_cache
def get_date_range(period: str):
    now = datetime.now(timezone.utc)
    if period == "this_month":
        start = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        return start, now
    elif period == "last_month":
        first = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        start = (first - timedelta(days=1)).replace(day=1)
        end = first - timedelta(seconds=1)
        return start, end
    elif period == "today":
        start = now.replace(hour=0, minute=0, second=0, microsecond=0)
        return start, now
    elif period == "this_week":
        start = (now - timedelta(days=now.weekday())).replace(hour=0, minute=0, second=0, microsecond=0)
        return start, now
    elif period == "last_30":
        return now - timedelta(days=30), now
    elif period == "last_60":
        return now - timedelta(days=60), now
    elif period == "last_90":
        return now - timedelta(days=90), now
    elif period == "this_quarter":
        q_month = ((now.month - 1) // 3) * 3 + 1
        start = now.replace(month=q_month, day=1, hour=0, minute=0, second=0, microsecond=0)
        return start, now
    elif period == "last_quarter":
        q_month = ((now.month - 1) // 3) * 3 + 1
        end = now.replace(month=q_month, day=1, hour=0, minute=0, second=0, microsecond=0) - timedelta(seconds=1)
        prev_q_month = ((end.month - 1) // 3) * 3 + 1
        start = end.replace(month=prev_q_month, day=1, hour=0, minute=0, second=0, microsecond=0)
        return start, end
    elif period == "ytd":
        start = now.replace(month=1, day=1, hour=0, minute=0, second=0, microsecond=0)
        return start, now
    elif period == "next_month":
        # First day of next month
        if now.month == 12:
            start = now.replace(year=now.year + 1, month=1, day=1, hour=0, minute=0, second=0, microsecond=0)
        else:
            start = now.replace(month=now.month + 1, day=1, hour=0, minute=0, second=0, microsecond=0)
        # Last moment of next month (one second before the month after)
        if start.month == 12:
            end = start.replace(year=start.year + 1, month=1) - timedelta(seconds=1)
        else:
            end = start.replace(month=start.month + 1) - timedelta(seconds=1)
        return start, end
    else:
        return now - timedelta(days=30), now


def _search_all(object_type: str, payload: dict, max_records: int = 10000) -> list:
    """Fetch all matching records up to max_records (HubSpot caps at 10,000 per search)."""
    results = []
    payload = {**payload, "limit": 200}
    after = None
    while True:
        if after:
            payload["after"] = after
        resp = requests.post(
            f"{BASE_URL}/crm/v3/objects/{object_type}/search",
            headers=HEADERS,
            json=payload,
        )
        if not resp.ok:
            break  # hit the 10k limit or other error — return what we have
        data = resp.json()
        results.extend(data.get("results", []))
        paging = data.get("paging", {})
        after = paging.get("next", {}).get("after")
        if not after or len(results) >= max_records:
            break
    return results


@ttl_cache
def get_lost_reason_labels() -> dict:
    """Return {internal_enum_value: display_label} for hs_closed_lost_reason.

    HubSpot enumeration properties return internal keys via the API, not
    display labels. This fetches the property definition so we can map
    keys → labels before classifying lost reasons.
    Returns {} if the property is free-text (no options) or on any error.
    """
    resp = requests.get(f"{BASE_URL}/crm/v3/properties/deals/hs_closed_lost_reason", headers=HEADERS)
    if not resp.ok:
        return {}
    return {opt["value"]: opt["label"] for opt in resp.json().get("options", [])}


@ttl_cache
def get_owners() -> dict:
    resp = requests.get(f"{BASE_URL}/crm/v3/owners?limit=200", headers=HEADERS)
    resp.raise_for_status()
    owners = {}
    for o in resp.json().get("results", []):
        name = f"{o.get('firstName', '')} {o.get('lastName', '')}".strip()
        owners[str(o["id"])] = {
            "id": str(o["id"]),
            "name": name,
            "last_name": o.get("lastName", ""),
            "first_name": o.get("firstName", ""),
            "email": o.get("email", ""),
            "user_id": str(o.get("userId", "")),  # portal user ID — needed to resolve goal assignees
        }
    return owners


@ttl_cache
def get_quotas(start: datetime, end: datetime) -> dict:
    """Return {owner_id: total_quota_amount} for goals overlapping [start, end].

    HubSpot goal targets are assigned by portal user ID (hs_assignee_user_id),
    not the CRM owner ID used on deals. We resolve the mapping via the userId
    field captured in get_owners().

    Amounts are summed when multiple goal periods overlap the window (e.g.
    three monthly quotas inside a quarterly reporting range).

    Returns {} gracefully if the crm.objects.goals.read scope is not enabled
    (HTTP 403) so the dashboard degrades to showing "—" instead of crashing.
    """
    owners = get_owners()
    user_id_to_owner_id = {
        v["user_id"]: v["id"] for v in owners.values() if v.get("user_id")
    }

    # HubSpot search API requires millisecond epoch timestamps for datetime
    # filters — ISO strings are silently ignored and return all records.
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
    try:
        resp = requests.post(
            f"{BASE_URL}/crm/v3/objects/goal_targets/search",
            headers=HEADERS,
            json=payload,
        )
        if resp.status_code == 403:
            return {}   # scope not enabled — degrade gracefully
        resp.raise_for_status()
    except requests.exceptions.HTTPError:
        return {}

    quotas: dict = {}
    for goal in resp.json().get("results", []):
        props    = goal.get("properties", {})

        # Only include revenue quota goals — skip pipeline, dial, and other
        # goal types HubSpot also returns (e.g. "Pipeline Generated Goal - 2026",
        # "Dials - 2026"). Without this filter all goal types get summed together,
        # inflating the displayed quota (e.g. $55K quota + $278K pipeline = $333K).
        goal_name = (props.get("hs_goal_name") or "").lower()
        if "quota" not in goal_name:
            continue

        user_id  = str(props.get("hs_assignee_user_id") or "")
        owner_id = user_id_to_owner_id.get(user_id)
        if not owner_id:
            continue
        try:
            amount = float(props.get("hs_target_amount") or 0)
        except (TypeError, ValueError):
            amount = 0.0
        if amount == 0:
            continue

        # Pro-rate only when the goal period is materially longer than the
        # requested window (e.g. an annual goal viewed in a single month).
        # When the goal period is roughly the same length as — or shorter
        # than — the window (e.g. a monthly goal viewed in "This Month"),
        # use the full goal amount so that mid-month views aren't penalised.
        #
        # Threshold: pro-rate only if goal_duration > 2 × window_duration.
        #   • Annual (~365 d) vs monthly window (~17–31 d) → pro-rate ✓
        #   • Monthly (~30 d) vs "This Month" window (~17 d) → full amount ✓
        #   • Quarterly (~90 d) vs monthly window → pro-rate (÷3) ✓
        #   • Quarterly vs quarterly window → full amount ✓
        window_secs = max((end - start).total_seconds(), 1)
        start_raw = props.get("hs_start_datetime")
        end_raw   = props.get("hs_end_datetime")
        try:
            goal_start = _parse_hs_datetime(start_raw)
            goal_end   = _parse_hs_datetime(end_raw)
            goal_secs  = max((goal_end - goal_start).total_seconds(), 1)
            logger.info(
                "quota goal owner=%s amount=%.0f start_raw=%r end_raw=%r "
                "goal_days=%.1f window_days=%.1f",
                owner_id, amount, start_raw, end_raw,
                goal_secs / 86400, window_secs / 86400,
            )
            if goal_secs > window_secs * 2:
                # Goal spans much more than the window → allocate the overlap fraction
                overlap_secs = max(
                    (min(end, goal_end) - max(start, goal_start)).total_seconds(), 0
                )
                prorated = amount * (overlap_secs / goal_secs)
                logger.info("  → pro-rated to %.2f (overlap %.1f d)", prorated, overlap_secs / 86400)
            else:
                prorated = amount   # goal fits within (or matches) the window
                logger.info("  → full amount %.2f (goal ≤ 2× window)", prorated)
        except Exception as exc:
            logger.warning(
                "quota parse error owner=%s start_raw=%r end_raw=%r: %s — using full amount",
                owner_id, start_raw, end_raw, exc,
            )
            prorated = amount   # fallback: use full amount if dates can't be parsed

        quotas[owner_id] = quotas.get(owner_id, 0.0) + prorated

    return quotas


@ttl_cache
def get_deals(start: datetime, end: datetime, date_field: str = "createdate") -> list:
    start_ts = int(start.timestamp() * 1000)
    end_ts = int(end.timestamp() * 1000)
    # Only filter on createdate or closedate — hs_date_entered_* are not filterable
    safe_field = date_field if date_field in ("createdate", "closedate") else "createdate"
    payload = {
        "filterGroups": [
            {
                "filters": [
                    {"propertyName": safe_field, "operator": "GTE", "value": str(start_ts)},
                    {"propertyName": safe_field, "operator": "LTE", "value": str(end_ts)},
                    {"propertyName": "pipeline", "operator": "EQ", "value": "31544320"},
                ]
            }
        ],
        "properties": [
            "dealname", "dealstage", "pipeline", "amount", "closedate", "createdate",
            "dealtype", "hubspot_owner_id", "hs_deal_stage_probability", "hs_is_closed_won",
            "hs_is_closed_lost", "deal_source", "hs_analytics_source",
            "closed_lost_reason", "closed_lost_sub_reason", "last_touch_channel",
            "num_associated_contacts",
            "hs_date_entered_71300357", "hs_date_entered_71300358",
            "hs_date_entered_1294419353", "hs_date_entered_71300359",
            "hs_date_entered_71300362", "hs_date_entered_71300363",
        ],
    }
    return _search_all("deals", payload)


@ttl_cache
def get_all_open_deals(start: datetime = None, end: datetime = None) -> list:
    filters = [
        {"propertyName": "pipeline", "operator": "EQ", "value": "31544320"},
        {"propertyName": "hs_is_closed_won", "operator": "EQ", "value": "false"},
        {"propertyName": "hs_is_closed_lost", "operator": "EQ", "value": "false"},
    ]
    if start and end:
        filters.append({"propertyName": "closedate", "operator": "GTE", "value": str(int(start.timestamp() * 1000))})
        filters.append({"propertyName": "closedate", "operator": "LTE", "value": str(int(end.timestamp() * 1000))})
    payload = {
        "filterGroups": [{"filters": filters}],
        "properties": [
            "dealname", "dealstage", "pipeline", "amount", "closedate", "createdate",
            "dealtype", "hubspot_owner_id", "hs_is_closed_won", "hs_is_closed_lost",
            "deal_source", "hs_deal_stage_probability", "hs_manual_forecast_category",
            "hs_date_entered_71300358",
        ],
    }
    return _search_all("deals", payload)


@ttl_cache
def get_calls(start: datetime, end: datetime) -> list:
    start_ts = int(start.timestamp() * 1000)
    end_ts = int(end.timestamp() * 1000)
    payload = {
        "filterGroups": [
            {
                "filters": [
                    {"propertyName": "hs_timestamp", "operator": "GTE", "value": str(start_ts)},
                    {"propertyName": "hs_timestamp", "operator": "LTE", "value": str(end_ts)},
                ]
            }
        ],
        "properties": [
            "hs_timestamp", "hs_createdate", "hubspot_owner_id", "hs_call_disposition",
            "hs_call_duration", "hs_call_direction",
        ],
    }
    return _search_all("calls", payload)


def get_meetings(start: datetime, end: datetime) -> list:
    start_ts = int(start.timestamp() * 1000)
    end_ts = int(end.timestamp() * 1000)
    payload = {
        "filterGroups": [
            {
                "filters": [
                    {"propertyName": "hs_timestamp", "operator": "GTE", "value": str(start_ts)},
                    {"propertyName": "hs_timestamp", "operator": "LTE", "value": str(end_ts)},
                ]
            }
        ],
        "properties": ["hs_timestamp", "hubspot_owner_id", "hs_meeting_outcome", "hs_createdate"],
    }
    return _search_all("meetings", payload)


@ttl_cache
def get_contacts_inbound(start: datetime, end: datetime) -> list:
    start_ts = int(start.timestamp() * 1000)
    end_ts = int(end.timestamp() * 1000)
    payload = {
        "filterGroups": [
            {
                "filters": [
                    {"propertyName": "hs_analytics_source", "operator": "HAS_PROPERTY"},
                    {"propertyName": "createdate", "operator": "GTE", "value": str(start_ts)},
                    {"propertyName": "createdate", "operator": "LTE", "value": str(end_ts)},
                ]
            }
        ],
        "properties": [
            "firstname", "lastname", "email", "createdate", "hubspot_owner_id",
            "hs_analytics_source", "hs_analytics_source_data_1",
            "hs_lead_status", "lifecyclestage", "num_associated_deals",
        ],
    }
    return _search_all("contacts", payload)


@ttl_cache
def get_list_contacts(list_id: int, start: datetime, end: datetime) -> list:
    """Fetch contacts in a HubSpot list, filtered by demo_request_submitted_date in [start, end]."""
    props = [
        "firstname", "lastname", "email", "createdate", "hubspot_owner_id",
        "lifecyclestage", "utm_source",
        "demo_request_submitted_date", "first_sales_activity_after_demo_request",
    ]

    # Page through all list members
    member_ids = []
    after = None
    while True:
        url = f"{BASE_URL}/crm/v3/lists/{list_id}/memberships?limit=100"
        if after:
            url += f"&after={after}"
        resp = requests.get(url, headers=HEADERS)
        if not resp.ok:
            logger.warning("List %s memberships error: %s", list_id, resp.text)
            break
        data = resp.json()
        for r in data.get("results", []):
            member_ids.append(str(r["recordId"]))
        after = data.get("paging", {}).get("next", {}).get("after")
        if not after:
            break

    if not member_ids:
        return []

    # Batch read properties, then filter by demo_request_submitted_date
    contacts = []
    for i in range(0, len(member_ids), 100):
        batch = member_ids[i : i + 100]
        resp = requests.post(
            f"{BASE_URL}/crm/v3/objects/contacts/batch/read",
            headers=HEADERS,
            json={"inputs": [{"id": cid} for cid in batch], "properties": props},
        )
        if not resp.ok:
            continue
        for c in resp.json().get("results", []):
            raw = c["properties"].get("demo_request_submitted_date") or ""
            try:
                dt = _parse_hs_datetime(raw)
                if start <= dt <= end:
                    contacts.append(c)
            except ValueError:
                pass
    return contacts


def _batch_associations(from_type: str, to_type: str, from_ids: list) -> dict:
    """Batch fetch associations. Returns {from_id: [to_id, ...]}."""
    result = {}
    for i in range(0, len(from_ids), 100):
        batch = from_ids[i:i + 100]
        resp = requests.post(
            f"{BASE_URL}/crm/v4/associations/{from_type}/{to_type}/batch/read",
            headers=HEADERS,
            json={"inputs": [{"id": str(fid)} for fid in batch]},
        )
        if not resp.ok:
            continue
        for item in resp.json().get("results", []):
            from_id = str(item.get("from", {}).get("id", ""))
            to_ids = [str(t["toObjectId"]) for t in item.get("to", [])]
            if from_id and to_ids:
                result[from_id] = to_ids
    return result


@ttl_cache
def get_deal_contact_windows() -> dict:
    """
    Return {contact_id: [(open_start_ms, open_end_ms_or_None), ...]} for NB deals.
    open_end_ms is None if the deal is still open.
    Lets us check if a call happened while a deal was active for that contact's company.

    Two filter groups (HubSpot treats them as OR):
      1. All currently-open deals — no date cap, because an old open deal
         still means today's calls to that contact are not cold outreach.
      2. Deals closed in the last 12 months — a deal closed >12 months ago
         cannot affect any reporting period (YTD is the longest at ~12 months).
    This keeps the in-memory footprint bounded while remaining fully correct.
    """
    closed_cutoff_ms = int(
        (datetime.now(timezone.utc) - timedelta(days=365)).timestamp() * 1000
    )
    payload = {
        "filterGroups": [
            # Group 1 — open deals (any age)
            {"filters": [
                {"propertyName": "pipeline",       "operator": "EQ", "value": "31544320"},
                {"propertyName": "hs_is_closed_won",  "operator": "EQ", "value": "false"},
                {"propertyName": "hs_is_closed_lost", "operator": "EQ", "value": "false"},
            ]},
            # Group 2 — recently closed deals (last 12 months)
            {"filters": [
                {"propertyName": "pipeline",   "operator": "EQ",  "value": "31544320"},
                {"propertyName": "closedate",  "operator": "GTE", "value": str(closed_cutoff_ms)},
            ]},
        ],
        "properties": ["createdate", "closedate", "hs_is_closed_won", "hs_is_closed_lost"],
    }
    all_deals = _search_all("deals", payload)
    if not all_deals:
        return {}

    # Build deal_id → (open_start_ms, open_end_ms)
    deal_windows = {}
    for d in all_deals:
        props = d["properties"]
        try:
            open_start = int(datetime.fromisoformat(
                (props.get("createdate") or "").replace("Z", "+00:00")).timestamp() * 1000)
        except Exception:
            continue
        open_end = None
        is_closed = props.get("hs_is_closed_won") == "true" or props.get("hs_is_closed_lost") == "true"
        if is_closed and props.get("closedate"):
            try:
                open_end = int(datetime.fromisoformat(
                    props["closedate"].replace("Z", "+00:00")).timestamp() * 1000)
            except Exception:
                pass
        deal_windows[d["id"]] = (open_start, open_end)

    deal_ids = list(deal_windows.keys())

    # deals → companies → contacts
    deal_to_companies = _batch_associations("deals", "companies", deal_ids)
    company_ids = list({cid for cids in deal_to_companies.values() for cid in cids})
    if not company_ids:
        return {}
    company_to_contacts = _batch_associations("companies", "contacts", company_ids)

    # Map company → deal windows
    company_windows: dict = {}
    for deal_id, window in deal_windows.items():
        for cid in deal_to_companies.get(deal_id, []):
            company_windows.setdefault(cid, []).append(window)

    # Map contact → deal windows (via company)
    contact_windows: dict = {}
    for company_id, contacts in company_to_contacts.items():
        windows = company_windows.get(company_id, [])
        for contact_id in contacts:
            contact_windows.setdefault(contact_id, []).extend(windows)

    return contact_windows


@ttl_cache
def get_companies_for_coverage() -> list:
    """Fetch all companies owned by team members for book coverage analysis."""
    owner_ids = list(get_team_owner_ids())
    if not owner_ids:
        return []
    all_companies = []
    # HubSpot filterGroups are OR-ed; max 5 per request
    for i in range(0, len(owner_ids), 5):
        batch = owner_ids[i:i + 5]
        payload = {
            "filterGroups": [
                {"filters": [{"propertyName": "hubspot_owner_id", "operator": "EQ", "value": oid}]}
                for oid in batch
            ],
            "properties": [
                "hubspot_owner_id",
                "icp_rank",
                "notes_last_activity_date",
                "notes_last_contacted",
                "hs_last_call_date",
                "name",
                "in_active_sequence",
            ],
        }
        all_companies.extend(_search_all("companies", payload))
    return all_companies


@ttl_cache
def get_sequence_enrolled_company_ids() -> set:
    """Return the set of company IDs that have at least one contact in a sequence.

    Strategy: fetch contacts (by team owner) currently enrolled in a sequence,
    then batch-resolve their associated companies.
    """
    owner_ids = list(get_team_owner_ids())
    if not owner_ids:
        return set()

    seq_contacts = []
    for i in range(0, len(owner_ids), 5):
        batch = owner_ids[i:i + 5]
        payload = {
            "filterGroups": [
                {"filters": [
                    {"propertyName": "hubspot_owner_id", "operator": "EQ", "value": oid},
                    {"propertyName": "hs_sequences_is_enrolled", "operator": "EQ", "value": "true"},
                ]}
                for oid in batch
            ],
            "properties": ["hubspot_owner_id"],
        }
        seq_contacts.extend(_search_all("contacts", payload))

    if not seq_contacts:
        return set()

    contact_ids = [c["id"] for c in seq_contacts]
    contact_to_companies = _batch_associations("contacts", "companies", contact_ids)

    company_ids: set = set()
    for companies in contact_to_companies.values():
        company_ids.update(companies)
    return company_ids


@ttl_cache
def get_overdue_sequence_tasks() -> list:
    """Fetch overdue (past-due, not-started) tasks for team members."""
    owner_ids = list(get_team_owner_ids())
    if not owner_ids:
        return []
    now_ts = str(int(datetime.now(timezone.utc).timestamp() * 1000))
    all_tasks = []
    for i in range(0, len(owner_ids), 5):
        batch = owner_ids[i:i + 5]
        payload = {
            "filterGroups": [
                {"filters": [
                    {"propertyName": "hubspot_owner_id", "operator": "EQ", "value": oid},
                    {"propertyName": "hs_task_status", "operator": "EQ", "value": "NOT_STARTED"},
                    {"propertyName": "hs_timestamp", "operator": "LTE", "value": now_ts},
                ]}
                for oid in batch
            ],
            "properties": ["hubspot_owner_id", "hs_task_type", "hs_task_status", "hs_timestamp"],
        }
        all_tasks.extend(_search_all("tasks", payload))
    return all_tasks


def get_call_to_contact_map(call_ids: list) -> dict:
    """Return {call_id: contact_id} for each call.

    Not cached — always called from within compute_call_stats which is itself
    cached, so caching here only wastes memory (a huge tuple of all call IDs
    becomes the cache key, repeated for every period).
    """
    call_to_contacts = _batch_associations("calls", "contacts", call_ids)
    return {call_id: contacts[0] for call_id, contacts in call_to_contacts.items() if contacts}


@ttl_cache
def get_forecast_submissions() -> list:
    """Fetch forecast submissions from HubSpot's Forecast Read API (beta, Jan 2026).

    Requires crm.objects.forecasts.read scope on the token.
    Returns raw list of forecast objects; empty list if the API is unavailable
    or the token lacks the required scope.
    """
    # Discover all available properties first so we fetch everything useful.
    # Fall back to a known-good minimal set if the schema endpoint fails.
    props_to_fetch = [
        "hs_created_by_user_id",
        "hs_createdate",
        "hs_lastmodifieddate",
        "hs_milestone",
        "hs_object_id",
        "hs_team_id",
        "hs_year",
        # Amount field — name is not yet confirmed in public docs; try common variants.
        "hs_forecasted_amount",
        "hs_amount",
        "hs_submission_amount",
        "hs_target_amount",
    ]

    payload = {
        "filterGroups": [{"filters": []}],
        "properties": props_to_fetch,
        "limit": 200,
    }
    try:
        return _search_all("forecasts", payload)
    except Exception as exc:
        logger.warning("get_forecast_submissions: %s", exc)
        return []
