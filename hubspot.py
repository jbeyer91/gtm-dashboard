import os
import requests
from datetime import datetime, timedelta, timezone
from functools import lru_cache

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
        end = now.replace(month=q_month, day=1) - timedelta(seconds=1)
        prev_q_month = ((end.month - 1) // 3) * 3 + 1
        start = end.replace(month=prev_q_month, day=1, hour=0, minute=0, second=0, microsecond=0)
        return start, end
    elif period == "ytd":
        start = now.replace(month=1, day=1, hour=0, minute=0, second=0, microsecond=0)
        return start, now
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
        if not after or len(results) >= min(data.get("total", 0), max_records):
            break
    return results


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
        }
    return owners


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
            "hs_closed_lost_reason", "num_associated_contacts",
            "hs_date_entered_71300357", "hs_date_entered_71300358",
            "hs_date_entered_1294419353", "hs_date_entered_71300359",
            "hs_date_entered_71300362", "hs_date_entered_71300363",
        ],
    }
    return _search_all("deals", payload)


def get_all_open_deals() -> list:
    payload = {
        "filterGroups": [
            {
                "filters": [
                    {"propertyName": "pipeline", "operator": "EQ", "value": "31544320"},
                    {"propertyName": "hs_is_closed_won", "operator": "EQ", "value": "false"},
                    {"propertyName": "hs_is_closed_lost", "operator": "EQ", "value": "false"},
                ]
            }
        ],
        "properties": [
            "dealname", "dealstage", "pipeline", "amount", "closedate", "createdate",
            "dealtype", "hubspot_owner_id", "hs_is_closed_won", "hs_is_closed_lost",
            "hs_analytics_source", "num_notes_and_activities",
        ],
    }
    return _search_all("deals", payload)


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
