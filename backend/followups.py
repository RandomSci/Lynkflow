"""
Persistent follow-up tasks for Lynkflow.

Stored as JSON next to the existing call history/config files so follow-ups
survive refreshes, frontend reloads, and backend restarts.
"""

import hashlib
import json
import re
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

_FOLLOWUPS_FILE = Path(__file__).parent / "followups.json"
_MAX_TEXT = 1200
_STATUSES = {"pending", "attempted", "scheduled", "calling", "completed", "failed", "cancelled"}
_PRIORITIES = {"hot", "warm", "low"}


def _now() -> str:
    return datetime.now().isoformat(timespec="seconds")


def _load_store() -> dict:
    if not _FOLLOWUPS_FILE.exists():
        return {"follow_ups": []}
    try:
        data = json.loads(_FOLLOWUPS_FILE.read_text())
        if isinstance(data.get("follow_ups"), list):
            return data
    except Exception as e:
        print(f"[FOLLOWUPS] read failed: {e}")
    return {"follow_ups": []}


def _save_store(data: dict) -> None:
    _FOLLOWUPS_FILE.write_text(json.dumps(data, indent=2))


def _clean_text(value, max_len: int = _MAX_TEXT):
    if value is None:
        return None
    text = re.sub(r"\s+", " ", str(value)).strip()
    return text[:max_len] if text else None


def _clean_email(value):
    text = _clean_text(value, 254)
    if not text:
        return None
    m = re.search(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}", text)
    return m.group(0).lower() if m else None


def _clean_phone(value):
    text = _clean_text(value, 40)
    return text or None


def _local_now(item: dict) -> str | None:
    tz = item.get("lead_timezone_iana") or item.get("timezone_iana")
    if not tz:
        return None
    try:
        return datetime.now(ZoneInfo(str(tz))).isoformat(timespec="seconds")
    except Exception:
        return None


def _append_details(existing: str | None, addition: str | None, local_time: str | None = None) -> str | None:
    addition = _clean_text(addition, 1500)
    if not addition:
        return _clean_text(existing, 1500)
    prefix = f"[{local_time}] " if local_time else ""
    base = _clean_text(existing, 1500)
    merged = f"{base} {prefix}{addition}" if base else f"{prefix}{addition}"
    return _clean_text(merged, 2500)


def _bool_or_none(value):
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    text = str(value).strip().lower()
    if text in {"true", "yes", "1"}:
        return True
    if text in {"false", "no", "0"}:
        return False
    return None


def _append_email_event(item: dict, event: dict) -> list[dict]:
    events = list(item.get("email_events") or [])
    clean = {k: v for k, v in event.items() if v not in (None, "")}
    clean.setdefault("ts", _now())
    events.append(clean)
    return events[-50:]


def _is_twilio_recording(recording: str) -> bool:
    return str(recording or "").lower().endswith("_twilio.wav")


def _preferred_recording(existing: str | None, incoming: str) -> str:
    if not existing:
        return incoming
    if _is_twilio_recording(incoming):
        return incoming
    if _is_twilio_recording(existing):
        return existing
    return incoming


def _stable_id(previous_call_id: str, phone: str = "") -> str:
    key = previous_call_id or phone or _now()
    return "fu_" + hashlib.sha1(str(key).encode()).hexdigest()[:16]


def list_follow_ups(status: str = "") -> list[dict]:
    data = _load_store()
    items = data.get("follow_ups", [])
    if status:
        wanted = status.strip().lower()
        items = [f for f in items if str(f.get("status", "")).lower() == wanted]
    return sorted(items, key=lambda f: f.get("updated_at") or f.get("created_at") or "", reverse=True)


def get_follow_up(follow_up_id: str) -> dict | None:
    for item in _load_store().get("follow_ups", []):
        if item.get("id") == follow_up_id:
            return item
    return None


def find_by_previous_call_id(previous_call_id: str) -> dict | None:
    if not previous_call_id:
        return None
    for item in _load_store().get("follow_ups", []):
        if item.get("previous_call_id") == previous_call_id:
            return item
    return None


def find_by_call_id(call_id: str) -> dict | None:
    if not call_id:
        return None
    for item in _load_store().get("follow_ups", []):
        if item.get("call_id") == call_id or call_id in (item.get("call_ids") or []):
            return item
    return None


def _normalise_payload(payload: dict, call: dict, email_delivery_status: str = "unknown") -> dict:
    priority = str(payload.get("priority") or "warm").strip().lower()
    if priority not in _PRIORITIES:
        priority = "warm"

    previous_call_id = str(call.get("call_sid") or payload.get("previous_call_id") or call.get("recording") or "").strip()
    phone = _clean_phone(payload.get("phone") or call.get("phone"))
    verified_delivery = _clean_text(email_delivery_status, 80)
    payload_delivery = _clean_text(payload.get("email_delivery_status"), 80)
    delivery_status = verified_delivery if verified_delivery and verified_delivery != "unknown" else payload_delivery
    return {
        "priority": priority,
        "business": _clean_text(payload.get("business") or call.get("business"), 180),
        "phone": phone,
        "contact_role": _clean_text(payload.get("previous_contact_role") or payload.get("contact_role"), 120),
        "contact_name": _clean_text(payload.get("previous_contact_name") or payload.get("contact_name"), 120),
        "previous_contact_role": _clean_text(payload.get("previous_contact_role") or payload.get("contact_role"), 120),
        "previous_contact_name": _clean_text(payload.get("previous_contact_name") or payload.get("contact_name"), 120),
        "current_contact_role": _clean_text(payload.get("current_contact_role"), 120),
        "current_contact_name": _clean_text(payload.get("current_contact_name"), 120),
        "email": _clean_email(payload.get("email")),
        "previous_call_id": previous_call_id,
        "previous_call_date": _clean_text(call.get("date") or payload.get("previous_call_date"), 80),
        "previous_call_local_date": _clean_text(call.get("lead_local_date") or payload.get("previous_call_local_date"), 80),
        "previous_call_local_display": _clean_text(call.get("lead_local_display") or payload.get("previous_call_local_display"), 120),
        "lead_timezone": _clean_text(call.get("lead_timezone") or payload.get("lead_timezone"), 80),
        "lead_timezone_iana": _clean_text(call.get("lead_timezone_iana") or payload.get("lead_timezone_iana"), 120),
        "previous_recording": _clean_text(call.get("recording"), 260),
        "pain_point": _clean_text(payload.get("pain_point")),
        "current_solution": _clean_text(payload.get("current_solution")),
        "interest_signal": _clean_text(payload.get("interest_signal")),
        "previous_action": _clean_text(payload.get("previous_action")),
        "email_delivery_status": delivery_status or "unknown",
        "email_received": _bool_or_none(payload.get("email_received")),
        "prospect_reported_not_received": _bool_or_none(payload.get("prospect_reported_not_received")),
        "email_status_details": _clean_text(payload.get("email_status_details"), 1000),
        "next_action": _clean_text(payload.get("next_action"), 1000),
        "follow_up_goal": _clean_text(payload.get("follow_up_goal")),
        "agent_summary": _clean_text(payload.get("agent_summary") or payload.get("context_summary")),
        "context_summary": _clean_text(payload.get("context_summary")),
        "details": _clean_text(payload.get("details"), 1500),
        "reason": _clean_text(payload.get("reason")),
        "scheduled_for": _clean_text(payload.get("scheduled_for"), 80),
    }


def create_or_update_from_analysis(call: dict, decision: dict, email_delivery_status: str = "unknown") -> dict:
    """Create/update one follow-up for one source call. Idempotent by call ID."""
    if not decision or not decision.get("eligible"):
        return {"created": False, "updated": False, "follow_up": None}

    fields = _normalise_payload(decision, call, email_delivery_status=email_delivery_status)
    previous_call_id = fields.get("previous_call_id") or ""
    if not previous_call_id:
        previous_call_id = str(call.get("recording") or call.get("phone") or "")
        fields["previous_call_id"] = previous_call_id

    data = _load_store()
    items = data.setdefault("follow_ups", [])
    existing = None
    for item in items:
        if item.get("previous_call_id") == previous_call_id:
            existing = item
            break

    now = _now()
    if existing:
        existing.update({k: v for k, v in fields.items() if v is not None})
        existing["updated_at"] = now
        existing.setdefault("status", "pending")
        existing.setdefault("attempts", 0)
        existing.setdefault("last_attempt_at", None)
        existing.setdefault("result", None)
        existing.setdefault("call_id", None)
        existing.setdefault("call_ids", [])
        existing.setdefault("analysis", {})["follow_up"] = decision
        _save_store(data)
        return {"created": False, "updated": True, "follow_up": existing}

    item = {
        "id": _stable_id(previous_call_id, fields.get("phone") or ""),
        "created_at": now,
        "updated_at": now,
        "status": "pending",
        **fields,
        "attempts": 0,
        "last_attempt_at": None,
        "result": None,
        "call_id": None,
        "call_ids": [],
        "analysis": {"follow_up": decision},
    }
    items.append(item)
    _save_store(data)
    return {"created": True, "updated": False, "follow_up": item}


def update_follow_up(follow_up_id: str, updates: dict) -> dict | None:
    data = _load_store()
    now = _now()
    allowed = {
        "status", "priority", "business", "phone", "contact_role", "contact_name", "email",
        "pain_point", "current_solution", "interest_signal", "previous_action",
        "email_delivery_status", "email_received", "prospect_reported_not_received", "email_status_details", "email_events", "next_action",
        "follow_up_goal", "agent_summary", "context_summary", "details", "scheduled_for",
        "previous_contact_role", "previous_contact_name", "current_contact_role", "current_contact_name",
        "previous_call_local_date", "previous_call_local_display", "lead_timezone", "lead_timezone_iana", "last_attempt_local_at",
        "attempts", "last_attempt_at", "result", "call_id", "call_ids", "attempt_history", "follow_up_recording", "follow_up_recordings",
    }
    for item in data.get("follow_ups", []):
        if item.get("id") != follow_up_id:
            continue
        for key, value in (updates or {}).items():
            if key not in allowed:
                continue
            if key == "status":
                value = str(value or "").lower()
                if value not in _STATUSES:
                    continue
            elif key == "priority":
                value = str(value or "").lower()
                if value not in _PRIORITIES:
                    value = "warm"
            elif key == "email":
                value = _clean_email(value)
            elif key == "phone":
                value = _clean_phone(value)
            elif key == "attempts":
                value = max(0, int(value or 0))
            elif key in {"email_received", "prospect_reported_not_received"}:
                value = _bool_or_none(value)
            elif key == "call_ids":
                value = [str(v) for v in value if v]
            elif key == "follow_up_recordings":
                value = [Path(str(v)).name for v in value if v]
            elif key == "attempt_history":
                value = [v for v in value if isinstance(v, dict)][-100:]
            elif key == "email_events":
                value = [v for v in value if isinstance(v, dict)][-50:]
            elif isinstance(value, str) or value is None:
                value = _clean_text(value)
            item[key] = value
        item["updated_at"] = now
        _save_store(data)
        return item
    return None


def mark_call_started(follow_up_id: str, call_id: str) -> dict | None:
    item = get_follow_up(follow_up_id)
    if not item:
        return None
    call_ids = list(dict.fromkeys((item.get("call_ids") or []) + ([call_id] if call_id else [])))
    started_at = _now()
    local_started_at = _local_now(item)
    history = list(item.get("attempt_history") or [])
    history.append({
        "attempt": int(item.get("attempts") or 0) + 1,
        "call_id": call_id,
        "started_at": started_at,
        "local_started_at": local_started_at,
        "status": "calling",
        "reason": item.get("follow_up_goal") or item.get("next_action") or item.get("reason"),
    })
    return update_follow_up(follow_up_id, {
        "status": "calling",
        "attempts": int(item.get("attempts") or 0) + 1,
        "last_attempt_at": started_at,
        "last_attempt_local_at": local_started_at,
        "call_id": call_id,
        "call_ids": call_ids,
        "attempt_history": history,
    })


def complete_call_attempt(follow_up_id: str, call: dict, result: dict | None = None) -> dict | None:
    item = get_follow_up(follow_up_id)
    if not item:
        return None
    result = result or {}
    status = str(result.get("status") or "completed").lower()
    if status not in _STATUSES:
        status = "completed"
    if status == "pending" and int(item.get("attempts") or 0) > 0:
        status = "attempted"
    updates = {
        "status": status,
        "result": _clean_text(result.get("result") or call.get("outcome") or "completed"),
    }
    local_time = call.get("lead_local_display") or call.get("lead_local_date") or _local_now(item)
    if call.get("recording"):
        recording = Path(str(call.get("recording"))).name
        recordings = list(dict.fromkeys((item.get("follow_up_recordings") or []) + [recording]))
        updates["follow_up_recording"] = _preferred_recording(item.get("follow_up_recording"), recording)
        updates["follow_up_recordings"] = recordings
    if not result.get("details") and updates.get("result"):
        updates["details"] = _append_details(item.get("details"), f"Follow-up call result: {updates.get('result')}", local_time)
    if result.get("details"):
        updates["details"] = _append_details(item.get("details"), result.get("details"), local_time)
    email_updates = {}
    if result.get("email_delivery_status"):
        new_status = str(result.get("email_delivery_status") or "").strip().lower()
        if new_status == "email_failed" and item.get("email_delivery_status") == "email_sent":
            new_status = "email_sent"
        if new_status in {"email_sent", "email_failed", "email_ready_for_review", "email_generated", "unknown"}:
            email_updates["email_delivery_status"] = new_status
    if "email_received" in result:
        received = _bool_or_none(result.get("email_received"))
        if received is not None:
            email_updates["email_received"] = received
    if "prospect_reported_not_received" in result:
        not_received = _bool_or_none(result.get("prospect_reported_not_received"))
        if not_received is not None:
            email_updates["prospect_reported_not_received"] = not_received
    if result.get("email_status_details"):
        email_updates["email_status_details"] = result.get("email_status_details")
    if result.get("next_action"):
        email_updates["next_action"] = result.get("next_action")
    if email_updates:
        event = {
            "type": "prospect_email_update",
            "local_time": local_time,
            "email_delivery_status": email_updates.get("email_delivery_status", item.get("email_delivery_status")),
            "email_received": email_updates.get("email_received"),
            "prospect_reported_not_received": email_updates.get("prospect_reported_not_received"),
            "details": email_updates.get("email_status_details"),
        }
        email_updates["email_events"] = _append_email_event(item, event)
        updates.update(email_updates)
    if result.get("scheduled_for"):
        updates["scheduled_for"] = result.get("scheduled_for")
    if status in {"scheduled"}:
        for key in ("follow_up_goal", "scheduled_for"):
            if result.get(key):
                updates[key] = result.get(key)
    elif status == "completed":
        for key in ("email",):
            if result.get(key):
                updates[key] = result.get(key)
    for source, target in (("current_contact_role", "current_contact_role"), ("current_contact_name", "current_contact_name")):
        if result.get(source):
            updates[target] = result.get(source)
    if result.get("contact_role") and not result.get("current_contact_role"):
        updates["current_contact_role"] = result.get("contact_role")
    if result.get("contact_name") and not result.get("current_contact_name"):
        updates["current_contact_name"] = result.get("contact_name")
    history = list(item.get("attempt_history") or [])
    call_id = call.get("call_sid") or item.get("call_id")
    attempt_record = None
    for rec in reversed(history):
        if call_id and rec.get("call_id") == call_id:
            attempt_record = rec
            break
    if not attempt_record:
        attempt_record = {"attempt": int(item.get("attempts") or 0), "call_id": call_id}
        history.append(attempt_record)
    attempt_record.update({
        "ended_at": _now(),
        "local_ended_at": local_time,
        "status": status,
        "result": updates.get("result"),
        "reason": result.get("reason") or result.get("details") or updates.get("result"),
    })
    if call.get("recording"):
        incoming_recording = Path(str(call.get("recording"))).name
        attempt_record["recording"] = _preferred_recording(attempt_record.get("recording"), incoming_recording)
    updates["attempt_history"] = history
    updated = update_follow_up(follow_up_id, updates)
    if updated:
        analysis = updated.setdefault("analysis", {})
        analysis["latest_follow_up_result"] = result
        data = _load_store()
        for item in data.get("follow_ups", []):
            if item.get("id") == follow_up_id:
                item["analysis"] = analysis
                item["updated_at"] = _now()
                updated = item
                break
        _save_store(data)
    return updated


def mark_call_recording(call_id: str, recording: str) -> dict | None:
    if not call_id or not recording:
        return None
    recording = Path(str(recording)).name
    data = _load_store()
    now = _now()
    for item in data.get("follow_ups", []):
        if item.get("call_id") != call_id and call_id not in (item.get("call_ids") or []):
            continue
        recordings = list(dict.fromkeys((item.get("follow_up_recordings") or []) + [recording]))
        item["follow_up_recording"] = _preferred_recording(item.get("follow_up_recording"), recording)
        item["follow_up_recordings"] = recordings
        history = list(item.get("attempt_history") or [])
        for rec in reversed(history):
            if rec.get("call_id") == call_id:
                rec["recording"] = _preferred_recording(rec.get("recording"), recording)
                break
        if history:
            item["attempt_history"] = history
        item["updated_at"] = now
        _save_store(data)
        return item
    return None


def note_email_sent(to_email: str, subject: str = "") -> list[dict]:
    email = _clean_email(to_email)
    if not email:
        return []
    data = _load_store()
    changed = []
    now = _now()
    for item in data.get("follow_ups", []):
        if str(item.get("email") or "").lower() != email:
            continue
        item["email_delivery_status"] = "email_sent"
        if item.get("email_received") is None:
            item["email_received"] = False
        if item.get("prospect_reported_not_received") is None:
            item["prospect_reported_not_received"] = False
        detail = f"Sent from lynkflowagent@gmail.com to {email}."
        if subject:
            detail += f" Subject: {str(subject)[:200]}."
        item["email_status_details"] = detail
        item["email_events"] = _append_email_event(item, {
            "type": "email_sent",
            "from": "lynkflowagent@gmail.com",
            "to": email,
            "subject": _clean_text(subject, 200),
            "details": detail,
        })
        item.setdefault("next_action", "Follow up to confirm whether the prospect received the information and wants to discuss next steps.")
        if not item.get("previous_action"):
            item["previous_action"] = "Email sent."
        item["last_email_sent_at"] = now
        item["last_email_subject"] = _clean_text(subject, 200)
        item["updated_at"] = now
        changed.append(item)
    if changed:
        _save_store(data)
    return changed


def note_email_failed(to_email: str, subject: str = "", error: str = "") -> list[dict]:
    email = _clean_email(to_email)
    if not email:
        return []
    data = _load_store()
    changed = []
    now = _now()
    for item in data.get("follow_ups", []):
        if str(item.get("email") or "").lower() != email:
            continue
        if item.get("email_delivery_status") != "email_sent":
            item["email_delivery_status"] = "email_failed"
        detail = f"Verified system send failure from lynkflowagent@gmail.com to {email}."
        if error:
            detail += f" Error: {_clean_text(error, 300)}."
        item["email_status_details"] = detail
        item["email_events"] = _append_email_event(item, {
            "type": "email_failed",
            "from": "lynkflowagent@gmail.com",
            "to": email,
            "subject": _clean_text(subject, 200),
            "details": detail,
        })
        item["next_action"] = "Resolve the verified email sending failure before telling the prospect an email was sent."
        item["updated_at"] = now
        changed.append(item)
    if changed:
        _save_store(data)
    return changed
