from dotenv import load_dotenv
load_dotenv()
import asyncio
import base64
import json
import os
import hashlib
import secrets
import urllib.parse
import time
import uuid
import re
from datetime import datetime
from zoneinfo import ZoneInfo
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.utils import formataddr
from pathlib import Path
from fastapi import FastAPI, HTTPException, Request, WebSocket
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse, JSONResponse, StreamingResponse, Response
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import httpx
from twilio.jwt.access_token import AccessToken
from twilio.jwt.access_token.grants import VoiceGrant
from twilio.twiml.voice_response import VoiceResponse, Dial
from agent_config import (
    AgentConfig,
    PUBLIC_CONTACT_EMAIL,
    PUBLIC_WEBSITE_LABEL,
    PUBLIC_WEBSITE_URL,
    load_agent_config,
    save_agent_config as _save_agent_config,
)
from agent_live_ws import GPTLiveCallHandler


OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
AUDIO_CACHE_DIR = Path(__file__).parent.parent / "frontend" / "static" / "audio"
AUDIO_CACHE_DIR.mkdir(parents=True, exist_ok=True)

TWILIO_ACCOUNT_SID = os.getenv("TWILIO_ACCOUNT_SID")
TWILIO_AUTH_TOKEN = os.getenv("TWILIO_AUTH_TOKEN")
TWILIO_CALLER_ID = os.getenv("TWILIO_CALLER_ID")
TWILIO_TWIML_APP_SID = os.getenv("TWILIO_TWIML_APP_SID")
TWILIO_API_KEY_SID = os.getenv("TWILIO_API_KEY_SID")
TWILIO_API_KEY_SECRET = os.getenv("TWILIO_API_KEY_SECRET")
GOOGLE_SHEETS_API_KEY = os.getenv("GOOGLE_SHEETS_API_KEY")
GOOGLE_SHEET_ID = os.getenv("GOOGLE_SHEET_ID")
LYNKFLOW_CONSOLE_ID = os.getenv("LynkFlow_Console_ID") or os.getenv("LYNKFLOW_CONSOLE_ID")
LYNKFLOW_CONSOLE_SECRET = os.getenv("LynkFlow_Console_Secret") or os.getenv("LYNKFLOW_CONSOLE_SECRET")

app = FastAPI()
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])
app.mount("/static", StaticFiles(directory=Path(__file__).parent.parent / "frontend" / "static"), name="static")

# ── Agent call state (in-memory per server process) ──────────────────────────
_agent_events: dict[str, list] = {}       # call_sid → buffered events
_agent_queues: dict[str, asyncio.Queue] = {}  # call_sid → live queue for SSE
_agent_handlers: dict = {}     # call_sid -> GPTLiveCallHandler
_call_leads: dict[str, dict] = {}  # call_sid -> lead metadata for late Twilio callbacks
_pending_recordings: dict[str, str] = {}  # call_sid -> Twilio recording saved before history write
_REC_DIR = Path(__file__).parent / "recordings"
_REC_DIR.mkdir(exist_ok=True)
_TRANSCRIPT_DIR = _REC_DIR / "transcripts"
_TRANSCRIPT_DIR.mkdir(exist_ok=True)
_ANALYST_CHAT_FILE = Path(__file__).parent / "analytics_chats.json"
_EMAIL_TOKEN_FILE = Path(__file__).parent / "email_token.json"
_EMAIL_SEND_LOG_FILE = Path(__file__).parent / "email_send_log.jsonl"
_EMAIL_OAUTH_STATES: dict[str, dict] = {}
app.mount("/recordings", StaticFiles(directory=_REC_DIR), name="recordings")

_GMAIL_SEND_SCOPE = "https://www.googleapis.com/auth/gmail.send"

ANALYST_ABOUT = (
    "Lynkflow builds AI voice agents for local service businesses. "
    "The core offer is simple: help businesses never miss customer calls when they are busy, after hours, or already on another call. "
    "The outbound AI agent Anna may have reached out to a business, spoken with a receptionist, owner, or staff member, and gathered useful info such as email, phone number, owner availability, interest level, or callback timing. "
    "Follow-up emails should be short, professional, copy-ready, and mention that Anna from Lynkflow reached out about helping them handle customer calls so they do not miss leads. "
    f"Use {PUBLIC_CONTACT_EMAIL} as Lynkflow's email. In HTML email drafts, link to {PUBLIC_WEBSITE_URL} with the visible text {PUBLIC_WEBSITE_LABEL}. "
    "Do not overpromise, do not invent facts, do not claim they were interested unless the transcript supports it, and do not use em dashes or long dashes."
)

# ── Auto dialer state (in-memory per server process) ─────────────────────────
_autodial_state = {
    "running": False,
    "concurrency": 1,
    "base_url": "",
    "queue": [],
    "active": {},
    "completed": 0,
    "failed": 0,
    "skipped": 0,
    "call_limit": 0,
    "eligible_total": 0,
    "target_total": 0,
    "started_at": None,
    "task": None,
    "events": [],
    "test_mode": False,
    "sim_scenario": "mixed",
    "sim_endpoint": "",
}
_autodial_listeners: set[asyncio.Queue] = set()
_autodial_lock = asyncio.Lock()
_followup_listeners: set[asyncio.Queue] = set()

_AUTODIAL_ELIGIBLE_STATUSES = {"", "new", "retry", "queued"}
_ANALYST_ACTION_CREATE_FOLLOW_UP = "CREATE_FOLLOW_UP"
_ANALYST_ACTION_UPDATE_CALL_OUTCOMES = "UPDATE_CALL_OUTCOMES"
_ANALYST_ACTION_MARKER_CREATE_FOLLOW_UP = "[ACTION:CREATE_FOLLOW_UP]"
_ANALYST_ACTION_MARKER_UPDATE_CALL_OUTCOMES = "[ACTION:UPDATE_CALL_OUTCOMES]"
_ALLOWED_ANALYST_ACTIONS = {_ANALYST_ACTION_CREATE_FOLLOW_UP, _ANALYST_ACTION_UPDATE_CALL_OUTCOMES}
_ALLOWED_CALL_OUTCOMES = {
    "interested", "skeptical", "callback", "not_interested", "voicemail",
    "gatekeeper", "wrong_number", "no_answer", "ivr", "conversation",
    "busy", "failed", "do_not_call", "unknown",
}
_CALL_OUTCOME_ALIASES = {
    "interested owner": "interested",
    "owner_interested": "interested",
    "warm": "interested",
    "warm_lead": "interested",
    "skeptical owner": "skeptical",
    "owner_skeptical": "skeptical",
    "callback request": "callback",
    "call_back": "callback",
    "call back": "callback",
    "not interested": "not_interested",
    "no interest": "not_interested",
    "declined": "not_interested",
    "voicemail left": "voicemail",
    "left voicemail": "voicemail",
    "gate keeper": "gatekeeper",
    "receptionist": "gatekeeper",
    "wrong number": "wrong_number",
    "bad number": "wrong_number",
    "no answer": "no_answer",
    "did not answer": "no_answer",
    "dnc": "do_not_call",
    "do not call": "do_not_call",
}


def _phone_key(phone: str) -> str:
    d = "".join(ch for ch in str(phone or "") if ch.isdigit())
    return d[-10:] if len(d) >= 10 else d


def _format_us_phone(phone: str) -> str:
    d = "".join(ch for ch in str(phone or "") if ch.isdigit())
    if len(d) == 10:
        return f"+1{d}"
    if len(d) == 11 and d.startswith("1"):
        return f"+{d}"
    return str(phone or "").strip()


def _lead_timezone(lead: dict) -> str:
    for key in ("Timezone", "Time Zone", "timezone", "time_zone", "TZ", "tz"):
        val = str((lead or {}).get(key, "")).strip()
        if val:
            return val
    state = str((lead or {}).get("State") or (lead or {}).get("state") or "").strip()
    state_map = {
        "CT":"ET","DE":"ET","FL":"ET","GA":"ET","IN":"ET","KY":"ET","ME":"ET","MD":"ET","MA":"ET","MI":"ET","NH":"ET","NJ":"ET","NY":"ET","NC":"ET","OH":"ET","PA":"ET","RI":"ET","SC":"ET","TN":"ET","VT":"ET","VA":"ET","WV":"ET","DC":"ET",
        "AL":"CT","AR":"CT","IL":"CT","IA":"CT","LA":"CT","MN":"CT","MS":"CT","MO":"CT","NE":"CT","ND":"CT","OK":"CT","SD":"CT","TX":"CT","WI":"CT","KS":"CT",
        "AZ":"MT","CO":"MT","ID":"MT","MT":"MT","NM":"MT","UT":"MT","WY":"MT",
        "CA":"PT","NV":"PT","OR":"PT","WA":"PT",
    }
    named = {
        "connecticut":"ET","delaware":"ET","florida":"ET","georgia":"ET","indiana":"ET","kentucky":"ET","maine":"ET","maryland":"ET","massachusetts":"ET","michigan":"ET","new hampshire":"ET","new jersey":"ET","new york":"ET","north carolina":"ET","ohio":"ET","pennsylvania":"ET","rhode island":"ET","south carolina":"ET","tennessee":"ET","vermont":"ET","virginia":"ET","west virginia":"ET","district of columbia":"ET",
        "alabama":"CT","arkansas":"CT","illinois":"CT","iowa":"CT","louisiana":"CT","minnesota":"CT","mississippi":"CT","missouri":"CT","nebraska":"CT","north dakota":"CT","oklahoma":"CT","south dakota":"CT","texas":"CT","wisconsin":"CT","kansas":"CT",
        "arizona":"MT","colorado":"MT","idaho":"MT","montana":"MT","new mexico":"MT","utah":"MT","wyoming":"MT",
        "california":"PT","nevada":"PT","oregon":"PT","washington":"PT",
    }
    if state:
        return state_map.get(state.upper()) or named.get(state.lower(), "")
    return ""


def _timezone_iana(tz: str) -> str:
    key = str(tz or "").strip().lower()
    aliases = {
        "et": "America/New_York", "est": "America/New_York", "edt": "America/New_York", "eastern": "America/New_York", "eastern time": "America/New_York",
        "ct": "America/Chicago", "cst": "America/Chicago", "cdt": "America/Chicago", "central": "America/Chicago", "central time": "America/Chicago",
        "mt": "America/Denver", "mst": "America/Denver", "mdt": "America/Denver", "mountain": "America/Denver", "mountain time": "America/Denver",
        "pt": "America/Los_Angeles", "pst": "America/Los_Angeles", "pdt": "America/Los_Angeles", "pacific": "America/Los_Angeles", "pacific time": "America/Los_Angeles",
        "az": "America/Phoenix", "arizona": "America/Phoenix",
    }
    if key in aliases:
        return aliases[key]
    try:
        ZoneInfo(str(tz or ""))
        return str(tz)
    except Exception:
        return ""


def _lead_time_fields(lead: dict, ts: float | None = None) -> dict:
    ts = ts or time.time()
    tz_label = _lead_timezone(lead)
    tz_iana = _timezone_iana(tz_label)
    out = {"ts": ts, "date": datetime.fromtimestamp(ts).isoformat(timespec="seconds")}
    if tz_label:
        out["lead_timezone"] = tz_label
    if tz_iana:
        out["lead_timezone_iana"] = tz_iana
        local = datetime.fromtimestamp(ts, ZoneInfo(tz_iana))
        out["lead_local_date"] = local.isoformat(timespec="seconds")
        out["lead_local_display"] = local.strftime("%Y-%m-%d %I:%M %p %Z")
    return out


def _is_autodial_eligible(lead: dict) -> bool:
    if not _phone_key(lead.get("Phone", "")):
        return False
    status = str(lead.get("Status", "")).strip().lower()
    return status in _AUTODIAL_ELIGIBLE_STATUSES


def _autodial_snapshot() -> dict:
    active = _autodial_state["active"]
    target_total = int(_autodial_state.get("target_total") or 0)
    attempted = _autodial_state["completed"] + _autodial_state["failed"] + len(active)
    return {
        "running": _autodial_state["running"],
        "concurrency": _autodial_state["concurrency"],
        "test_mode": _autodial_state.get("test_mode", False),
        "sim_scenario": _autodial_state.get("sim_scenario", "mixed"),
        "call_limit": _autodial_state.get("call_limit", 0),
        "eligible_total": _autodial_state.get("eligible_total", 0),
        "target_total": target_total,
        "attempted": attempted,
        "remaining": max(0, target_total - attempted) if target_total else len(_autodial_state["queue"]),
        "queued": len(_autodial_state["queue"]),
        "active": len(active),
        "completed": _autodial_state["completed"],
        "failed": _autodial_state["failed"],
        "skipped": _autodial_state["skipped"],
        "active_calls": [
            {
                "call_sid": sid,
                "name": item.get("lead", {}).get("Name", ""),
                "phone": item.get("phone", ""),
                "mode": item.get("mode", "live"),
                "engine": item.get("engine", "GPT-Live" if item.get("mode", "live") == "live" else "GPT Lead Test"),
                "state": item.get("state", "dialing"),
                "last_speaker": item.get("last_speaker", ""),
                "last_text": item.get("last_text", ""),
                "updated_at": item.get("updated_at"),
                "started_at": item.get("started_at"),
            }
            for sid, item in active.items()
        ],
    }


async def _autodial_broadcast(event: dict):
    event = {**event, "snapshot": _autodial_snapshot(), "ts": datetime.now().strftime("%H:%M:%S")}
    _autodial_state["events"].append(event)
    _autodial_state["events"] = _autodial_state["events"][-100:]
    for q in list(_autodial_listeners):
        try:
            await q.put(event)
        except Exception:
            _autodial_listeners.discard(q)


async def _followup_broadcast(event: dict):
    event = {**event, "ts": datetime.now().strftime("%H:%M:%S")}
    for q in list(_followup_listeners):
        try:
            await q.put(event)
        except Exception:
            _followup_listeners.discard(q)


def _followup_counts(items: list[dict] | None = None) -> dict:
    if items is None:
        from followups import list_follow_ups
        items = list_follow_ups()
    counts = {
        "total": len(items), "pending": 0, "attempted": 0, "scheduled": 0,
        "calling": 0, "completed": 0, "failed": 0, "cancelled": 0,
        "hot": 0, "warm": 0, "low": 0, "needs_action": 0,
    }
    for item in items:
        status = str(item.get("status") or "pending").lower()
        if status in counts:
            counts[status] += 1
        priority = str(item.get("priority") or "warm").lower()
        if priority in counts:
            counts[priority] += 1
        if status in {"pending", "attempted"}:
            counts["needs_action"] += 1
    return counts


async def _autodial_note_call_event(call_sid: str, event: dict):
    async with _autodial_lock:
        item = _autodial_state["active"].get(call_sid)
        if not item:
            return

        payload = {
            "type": "call_event",
            "call_sid": call_sid,
            "name": item.get("lead", {}).get("Name", ""),
            "phone": item.get("phone", ""),
        }

        if event.get("type") == "status":
            item["state"] = event.get("state", "")
            item["updated_at"] = time.time()
            payload.update({
                "event": "status",
                "state": event.get("state", ""),
                "message": event.get("message", ""),
            })
        elif event.get("type") in ("transcript", "transcript_final"):
            text = (event.get("text") or "").strip()
            if not text:
                return
            item["last_speaker"] = event.get("speaker", "")
            item["last_text"] = text
            item["updated_at"] = time.time()
            payload.update({
                "event": "transcript",
                "speaker": event.get("speaker", ""),
                "text": text,
            })
        else:
            return

    await _autodial_broadcast(payload)

class TTSRequest(BaseModel):
    text: str
    section_id: str


@app.get("/")
async def root():
    response = FileResponse(Path(__file__).parent.parent / "frontend" / "index.html")
    response.headers["Cache-Control"] = "no-cache, no-store, must-revalidate"
    response.headers["Pragma"] = "no-cache"
    response.headers["Expires"] = "0"
    return response


@app.post("/api/tts")
async def generate_tts(req: TTSRequest):
    if not OPENAI_API_KEY:
        raise HTTPException(status_code=500, detail="OPENAI_API_KEY not set in .env")

    cache_key = hashlib.md5(f"{req.section_id}:{req.text}".encode()).hexdigest()
    audio_path = AUDIO_CACHE_DIR / f"{cache_key}.mp3"

    if audio_path.exists():
        return JSONResponse({"url": f"/static/audio/{cache_key}.mp3", "cached": True})

    async with httpx.AsyncClient(timeout=30) as client:
        resp = await client.post(
            "https://api.openai.com/v1/audio/speech",
            headers={"Authorization": f"Bearer {OPENAI_API_KEY}", "Content-Type": "application/json"},
            json={
                "model": "gpt-4o-mini-tts",
                "voice": "alloy",
                "input": req.text,
                "response_format": "mp3",
            },
        )

    if resp.status_code != 200:
        raise HTTPException(status_code=502, detail=f"OpenAI audio preview error: {resp.text}")

    audio_path.write_bytes(resp.content)
    return JSONResponse({"url": f"/static/audio/{cache_key}.mp3", "cached": False})


async def _fetch_leads_from_sheet() -> list[dict]:
    if not GOOGLE_SHEETS_API_KEY or not GOOGLE_SHEET_ID:
        raise HTTPException(status_code=500, detail="Google Sheets credentials not set in .env")
    url = f"https://sheets.googleapis.com/v4/spreadsheets/{GOOGLE_SHEET_ID}/values/Sheet1?key={GOOGLE_SHEETS_API_KEY}"
    async with httpx.AsyncClient(timeout=15) as client:
        resp = await client.get(url)
    if resp.status_code != 200:
        raise HTTPException(status_code=502, detail=f"Sheets error: {resp.text}")
    data = resp.json()
    rows = data.get("values", [])
    if len(rows) < 2:
        return []
    headers = rows[0]
    leads = []
    for row in rows[1:]:
        lead = {}
        for i, h in enumerate(headers):
            lead[h.strip()] = row[i].strip() if i < len(row) else ""
        if lead.get("Phone"):
            leads.append(lead)
    return leads


async def _update_lead_record(name: str, phone: str, status: str, notes: str = "", lead: dict | None = None) -> dict:
    if not status:
        raise HTTPException(status_code=400, detail="status required")

    date_str = datetime.now().strftime("%Y-%m-%d %H:%M")
    lead = lead or {}

    apps_script_url = os.getenv("GOOGLE_APPS_SCRIPT_URL")
    if apps_script_url:
        try:
            async with httpx.AsyncClient(timeout=12) as client:
                resp = await client.post(
                    apps_script_url,
                    json={"name": name, "phone": phone, "status": status,
                          "notes": notes, "date": date_str},
                )
            result = resp.json()
            if result.get("ok"):
                return {"success": True, "method": "apps_script"}
        except Exception as e:
            print(f"Apps Script update failed ({e}), falling back to n8n")

    webhook_payload = {
        "name":   name,
        "phone":  phone,
        "status": status,
        "notes":  notes,
        "date":   date_str,
        **lead,
    }
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            resp = await client.post(
                "https://boil-refined-shingle.ngrok-free.dev/webhook/6ba4a9af-5e6c-43bd-84de-2cd6f3d59b8f",
                json=webhook_payload,
            )
        return {"success": resp.status_code == 200, "method": "n8n_webhook"}
    except Exception as e:
        return {"success": False, "error": str(e)}


async def _fetch_twilio_call_price(call_sid: str) -> float | None:
    if not call_sid or not TWILIO_ACCOUNT_SID:
        return None
    async with httpx.AsyncClient(timeout=10) as client:
        resp = await client.get(
            f"https://api.twilio.com/2010-04-01/Accounts/{TWILIO_ACCOUNT_SID}/Calls/{call_sid}.json",
            auth=(TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN),
        )
    if resp.status_code != 200:
        return None
    price = resp.json().get("price")
    if price in (None, ""):
        return None
    return abs(float(price))


async def _store_twilio_actual_price(call_sid: str):
    for delay in (3, 6, 10, 20):
        await asyncio.sleep(delay)
        try:
            price = await _fetch_twilio_call_price(call_sid)
            if price is None:
                continue
            from call_history import update_twilio_price
            if update_twilio_price(call_sid, price):
                print(f"[TWILIO PRICE] stored actual price for {call_sid}: {price}")
            return
        except Exception as e:
            print(f"[TWILIO PRICE] fetch failed for {call_sid}: {e}")


def _safe_recording_name(name: str) -> str:
    text = re.sub(r"[^A-Za-z0-9_. -]+", "", str(name or "")).strip()
    return re.sub(r"\s+", " ", text)[:42] or "Twilio Recording"


async def _download_twilio_recording(call_sid: str, recording_sid: str, recording_url: str, lead_name: str = "") -> str:
    if not recording_url:
        return ""
    url = recording_url if recording_url.endswith(".wav") else f"{recording_url}.wav"
    async with httpx.AsyncClient(timeout=30) as client:
        resp = await client.get(url, auth=(TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN))
    if resp.status_code != 200:
        raise RuntimeError(f"Twilio recording download failed: {resp.status_code} {resp.text[:200]}")

    name = _safe_recording_name(lead_name)
    suffix = (recording_sid or call_sid or uuid.uuid4().hex)[-6:]
    filename = f"{datetime.now().strftime('%Y%m%d_%H%M%S')}_{name}_{suffix}_twilio.wav"
    (_REC_DIR / filename).write_bytes(resp.content)
    return filename


async def _fetch_latest_twilio_recording(call_sid: str) -> dict | None:
    if not call_sid or not TWILIO_ACCOUNT_SID:
        return None
    async with httpx.AsyncClient(timeout=12) as client:
        resp = await client.get(
            f"https://api.twilio.com/2010-04-01/Accounts/{TWILIO_ACCOUNT_SID}/Calls/{call_sid}/Recordings.json",
            auth=(TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN),
        )
    if resp.status_code != 200:
        print(f"[RECORDING RECOVERY] list failed for {call_sid}: {resp.status_code} {resp.text[:200]}")
        return None
    recordings = resp.json().get("recordings", []) or []
    completed = [r for r in recordings if r.get("status") == "completed"]
    if not completed:
        return None
    return sorted(completed, key=lambda r: r.get("date_created") or "", reverse=True)[0]


async def _recover_twilio_recording(call_sid: str, lead_name: str = "") -> str:
    for delay in (2, 5, 10, 20):
        await asyncio.sleep(delay)
        try:
            rec = await _fetch_latest_twilio_recording(call_sid)
            if not rec:
                continue
            recording_sid = rec.get("sid", "")
            if not recording_sid:
                continue
            recording_url = f"https://api.twilio.com/2010-04-01/Accounts/{TWILIO_ACCOUNT_SID}/Recordings/{recording_sid}"
            filename = await _download_twilio_recording(call_sid, recording_sid, recording_url, lead_name)
            if filename:
                from call_history import update_recording_file
                update_recording_file(call_sid, filename)
                try:
                    from followups import mark_call_recording
                    updated = mark_call_recording(call_sid, filename)
                    if updated:
                        await _followup_broadcast({"type": "follow_up_updated", "follow_up": updated, "message": "Follow-up recording recovered"})
                except Exception as e:
                    print(f"[FOLLOWUP] recording recovery link failed: {e}")
                await _autodial_broadcast({
                    "type": "recording_ready",
                    "call_sid": call_sid,
                    "name": lead_name,
                    "recording": filename,
                    "message": "Twilio dual-channel recording recovered",
                })
                print(f"[RECORDING RECOVERY] stored Twilio recording for {call_sid}: {filename}")
                return filename
        except Exception as e:
            print(f"[RECORDING RECOVERY] failed for {call_sid}: {e}")
    return ""


def _safe_recording_path(recording: str) -> Path:
    name = Path(str(recording or "")).name
    if not name:
        raise HTTPException(400, "recording required")
    path = (_REC_DIR / name).resolve()
    rec_root = _REC_DIR.resolve()
    if rec_root not in path.parents or not path.exists() or not path.is_file():
        raise HTTPException(404, "recording not found")
    if path.suffix.lower() not in {".wav", ".mp3", ".m4a", ".txt"}:
        raise HTTPException(400, "unsupported recording type")
    return path


def _transcript_cache_path(recording: str) -> Path:
    name = Path(str(recording or "")).name
    digest = hashlib.md5(name.encode()).hexdigest()[:10]
    stem = re.sub(r"[^A-Za-z0-9_.-]+", "_", Path(name).stem)[:80]
    return _TRANSCRIPT_DIR / f"{stem}_{digest}.json"


def _extract_contact_candidates(text: str) -> dict:
    email_pattern = r"[A-Za-z0-9._%+-]+\s*(?:@|\bat\b)\s*[A-Za-z0-9.-]+\s*(?:\.|\bdot\b)\s*[A-Za-z]{2,}"
    raw_emails = re.findall(email_pattern, text, flags=re.IGNORECASE)
    emails = []
    for e in raw_emails:
        cleaned = e.lower().strip()
        cleaned = re.sub(r"\s+at\s+", "@", cleaned)
        cleaned = re.sub(r"\s+dot\s+", ".", cleaned)
        cleaned = re.sub(r"\s+", "", cleaned)
        if "@" in cleaned and "." in cleaned:
            emails.append(cleaned)

    spoken_pattern = r"\b([A-Za-z][A-Za-z0-9._-]*(?:\s+[A-Za-z][A-Za-z0-9._-]*){0,2})\s+at\s+([A-Za-z0-9][A-Za-z0-9-]*(?:\s+[A-Za-z0-9][A-Za-z0-9-]*){0,4})\s+dot\s+([A-Za-z]{2,})\b"
    for local, domain, tld in re.findall(spoken_pattern, text, flags=re.IGNORECASE):
        local_clean = re.sub(r"[^a-z0-9._%+-]+", "", local.lower())
        local_clean = re.sub(r"^(itis|its|is|emailis)", "", local_clean)
        domain_clean = re.sub(r"[^a-z0-9-]+", "", domain.lower())
        if local_clean and domain_clean:
            emails.append(f"{local_clean}@{domain_clean}.{tld.lower()}")

    phone_pattern = r"(?:\+?1[\s.-]?)?(?:\(?\d{3}\)?[\s.-]?)\d{3}[\s.-]?\d{4}"
    phones = re.findall(phone_pattern, text)
    return {
        "emails": sorted(set(emails)),
        "phones": sorted(set(p.strip() for p in phones if p.strip())),
    }


def _sanitize_analyst_answer(text: str) -> str:
    text = re.sub(r"[—–]", "-", str(text or ""))
    text = re.sub(
        r"(?i)paste this into follow-ups\s*>\s*update json\.?",
        "Use the Apply to Follow-Ups button shown under the JSON.",
        text,
    )
    text = re.sub(
        r"(?i)the json is ready to apply using the [^.]*\. ?",
        "The follow-up JSON is ready.",
        text,
    )
    return text.strip()


def _email_oauth_configured() -> bool:
    return bool(LYNKFLOW_CONSOLE_ID and LYNKFLOW_CONSOLE_SECRET)


def _load_email_token() -> dict:
    if not _EMAIL_TOKEN_FILE.exists():
        return {}
    try:
        data = json.loads(_EMAIL_TOKEN_FILE.read_text())
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def _save_email_token(data: dict) -> None:
    _EMAIL_TOKEN_FILE.write_text(json.dumps(data, indent=2))
    try:
        _EMAIL_TOKEN_FILE.chmod(0o600)
    except Exception:
        pass


def _email_redirect_uri(request: Request) -> str:
    explicit = os.getenv("LYNKFLOW_EMAIL_REDIRECT_URI") or os.getenv("EMAIL_REDIRECT_URI")
    if explicit:
        return explicit.rstrip("/")
    try:
        cfg_base_url = (load_agent_config().base_url or "").strip()
    except Exception:
        cfg_base_url = ""
    base = (
        os.getenv("LynkFlow_Console_URI")
        or os.getenv("LYNKFLOW_CONSOLE_URI")
        or cfg_base_url
        or os.getenv("LYNKFLOW_PUBLIC_URL")
        or os.getenv("PUBLIC_URL")
        or str(request.base_url).rstrip("/")
    ).rstrip("/")
    if base.endswith("/api/email/oauth/callback"):
        return base
    return f"{base}/api/email/oauth/callback"


async def _get_gmail_access_token() -> str | None:
    token = _load_email_token()
    access_token = token.get("access_token")
    expires_at = float(token.get("expires_at") or 0)
    if access_token and expires_at > time.time() + 60:
        return access_token

    refresh_token = token.get("refresh_token")
    if not refresh_token or not _email_oauth_configured():
        return None

    async with httpx.AsyncClient(timeout=15) as client:
        resp = await client.post(
            "https://oauth2.googleapis.com/token",
            data={
                "client_id": LYNKFLOW_CONSOLE_ID,
                "client_secret": LYNKFLOW_CONSOLE_SECRET,
                "refresh_token": refresh_token,
                "grant_type": "refresh_token",
            },
        )
    if resp.status_code != 200:
        print(f"[EMAIL] refresh failed: {resp.status_code} {resp.text[:300]}")
        return None
    fresh = resp.json()
    token.update({
        "access_token": fresh.get("access_token"),
        "expires_at": time.time() + int(fresh.get("expires_in") or 3600),
        "scope": fresh.get("scope", token.get("scope", _GMAIL_SEND_SCOPE)),
        "token_type": fresh.get("token_type", token.get("token_type", "Bearer")),
    })
    _save_email_token(token)
    return token.get("access_token")


def _plain_text_from_html(html: str) -> str:
    text = re.sub(r"(?is)<(script|style).*?>.*?</\1>", "", html or "")
    text = re.sub(r"(?i)<br\s*/?>", "\n", text)
    text = re.sub(r"(?i)</p\s*>", "\n\n", text)
    text = re.sub(r"<[^>]+>", "", text)
    text = text.replace("&nbsp;", " ").replace("&amp;", "&").replace("&lt;", "<").replace("&gt;", ">")
    return re.sub(r"\n{3,}", "\n\n", text).strip()


def _validate_recipient_email(addr: str) -> str:
    addr = str(addr or "").strip()
    if not re.fullmatch(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}", addr):
        raise HTTPException(400, "valid recipient email required")
    return addr


async def _send_gmail_message(to_email: str, subject: str, html: str, text: str = "") -> dict:
    access_token = await _get_gmail_access_token()
    if not access_token:
        raise HTTPException(401, "Gmail is not connected")

    to_email = _validate_recipient_email(to_email)
    subject = str(subject or "").strip()
    html = str(html or "").strip()
    if not subject:
        raise HTTPException(400, "subject required")
    if not html:
        raise HTTPException(400, "html body required")

    msg = MIMEMultipart("alternative")
    msg["To"] = to_email
    msg["From"] = formataddr(("Anna from Lynkflow", PUBLIC_CONTACT_EMAIL))
    msg["Subject"] = subject[:200]
    msg.attach(MIMEText(text or _plain_text_from_html(html), "plain", "utf-8"))
    msg.attach(MIMEText(html, "html", "utf-8"))
    raw = base64.urlsafe_b64encode(msg.as_bytes()).decode("ascii")

    async with httpx.AsyncClient(timeout=20) as client:
        resp = await client.post(
            "https://gmail.googleapis.com/gmail/v1/users/me/messages/send",
            headers={"Authorization": f"Bearer {access_token}", "Content-Type": "application/json"},
            json={"raw": raw},
        )
    if resp.status_code not in (200, 201):
        raise HTTPException(502, f"Gmail send failed: {resp.text}")
    data = resp.json()
    return {"success": True, "id": data.get("id"), "thread_id": data.get("threadId")}


def _log_email_sent(to_email: str, subject: str, result: dict) -> dict:
    entry = {
        "to": str(to_email or "").strip().lower(),
        "subject": str(subject or "").strip()[:200],
        "sent_at": datetime.now().isoformat(timespec="seconds"),
        "gmail_id": result.get("id"),
        "thread_id": result.get("thread_id"),
        "status": "email_sent",
    }
    try:
        with _EMAIL_SEND_LOG_FILE.open("a") as f:
            f.write(json.dumps(entry) + "\n")
    except Exception as e:
        print(f"[EMAIL] send log failed: {e}")
    return entry


def _email_delivery_status_for(email: str) -> str:
    target = str(email or "").strip().lower()
    if not target or not _EMAIL_SEND_LOG_FILE.exists():
        return "unknown"
    try:
        for line in _EMAIL_SEND_LOG_FILE.read_text().splitlines():
            if not line.strip():
                continue
            try:
                item = json.loads(line)
            except Exception:
                continue
            if str(item.get("to") or "").strip().lower() == target and item.get("status") == "email_sent":
                return "email_sent"
    except Exception as e:
        print(f"[EMAIL] send log read failed: {e}")
    return "unknown"


async def _transcribe_recording(recording: str) -> dict:
    path = _safe_recording_path(recording)
    cache = _transcript_cache_path(recording)
    if cache.exists():
        try:
            return json.loads(cache.read_text())
        except Exception:
            pass

    if path.suffix.lower() == ".txt":
        text = path.read_text(errors="ignore")
    else:
        if not OPENAI_API_KEY:
            raise HTTPException(500, "OPENAI_API_KEY not set")
        async with httpx.AsyncClient(timeout=180) as client:
            with path.open("rb") as f:
                resp = await client.post(
                    "https://api.openai.com/v1/audio/transcriptions",
                    headers={"Authorization": f"Bearer {OPENAI_API_KEY}"},
                    data={"model": "whisper-1", "response_format": "json"},
                    files={"file": (path.name, f, "audio/wav")},
                )
        if resp.status_code != 200:
            raise HTTPException(502, f"Transcription failed: {resp.text}")
        text = resp.json().get("text", "").strip()

    data = {
        "recording": Path(recording).name,
        "transcript": text,
        "contacts": _extract_contact_candidates(text),
        "cached": False,
        "created_at": datetime.now().isoformat(timespec="seconds"),
    }
    cache.write_text(json.dumps(data, indent=2))
    return data


async def _ask_transcript_agent(recording: str, question: str) -> dict:
    if not OPENAI_API_KEY:
        raise HTTPException(500, "OPENAI_API_KEY not set")
    transcript = await _transcribe_recording(recording)
    call = _find_call(recording) or {}
    q = (question or "").strip()
    if not q:
        q = "Summarize this call, extract any emails or phone numbers, and draft a short follow-up message I can copy."

    prompt = (
        "You are a call QA assistant for Lynkflow. Answer only from the transcript. "
        "If a detail is uncertain, say it is uncertain. Extract emails, phone numbers, names, callback times, and useful follow-up context. "
        f"When drafting an email, use {PUBLIC_CONTACT_EMAIL} as Lynkflow's email and include a website link as "
        f"<a href=\"{PUBLIC_WEBSITE_URL}\">{PUBLIC_WEBSITE_LABEL}</a>. "
        "Return the draft with separate sections named 'Email subject' and 'HTML body'. "
        "Put the subject in a fenced code block tagged subject and the HTML email in a fenced code block tagged html. "
        "If the operator asks to create, draft, write, prepare, or send an email, always include both fenced blocks. "
        "Make the HTML responsive with inline CSS, a clean dark/blue Lynkflow style, mobile-safe width, and no external images unless they are fluid. Do not invent facts. "
        "If and only if the operator explicitly asks you to update, apply, change, set, mark, move, or persist this call's analytics outcome, append [ACTION:UPDATE_CALL_OUTCOMES] on its own line, then one JSON object, then [/ACTION] on its own line. The JSON must contain updates with one item using the exact call_sid from metadata, outcome, reason, evidence, and confidence from 0 to 1. Allowed outcomes are interested, skeptical, callback, not_interested, voicemail, gatekeeper, wrong_number, no_answer, ivr, conversation, busy, failed, do_not_call, unknown. Do not emit action blocks for ordinary summaries or non-persistent classification questions. "
    )
    async with httpx.AsyncClient(timeout=180) as client:
        resp = await client.post(
            "https://api.openai.com/v1/chat/completions",
            headers={"Authorization": f"Bearer {OPENAI_API_KEY}", "Content-Type": "application/json"},
            json={
                "model": "gpt-4.5-mini",
                "temperature": 0.2,
                "messages": [
                    {"role": "system", "content": prompt},
                    {"role": "user", "content": f"Call metadata:\n{json.dumps(_compact_call_for_analysis(call, transcript), ensure_ascii=False)}\n\nTranscript:\n{transcript['transcript']}\n\nQuestion:\n{q}"},
                ],
            },
        )
    if resp.status_code != 200:
        raise HTTPException(502, f"Transcript agent failed: {resp.text}")
    answer = resp.json()["choices"][0]["message"].get("content", "")
    answer = _sanitize_analyst_answer(answer)
    return {"recording": Path(recording).name, "question": q, "answer": answer, "transcript": transcript, "call": call}


def _load_analyst_chats() -> dict:
    if not _ANALYST_CHAT_FILE.exists():
        return {"chats": []}
    try:
        data = json.loads(_ANALYST_CHAT_FILE.read_text())
        if isinstance(data.get("chats"), list):
            return data
    except Exception:
        pass
    return {"chats": []}


def _save_analyst_chats(data: dict) -> None:
    _ANALYST_CHAT_FILE.write_text(json.dumps(data, indent=2))


def _find_analyst_chat(data: dict, chat_id: str) -> dict:
    for chat in data.get("chats", []):
        if chat.get("id") == chat_id:
            return chat
    raise HTTPException(404, "chat not found")


def _new_analyst_chat(title: str = "New chat") -> dict:
    now = datetime.now().isoformat(timespec="seconds")
    return {
        "id": uuid.uuid4().hex,
        "title": title or "New chat",
        "created_at": now,
        "updated_at": now,
        "messages": [],
        "attachments": [],
    }


def _recording_catalog(days: int = 365, q: str = "") -> list[dict]:
    from call_history import load_calls
    query = (q or "").strip().lower()
    calls = sorted(load_calls(days), key=lambda c: c.get("ts", 0), reverse=True)
    out = []
    seen = set()
    for c in calls:
        recording = c.get("recording") or ""
        if not recording or recording in seen:
            continue
        seen.add(recording)
        item = {
            "recording": recording,
            "business": c.get("business", ""),
            "phone": c.get("phone", ""),
            "outcome": c.get("outcome", ""),
            "duration_s": c.get("duration_s", 0),
            "date": c.get("date", ""),
            "ts": c.get("ts", 0),
        }
        haystack = " ".join(str(v) for v in item.values()).lower()
        if query and query not in haystack:
            continue
        out.append(item)
    return out


def _normalise_analytics_days(value, default: int = 30, max_days: int = 3650) -> int:
    try:
        days = int(value or default)
    except Exception:
        days = default
    return max(1, min(max_days, days))


def _recording_ext(recording: str) -> str:
    ext = Path(str(recording or "")).suffix.lower().lstrip(".")
    return ext or "missing"


def _increment_count(target: dict, key: str, amount: int = 1) -> None:
    target[key] = int(target.get(key) or 0) + amount


def _directory_extension_counts(directory: Path, suffixes: set[str] | None = None) -> dict:
    counts = {"total": 0, "by_extension": {}, "available": directory.exists()}
    if not directory.exists():
        return counts
    for path in directory.iterdir():
        if not path.is_file():
            continue
        ext = path.suffix.lower().lstrip(".") or "missing"
        if suffixes and ext not in suffixes:
            continue
        counts["total"] += 1
        _increment_count(counts["by_extension"], ext)
    return counts


def _follow_up_store_counts() -> dict:
    try:
        from followups import list_follow_ups
        items = list_follow_ups()
    except Exception as e:
        return {"total": 0, "by_status": {}, "error": str(e)}
    by_status = {}
    for item in items:
        _increment_count(by_status, str(item.get("status") or "unknown").lower())
    return {"total": len(items), "by_status": by_status}


def _analytics_data_snapshot(days: int = 30) -> dict:
    from call_history import load_calls
    days = _normalise_analytics_days(days)
    calls = load_calls(days)
    recording_counts = {"wav": 0, "txt": 0, "mp3": 0, "m4a": 0, "missing": 0, "other": 0}
    outcome_counts = {}
    linked_transcript_calls = 0
    linked_transcript_recordings = set()
    unique_recordings = set()
    call_sids = []

    for call in calls:
        recording = str(call.get("recording") or "")
        ext = _recording_ext(recording)
        _increment_count(recording_counts, ext if ext in recording_counts else "other")
        _increment_count(outcome_counts, str(call.get("outcome") or "unknown").lower())
        if recording:
            unique_recordings.add(recording)
            if _transcript_cache_path(recording).exists():
                linked_transcript_calls += 1
                linked_transcript_recordings.add(recording)
        if call.get("call_sid"):
            call_sids.append(str(call.get("call_sid")))

    unique_call_sids = set(call_sids)
    recording_inventory = _directory_extension_counts(_REC_DIR, {"wav", "txt", "mp3", "m4a"})
    transcript_inventory = _directory_extension_counts(_TRANSCRIPT_DIR, {"json"})
    follow_ups = _follow_up_store_counts()
    total_calls = len(calls)
    timestamps = [float(c.get("ts") or 0) for c in calls if c.get("ts")]
    oldest_ts = min(timestamps) if timestamps else 0
    latest_ts = max(timestamps) if timestamps else 0

    return {
        "source": "backend_counter",
        "days": days,
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "range": {
            "oldest_call_ts": oldest_ts,
            "oldest_call_date": datetime.fromtimestamp(oldest_ts).isoformat(timespec="seconds") if oldest_ts else None,
            "latest_call_ts": latest_ts,
            "latest_call_date": datetime.fromtimestamp(latest_ts).isoformat(timespec="seconds") if latest_ts else None,
        },
        "counts": {
            "call_records": total_calls,
            "unique_call_sids": len(unique_call_sids),
            "duplicate_call_sid_records": max(0, len(call_sids) - len(unique_call_sids)),
            "recordings": {
                "total_linked_to_call_records": total_calls - recording_counts.get("missing", 0),
                "unique_linked_recordings": len(unique_recordings),
                "by_call_record_extension": recording_counts,
            },
            "transcripts": {
                "linked_call_records": linked_transcript_calls,
                "unique_linked_recordings": len(linked_transcript_recordings),
                "cache_files": transcript_inventory["total"],
            },
            "follow_ups": follow_ups,
            "outcomes": outcome_counts,
        },
        "methods": {
            "call_history_entries": {
                "description": "Count parsed call_history.jsonl entries inside the selected day range.",
                "count": total_calls,
            },
            "recording_field_by_extension": {
                "description": "Count each call record's recording field by file extension.",
                "counts": recording_counts,
            },
            "linked_transcript_cache": {
                "description": "For each call record recording, check whether its expected transcript cache JSON exists.",
                "linked_call_records": linked_transcript_calls,
                "unique_linked_recordings": len(linked_transcript_recordings),
            },
            "recording_directory_inventory": {
                "description": "Count actual recording files currently present on disk, independent of call-history date filtering.",
                **recording_inventory,
            },
            "transcript_directory_inventory": {
                "description": "Count actual transcript cache JSON files currently present on disk, including orphaned cache files.",
                **transcript_inventory,
            },
            "follow_up_store": {
                "description": "Count current follow-up tasks from the follow-up store.",
                **follow_ups,
            },
        },
    }


def _is_analytics_count_question(question: str) -> bool:
    text = str(question or "").strip()
    q = text.lower()
    explicit_request_terms = (
        "how many", "please count", "count the", "count entries", "count records",
        "provide a breakdown", "breakdown of", "show me", "tell me", "report",
        "what are the counts", "what are the totals", "get_data", "updated counts",
        "latest counts", "current counts", "new totals", "total calls", "total call records",
    )
    if not any(term in q for term in explicit_request_terms):
        return False
    future_context_terms = (
        "that's the current data", "that is the current data", "i'll add more",
        "i will add more", "will add more", "add more that's latest", "add more thats latest",
        "latest will be analyzed", "analyzed properly later", "analyse properly later",
    )
    if any(term in q for term in future_context_terms) and not any(term in q for term in ("updated counts", "latest counts", "current counts", "get_data")):
        return False
    if any(term in q for term in ("updated counts", "latest counts", "current counts", "get_data")):
        return True
    inventory_terms = (
        "call record", "call records", "total calls", "calls total", "current call context", "audio", "audios",
        "recording", "recordings", "wav", "txt", "transcript", "transcripts",
        "follow-up", "follow up", "followups", "context", "used",
    )
    return any(term in q for term in inventory_terms)


def _analytics_count_answer(snapshot: dict) -> str:
    counts = snapshot.get("counts", {})
    date_range = snapshot.get("range", {})
    recordings = counts.get("recordings", {}).get("by_call_record_extension", {})
    transcripts = counts.get("transcripts", {})
    follow_ups = counts.get("follow_ups", {})
    return (
        "**Verified Backend Counts**\n"
        f"For the selected `{snapshot.get('days')}` day range, these numbers come from the backend counter, not from model-estimated prompt context.\n\n"
        f"Oldest call in range: `{date_range.get('oldest_call_date') or 'none'}`. Latest call in range: `{date_range.get('latest_call_date') or 'none'}`.\n\n"
        "| Metric | Count |\n"
        "|---|---:|\n"
        f"| Total call records | {counts.get('call_records', 0)} |\n"
        f"| Unique call SIDs | {counts.get('unique_call_sids', 0)} |\n"
        f"| Duplicate call SID records | {counts.get('duplicate_call_sid_records', 0)} |\n"
        f"| Calls with `.wav` recording | {recordings.get('wav', 0)} |\n"
        f"| Calls with `.txt` recording | {recordings.get('txt', 0)} |\n"
        f"| Calls with other recording types | {recordings.get('mp3', 0) + recordings.get('m4a', 0) + recordings.get('other', 0)} |\n"
        f"| Calls missing recording field | {recordings.get('missing', 0)} |\n"
        f"| Call records with linked transcript cache | {transcripts.get('linked_call_records', 0)} |\n"
        f"| Unique recordings with linked transcript cache | {transcripts.get('unique_linked_recordings', 0)} |\n"
        f"| Transcript cache files on disk | {transcripts.get('cache_files', 0)} |\n"
        f"| Current follow-ups | {follow_ups.get('total', 0)} |\n\n"
        "Counting methods available to the Analyst: call-history entry count, recording-field extension count, linked transcript cache check, recording directory inventory, transcript directory inventory, and follow-up store count."
    )


def _is_future_data_readiness_message(question: str) -> bool:
    q = str(question or "").lower()
    return any(term in q for term in (
        "i'll add more", "i will add more", "will add more", "add more that's latest",
        "add more thats latest", "latest will be analyzed", "analyzed properly later",
        "analyse properly later", "make sure the latest",
    ))


def _future_data_readiness_answer(snapshot: dict) -> str:
    counts = snapshot.get("counts", {})
    date_range = snapshot.get("range", {})
    transcripts = counts.get("transcripts", {})
    return (
        "**Ready For Latest Data**\n"
        "When new calls are appended to call history and new transcript caches are created, the Analyst will use the backend counter again instead of reusing this chat's old numbers.\n\n"
        f"Current backend baseline for the selected `{snapshot.get('days')}` day range: `{counts.get('call_records', 0)}` call records, `{transcripts.get('linked_call_records', 0)}` linked transcript call records.\n"
        f"Current latest call seen by the backend: `{date_range.get('latest_call_date') or 'none'}`.\n\n"
        "After adding the latest batch, ask for `updated counts` or `latest counts` and the backend will recalculate from storage at request time."
    )


def _analyst_data_tools() -> list[dict]:
    return [{
        "type": "function",
        "function": {
            "name": "GET_DATA",
            "description": "Return exact backend analytics counts for calls, recordings, transcripts, outcomes, and follow-ups. Use this for any count, total, breakdown, or statistics question.",
            "parameters": {
                "type": "object",
                "properties": {
                    "dataset": {
                        "type": "string",
                        "enum": ["analytics_counts"],
                        "description": "The dataset to retrieve. Use analytics_counts for call, recording, transcript, and follow-up numbers.",
                    }
                },
                "required": ["dataset"],
                "additionalProperties": False,
            },
        },
    }]


def _run_analyst_data_tool(name: str, arguments: str, context: dict) -> dict:
    if name != "GET_DATA":
        return {"error": f"Unsupported tool: {name}"}
    try:
        args = json.loads(arguments or "{}")
    except Exception:
        args = {}
    dataset = args.get("dataset") or "analytics_counts"
    if dataset != "analytics_counts":
        return {"error": f"Unsupported dataset: {dataset}"}
    return context.get("data_snapshot") or _analytics_data_snapshot(context.get("days") or 30)


def _analyst_context_metadata(context: dict, get_data_used: bool = False) -> dict:
    snapshot = context.get("data_snapshot") or {}
    counts = snapshot.get("counts") or {}
    transcripts = counts.get("transcripts") or {}
    follow_ups = counts.get("follow_ups") or {}
    return {
        "calls": counts.get("call_records", len(context.get("calls") or [])),
        "transcripts": transcripts.get("linked_call_records", len(context.get("transcripts") or [])),
        "attachments": len(context.get("attachments") or []),
        "follow_ups": follow_ups.get("total", len(context.get("follow_up_action_plan") or [])),
        "data_source": snapshot.get("source") or "context",
        "get_data_used": get_data_used,
        "verified_counts": counts,
    }


def _call_matches_question(call: dict, question: str) -> bool:
    q = question.lower()
    business = str(call.get("business") or "").lower()
    if business and business in q:
        return True
    tokens = [t for t in re.split(r"\W+", business) if len(t) >= 4]
    return bool(tokens and sum(1 for t in tokens if t in q) >= min(2, len(tokens)))


def _is_broad_analytics_question(question: str) -> bool:
    q = question.lower()
    return any(k in q for k in (
        "which", "anyone", "all", "these", "provided", "email", "number",
        "phone", "info", "information", "interested", "callback", "summary",
        "summarize", "tell me about", "what happened",
    ))


def _followup_action_plan_context(limit: int = 25) -> list[dict]:
    try:
        from followups import list_follow_ups
        items = list_follow_ups()
    except Exception:
        return []
    priority_rank = {"hot": 0, "warm": 1, "low": 2}
    status_rank = {"calling": 0, "pending": 1, "scheduled": 2, "failed": 3, "attempted": 4, "completed": 5, "cancelled": 6}
    items = sorted(
        items,
        key=lambda f: (
            priority_rank.get(str(f.get("priority") or "warm").lower(), 1),
            status_rank.get(str(f.get("status") or "pending").lower(), 9),
            str(f.get("updated_at") or ""),
        ),
    )
    plan = []
    for f in items[:limit]:
        plan.append({
            "id": f.get("id"),
            "business": f.get("business"),
            "phone": f.get("phone"),
            "email": f.get("email"),
            "priority": f.get("priority"),
            "status": f.get("status"),
            "attempts": f.get("attempts"),
            "next_action": f.get("next_action"),
            "follow_up_goal": f.get("follow_up_goal"),
            "reason": f.get("reason"),
            "details": f.get("details"),
            "email_delivery_status": f.get("email_delivery_status"),
            "email_received": f.get("email_received"),
            "prospect_reported_not_received": f.get("prospect_reported_not_received"),
            "email_status_details": f.get("email_status_details"),
            "last_attempt_at": f.get("last_attempt_at"),
            "result": f.get("result"),
        })
    return plan


async def _build_analyst_context(question: str, days: int = 30, attachments: list[dict] | None = None) -> dict:
    from call_history import load_calls
    cfg = load_agent_config()
    days = _normalise_analytics_days(days)
    calls = sorted(load_calls(days), key=lambda c: c.get("ts", 0), reverse=True)
    recent = calls
    matched = [c for c in recent if _call_matches_question(c, question)]
    broad = _is_broad_analytics_question(question)

    attachments = attachments or []
    attached_recordings = {a.get("recording") for a in attachments if a.get("recording")}
    attached_calls = [c for c in calls if c.get("recording") in attached_recordings]
    for a in attachments:
        if not any(c.get("recording") == a.get("recording") for c in attached_calls):
            attached_calls.append({
                "business": a.get("business", ""),
                "phone": a.get("phone", ""),
                "outcome": a.get("outcome", ""),
                "duration_s": a.get("duration_s", 0),
                "recording": a.get("recording", ""),
                "date": a.get("date", ""),
            })

    selected = attached_calls + (matched or recent)
    deduped = []
    seen_keys = set()
    for call in selected:
        key = call.get("recording") or call.get("call_sid") or call.get("business")
        if key in seen_keys:
            continue
        seen_keys.add(key)
        deduped.append(call)
    selected = deduped
    transcripts = []
    auto_transcribed = []

    for call in selected:
        recording = call.get("recording")
        if not recording:
            continue
        cache = _transcript_cache_path(recording)
        should_auto = recording in attached_recordings or bool(matched) or cache.exists()
        if not cache.exists() and not should_auto:
            continue
        try:
            was_cached = cache.exists()
            t = await _transcribe_recording(recording)
            transcripts.append({
                "call_sid": call.get("call_sid", ""),
                "business": call.get("business", ""),
                "phone": call.get("phone", ""),
                "outcome": call.get("outcome", ""),
                "lead_status": call.get("lead_status", ""),
                "outcome_reason": call.get("outcome_reason", ""),
                "recording": recording,
                "transcript": t.get("transcript", ""),
                "contacts": t.get("contacts", {}),
            })
            if not was_cached:
                auto_transcribed.append(recording)
        except Exception as e:
            transcripts.append({
                "call_sid": call.get("call_sid", ""),
                "business": call.get("business", ""),
                "phone": call.get("phone", ""),
                "outcome": call.get("outcome", ""),
                "lead_status": call.get("lead_status", ""),
                "outcome_reason": call.get("outcome_reason", ""),
                "recording": recording,
                "error": str(e),
            })

    summary_rows = []
    for c in recent:
        summary_rows.append({
            "call_sid": c.get("call_sid", ""),
            "business": c.get("business", ""),
            "phone": c.get("phone", ""),
            "outcome": c.get("outcome", ""),
            "lead_status": c.get("lead_status", ""),
            "outcome_reason": c.get("outcome_reason", ""),
            "outcome_evidence": c.get("outcome_evidence", ""),
            "duration_s": c.get("duration_s", 0),
            "recording": c.get("recording", ""),
            "followup_note": c.get("followup_note", ""),
            "details": c.get("details", ""),
            "date": c.get("date", ""),
        })

    return {
        "days": days,
        "about_lynkflow": ANALYST_ABOUT,
        "current_agent_system_prompt": getattr(cfg, "system_prompt", ""),
        "current_first_message": getattr(cfg, "first_message", ""),
        "current_price": "$350 setup and $100/month unless your system prompt says otherwise",
        "data_snapshot": _analytics_data_snapshot(days),
        "calls": summary_rows,
        "attachments": attachments,
        "transcripts": transcripts,
        "auto_transcribed": auto_transcribed,
        "follow_up_action_plan": _followup_action_plan_context(),
    }


async def _ask_global_analyst(chat: dict, question: str, days: int = 30) -> dict:
    days = _normalise_analytics_days(days)
    if _is_analytics_count_question(question):
        snapshot = _analytics_data_snapshot(days)
        context_meta = _analyst_context_metadata({
            "days": days,
            "data_snapshot": snapshot,
            "attachments": chat.get("attachments", []),
        }, get_data_used=True)
        return {"answer": _analytics_count_answer(snapshot), "context": context_meta}

    if _is_future_data_readiness_message(question):
        snapshot = _analytics_data_snapshot(days)
        context_meta = _analyst_context_metadata({
            "days": days,
            "data_snapshot": snapshot,
            "attachments": chat.get("attachments", []),
        }, get_data_used=True)
        return {"answer": _future_data_readiness_answer(snapshot), "context": context_meta}

    if not OPENAI_API_KEY:
        raise HTTPException(500, "OPENAI_API_KEY not set")
    context = await _build_analyst_context(question, days, chat.get("attachments", []))
    prior = [m for m in chat.get("messages", []) if m.get("role") in {"user", "assistant"}]
    system = (
        "You are Lynkflow's internal call analyst. You know Lynkflow's offer and you have access to call history and available transcripts. "
        "Attached recordings are selected by the operator and are the highest-priority context. "
        "For any count, total, percentage, breakdown, stats, records, recordings, transcript, or follow-up quantity question, call the GET_DATA tool and use its backend_counter result as the source of truth. Do not count the calls or transcripts arrays manually and do not estimate. "
        "The context also includes follow_up_action_plan, which is the current execution ledger: what follow-ups exist, what was attempted, email state, and next priority. Use it when the operator asks what to do next or how to prevent the follow-up flow from failing. "
        "Answer the operator's question directly. Use clean Markdown headings, numbered lists, and bullets. Do not show raw Markdown markers as decoration. Do not wrap normal explanations in code fences. "
        "If you write a numbered section title, use format like '1. **Title:** explanation' so it renders cleanly. "
        "If they ask about a specific business, focus on that business. "
        "If they ask which calls provided information, use a compact markdown table with business, info found, evidence, and next action when that is clearer than prose. "
        "If asked to draft an email/message, create a polished copy-ready HTML email, not plain text unless the operator specifically asks for plain text. "
        "Separate the output into two sections named exactly 'Email subject' and 'HTML body'. Put the subject in a fenced code block tagged subject and the email body in a fenced code block tagged html. "
        "If the operator asks to create, draft, write, prepare, or send an email, always include both fenced blocks so the UI can show the recipient input and send button. "
        "The HTML body should look like Lynkflow's website: clean dark navy background, blue/cyan accent, rounded white/dark content card, strong headline, concise paragraphs, and clear CTA. Use inline CSS suitable for email clients. "
        "Make the HTML responsive for phones, laptops, and desktops: max-width around 640px, width 100%, mobile-safe padding, and any image/banner must use width:100%; max-width:100%; height:auto. "
        f"Include Lynkflow's website only inside the HTML email as <a href=\"{PUBLIC_WEBSITE_URL}\">{PUBLIC_WEBSITE_LABEL}</a>. Do not show the raw long URL as visible text. "
        f"Use {PUBLIC_CONTACT_EMAIL} as the sender/contact email when needed. "
        "The draft should mention Anna from Lynkflow reached out about helping them handle customer calls so they do not miss leads. "
        "If a receptionist gave an email, write the message as a professional follow-up to the owner or office manager without pretending the owner was interested. "
        "Never invent emails, phone numbers, callback times, or interest. If transcript evidence is missing, say so. "
        "Controlled actions: only append action blocks after the operator explicitly asks you to update, apply, move, change, set, mark, or persist records. Never update records from a casual summary or non-persistent classification request. "
        "To update call analytics outcomes, append [ACTION:UPDATE_CALL_OUTCOMES] on its own line, then one JSON object, then [/ACTION] on its own line. The JSON must contain an updates array. Each item must include call_sid from context or recording, outcome, reason, evidence, and confidence from 0 to 1. Allowed outcomes are interested, skeptical, callback, not_interested, voicemail, gatekeeper, wrong_number, no_answer, ivr, conversation, busy, failed, do_not_call, unknown. Use exact call_sid values, never loose business-name matching. Do not update an outcome unless the transcript or call metadata clearly supports it. "
        "Only if the operator explicitly asks you to create, add, prepare, or persist a follow-up task, and the transcript/call evidence supports it, append a follow-up action block after your normal answer. "
        "The exact format is [ACTION:CREATE_FOLLOW_UP] on its own line, then one JSON object, then [/ACTION] on its own line. "
        "The JSON must include previous_call_id using the call_sid from the context, plus business, phone, priority, reason, previous_contact_role, previous_contact_name, email, pain_point, current_solution, interest_signal, previous_action, follow_up_goal, agent_summary, context_summary, details, and scheduled_for. Use null for unknown fields. "
        "For every interested lead action, include a details field. Details must be clear, concise, and factual: who was actually spoken to including role and name if known or 'name not provided', what was discussed, pain points/current process/questions, information provided such as email or callback time, and any test-call, non-staff, unusual event, or identity-confusion clarification. Do not invent missing information. "
        "Do not emit action blocks for summaries, candidate lists, email drafts, or general recommendations. Never emit unapproved action names. "
        "If asked to update an existing follow-up, action plan, next_action, details, email status, attempts, or result, do not tell the operator to edit files, paste manually, click a button, or manually replace records. Return one complete fenced json object with id when known and all fields that should change. Do not narrate UI mechanics. Do not imply that you applied it unless an API/tool result proves it. "
        "If asked to update, improve, rewrite, or create a system prompt, return a complete replacement prompt, not a patch, not fragments, and not only the changed sections. Use this exact structure: short explanation first, then a section named 'Complete system prompt', then one fenced code block tagged system-prompt containing the full prompt from top to bottom. The code block must include all roles, objectives, context rules, action rules, communication rules, email rules, end-call rules, and hard constraints needed to run safely. "
        "For follow-up agent prompt updates, include these sections inside the system-prompt block: ROLE, SOURCE OF TRUTH, OBJECTIVE, EXECUTION PRIORITY, OPENING RULES, COMMUNICATION STYLE, EMAIL HANDLING, ACTION COLLECTION, ENDING RULES, HARD RULES. "
        "Do not use em dashes or long dashes. Use commas, periods, colons, or simple hyphens only."
    )
    messages = [{"role": "system", "content": system}]
    for m in prior:
        messages.append({"role": m["role"], "content": m.get("content", "")})
    messages.append({
        "role": "user",
        "content": f"Call context JSON:\n{json.dumps(context, ensure_ascii=False)}\n\nQuestion:\n{question}",
    })
    get_data_used = False
    async with httpx.AsyncClient(timeout=180) as client:
        resp = await client.post(
            "https://api.openai.com/v1/chat/completions",
            headers={"Authorization": f"Bearer {OPENAI_API_KEY}", "Content-Type": "application/json"},
            json={"model": "gpt-4.1", "temperature": 0.2, "messages": messages, "tools": _analyst_data_tools(), "tool_choice": "auto"},
        )
        if resp.status_code != 200:
            raise HTTPException(502, f"Analyst failed: {resp.text}")
        message = resp.json()["choices"][0]["message"]
        tool_calls = message.get("tool_calls") or []
        if tool_calls:
            messages.append({
                "role": "assistant",
                "content": message.get("content") or "",
                "tool_calls": tool_calls,
            })
            for tool_call in tool_calls:
                fn = tool_call.get("function") or {}
                result = _run_analyst_data_tool(fn.get("name") or "", fn.get("arguments") or "{}", context)
                if fn.get("name") == "GET_DATA":
                    get_data_used = True
                messages.append({
                    "role": "tool",
                    "tool_call_id": tool_call.get("id"),
                    "content": json.dumps(result, ensure_ascii=False),
                })
            resp = await client.post(
                "https://api.openai.com/v1/chat/completions",
                headers={"Authorization": f"Bearer {OPENAI_API_KEY}", "Content-Type": "application/json"},
                json={"model": "gpt-4.1", "temperature": 0.2, "messages": messages},
            )
        elif re.search(r"\bGET_DATA\b", message.get("content") or ""):
            get_data_used = True
            messages.append({"role": "assistant", "content": message.get("content") or ""})
            messages.append({
                "role": "user",
                "content": "Backend GET_DATA result:\n"
                + json.dumps(context.get("data_snapshot") or _analytics_data_snapshot(days), ensure_ascii=False)
                + "\n\nAnswer the original question using only this backend_counter data for numeric counts.",
            })
            resp = await client.post(
                "https://api.openai.com/v1/chat/completions",
                headers={"Authorization": f"Bearer {OPENAI_API_KEY}", "Content-Type": "application/json"},
                json={"model": "gpt-4.1", "temperature": 0.2, "messages": messages},
            )
    if resp.status_code != 200:
        raise HTTPException(502, f"Analyst failed: {resp.text}")
    answer = resp.json()["choices"][0]["message"].get("content", "")
    answer = _sanitize_analyst_answer(answer)
    return {
        "answer": answer,
        "context": _analyst_context_metadata(context, get_data_used=get_data_used),
    }


def _extract_json_object(text: str) -> dict:
    text = (text or "").strip()
    if not text:
        return {}
    try:
        return json.loads(text)
    except Exception:
        pass
    match = re.search(r"\{[\s\S]*\}", text)
    if match:
        try:
            return json.loads(match.group(0))
        except Exception:
            return {}
    return {}


def _extract_follow_up_update_json(value) -> dict:
    if isinstance(value, dict):
        return value
    text = str(value or "").strip()
    if not text:
        return {}
    fenced = re.search(r"```(?:json)?\s*([\s\S]*?)\s*```", text, flags=re.IGNORECASE)
    if fenced:
        try:
            data = json.loads(fenced.group(1).strip())
            return data if isinstance(data, dict) else {}
        except Exception:
            pass
    return _extract_json_object(text)


def _match_follow_up_for_update(data: dict) -> str:
    follow_up_id = str(data.get("id") or data.get("follow_up_id") or "").strip()
    if follow_up_id:
        return follow_up_id

    try:
        from followups import list_follow_ups
        items = list_follow_ups()
    except Exception:
        items = []
    phone = _phone_key(str(data.get("phone") or ""))
    email = str(data.get("email") or "").strip().lower()
    business = re.sub(r"\s+", " ", str(data.get("business") or "").strip().lower())
    for item in items:
        item_phone = _phone_key(str(item.get("phone") or ""))
        item_email = str(item.get("email") or "").strip().lower()
        item_business = re.sub(r"\s+", " ", str(item.get("business") or "").strip().lower())
        if phone and item_phone and phone == item_phone:
            return item.get("id", "")
        if email and item_email and email == item_email:
            return item.get("id", "")
        if business and item_business and business == item_business:
            return item.get("id", "")
    return ""


def _parse_analyst_action_blocks(text: str) -> tuple[str, list[dict]]:
    """Extract strict action blocks without treating normal prose as actions."""
    actions = []
    pattern = re.compile(r"\[ACTION:([A-Z_]+)\]\s*([\s\S]*?)\s*\[/ACTION\]", re.MULTILINE)

    def repl(match: re.Match) -> str:
        action_type = match.group(1).strip()
        raw_payload = match.group(2).strip()
        if action_type not in _ALLOWED_ANALYST_ACTIONS:
            actions.append({"type": action_type, "valid": False, "error": "unapproved action"})
            return ""
        try:
            payload = json.loads(raw_payload)
        except Exception:
            actions.append({"type": action_type, "valid": False, "error": "invalid JSON payload"})
            return ""
        if not isinstance(payload, dict):
            actions.append({"type": action_type, "valid": False, "error": "payload must be a JSON object"})
            return ""
        actions.append({"type": action_type, "marker": f"[ACTION:{action_type}]", "payload": payload, "valid": True})
        return ""

    clean = pattern.sub(repl, text or "")
    clean = re.sub(r"\n{3,}", "\n\n", clean).strip()
    return clean, actions


def _extract_structured_analyst_actions(data: dict) -> list[dict]:
    actions = []
    raw_actions = data.get("actions") if isinstance(data, dict) else None
    if not isinstance(raw_actions, list):
        return actions
    for raw in raw_actions:
        if not isinstance(raw, dict):
            actions.append({"type": "", "valid": False, "error": "action must be an object"})
            continue
        marker = str(raw.get("marker") or "").strip()
        action_type = str(raw.get("type") or "").strip()
        if marker == _ANALYST_ACTION_MARKER_CREATE_FOLLOW_UP:
            action_type = _ANALYST_ACTION_CREATE_FOLLOW_UP
        if marker == _ANALYST_ACTION_MARKER_UPDATE_CALL_OUTCOMES:
            action_type = _ANALYST_ACTION_UPDATE_CALL_OUTCOMES
        if action_type not in _ALLOWED_ANALYST_ACTIONS:
            actions.append({"type": action_type, "marker": marker, "valid": False, "error": "unapproved action"})
            continue
        if marker and marker != f"[ACTION:{action_type}]":
            actions.append({"type": action_type, "marker": marker, "valid": False, "error": "invalid marker"})
            continue
        payload = raw.get("payload")
        if not isinstance(payload, dict):
            actions.append({"type": action_type, "marker": marker, "valid": False, "error": "payload must be an object"})
            continue
        actions.append({"type": action_type, "marker": f"[ACTION:{action_type}]", "payload": payload, "valid": True})
    return actions


def _validate_create_follow_up_action_payload(payload: dict, call: dict | None = None) -> tuple[bool, str, dict, dict]:
    call = call or {}
    previous_call_id = str(payload.get("previous_call_id") or payload.get("call_sid") or call.get("call_sid") or call.get("recording") or "").strip()
    if not previous_call_id:
        return False, "previous_call_id is required", {}, {}

    source_call = call or _find_call(previous_call_id) or {}
    if not source_call:
        return False, "previous call not found", {}, {}

    decision = {
        "eligible": True,
        "priority": payload.get("priority") or "warm",
        "reason": payload.get("reason"),
        "business": payload.get("business") or source_call.get("business"),
        "phone": payload.get("phone") or source_call.get("phone"),
        "contact_role": payload.get("contact_role"),
        "contact_name": payload.get("contact_name"),
        "previous_contact_role": payload.get("previous_contact_role") or payload.get("contact_role"),
        "previous_contact_name": payload.get("previous_contact_name") or payload.get("contact_name"),
        "current_contact_role": payload.get("current_contact_role"),
        "current_contact_name": payload.get("current_contact_name"),
        "email": payload.get("email"),
        "pain_point": payload.get("pain_point"),
        "current_solution": payload.get("current_solution"),
        "interest_signal": payload.get("interest_signal"),
        "previous_action": payload.get("previous_action"),
        "email_delivery_status": payload.get("email_delivery_status"),
        "email_received": payload.get("email_received"),
        "prospect_reported_not_received": payload.get("prospect_reported_not_received"),
        "email_status_details": payload.get("email_status_details"),
        "next_action": payload.get("next_action"),
        "follow_up_goal": payload.get("follow_up_goal"),
        "agent_summary": payload.get("agent_summary"),
        "context_summary": payload.get("context_summary"),
        "details": payload.get("details"),
        "lead_timezone": payload.get("lead_timezone") or source_call.get("lead_timezone"),
        "lead_timezone_iana": payload.get("lead_timezone_iana") or source_call.get("lead_timezone_iana"),
        "previous_call_local_date": payload.get("previous_call_local_date") or source_call.get("lead_local_date"),
        "previous_call_local_display": payload.get("previous_call_local_display") or source_call.get("lead_local_display"),
        "scheduled_for": payload.get("scheduled_for"),
        "previous_call_id": previous_call_id,
    }
    if not decision.get("business") or not decision.get("phone"):
        return False, "business and phone are required", {}, {}
    if not decision.get("reason") or not decision.get("follow_up_goal"):
        return False, "reason and follow_up_goal are required", {}, {}
    if not (decision.get("interest_signal") or decision.get("context_summary") or decision.get("agent_summary")):
        return False, "commercial evidence summary is required", {}, {}
    if not decision.get("details"):
        return False, "details is required for interested follow-up leads", {}, {}
    return True, "", decision, source_call


async def _execute_analyst_actions(actions: list[dict], source_call: dict | None = None) -> list[dict]:
    results = []
    for action in actions or []:
        action_type = action.get("type")
        if not action.get("valid") or action_type not in _ALLOWED_ANALYST_ACTIONS:
            results.append({"type": action_type, "executed": False, "error": action.get("error") or "invalid action"})
            continue

        if action_type == _ANALYST_ACTION_UPDATE_CALL_OUTCOMES:
            payload = action.get("payload") or {}
            try:
                result = _apply_call_outcome_updates(payload, updated_by="ai_analyst", require_evidence=True)
                results.append({"type": action_type, "executed": result.get("updated", 0) > 0, **result})
            except Exception as e:
                results.append({"type": action_type, "executed": False, "error": str(e)})
            continue

        if action_type != _ANALYST_ACTION_CREATE_FOLLOW_UP:
            results.append({"type": action_type, "executed": False, "error": "unsupported action"})
            continue

        payload = action.get("payload") or {}
        call_id = str(payload.get("previous_call_id") or payload.get("call_sid") or "").strip()
        call = _find_call(call_id) if call_id else None
        if call_id and not call and source_call and call_id in {str(source_call.get("call_sid") or ""), str(source_call.get("recording") or "")}:
            call = source_call
        elif call_id and not call:
            results.append({"type": action_type, "executed": False, "error": "previous call not found"})
            continue
        elif not call and source_call:
            call = source_call
        ok, error, decision, action_call = _validate_create_follow_up_action_payload(payload, call)
        if not ok:
            results.append({"type": action_type, "executed": False, "error": error})
            continue

        try:
            from followups import create_or_update_from_analysis
            email_status = _email_delivery_status_for(decision.get("email") or "")
            result = create_or_update_from_analysis(action_call, decision, email_delivery_status=email_status)
            follow_up = result.get("follow_up")
            if decision.get("details") and action_call.get("call_sid"):
                try:
                    from call_history import update_call_details
                    update_call_details(action_call.get("call_sid"), decision.get("details"))
                except Exception as e:
                    print(f"[HISTORY] analyst details patch failed: {e}")
            if follow_up:
                await _followup_broadcast({
                    "type": "follow_up_created" if result.get("created") else "follow_up_updated",
                    "follow_up": follow_up,
                    "message": "Analyst action created a follow-up" if result.get("created") else "Analyst action updated a follow-up",
                })
            results.append({
                "type": action_type,
                "executed": bool(follow_up),
                "created": bool(result.get("created")),
                "updated": bool(result.get("updated")),
                "follow_up_id": follow_up.get("id") if follow_up else None,
            })
        except Exception as e:
            results.append({"type": action_type, "executed": False, "error": str(e)})
    return results


async def _ask_followup_analyst_json(messages: list[dict], max_tokens: int = 900) -> dict:
    if not OPENAI_API_KEY:
        return {}
    async with httpx.AsyncClient(timeout=90) as client:
        resp = await client.post(
            "https://api.openai.com/v1/chat/completions",
            headers={"Authorization": f"Bearer {OPENAI_API_KEY}", "Content-Type": "application/json"},
            json={
                "model": "gpt-4.1",
                "temperature": 0.1,
                "max_tokens": max_tokens,
                "response_format": {"type": "json_object"},
                "messages": messages,
            },
        )
    if resp.status_code != 200:
        print(f"[FOLLOWUP ANALYST] OpenAI failed: {resp.status_code} {resp.text[:300]}")
        return {}
    return _extract_json_object(resp.json()["choices"][0]["message"].get("content", "{}"))


def _compact_call_for_analysis(call: dict, transcript: dict) -> dict:
    return {
        "call_sid": call.get("call_sid"),
        "business": call.get("business"),
        "phone": call.get("phone"),
        "city": call.get("city"),
        "category": call.get("category"),
        "outcome": call.get("outcome"),
        "lead_status": call.get("lead_status"),
        "outcome_reason": call.get("outcome_reason"),
        "outcome_evidence": call.get("outcome_evidence"),
        "date": call.get("date"),
        "lead_timezone": call.get("lead_timezone"),
        "lead_timezone_iana": call.get("lead_timezone_iana"),
        "lead_local_date": call.get("lead_local_date"),
        "lead_local_display": call.get("lead_local_display"),
        "duration_s": call.get("duration_s"),
        "turns": call.get("turns"),
        "recording": call.get("recording"),
        "call_mode": call.get("call_mode"),
        "follow_up_id": call.get("follow_up_id"),
        "followup_note": call.get("followup_note"),
        "details": call.get("details"),
        "contacts": transcript.get("contacts", {}),
        "transcript": transcript.get("transcript", ""),
    }


async def _initial_followup_decision(call: dict, transcript: dict) -> dict:
    system = """
You are Lynkflow's internal call analyst. Decide whether a completed outbound call deserves a commercial follow-up task.

Use only the provided call metadata and transcript. Do not infer facts that are not in the conversation. Unknown fields must be null.

Use call metadata lead_local_display/lead_local_date and lead_timezone as the authoritative local time for the lead. Include the local previous call time in details when available.

Lynkflow is an AI voice receptionist / AI call handling system. Do not describe it as a human-staffed receptionist service.

Always classify the call outcome for analytics. Prefer the most specific supported outcome over generic conversation. Use interested for clear buying interest or a positive decision-maker signal. Use callback when they requested a later call. Use skeptical when they engaged but raised doubts or objections without declining. Use not_interested when they clearly declined. Use voicemail, gatekeeper, wrong_number, no_answer, ivr, busy, failed, do_not_call, unknown, or conversation when those are the best evidence-based labels.

Create a follow-up only when there is meaningful commercial evidence, such as a decision maker showing interest, asking questions, asking for info/demo/callback, providing an email for information, agreeing to continue later, or showing interest without completing the next step.

Do not create a follow-up merely because someone answered, a receptionist answered, the call connected, voicemail/IVR occurred, the prospect said no, the number was wrong, they already have a solution and showed no interest, or there is no clear commercial signal.

Return strict JSON only. The actions array must always include one UPDATE_CALL_OUTCOMES action for this call. Include CREATE_FOLLOW_UP only when a follow-up should be created.

Shape when a follow-up should be created:
{
  "assistant_text": "brief evidence summary",
  "actions": [
    {
      "marker": "[ACTION:UPDATE_CALL_OUTCOMES]",
      "payload": {
        "updates": [
          {
            "call_sid": "call_sid from the provided call metadata",
            "outcome": "interested|skeptical|callback|not_interested|voicemail|gatekeeper|wrong_number|no_answer|ivr|conversation|busy|failed|do_not_call|unknown",
            "reason": "short reason for this analytics label",
            "evidence": "exact transcript or metadata evidence for the label",
            "confidence": 0.0
          }
        ]
      }
    },
    {
      "marker": "[ACTION:CREATE_FOLLOW_UP]",
      "payload": {
        "previous_call_id": "call_sid from the provided call metadata",
        "priority": "hot|warm|low",
        "reason": "short evidence-based reason",
        "business": "business or null",
        "phone": "phone or null",
        "contact_role": "role or null",
        "contact_name": "name or null",
        "previous_contact_role": "role from the original lead conversation or null",
        "previous_contact_name": "name from the original lead conversation, or null if name not provided",
        "lead_timezone": "lead timezone from call metadata or null",
        "lead_timezone_iana": "IANA timezone from call metadata or null",
        "previous_call_local_date": "lead-local ISO date/time from call metadata or null",
        "previous_call_local_display": "lead-local display date/time from call metadata or null",
        "email": "email or null",
        "pain_point": "pain discussed or null",
        "current_solution": "current handling or null",
        "interest_signal": "exact commercial signal or null",
        "previous_action": "what happened or was requested, not invented",
        "email_delivery_status": "unknown",
        "email_received": false,
        "prospect_reported_not_received": false,
        "email_status_details": "concise verified email state or null",
        "next_action": "appropriate next step based on verified information or null",
        "follow_up_goal": "specific next call objective",
        "agent_summary": "2-4 concise sentences for the follow-up caller: who they spoke with, what was discussed, pain, interest, previous action/email status, and the goal. Do not include the full transcript.",
        "context_summary": "concise factual prior-call context",
        "details": "clear factual analyst details: who was spoken to, whether name was provided, what was discussed, info provided, and any test/persona/identity confusion flags. Do not invent.",
        "scheduled_for": null
      }
    }
  ]
}

If no follow-up is warranted, return only the UPDATE_CALL_OUTCOMES action:
{"assistant_text":"short evidence-based reason","actions":[{"marker":"[ACTION:UPDATE_CALL_OUTCOMES]","payload":{"updates":[{"call_sid":"call_sid from the provided call metadata","outcome":"one allowed outcome","reason":"short reason","evidence":"exact transcript or metadata evidence","confidence":0.0}]}}]}
""".strip()
    payload = _compact_call_for_analysis(call, transcript)
    result = await _ask_followup_analyst_json([
        {"role": "system", "content": system},
        {"role": "user", "content": json.dumps(payload, ensure_ascii=False)},
    ], max_tokens=1300)
    return result if isinstance(result, dict) else {"assistant_text": "Analyst returned no object.", "actions": []}


async def _followup_call_result_decision(call: dict, transcript: dict, follow_up: dict) -> dict:
    system = """
You are Lynkflow's internal follow-up call analyst. Evaluate a completed follow-up call and update the existing follow-up task.

Use only the stored follow-up context, call metadata, and transcript. Do not invent names, promises, emails, demo requests, or callback times.

Keep previous-contact identity and current-caller identity separate. If the current caller says "This is Pam", record current_contact_name as Pam, but do not overwrite previous_contact_name unless the transcript explicitly confirms Pam was the original previous contact.

Lynkflow is an AI voice receptionist / AI call handling system. Do not describe it as a human-staffed receptionist service.

Use the latest_call lead_local_display/lead_local_date when describing when the call happened. If available, include that local time in details.

Email state rules:
- email_delivery_status=email_sent only means the system verified sending succeeded.
- Never change email_delivery_status from email_sent to email_failed because the prospect says they did not receive it.
- Use email_failed only for verified system sending failure evidence.
- Set email_received=true only if the prospect explicitly confirms receiving the email.
- Set prospect_reported_not_received=true when the prospect explicitly says they did not receive it.
- Preserve historical verified email facts in email_status_details/details; do not overwrite sent with received/not received.
- If prospect did not receive it, next_action should ask them to check spam/junk and, if still missing, ask for a preferred alternate email. Do not automatically resend unless explicitly authorized.

Choose status:
- completed: the follow-up reached a clear resolution, next step was handled, or the prospect clearly declined.
- pending: no answer, voicemail, IVR, busy/no clear contact, or another attempt is still commercially reasonable. The backend will display this as attempted when at least one follow-up call has already been placed.
- scheduled: the prospect requested a specific future callback.
- failed: the call failed, wrong number, or no further useful path exists.
- cancelled: they asked not to be contacted again.

If the current caller says this is the wrong number, return status="failed" and result="wrong_number". If they gave a current name, put it in current_contact_name only.

Return strict JSON only with this shape:
{
  "follow_up_result": {
    "status": "completed|pending|scheduled|failed|cancelled",
    "result": "short factual result",
    "reason": "why this status was chosen",
    "scheduled_for": "callback time or null",
    "contact_role": "role or null",
    "contact_name": "name or null",
    "current_contact_role": "role stated in this follow-up call or null",
    "current_contact_name": "name stated in this follow-up call or null. Never use this as previous_contact_name.",
    "email": "email or null",
    "email_delivery_status": "email_sent|email_failed|email_ready_for_review|email_generated|unknown|null",
    "email_received": true|false|null,
    "prospect_reported_not_received": true|false|null,
    "email_status_details": "latest verified email state without contradicting history or null",
    "next_action": "appropriate next step based on latest verified email state or null",
    "pain_point": "updated pain or null",
    "current_solution": "updated current solution or null",
    "interest_signal": "updated interest signal or null",
    "previous_action": "latest action or null",
    "follow_up_goal": "next goal if still pending/scheduled, else null",
    "agent_summary": "updated concise summary for any later follow-up caller, no full transcript",
    "context_summary": "updated concise context",
    "details": "append-only factual detail from this call including lead-local call time, who answered, whether they are the original contact, new info, wrong number/test/persona flags, and identity confusion. Do not erase prior history."
  }
}
""".strip()
    payload = {
        "existing_follow_up": follow_up,
        "latest_call": _compact_call_for_analysis(call, transcript),
    }
    result = await _ask_followup_analyst_json([
        {"role": "system", "content": system},
        {"role": "user", "content": json.dumps(payload, ensure_ascii=False)},
    ])
    decision = result.get("follow_up_result") if isinstance(result, dict) else None
    return decision if isinstance(decision, dict) else {"status": "completed", "result": call.get("outcome") or "completed", "reason": "Analyst returned no follow_up_result object."}


async def _analyze_call_for_follow_up(call: dict):
    recording = call.get("recording") or ""
    if not recording:
        return
    try:
        transcript = await _transcribe_recording(recording)
    except Exception as e:
        print(f"[FOLLOWUP ANALYST] transcript unavailable for {recording}: {e}")
        return

    if not (transcript.get("transcript") or "").strip():
        return

    try:
        from followups import complete_call_attempt, get_follow_up
        if call.get("call_mode") == "follow_up" and call.get("follow_up_id"):
            follow_up = get_follow_up(call.get("follow_up_id"))
            if not follow_up:
                return
            decision = await _followup_call_result_decision(call, transcript, follow_up)
            if decision.get("details") and call.get("call_sid"):
                try:
                    from call_history import update_call_details
                    update_call_details(call.get("call_sid"), decision.get("details"))
                except Exception as e:
                    print(f"[HISTORY] follow-up details patch failed: {e}")
            updated = complete_call_attempt(follow_up["id"], call, decision)
            if updated:
                await _followup_broadcast({"type": "follow_up_updated", "follow_up": updated, "message": "Follow-up call analyzed"})
            return

        analysis = await _initial_followup_decision(call, transcript)
        action_results = await _execute_analyst_actions(_extract_structured_analyst_actions(analysis), source_call=call)
        if not any(r.get("executed") for r in action_results):
            print(f"[FOLLOWUP ANALYST] no follow-up action for {call.get('call_sid')}: {analysis.get('assistant_text', '')}")
    except Exception as e:
        print(f"[FOLLOWUP ANALYST] failed for {call.get('call_sid')}: {e}")


def _find_call(call_id: str) -> dict | None:
    if not call_id:
        return None
    from call_history import load_calls
    for call in sorted(load_calls(3650), key=lambda c: c.get("ts", 0), reverse=True):
        if call.get("call_sid") == call_id or call.get("recording") == call_id:
            return call
    return None


def _normalise_call_outcome(value: str) -> str:
    raw = re.sub(r"\s+", " ", str(value or "").strip().lower())
    if raw in _CALL_OUTCOME_ALIASES:
        return _CALL_OUTCOME_ALIASES[raw]
    cleaned = re.sub(r"[^a-z0-9]+", "_", raw).strip("_")
    return _CALL_OUTCOME_ALIASES.get(cleaned, cleaned)


def _call_update_items(payload: dict) -> list[dict]:
    updates = payload.get("updates") or payload.get("items") or payload.get("calls")
    if isinstance(updates, list):
        return [x for x in updates if isinstance(x, dict)]
    return [payload]


def _apply_call_outcome_update(item: dict, updated_by: str = "operator", require_evidence: bool = False) -> dict:
    call_id = str(item.get("call_sid") or item.get("call_id") or item.get("previous_call_id") or item.get("recording") or "").strip()
    if not call_id:
        return {"executed": False, "error": "call_sid or recording is required"}

    call = _find_call(call_id)
    if not call:
        return {"executed": False, "call_id": call_id, "error": "call not found"}

    outcome = _normalise_call_outcome(item.get("outcome") or item.get("lead_status") or item.get("status") or "")
    if outcome not in _ALLOWED_CALL_OUTCOMES:
        return {"executed": False, "call_id": call_id, "business": call.get("business"), "error": f"unsupported outcome: {outcome or 'missing'}"}

    reason = re.sub(r"\s+", " ", str(item.get("reason") or item.get("outcome_reason") or "")).strip()[:800]
    evidence = re.sub(r"\s+", " ", str(item.get("evidence") or item.get("outcome_evidence") or "")).strip()[:1200]
    if require_evidence and (not reason or not evidence):
        return {"executed": False, "call_id": call_id, "business": call.get("business"), "error": "reason and evidence are required for Analyst outcome updates"}

    confidence = item.get("confidence") or item.get("outcome_confidence")
    try:
        confidence = float(confidence) if confidence not in (None, "") else None
        if confidence is not None:
            confidence = max(0.0, min(1.0, confidence))
    except Exception:
        confidence = None

    fields = {
        "outcome": outcome,
        "lead_status": outcome,
        "outcome_reason": reason or "Manual analytics update",
        "outcome_evidence": evidence,
        "outcome_updated_at": datetime.now().isoformat(timespec="seconds"),
        "outcome_updated_by": updated_by,
    }
    if confidence is not None:
        fields["outcome_confidence"] = confidence

    try:
        from call_history import update_call_fields
        ok = update_call_fields(call.get("call_sid"), fields)
    except Exception as e:
        return {"executed": False, "call_id": call_id, "business": call.get("business"), "error": str(e)}

    return {
        "executed": bool(ok),
        "call_sid": call.get("call_sid"),
        "recording": call.get("recording"),
        "business": call.get("business"),
        "previous_outcome": call.get("outcome"),
        "outcome": outcome,
        "reason": fields["outcome_reason"],
    }


def _apply_call_outcome_updates(payload: dict, updated_by: str = "operator", require_evidence: bool = False) -> dict:
    items = _call_update_items(payload)
    if not items:
        return {"success": False, "updated": 0, "failed": 0, "results": [], "error": "no updates supplied"}
    results = [_apply_call_outcome_update(item, updated_by=updated_by, require_evidence=require_evidence) for item in items[:200]]
    updated = sum(1 for r in results if r.get("executed"))
    failed = len(results) - updated
    return {"success": failed == 0, "updated": updated, "failed": failed, "results": results}


def _is_call_outcome_update_request(question: str) -> bool:
    q = str(question or "").lower()
    action_terms = ("update", "change", "apply", "set", "mark", "move", "persist")
    target_terms = ("outcome", "status", "lead", "business", "call", "interested", "skeptical", "callback", "voicemail", "not interested")
    return any(term in q for term in action_terms) and any(term in q for term in target_terms)


def _guard_outcome_update_actions(actions: list[dict], question: str) -> list[dict]:
    if _is_call_outcome_update_request(question):
        return actions
    guarded = []
    for action in actions or []:
        if action.get("type") == _ANALYST_ACTION_UPDATE_CALL_OUTCOMES:
            blocked = {**action, "valid": False, "error": "operator did not explicitly request outcome updates"}
            guarded.append(blocked)
        else:
            guarded.append(action)
    return guarded


def _runtime_agent_config_for_mode(cfg: AgentConfig, call_mode: str = "outbound") -> AgentConfig:
    cfg.voice_engine = "gpt_live"
    cfg.followup_voice_engine = "gpt_live"
    if call_mode != "follow_up":
        cfg.voice_engine = "gpt_live"
        return cfg
    runtime = cfg.model_copy(deep=True)
    overrides = {
        "first_message": "followup_first_message",
        "system_prompt": "followup_system_prompt",
        "end_call_phrases": "followup_end_call_phrases",
        "model": "followup_model",
        "temperature": "followup_temperature",
        "max_tokens": "followup_max_tokens",
        "live_model": "followup_live_model",
        "live_voice": "followup_live_voice",
        "tone": "followup_tone",
        "endpointing_ms": "followup_endpointing_ms",
        "utterance_end_ms": "followup_utterance_end_ms",
        "silence_timeout_s": "followup_silence_timeout_s",
        "max_duration_s": "followup_max_duration_s",
        "allow_interruption": "followup_allow_interruption",
        "voicemail_enabled": "followup_voicemail_enabled",
        "voicemail_message": "followup_voicemail_message",
        "callback_number": "followup_callback_number",
    }
    for target, source in overrides.items():
        value = getattr(cfg, source, None)
        if value is not None:
            setattr(runtime, target, value)
    runtime.voice_engine = "gpt_live"
    return runtime


def _runtime_agent_config(call_mode: str = "outbound") -> AgentConfig:
    return _runtime_agent_config_for_mode(load_agent_config(), call_mode)


def _compact_lead_context(lead: dict) -> dict:
    allowed = (
        "Name", "Phone", "City", "State", "Category", "Timezone", "Status",
        "Notes", "Address", "Website", "Email", "Owner", "Manager",
        "Business", "Company", "Source", "Last Call", "Last Outcome",
    )
    context = {k: str((lead or {}).get(k) or "").strip() for k in allowed if str((lead or {}).get(k) or "").strip()}
    notes = context.get("Notes")
    if notes and len(notes) > 600:
        context["Notes"] = notes[:600] + "..."
    return context


async def _start_agent_call(phone: str, lead: dict, base_url: str, call_mode: str = "outbound", follow_up_id: str = "") -> str:
    phone = _format_us_phone(phone)
    base_url = (base_url or "").rstrip("/")

    if not phone:
        raise HTTPException(400, "phone required")
    if not base_url:
        raise HTTPException(400, "base_url required (your ngrok / production URL)")
    if not all([TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN, TWILIO_CALLER_ID]):
        raise HTTPException(500, "Twilio credentials not set in .env")

    cfg = load_agent_config()
    if cfg.base_url != base_url:
        cfg.base_url = base_url
        _save_agent_config(cfg)

    lead_phone = urllib.parse.quote(phone)
    lead_name  = urllib.parse.quote(lead.get("Name", ""))
    lead_city  = urllib.parse.quote(lead.get("City", ""))
    lead_cat   = urllib.parse.quote(lead.get("Category", ""))
    lead_tz    = urllib.parse.quote(_lead_timezone(lead))
    lead_mode  = urllib.parse.quote(call_mode or "outbound")
    lead_fu    = urllib.parse.quote(follow_up_id or "")
    lead_ctx   = urllib.parse.quote(json.dumps(_compact_lead_context(lead), ensure_ascii=False)[:1400])
    twiml_url  = (
        f"{base_url}/api/agent/twiml"
        f"?phone={lead_phone}&name={lead_name}&city={lead_city}&category={lead_cat}"
        f"&timezone={lead_tz}&call_mode={lead_mode}&follow_up_id={lead_fu}&lead_context={lead_ctx}"
    )

    async with httpx.AsyncClient(timeout=15) as client:
        r = await client.post(
            f"https://api.twilio.com/2010-04-01/Accounts/{TWILIO_ACCOUNT_SID}/Calls.json",
            auth=(TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN),
            data={
                "To":   phone,
                "From": TWILIO_CALLER_ID,
                "Url":  twiml_url,
                "StatusCallback": f"{base_url}/api/agent/call-status",
                "StatusCallbackMethod": "POST",
                "StatusCallbackEvent": ["initiated", "ringing", "answered", "completed"],
                "Record": "true",
                "RecordingChannels": "dual",
                "RecordingStatusCallback": f"{base_url}/api/agent/recording-status",
                "RecordingStatusCallbackMethod": "POST",
                "RecordingStatusCallbackEvent": ["completed"],
                "Timeout": "30",
            },
        )

    if r.status_code not in (200, 201):
        raise HTTPException(502, f"Twilio error: {r.text}")

    call_sid = r.json()["sid"]
    _agent_queues[call_sid] = asyncio.Queue()
    _agent_events[call_sid] = []
    _call_leads[call_sid] = {**lead, "Phone": phone, "call_mode": call_mode or "outbound", "follow_up_id": follow_up_id or ""}
    return call_sid


@app.post("/api/update-lead")
async def update_lead(request: Request):
    """
    Updates ONLY the Status, Notes, and Call Date columns for a lead.
    Preserves all other columns (Phone, Name, Address, etc.) — fixes the
    data-deletion bug caused by the old n8n full-row-replace approach.

    Priority order:
      1. GOOGLE_APPS_SCRIPT_URL  — Apps Script web app (recommended, no extra creds)
      2. n8n webhook fallback    — sends all fields so n8n can do a proper update

    To use Apps Script: paste and deploy the script below as a web app
    (Execute as: Me, Access: Anyone) then set GOOGLE_APPS_SCRIPT_URL in .env.

    --- Apps Script (Tools → Script editor in your sheet) ---
    function doPost(e) {
      var d = JSON.parse(e.postData.contents);
      var sh = SpreadsheetApp.openById('YOUR_SHEET_ID').getActiveSheet();
      var vals = sh.getDataRange().getValues();
      var hdrs = vals[0].map(function(h){ return String(h).trim().toLowerCase(); });
      var phoneCol   = hdrs.indexOf('phone');
      var nameCol    = hdrs.indexOf('name');
      var statusCol  = hdrs.indexOf('status');
      var notesCol   = hdrs.indexOf('notes');
      var dateCol    = hdrs.indexOf('call date');
      if (dateCol < 0) dateCol = hdrs.indexOf('date');
      var clean = function(p){ return String(p||'').replace(/\\D/g,''); };
      var tp = clean(d.phone);
      for (var i = 1; i < vals.length; i++) {
        var rp = clean(vals[i][phoneCol] || '');
        var rn = String(vals[i][nameCol] || '').trim().toLowerCase();
        if ((tp && (rp === tp || tp.endsWith(rp))) || (d.name && rn === d.name.trim().toLowerCase())) {
          var row = i + 1;
          if (statusCol >= 0) sh.getRange(row, statusCol+1).setValue(d.status);
          if (notesCol  >= 0 && d.notes) sh.getRange(row, notesCol+1).setValue(d.notes);
          if (dateCol   >= 0) sh.getRange(row, dateCol+1).setValue(new Date().toLocaleString());
          return ContentService.createTextOutput(JSON.stringify({ok:true})).setMimeType(ContentService.MimeType.JSON);
        }
      }
      return ContentService.createTextOutput(JSON.stringify({ok:false,error:'row not found'})).setMimeType(ContentService.MimeType.JSON);
    }
    ---------------------------------------------------------
    """
    data   = await request.json()
    name   = data.get("name", "")
    phone  = data.get("phone", "")
    status = data.get("status", "")
    notes  = data.get("notes", "")
    lead   = data.get("lead", {})   # full lead object from frontend

    return JSONResponse(await _update_lead_record(name, phone, status, notes, lead))


@app.get("/api/twilio/token")
async def get_twilio_token():
    if not all([TWILIO_ACCOUNT_SID, TWILIO_API_KEY_SID, TWILIO_API_KEY_SECRET, TWILIO_TWIML_APP_SID]):
        raise HTTPException(status_code=500, detail="Twilio credentials not set in .env")
    token = AccessToken(TWILIO_ACCOUNT_SID, TWILIO_API_KEY_SID, TWILIO_API_KEY_SECRET, identity="odelyn")
    grant = VoiceGrant(outgoing_application_sid=TWILIO_TWIML_APP_SID, incoming_allow=False)
    token.add_grant(grant)
    return JSONResponse({"token": token.to_jwt()})


@app.post("/api/twilio/voice")
async def twiml_voice(request: Request):
    from fastapi.responses import Response
    form = await request.form()
    to = form.get("To", "")
    response = VoiceResponse()
    dial = Dial(caller_id=TWILIO_CALLER_ID)
    dial.number(to)
    response.append(dial)
    return Response(content=str(response), media_type="application/xml")


@app.get("/api/leads")
async def get_leads():
    return JSONResponse(await _fetch_leads_from_sheet())


@app.get("/api/modules")
async def get_modules():
    modules = [
        {
            "id": "greeting",
            "title": "Opening & Greeting",
            "icon": "👋",
            "explanation": "The first 5 seconds decide everything. Most plumbers hang up the moment they sense a sales call. The transparent opener flips that -- you admit it is a cold call upfront, which disarms them and earns you 30 seconds. The goal of this call is NOT to close. It is to book a 10-minute Zoom or callback. That is a much easier yes.",
            "tips": [
                "Smile before you dial -- they can genuinely hear it in your voice",
                "Say their business name as a question first -- confirm you have the right person",
                "Admit it is a cold call immediately -- it disarms them and builds trust",
                "Give them the choice to hang up -- most will not take it",
                "Your goal is a Zoom or callback, not a close. Keep that in mind the whole call",
                "Tone matters more than words -- calm, confident, not robotic",
            ],
            "script": "Hi, am I reaching the owner of [Business Name]? Yeah, so I am going to be honest with you -- this is a cold call, so I do have something to pitch to your business. Would you like to hang up now or give me 30 seconds and then you can decide?",
            "sample_label": "Hear the opening",
        },
        {
            "id": "gatekeeper",
            "title": "Getting Past the Gatekeeper",
            "icon": "🚪",
            "explanation": "A gatekeeper is anyone who picks up the phone that isn't the owner — a receptionist, an office assistant, a spouse, an employee. They are not the decision maker. Do not pitch them. Your only job here is to get to the owner politely and confidently. Never lie, never be aggressive, never over-explain.",
            "tips": [
                "Sound like you belong — confident, not uncertain",
                "Don't over-explain what you're calling about to the gatekeeper",
                "Use the owner's first name if you have it — it signals familiarity",
                "If they ask what it's about, keep it vague but professional",
                "If the owner is truly unavailable, get a specific callback time — not 'try again later'",
            ],
            "script": "Hi there! Is the owner available? ... [If asked what it's about]: I'm calling about their phone system — it'll just take a minute. ... [If owner is unavailable]: No worries at all. What's the best time to reach them today — is morning or afternoon better? ... [When you reach the owner]: Hi! My name is Odelyn from Lynkflow — your team said you'd be the right person to speak with. I'll be quick, I promise.",
            "sample_label": "Hear gatekeeper navigation",
        },
        {
            "id": "dm_check",
            "title": "Decision Maker Check",
            "icon": "🎯",
            "explanation": "Even if someone picks up and seems engaged, always confirm they're the actual decision maker before launching into your pitch. Small plumbing shops often have employees or family members answering. Pitching the wrong person means your time is wasted and the message will almost never get passed on correctly.",
            "tips": [
                "Ask directly but warmly — it's not rude, it's professional",
                "If they say 'I can pass on the message' — that's a no. Get to the owner",
                "If they say 'I handle that too' — ask 'Are you the owner or manager?' to confirm",
                "Don't pitch until you've confirmed",
            ],
            "script": "Before I go further — are you the owner of the business, or the person who makes decisions on tools and services? ... [If yes]: Perfect, you're exactly who I need. ... [If no]: Got it — no problem at all. Is the owner available right now, or what's the best time I can reach them directly?",
            "sample_label": "Hear the DM check",
        },
        {
            "id": "pitch",
            "title": "The Core Pitch",
            "icon": "⚡",
            "explanation": "This is your 45-second window. Lead with the pain — missed calls cost real money. Then drop the solution. Do not over-explain the technology. Plumbers do not care how it's built. They care that they stop losing jobs to missed calls. Be punchy. Pause after your hook. Let it land.",
            "tips": [
                "Lead with the problem, not the product — make them feel the pain first",
                "Use specific scenarios — '2am', 'three customers at once', 'elbow-deep in a drain'",
                "Don't say 'AI' too early — say 'smart answering system' first if they seem traditional",
                "Pause after 'walking straight to your competitor' — let that sit for a second",
                "Keep it under 50 seconds. If you're going longer, you're over-explaining",
            ],
            "script": "I checked out your listing on Google and saw you are a plumbing business. We help plumbers make sure they never miss a customer call -- we build a 24/7 AI system that answers every call automatically, even when you are out on a job, and sends you a full summary by text right away. No contracts, month to month, cancel anytime. Does that sound like something worth a quick 10-minute Zoom so I can show you exactly how it works?",
            "sample_label": "Hear the full pitch",
        },
        {
            "id": "objections",
            "title": "Handling Objections",
            "icon": "🛡️",
            "explanation": "Objections are not rejections — they are questions in disguise. When someone says 'I'm not interested,' they usually mean 'I don't see the value yet.' Your job is to stay calm, acknowledge what they said, and redirect. Never argue. Never panic. Never raise your voice. Three solid objections in a row with no opening = disposition NI and move on.",
            "tips": [
                "Let them finish completely before you respond — interrupting kills the call",
                "Acknowledge first ('That makes sense', 'I completely understand') before redirecting",
                "Match their energy — calm caller, be calm. Direct caller, be direct",
                "Never read your rebuttal robotically — make it sound like your own words",
                "After three objections with no opening, let go gracefully. Don't push",
            ],
            "subsections": [
                {
                    "id": "obj_not_interested",
                    "label": "Not Interested",
                    "trigger": "I'm not interested / We're good / No thanks",
                    "script": "That's completely fair — I hear that a lot, honestly. Most owners I speak with said the same thing until I asked them one question: when you're out on a job and your phone rings, what actually happens to that call right now? ... [Let them answer] ... That's exactly the gap we close. It's not about replacing anything you have — it's about making sure zero calls slip through when you're unavailable. Would it be worth 30 more seconds to hear how it works?",
                    "second_objection": "I totally respect that. Last thing I'll say — on average, a missed plumbing call is worth anywhere from $200 to $2,000 depending on the job. If this saves you even one call a month you would've lost, it pays for itself immediately. That's all I wanted to put in front of you.",
                    "third_objection": "Understood — I won't take any more of your time. I appreciate you listening. Have a great rest of your day.",
                },
                {
                    "id": "obj_busy",
                    "label": "I'm Busy / Bad Time",
                    "trigger": "I'm busy right now / Not a good time / Call me back",
                    "script": "I completely understand — I'll be really quick. 30 seconds, and if it's not relevant I'll let you go immediately. The reason I'm calling is actually about what happens to your calls when you're this busy — which sounds like it's pretty often. Can I have just 30 seconds?",
                    "second_objection": "Of course — I don't want to hold you up. What's the best time to reach you today? Morning or afternoon?",
                    "third_objection": "No problem at all. I'll try you another time. Thanks for picking up.",
                },
                {
                    "id": "obj_already_have",
                    "label": "Already Have Something",
                    "trigger": "We have voicemail / We have a receptionist / We already use an answering service",
                    "script": "That's great — glad you have something in place. Quick question though — does it answer at 2am? And if two customers call at the exact same time, does it handle both of them simultaneously? Most services miss at least one of those. Lynkflow doesn't. And after every single call, you get a full summary of what was said sent straight to your phone. No call slips through, ever.",
                    "second_objection": "Makes sense. What if I told you most of our clients kept what they had and just added Lynkflow as the overflow — so nothing falls through the cracks after hours or during busy periods? It's not an either-or.",
                    "third_objection": "Understood, I appreciate your time. If that ever changes, we're here.",
                },
                {
                    "id": "obj_cost",
                    "label": "How Much Does It Cost",
                    "trigger": "What's the price? / How much is it? / That sounds expensive",
                    "script": "It's a one-time setup fee between $300 and $500 depending on how we customize it for your business, and then $100 to $150 a month after that. To put that in perspective — one missed call from a decent job is usually $500 to $2,000. If Lynkflow saves you even two calls a month you would've missed, it's paid for itself several times over. Most of our clients see that within the first week.",
                    "second_objection": "I hear you on the cost — and I want to be straight with you, it's not the cheapest option out there. But it's also not a voicemail box. It's a live system that talks to your customers, qualifies them, and books them in. The value is in never losing a lead again.",
                    "third_objection": "Fair enough — I appreciate the honest conversation. If budget opens up down the line, we'd love to work with you.",
                },
                {
                    "id": "obj_think_about",
                    "label": "Need to Think About It",
                    "trigger": "Let me think about it / I'll get back to you / Send me an email",
                    "script": "Of course — I respect that. Can I ask what part you want to think through? Sometimes I can answer it right now and save you the time. Is it the price, how it actually works, or something else? ... [After they answer]: That makes sense. How about this — I'll follow up with you on [specific day]. Does [morning or afternoon] work better for a quick 5-minute call?",
                    "second_objection": "Totally fine. I'll send you a quick summary so you have something to reference. What's the best email to send it to?",
                    "third_objection": "No problem at all. I'll leave it with you — and if you have any questions, just reach out. Have a good one.",
                },
                {
                    "id": "obj_scam",
                    "label": "Is This a Scam / Legit?",
                    "trigger": "How do I know this is real? / Sounds like a scam / I don't know you",
                    "script": "That's a completely valid concern — you should be skeptical of random calls, honestly. My name is Odelyn, I'm with Lynkflow. We're a small agency that builds AI phone systems specifically for trade businesses like plumbers. I'm not asking for any payment or information right now — I can send more information by email from lynkflowagent@gmail.com so you can review it before deciding anything.",
                    "second_objection": "Completely fair. No pressure at all — if you want, I can send a short email with the details so you can review it and connect later with no commitment.",
                    "third_objection": "Understood. Thanks for being upfront. Have a good day.",
                },
                {
                    "id": "obj_no_need",
                    "label": "We Don't Miss Calls",
                    "trigger": "We always answer / I never miss calls / We're on top of it",
                    "script": "That's awesome — and I believe you during business hours. But what about 9pm on a Saturday when someone's pipe bursts? Or when you're on a job and two people call at the exact same time? Those are the moments we cover. Even the most on-top-of-it businesses have gaps after hours. Lynkflow fills those gaps without you having to do anything differently.",
                    "second_objection": "Fair enough — if you're genuinely covered around the clock with zero gaps, then honestly, you might not need us. Not every business does. I just wanted to make sure you'd heard about it.",
                    "third_objection": "I appreciate that. Good luck with everything — sounds like you're running a tight operation.",
                },
            ],
            "sample_label": "Hear objection handling",
        },
        {
            "id": "faqs",
            "title": "Plumber FAQs",
            "icon": "❓",
            "explanation": "These are real questions plumbers ask during calls. Know every answer cold. You should be able to answer any of these without hesitation — hesitation kills trust. Study these until they feel like your own words, not a script.",
            "faqs": [
                {
                    "id": "faq_real_person",
                    "question": "Are you a real person or is this a robot?",
                    "answer": "I'm a real person — my name is Odelyn. I work for Lynkflow and I'm calling because we help plumbing businesses like yours handle their calls more efficiently. The system I'm offering is automated, but I'm very much a real person talking to you right now.",
                },
                {
                    "id": "faq_location",
                    "question": "Where are you located? / Where are you calling from?",
                    "answer": "I'm based in the Philippines — we're a remote team that works with businesses across the US. A lot of companies work with offshore teams these days because it keeps costs down without sacrificing quality. The system itself is built and managed to US standards, and everything runs in the US for your customers.",
                },
                {
                    "id": "faq_accent",
                    "question": "I can't understand you / You have an accent",
                    "answer": "I completely understand — I appreciate you letting me know. I'll slow down a bit. [Slow your speech, speak clearly.] Is that better? I want to make sure you get the full picture before you decide anything.",
                },
                {
                    "id": "faq_script",
                    "question": "Are you reading from a script?",
                    "answer": "Ha — I have notes in front of me, sure, but I'm not reading word for word. I just want to make sure I give you accurate information about what we offer. Is there something specific you'd like to know that I can answer more directly?",
                },
                {
                    "id": "faq_how_it_works",
                    "question": "How does it actually work?",
                    "answer": "When a customer calls your business number, instead of going to voicemail or ringing out, it connects to your Lynkflow agent. The agent greets them with your business name, asks what they need, collects their info, and if they want to book a job, it checks your calendar and locks in a time. After the call ends, you get a text or email with everything — who called, what they need, what was booked. You don't have to do anything differently.",
                },
                {
                    "id": "faq_voicemail",
                    "question": "We already have voicemail — why do we need this?",
                    "answer": "Voicemail is what happens after you miss a call. The problem is most callers — over 80% — hang up when they hit voicemail and immediately call the next plumber on Google. That job is gone before you even hear the message. Lynkflow means you never miss the call in the first place. Every caller gets answered live, their details are captured, and you get notified instantly. No phone tag, no lost jobs.",
                },
                {
                    "id": "faq_leave_message",
                    "question": "The staff says 'I can take a message' or 'What do you want me to tell him?' — what do I say?",
                    "answer": "Don't hand them a full pitch, it never gets relayed correctly. Give them something short they can actually repeat, then pivot straight to getting a callback time. Say: 'Sure — just let them know Odelyn from Lynkflow called about their business phone line, and that we help plumbers stop losing jobs to missed calls. It only takes about 10 minutes to show them. When's the best time I can catch them directly — morning or afternoon?' The message gives them enough to sound legitimate, and the question gets you a real time instead of a dead end. Always write down the day and time they give you and put it in your notes so you actually call back when you said you would.",
                },
                {
                    "id": "faq_staff_wont_give_time",
                    "question": "The staff won't give me a callback time and just says 'I'll pass it along'",
                    "answer": "That's a soft no. Push once, politely: 'I appreciate that — the only thing is I'd hate to keep calling and bothering you. Is there a time of day they're usually around? Even a rough window helps.' If they still won't give you anything, don't fight it. Say: 'No worries at all, I'll try back another time. Thanks for your help.' Then disposition it as Callback with a note saying gatekeeper wouldn't give a time, and try again in a couple days at a different hour — early morning before 8am or late afternoon after 4pm local time often gets you the owner directly.",
                },
                {
                    "id": "faq_ivr",
                    "question": "We already have a system — callers press 1 for service, press 2 for billing, etc.",
                    "answer": "That's an IVR menu — and they're useful for routing, but they don't actually help the customer. Most callers, especially in an emergency, just want to talk to someone fast. When they hit 'press 1, press 2,' a big chunk of them hang up before they even finish the menu — especially older customers or anyone frustrated. Lynkflow replaces that friction with a live conversation. The agent greets them, finds out what they need, and handles it — no menus, no waiting, no hang-ups. It's a better experience for your customer and more jobs captured for you.",
                },
                {
                    "id": "faq_staff_answers",
                    "question": "Someone on my staff answers — what do they tell me about the call?",
                    "answer": "That's actually one of the biggest gaps we fix. When a staff member takes a call, the quality of what gets relayed to you depends entirely on that person — and details get lost all the time. With Lynkflow, every single call gets logged automatically. After every call, you get a full summary: who called, what they needed, whether they booked, and their contact info — all in one place, the moment the call ends. No playing telephone with your team. If a staff member takes the call and an owner callback is needed, Lynkflow captures the name, number, and the best time to reach them so you call back knowing exactly what the situation is.",
                },
                {
                    "id": "faq_receptionist",
                    "question": "Will this replace my receptionist?",
                    "answer": "Not necessarily — think of it as backup that never sleeps. If you have a receptionist, Lynkflow handles overflow when they're busy, after-hours calls, and weekends so nothing ever slips through. If you don't have a receptionist, it fills that gap at a fraction of the cost — no hiring, no sick days, no training. Either way, zero calls get missed.",
                },
                {
                    "id": "faq_simultaneous",
                    "question": "What if multiple people call at the same time?",
                    "answer": "Lynkflow handles all of them simultaneously. If three customers call at 11am while you're on a job, all three get answered at the same time. No busy signals, no one waiting on hold. Each caller gets a full conversation, their details get captured, and you get three separate notifications. A human receptionist can only handle one call at a time — Lynkflow has no limit.",
                },
                {
                    "id": "faq_sound_fake",
                    "question": "Will it sound fake? Will my customers know it's not a real person?",
                    "answer": "Honestly, modern AI voice systems sound very natural — most callers don't realize they're not talking to a human unless they're told. And even if a customer figures it out, the experience is still smooth and professional. They still get answered, their info is captured, and their job gets booked. That's what matters to them at the end of the day.",
                },
                {
                    "id": "faq_existing_number",
                    "question": "Do I have to change my phone number?",
                    "answer": "No — you keep your existing number. We either forward calls to the Lynkflow system or set it up to activate after a certain number of rings. Your number stays the same, nothing changes for your existing customers.",
                },
                {
                    "id": "faq_calendar",
                    "question": "How does the calendar booking work?",
                    "answer": "We connect it to your Google Calendar. The agent checks your availability in real time and only offers times that you're free. When a customer picks a time, it gets added to your calendar automatically. You'll get a notification immediately.",
                },
                {
                    "id": "faq_cancel",
                    "question": "Can I cancel anytime?",
                    "answer": "Yes — there's no long-term contract. After the setup, it's month to month. If it's not working for you, you can cancel with notice and we'll remove everything cleanly.",
                },
                {
                    "id": "faq_setup_time",
                    "question": "How long does it take to set up?",
                    "answer": "Usually 24 to 48 hours from the time we get your details. We handle all the technical setup on our end — you just need to give us some information about your business and how you want calls handled. After that, it's live.",
                },
                {
                    "id": "faq_what_if_fails",
                    "question": "What if the system goes down or makes a mistake?",
                    "answer": "Like any technology, there's no such thing as 100% uptime — but the systems we use are enterprise-grade and highly reliable. If there's ever an issue, we handle it. And if the agent ever can't handle a call, it can be set to fall back to your voicemail or a direct line so nothing is ever completely dropped.",
                },
                {
                    "id": "faq_multiple_jobs",
                    "question": "What if I have multiple employees or trucks?",
                    "answer": "No problem — the system works for the whole business, not just one person. It books based on whatever availability you set, so whether you have one truck or five, it only schedules what you can actually handle.",
                },
                {
                    "id": "faq_trial",
                    "question": "Is there a free trial?",
                    "answer": "We don't offer a free trial as a standard — the setup takes real time and resources to build specifically for your business. What I can do is walk you through a live demo of exactly how it works before you commit to anything. That way you see it in action first.",
                },
                {
                    "id": "faq_competitors",
                    "question": "How is this different from [other service] / I've heard of similar things",
                    "answer": "There are other answering services out there — most of them give you a generic recorded message or a call center with people who don't know your business. What's different about Lynkflow is that it's built specifically for your plumbing business, uses your name, handles your scheduling, and sends you a real summary after every call. It's not a generic box — it's a custom system.",
                },
                {
                    "id": "faq_payment",
                    "question": "How do I pay?",
                    "answer": "We accept card payments online — it's straightforward and secure. Once you're ready to move forward, I'll send you a link and we can get the setup started right away.",
                },
            ],
            "sample_label": None,
        },
        {
            "id": "difficult_calls",
            "title": "Difficult Calls",
            "icon": "⚠️",
            "explanation": "Not every call is clean. Some plumbers are rude, aggressive, or deliberately difficult. This section gives you the exact words for every hard situation. The rules: never match aggression with aggression, never apologize for calling if you've been professional, and never let someone speak to you in a way that crosses into personal abuse. Stay calm. Stay professional. Exit when needed.",
            "tips": [
                "Take a breath before responding to aggression — your tone is everything",
                "Never argue, never raise your voice, never take it personally",
                "You are allowed to end a call if someone is abusive — do it calmly",
                "Disposition the call accurately after — don't let a rough call affect your next one",
                "If you're shaken after a hard call, take 30 seconds before dialing again",
            ],
            "subsections": [
                {
                    "id": "diff_rude",
                    "label": "Rude / Dismissive",
                    "trigger": "Just hanging up / Being short / Clearly annoyed",
                    "script": "I completely understand — I'll be quick and then I'll let you go. One thing before I do: [deliver one-sentence hook]. If that's not relevant, I won't call again. Does that sound fair?",
                    "note": "Stay warm. Don't mirror their energy. Give them one strong hook and let them decide.",
                },
                {
                    "id": "diff_aggressive",
                    "label": "Aggressive / Yelling",
                    "trigger": "Raising their voice / Demanding you stop calling",
                    "script": "I hear you — I'm going to let you go right now. I apologize for catching you at a bad time. I'll make a note not to call again. Have a good day.",
                    "note": "Do not try to save this call. Disposition as NI or DNC based on what they said. Move on immediately.",
                },
                {
                    "id": "diff_cursing",
                    "label": "Cursing at You",
                    "trigger": "Using profanity directed at you",
                    "script": "I'm going to stop you right there — I'm happy to talk about this professionally, but I'm not going to continue the conversation if it goes this direction. If you'd like to hear what I have to say, I'm here. If not, no hard feelings — have a good day.",
                    "note": "Say this once, calmly. If they continue, hang up without another word. Disposition as HU.",
                },
                {
                    "id": "diff_racist",
                    "label": "Racist / Discriminatory Remarks",
                    "trigger": "Making comments about your accent, nationality, or ethnicity",
                    "script": "I appreciate you taking my call. I'm going to keep this professional and focus on why I called — [pivot back to pitch]. If at any point you'd prefer not to continue, that's completely fine too.",
                    "note": "You do not have to engage with the comment at all. Acknowledge nothing, pivot immediately. If it continues or escalates, end the call: 'I'm going to let you go — take care.' Hang up. Disposition HU.",
                },
                {
                    "id": "diff_messing",
                    "label": "Wasting Time / Messing Around",
                    "trigger": "Asking nonsense questions / Playing games / Clearly not serious",
                    "script": "I want to make sure I'm using your time well — are you the right person to talk to about this, or should I reach out another time?",
                    "note": "If they're clearly not serious after one redirect, disposition as HU and move on. Your time is worth protecting too.",
                },
                {
                    "id": "diff_repeat_caller",
                    "label": "They've Been Called Before",
                    "trigger": "I've already told you no / You keep calling / I asked you not to call",
                    "script": "I sincerely apologize for that — that should not have happened. I'm making a note right now to remove your number from our list. You won't receive another call from us. I'm sorry for the inconvenience.",
                    "note": "Disposition immediately as DNC. Do not try to pitch. Do not explain. Just apologize and remove.",
                },
                {
                    "id": "diff_recording",
                    "label": "They Say They're Recording",
                    "trigger": "I'm recording this call / This is being recorded",
                    "script": "That's completely fine — I have nothing to hide. My name is Odelyn, I'm calling from Lynkflow, and I'm reaching out to tell you about our call answering service for plumbing businesses. Happy to continue.",
                    "note": "Don't panic. Don't hang up. Just continue professionally. You're not doing anything wrong.",
                },
                {
                    "id": "diff_dnc",
                    "label": "They Demand to Be on DNC",
                    "trigger": "Put me on your do not call list / Remove my number",
                    "script": "Absolutely — I'm removing your number right now. You will not receive another call from us. I apologize for the interruption. Have a good day.",
                    "note": "Disposition as DNC immediately. No exceptions.",
                },
            ],
            "sample_label": "Hear how to handle it",
        },
        {
            "id": "close",
            "title": "The Close",
            "icon": "✅",
            "explanation": "You are the closer. There is no transfer. When the prospect warms up — stop pitching and start closing. The most common mistake is continuing to sell after the customer is already interested. Ask for the yes, then go quiet and let them answer. Silence is not awkward — it's pressure working in your favor.",
            "tips": [
                "The moment they stop objecting and start asking questions, that's your signal to close",
                "Ask, then be silent — do not fill the silence with more pitch",
                "If they hesitate, ask 'What's holding you back?' — not 'Are you sure?'",
                "After a yes, move immediately to next steps — don't linger",
                "Confirm everything clearly: price, timeline, what happens next",
            ],
            "subsections": [
                {
                    "id": "close_standard",
                    "label": "Standard Close",
                    "trigger": "They seem interested / Questions are slowing down",
                    "script": "Based on everything I've shared — does this sound like something that would help your business? ... [Silence. Wait for their answer.] ... [If yes]: Perfect. Here's what happens next — setup takes 24 to 48 hours, and we build the agent specifically around your business. The setup fee is [price]. We can take care of that right now and have you live by [day]. Does that work for you?",
                },
                {
                    "id": "close_soft",
                    "label": "Soft Close",
                    "trigger": "They're still a little unsure but warming up",
                    "script": "Let me ask you this — if you knew for a fact that this would save you at least two or three missed calls a month, would that be worth $100 a month to you? ... [Wait] ... Most owners say yes to that instantly. And that's really the only bet we're asking you to make.",
                },
                {
                    "id": "close_urgency",
                    "label": "Urgency Close",
                    "trigger": "They keep delaying but seem interested",
                    "script": "I totally understand wanting to think it through. Here's the thing though — every day without this, your phone is still ringing when you can't answer it. Those calls aren't waiting around. The sooner we get this set up, the sooner you stop losing them. Can we get the ball rolling today?",
                },
                {
                    "id": "close_post_yes",
                    "label": "After They Say Yes",
                    "trigger": "They've agreed to move forward",
                    "script": "Amazing — I'm glad we connected. So here's what's going to happen: I'll send you a short form to fill in your business details, then we start the build. Setup is 24 to 48 hours. I'll also send the payment link to the email you give me now. What's the best email address for you? ... [Get email] ... And your business name exactly as you want it to appear when the agent answers? ... [Confirm details] ... Perfect. You'll hear from us within the hour. Welcome aboard.",
                },
            ],
            "sample_label": "Hear the close",
        },
        {
            "id": "voicemail",
            "title": "Voicemail Script",
            "icon": "📱",
            "explanation": "When you hit voicemail, you have one shot to leave a message that makes them curious enough to call back — or at least recognize your name when you call again. Keep it under 20 seconds. Don't pitch on voicemail. Create just enough curiosity to stay in their head.",
            "tips": [
                "20 seconds maximum — nobody listens to long voicemails",
                "Speak clearly and slightly slower than normal",
                "Leave your number twice — once at the start and once at the end",
                "Don't pitch — create curiosity only",
                "Always call back anyway — don't wait for them to return your call",
            ],
            "script": "Hi, this is Odelyn from Lynkflow — I was calling about a way to make sure your plumbing business never misses a customer call again, even at 2am or when you're out on a job. To see exactly what I mean, just call this number back: +1 401 386 9119. Talk soon.",
            "sample_label": "Hear the voicemail",
        },
        {
            "id": "callback",
            "title": "Callback Confirmation",
            "icon": "📅",
            "explanation": "When someone says 'call me back later,' most agents nod and hang up — then call at a random time and start from zero again. You need to lock in a specific time and set expectations so the callback feels like a scheduled meeting, not a cold call all over again. But the most powerful callback is when YOU called them first and hit voicemail — that missed call becomes your best pitch.",
            "tips": [
                "Always get a specific time — not 'sometime this week'",
                "Repeat the time back to them to confirm",
                "When you call back, reference the previous conversation immediately",
                "If they don't pick up at the agreed time, leave a voicemail referencing the scheduled callback",
                "If you hit their voicemail on a previous call — use it as proof of their problem when you call back",
                "Never start a callback from zero — always reference why you are calling back",
            ],
            "script": "Completely understand — what's the best time to reach you? Is [morning / afternoon] better? ... [Confirm time] ... Perfect — so I'll call you back on [day] at [time]. And it's still [their number] that's best? ... Great. I'll have more details ready for you then. Talk soon. ... [On callback]: Hi [name], this is Odelyn from Lynkflow — we spoke [yesterday / earlier this week] and scheduled this call. Did I catch you at an okay time?",
            "sample_label": "Hear the callback script",
            "aha_pitch": {
                "title": "The Aha Callback — When You Hit Their Voicemail First",
                "explanation": "This is your most powerful pitch. You literally experienced their problem firsthand — you called and they missed it. Use that. When you call back a No Answer or Voicemail lead, don't start from scratch. Open with proof.",
                "script": "Hey, I actually called your shop yesterday but got your voicemail. I am calling back because I help plumbing businesses make sure that never happens to their customers. Because if I was a homeowner with a burst pipe, I would not have left a message — I would have hung up and called your competitor down the street. We build 24/7 AI receptionist lines so that every single call gets answered, even when you are under a sink. Does that sound like something worth two minutes of your time?",
                "why_it_works": [
                    "It proves the problem — you are not guessing they miss calls, you literally experienced it",
                    "It makes the math real — a burst pipe call is worth $500 to $2,000, and it just went to a competitor",
                    "It is not a pitch — it is a story about something that actually happened",
                    "It creates instant credibility — you called, you noticed, you came back with a solution",
                ]
            },
        },
        {
            "id": "dispositions",
            "title": "Call Dispositions",
            "icon": "📋",
            "explanation": "A disposition is how you tag a call after it ends. This is your call log — it tells you what happened, what to do next, and keeps your pipeline clean. Tag every single call. No exceptions. Accurate tagging is what separates a professional agent from a lazy one.",
            "tips": [
                "Tag the call within 30 seconds of hanging up — while it's fresh",
                "Be honest — wrong tagging messes up your own follow-up pipeline",
                "DNC is a legal obligation — never ignore it",
                "SALE should always be followed by an immediate confirmation email or message",
            ],
            "dispositions": [
                {"code": "SALE", "label": "Sale Closed", "description": "They said yes and committed to moving forward. Log name, business, price agreed, and email. Send confirmation immediately."},
                {"code": "CB", "label": "Callback Scheduled", "description": "They asked you to call back at a specific time. Log the exact time and any context they gave you."},
                {"code": "DMNI", "label": "Decision Maker Not In", "description": "You reached someone but the owner was unavailable. Log when to call back and who you spoke with."},
                {"code": "NI", "label": "Not Interested", "description": "They clearly declined after your full attempt. Three solid objections with no opening. Move on."},
                {"code": "ANSMACHINE", "label": "Answering Machine", "description": "Reached voicemail. Note whether you left a message or not. Always call back regardless."},
                {"code": "HU", "label": "Hung Up", "description": "They hung up during the call. Note at what point — greeting, pitch, objection. This tells you where the drop-off is."},
                {"code": "DNC", "label": "Do Not Call", "description": "They explicitly asked to be removed. Remove immediately and permanently. This is not optional."},
                {"code": "LB", "label": "Language Barrier", "description": "Communication wasn't possible. Log and move on — don't waste time on an unproductive call."},
                {"code": "BUSY", "label": "Busy / Rescheduled", "description": "They were busy but open. Always get a specific callback time before ending — 'try later' doesn't count."},
                {"code": "DEAD", "label": "Dead Air / No Response", "description": "Line connected but no one responded. Could be a bad connection or auto-dialer artifact. Log and move on."},
                {"code": "RING", "label": "No Answer / Ringing", "description": "Phone rang but no one picked up and no voicemail. Try again at a different time of day."},
                {"code": "WRONG", "label": "Wrong Number", "description": "Number doesn't belong to the business you were trying to reach. Update your lead sheet."},
            ],
            "sample_label": None,
        },
        {
            "id": "simulations",
            "title": "Full Call Simulations",
            "icon": "🎭",
            "explanation": "Real full call scenarios from start to finish. The plumber lines are written — you read them out loud as if you are the plumber. Odelyn lines are voiced. Repeat each scenario until you can respond without hesitating.",
            "tips": [
                "Read the plumber lines out loud — don't just read them in your head",
                "Pause after each plumber line before playing Odelyn's response",
                "Try responding yourself first, then play the sample to compare",
                "Repeat each scenario at least 3 times until it feels natural",
                "Focus on tone — match the plumber energy, don't fight it",
            ],
            "scenarios": [
                {
                    "id": "sim_ideal_close",
                    "label": "Ideal Close",
                    "description": "Owner picks up directly, warms up quickly, closes on first attempt. The dream call.",
                    "exchanges": [
                        {"speaker": "plumber", "text": "Hello, this is Tom's Plumbing."},
                        {"speaker": "odelyn", "text": "Hi there! Is the owner available?", "audio_id": "sim_ideal_0"},
                        {"speaker": "plumber", "text": "Speaking, yeah."},
                        {"speaker": "odelyn", "text": "Perfect — my name is Odelyn, I'm calling from Lynkflow. We help plumbing businesses make sure they never miss a customer call — even when you're out on a job or it's 2am. I just need 60 seconds. Is now okay?", "audio_id": "sim_ideal_1"},
                        {"speaker": "plumber", "text": "Yeah sure, go ahead."},
                        {"speaker": "odelyn", "text": "Here's the thing — every time your phone rings and you can't get to it, that's a job walking straight to your competitor. Most people don't call back. Lynkflow gives you a 24/7 answering system that picks up every single call, finds out what the customer needs, and sends you a full summary by text right away. You never miss another lead.", "audio_id": "sim_ideal_2"},
                        {"speaker": "plumber", "text": "Huh. How much does it cost?"},
                        {"speaker": "odelyn", "text": "It's a one-time setup fee between $300 and $500 depending on how we customize it, and then $100 to $150 a month after that. To put that in perspective — one missed call from a decent job is usually $500 to $2,000. If this saves you even two calls a month you would have missed, it pays for itself immediately.", "audio_id": "sim_ideal_3"},
                        {"speaker": "plumber", "text": "That actually makes sense. How do we get started?"},
                        {"speaker": "odelyn", "text": "Perfect. Setup takes 24 to 48 hours and we build it specifically around your business. I'll send you a short form and a payment link to the email you give me now. What's the best email for you?", "audio_id": "sim_ideal_4"},
                    ]
                },
                {
                    "id": "sim_objection_close",
                    "label": "Objections Then Close",
                    "description": "Owner pushes back twice with objections but Odelyn handles them and closes.",
                    "exchanges": [
                        {"speaker": "plumber", "text": "Yeah who is this?"},
                        {"speaker": "odelyn", "text": "Hi! Is the owner available?", "audio_id": "sim_obj_0"},
                        {"speaker": "plumber", "text": "This is the owner, yeah."},
                        {"speaker": "odelyn", "text": "Perfect — my name is Odelyn, calling from Lynkflow. We help plumbing businesses make sure they never miss a customer call, even when you're out on a job. Do you have 60 seconds?", "audio_id": "sim_obj_1"},
                        {"speaker": "plumber", "text": "I'm not really interested, we're doing fine."},
                        {"speaker": "odelyn", "text": "That's completely fair — I hear that a lot. Can I ask one quick question? When you're out on a job and your phone rings, what happens to that call right now?", "audio_id": "sim_obj_2"},
                        {"speaker": "plumber", "text": "It goes to voicemail I guess."},
                        {"speaker": "odelyn", "text": "Right — and most callers don't leave voicemails. They just call the next plumber on Google. That's the gap we close. Lynkflow picks up every call you miss, gets the customer's details, and notifies you instantly so you can call them back before they go elsewhere.", "audio_id": "sim_obj_3"},
                        {"speaker": "plumber", "text": "How much does it run?"},
                        {"speaker": "odelyn", "text": "Setup is $300 to $500 one time, then $100 to $150 a month. One saved job a month pays for it several times over.", "audio_id": "sim_obj_4"},
                        {"speaker": "plumber", "text": "Let me think about it."},
                        {"speaker": "odelyn", "text": "Of course — can I ask what part you want to think through? Sometimes I can answer it right now. Is it the price, how it works, or something else?", "audio_id": "sim_obj_5"},
                        {"speaker": "plumber", "text": "I mean the price is a bit much right now."},
                        {"speaker": "odelyn", "text": "I understand. The $100 a month is less than what one missed call costs you. If it's tight right now, we can start with the basic setup at $300 and the lower monthly rate. Would that be easier to work with?", "audio_id": "sim_obj_6"},
                        {"speaker": "plumber", "text": "Yeah okay, let's do it."},
                        {"speaker": "odelyn", "text": "Perfect. I'll send you the details right now. What's the best email for you?", "audio_id": "sim_obj_7"},
                    ]
                },
                {
                    "id": "sim_gatekeeper",
                    "label": "Gatekeeper to Close",
                    "description": "Receptionist answers, Odelyn navigates to the owner and closes.",
                    "exchanges": [
                        {"speaker": "plumber", "text": "Thank you for calling ABC Plumbing, this is Rebecca, how can I help you?"},
                        {"speaker": "odelyn", "text": "Hi Rebecca! Is the owner available? I just need a quick minute with them.", "audio_id": "sim_gate_1"},
                        {"speaker": "plumber", "text": "Can I ask what this is regarding?"},
                        {"speaker": "odelyn", "text": "Sure — I'm calling about their phone system, it'll just take a minute.", "audio_id": "sim_gate_2"},
                        {"speaker": "plumber", "text": "Okay hold on... [owner picks up] Yeah this is Mike."},
                        {"speaker": "odelyn", "text": "Hi Mike! My name is Odelyn from Lynkflow — your team said you'd be the right person to speak with. I'll be quick. We help plumbing businesses make sure they never miss a customer call, even when you're out on a job. Do you have 60 seconds?", "audio_id": "sim_gate_3"},
                        {"speaker": "plumber", "text": "Sure what've you got."},
                        {"speaker": "odelyn", "text": "Every time your phone rings and you can't get to it, that's a job going to your competitor. Lynkflow picks up every call you miss — 24/7, even multiple calls at the same time — gets the customer's details, and sends you a summary instantly. You never lose another lead to a missed call.", "audio_id": "sim_gate_4"},
                        {"speaker": "plumber", "text": "We already have Rebecca answering calls."},
                        {"speaker": "odelyn", "text": "That's great — Rebecca handles the daytime calls perfectly. What about after hours? Or when she's on another call and a second one comes in? Lynkflow covers all of that without adding to her workload. It's overflow, not a replacement.", "audio_id": "sim_gate_5"},
                        {"speaker": "plumber", "text": "Hm. What does it cost?"},
                        {"speaker": "odelyn", "text": "Setup is $300 to $500 one time, then $100 to $150 a month. Most clients recover that cost in the first week from calls they would have missed after hours.", "audio_id": "sim_gate_6"},
                        {"speaker": "plumber", "text": "Alright send me the info."},
                        {"speaker": "odelyn", "text": "Perfect — what's the best email for you Mike?", "audio_id": "sim_gate_7"},
                    ]
                },
                {
                    "id": "sim_voicemail_objection",
                    "label": "Voicemail Objection",
                    "description": "Owner says they already have voicemail. Odelyn handles it and closes.",
                    "exchanges": [
                        {"speaker": "plumber", "text": "Hello?"},
                        {"speaker": "odelyn", "text": "Hi! Is the owner available?", "audio_id": "sim_vm_0"},
                        {"speaker": "plumber", "text": "Yeah that's me."},
                        {"speaker": "odelyn", "text": "Perfect — my name is Odelyn from Lynkflow. We help plumbing businesses make sure they never miss a customer call. Do you have 60 seconds?", "audio_id": "sim_vm_1"},
                        {"speaker": "plumber", "text": "We already have voicemail for that."},
                        {"speaker": "odelyn", "text": "Totally understand — here's the thing though. Over 80% of people who hit voicemail just hang up and call the next plumber. They don't leave a message. Lynkflow picks up live before it ever goes to voicemail, so the customer actually gets helped instead of going elsewhere.", "audio_id": "sim_vm_2"},
                        {"speaker": "plumber", "text": "Huh I never thought about it that way."},
                        {"speaker": "odelyn", "text": "Most owners don't until they add it up. Every missed call is a potential $500 to $2,000 job gone. Lynkflow costs $100 to $150 a month — one saved job covers months of the service.", "audio_id": "sim_vm_3"},
                        {"speaker": "plumber", "text": "How does it actually work?"},
                        {"speaker": "odelyn", "text": "When someone calls your number, instead of hitting voicemail, they get answered live by your Lynkflow agent. It greets them with your business name, finds out what they need, captures their info, and sends you a full summary by text right away. You call them back knowing exactly what the job is.", "audio_id": "sim_vm_4"},
                        {"speaker": "plumber", "text": "Okay I'm interested. What's the next step?"},
                        {"speaker": "odelyn", "text": "Setup takes 24 to 48 hours. I'll send you a short form and payment link right now. What's the best email for you?", "audio_id": "sim_vm_5"},
                    ]
                },
                {
                    "id": "sim_aggressive",
                    "label": "Aggressive Owner",
                    "description": "Owner is rude and dismissive. Odelyn stays calm, tries once, then exits professionally.",
                    "exchanges": [
                        {"speaker": "plumber", "text": "What do you want?"},
                        {"speaker": "odelyn", "text": "Hi — is the owner available?", "audio_id": "sim_agg_0"},
                        {"speaker": "plumber", "text": "I'm the owner. What do you want?"},
                        {"speaker": "odelyn", "text": "My name is Odelyn from Lynkflow. We help plumbing businesses stop losing jobs to missed calls. Do you have 60 seconds?", "audio_id": "sim_agg_1"},
                        {"speaker": "plumber", "text": "I get these calls every day. Not interested. Stop calling."},
                        {"speaker": "odelyn", "text": "I completely understand — I'll let you go. Before I do, if you're ever losing calls when you're out on jobs, we're here. I won't call again. Have a great day.", "audio_id": "sim_agg_2"},
                        {"speaker": "plumber", "text": "[hangs up]"},
                        {"speaker": "odelyn", "text": "[Disposition as DNC. Do not call again. Move to the next lead immediately. Don't let it affect your energy on the next call.]", "audio_id": None},
                    ]
                },
            ],
            "sample_label": None,
        },
        {
            "id": "cheatsheet",
            "title": "Call Cheat Sheet",
            "icon": "⚡",
            "explanation": "Everything you need during a live call. Keep this open while dialing. Don't overthink — just follow the flow.",
            "tips": [],
            "cheatsheet": {
                "flow": [
                    {"step": "1", "label": "Open", "text": "Hi, am I reaching the owner of [Business Name]?"},
                    {"step": "2", "label": "Disarm", "text": "Yeah, so I am going to be honest -- this is a cold call. Would you like to hang up now or give me 30 seconds and then you can decide?"},
                    {"step": "3", "label": "Pitch", "text": "I checked your Google listing. We help plumbers never miss a customer call -- 24/7 AI that answers every call automatically and texts you a summary instantly. No contracts, cancel anytime. If that sounds interesting, my partner can show you exactly how it works on a quick 10-minute Zoom. No commitment, just a live demo. Would that be worth your time?"},
                    {"step": "4", "label": "Book", "text": "What day and time works best for a quick 10-minute Zoom? I will send the link right after this call."},
                    {"step": "5", "label": "Confirm", "text": "Just to confirm -- [day] at [time] [timezone]. What is the best email to send the Zoom link to?"},
                    {"step": "6", "label": "Close", "text": "Perfect. You will get the Zoom link in a few minutes. Looking forward to showing you how it works. Have a great day!"},
                ],
                "objections": [
                    {"trigger": "Not interested", "reply": "Totally fair. When you miss a call on a job, where does that lead go?"},
                    {"trigger": "We have voicemail", "reply": "80% of callers hang up before leaving a voicemail. They call your competitor instead."},
                    {"trigger": "Too busy", "reply": "No problem — what day and time works better for our team to reach you?"},
                    {"trigger": "How much?", "reply": "$300-500 setup, $100-150/month. One saved job covers months of the service."},
                    {"trigger": "Already have a receptionist", "reply": "Great — we handle overflow and after-hours calls they cannot get to. Worth a quick look?"},
                    {"trigger": "Is this AI?", "reply": "Yes — and I am calling because we help plumbers never miss a job from a missed call. Worth a quick conversation?"},
                    {"trigger": "Send me info", "reply": "Absolutely — what is the best email for you?"},
                    {"trigger": "Think about it", "reply": "Of course — what part do you want to think through? I can answer it right now."},
                ],
                "statuses": [
                    {"code": "Called", "when": "Answered, had a conversation"},
                    {"code": "Voicemail", "when": "Left a voicemail"},
                    {"code": "VM No Msg", "when": "Voicemail, no message left"},
                    {"code": "No Answer", "when": "Rang, nobody picked up"},
                    {"code": "IVR", "when": "Hit automated system, press 1/8 etc"},
                    {"code": "Callback", "when": "They asked you to call back"},
                    {"code": "Interested", "when": "Pitched, they want more info"},
                    {"code": "NI", "when": "Clear no after pitch"},
                    {"code": "DNC", "when": "Asked to be removed — never call again"},
                    {"code": "Wrong #", "when": "Number does not match business"},
                ],
                "numbers": [
                    {"label": "Your caller ID", "value": "+1 (978) 684-3590"},
                    {"label": "Demo number (Rex)", "value": "+1 (401) 386-9119"},
                ],
                "attempts": [
                    "1st call — no answer → leave voicemail with demo number",
                    "2nd call — no answer → no voicemail, hang up",
                    "3rd call — no answer → status RING, move on",
                ]
            },
            "sample_label": None,
        },
        {
            "id": "zoom_demo",
            "title": "Zoom Demo Guide",
            "icon": "🖥️",
            "explanation": "This section is for Selwyn only. When Odelyn books a Zoom, you run it. Your job is to show Rex working live, answer technical questions, and close the deal. Keep it under 15 minutes. Be confident -- you built this.",
            "tips": [
                "Open with their name and business -- shows you know who they are",
                "Keep it under 15 minutes -- respect their time",
                "Show Rex answering a live call -- this is the money moment",
                "Show the email notification landing in real time",
                "Show the Google Sheets updating automatically",
                "Answer questions confidently -- you built every part of this",
                "Close at the end -- do not let them say they will think about it without a follow up time",
            ],
            "zoom_flow": [
                {"step": "1", "label": "Open", "text": "Hey [Name], thanks for making time. I am Selwyn, the one who builds these systems. I will keep this to 10 minutes. Is it okay if I share my screen?"},
                {"step": "2", "label": "Context", "text": "So Odelyn gave me a quick rundown -- you run [Business Name] and you are dealing with missed calls when you are out on jobs. That is exactly what we fix. Let me show you what your customers would experience."},
                {"step": "3", "label": "Live Call", "text": "I am going to call our demo number right now. Watch what happens. [Call +1 401 386 9119 -- Rex answers] That is exactly what your customers would hear -- branded with your business name, 24/7, even if three people call at the same time."},
                {"step": "4", "label": "Show Notification", "text": "Now watch your phone -- actually watch mine. [Show email landing] Every single call gets logged like this. You see exactly who called, what they need, and when. You call them back knowing everything already."},
                {"step": "5", "label": "Handle Questions", "text": "What questions do you have? [Answer everything honestly. If you do not know, say you will confirm and follow up.]"},
                {"step": "6", "label": "Pricing", "text": "Setup is between $300 and $500 depending on how we customize it for your business specifically. After that it is $100 to $150 a month. One saved job covers that instantly. Most of our clients see that in the first week."},
                {"step": "7", "label": "Close", "text": "What questions do you still have? [Handle them.] Here is what setup looks like. I need your business details for the agent script, a Google account so I can connect your calendar and call logs, and access to set up call forwarding from your business number. Once I have those it takes about 5 days to build, test, and go live. Sound good? [Wait.] Does it make sense to move forward? [Wait for yes.] Perfect. I will send the PayPal invoice now and start building. What email should I use?"},
            ],
            "sample_label": None,
        },
    ]
    return JSONResponse(modules)

# ═══════════════════════════════════════════════════════════════════════════════
# AGENT ROUTES
# ═══════════════════════════════════════════════════════════════════════════════

@app.get("/api/agent/config")
async def get_agent_config():
    return JSONResponse(load_agent_config().model_dump())


@app.post("/api/agent/config")
async def post_agent_config(request: Request):
    data = await request.json()
    cfg = AgentConfig(**data)
    cfg.voice_engine = "gpt_live"
    cfg.followup_voice_engine = "gpt_live"
    _save_agent_config(cfg)
    return JSONResponse({"success": True})


@app.post("/api/agent/initiate")
async def agent_initiate(request: Request):
    """
    Kick off an outbound call via Twilio REST API.
    Twilio calls the lead; when connected it fetches our TwiML which opens
    the media stream WebSocket back to this server.
    """
    data     = await request.json()
    phone    = data.get("phone", "").strip()
    lead     = data.get("lead", {})
    base_url = data.get("base_url", "").rstrip("/")
    call_mode = str(data.get("call_mode") or "outbound").strip() or "outbound"
    follow_up_id = str(data.get("follow_up_id") or "").strip()

    call_sid = await _start_agent_call(phone, lead, base_url, call_mode=call_mode, follow_up_id=follow_up_id)
    cfg = load_agent_config()
    return JSONResponse({
        "success": True,
        "call_sid": call_sid,
        "voice_engine": getattr(cfg, "voice_engine", "gpt_live"),
        "live_model": getattr(cfg, "live_model", "gpt-live-1"),
    })


def _status_from_outcome(outcome: str, call_status: str = "") -> str:
    outcome = (outcome or "").lower()
    call_status = (call_status or "").lower()
    if outcome == "interested":
        return "Interested"
    if outcome == "callback":
        return "Callback"
    if outcome == "voicemail_left":
        return "Voicemail Left"
    if outcome == "voicemail":
        return "VM No Message"
    if outcome == "ivr":
        return "IVR"
    if outcome == "not_interested":
        return "Not Interested"
    if outcome == "no_answer" or call_status == "no-answer":
        return "No Answer"
    if call_status == "busy":
        return "Busy"
    if call_status in ("failed", "canceled"):
        return "Call Failed"
    return "Called"


def _sim_initial_utterances(lead: dict, scenario: str) -> list[str]:
    business = lead.get("Name") or "the business"
    scenario = (scenario or "mixed").lower()
    if scenario == "owner_interested":
        return ["Hello, this is the owner."]
    if scenario == "owner_skeptical":
        return ["Hello, this is the owner."]
    if scenario == "owner_busy":
        return ["Hello, this is Mike, I'm the owner but I'm pretty busy."]
    if scenario == "recorded_message":
        return [
            "This call may be recorded for quality assurance purposes.",
            f"Thank you for calling {business}, this is Pam, how may I help you?",
        ]
    if scenario == "callback":
        return [f"Thank you for calling {business}, this is Jenny, how can I help you?"]
    if scenario == "not_interested":
        return ["Hello, this is the owner."]
    return [f"Thank you for calling {business}, this is Pam, how may I help you?"]


def _default_sim_leads(limit: int) -> list[dict]:
    names = [
        "Mother Modern Plumbing, Sewer & Drain",
        "Blair Norris Plumbing",
        "Carlisle Plumbing",
        "Zoom Drain",
        "Advanced Restoration Solutions",
        "NuFlow Indy",
        "RESTORM",
        "B N C Plumbing Company",
        "McDougalle Water Sewer Services",
        "AAA Acme Plumbing",
    ]
    leads = []
    for i in range(max(1, limit)):
        name = names[i % len(names)]
        if i >= len(names):
            name = f"{name} Test {i + 1}"
        leads.append({
            "Name": name,
            "Phone": f"+15550100{i:03d}",
            "City": "Test City",
            "State": "IN",
            "Category": "Plumber",
            "Timezone": "Eastern",
            "Status": "Queued",
            "Notes": "Generated GPT lead test record; no real phone call.",
        })
    return leads


def _sim_lead_prompt(lead: dict, scenario: str) -> str:
    business = lead.get("Name") or "the business"
    category = lead.get("Category") or "service business"
    timezone = _lead_timezone(lead) or "local time"
    scenario_notes = {
        "mixed": "Act as a realistic receptionist first. Ask who is calling or what it is about. Do not be overly helpful.",
        "owner_interested": "Act as an owner who has been losing missed calls and becomes genuinely interested if the caller explains Lynkflow clearly. Ask how it works, ask the price, then agree to a demo or ask for info by email.",
        "owner_skeptical": "Act as a skeptical owner. Ask if this is AI or a recorded message, then decide if the caller earns 30 seconds.",
        "owner_busy": "Act as a busy owner. If the caller is respectful, offer a better callback time.",
        "recorded_message": "Act as a receptionist who is suspicious that the caller is a recording or AI.",
        "callback": "Act as a receptionist who cannot transfer but can offer a callback time tomorrow morning.",
        "not_interested": "Act as an owner who is not interested and wants the call to end politely.",
    }
    return f"""
You are a simulated phone lead for QA testing an outbound AI caller.
Business: {business}
Category: {category}
Timezone: {timezone}
Scenario: {scenario_notes.get((scenario or 'mixed').lower(), scenario_notes['mixed'])}

Rules:
- Reply as the lead only. Do not explain your reasoning.
- Keep replies short and natural for a phone call: usually 3-15 words.
- You may interrupt, be confused, ask who is calling, ask whether this is AI, ask whether it is recorded, say the owner is unavailable, ask for a callback, or say no.
- Behave like a real business phone answerer. Do not be too cooperative unless this is the interested-owner scenario and the caller earns your interest.
- If the caller says goodbye or clearly ends the call, return done=true.

Return strict JSON only: {{"text":"lead reply", "done":false, "outcome":"conversation"}}
""".strip()


async def _openai_chat_json(messages: list[dict], model: str = "gpt-4o-mini", max_tokens: int = 120) -> dict:
    if not OPENAI_API_KEY:
        raise RuntimeError("OPENAI_API_KEY not set")
    async with httpx.AsyncClient(timeout=30) as client:
        resp = await client.post(
            "https://api.openai.com/v1/chat/completions",
            headers={"Authorization": f"Bearer {OPENAI_API_KEY}", "Content-Type": "application/json"},
            json={
                "model": model,
                "messages": messages,
                "temperature": 0.65,
                "max_tokens": max_tokens,
                "response_format": {"type": "json_object"},
            },
        )
    if resp.status_code != 200:
        raise RuntimeError(f"OpenAI simulator error: {resp.text}")
    content = resp.json()["choices"][0]["message"].get("content", "{}")
    try:
        return json.loads(content)
    except Exception:
        return {"text": content.strip(), "done": False, "outcome": "conversation"}


async def _simulated_lead_reply(lead: dict, scenario: str, history: list[dict], endpoint: str = "") -> dict:
    payload = {"lead": lead, "scenario": scenario, "history": history}
    if endpoint:
        async with httpx.AsyncClient(timeout=45) as client:
            resp = await client.post(endpoint, json=payload)
        if resp.status_code != 200:
            raise RuntimeError(f"sim endpoint error: {resp.status_code} {resp.text}")
        data = resp.json()
        return {
            "text": str(data.get("text") or data.get("reply") or "").strip(),
            "done": bool(data.get("done", False)),
            "outcome": data.get("outcome", "conversation"),
        }

    messages = [{"role": "system", "content": _sim_lead_prompt(lead, scenario)}]
    for item in history[-16:]:
        role = "assistant" if item.get("speaker") == "lead" else "user"
        messages.append({"role": role, "content": item.get("text", "")})
    data = await _openai_chat_json(messages)
    text = str(data.get("text") or data.get("reply") or "").strip()
    return {"text": text, "done": bool(data.get("done", False)), "outcome": data.get("outcome", "conversation")}


def _sim_agent_system_prompt(cfg: AgentConfig, lead: dict) -> str:
    business = lead.get("Name") or "the business"
    category = lead.get("Category") or "service business"
    city = lead.get("City") or ""
    timezone = _lead_timezone(lead) or "local time"
    call_mode = lead.get("call_mode") or "outbound"
    follow_up = lead.get("FollowUp") or {}
    follow_up_context = json.dumps(follow_up, ensure_ascii=False) if follow_up else "{}"
    return f"""
{getattr(cfg, 'system_prompt', '')}

Simulation mode for QA only. Real phone calls use GPT-Live through agent_live_ws.GPTLiveCallHandler.
Lead context: {business}, {category}{' in ' + city if city else ''}. Timezone: {timezone}. Call mode: {call_mode}.
Follow-up context, if any: {follow_up_context}

Respond as Anna, the Lynkflow AI voice agent. Keep replies short and natural for a phone call.
Do not invent names, emails, prices, promises, or prior conversation details.
Use [HANGUP] only as a silent control token when the simulated call should end.
""".strip()


async def _sim_agent_text_response(cfg: AgentConfig, conversation: list[dict], status_queue: asyncio.Queue) -> str | None:
    if not OPENAI_API_KEY:
        raise RuntimeError("OPENAI_API_KEY not set")
    async with httpx.AsyncClient(timeout=30) as client:
        resp = await client.post(
            "https://api.openai.com/v1/chat/completions",
            headers={"Authorization": f"Bearer {OPENAI_API_KEY}", "Content-Type": "application/json"},
            json={
                "model": cfg.model,
                "messages": conversation,
                "temperature": cfg.temperature,
                "max_tokens": cfg.max_tokens,
            },
        )
    if resp.status_code != 200:
        raise RuntimeError(f"OpenAI agent test error: {resp.text}")
    text = resp.json()["choices"][0]["message"].get("content", "").strip()
    clean = text.replace("[HANGUP]", "").strip()
    if clean:
        await status_queue.put({
            "type": "transcript_final", "speaker": "agent", "text": clean,
            "ts": datetime.now().strftime("%H:%M:%S"),
        })
    return text or None


async def _drain_sim_events(call_sid: str, queue: asyncio.Queue) -> str:
    last_agent = ""
    while True:
        try:
            event = queue.get_nowait()
        except asyncio.QueueEmpty:
            break
        if call_sid:
            _agent_events.setdefault(call_sid, []).append(event)
            _agent_events[call_sid] = _agent_events[call_sid][-200:]
            q = _agent_queues.get(call_sid)
            if q:
                await q.put(event)
        if event.get("type") in ("status", "transcript", "transcript_final"):
            await _autodial_note_call_event(call_sid, event)
        if event.get("type") in ("transcript", "transcript_final") and event.get("speaker") == "agent":
            last_agent = event.get("text", "")
    return last_agent


def _save_sim_transcript(lead: dict, phone: str, call_sid: str, scenario: str, history: list[dict], outcome: str) -> str:
    name = re.sub(r"[^A-Za-z0-9_. -]+", "", lead.get("Name") or "GPT Lead Test").strip()
    name = re.sub(r"\s+", " ", name)[:42] or "GPT Lead Test"
    suffix = call_sid[-6:] if call_sid else uuid.uuid4().hex[:6]
    filename = f"{datetime.now().strftime('%Y%m%d_%H%M%S')}_{name}_{suffix}_sim.txt"
    path = _REC_DIR / filename
    lines = [
        "GPT Lead Test Transcript",
        f"Business: {lead.get('Name', '')}",
        f"Phone: {phone}",
        f"Scenario: {scenario}",
        f"Outcome: {outcome}",
        f"Generated: {datetime.now().isoformat(timespec='seconds')}",
        "",
    ]
    for item in history:
        speaker = "Lead" if item.get("speaker") == "lead" else "Agent"
        lines.append(f"{speaker}: {item.get('text', '')}")
    path.write_text("\n".join(lines) + "\n")
    return filename


async def _finish_sim_call(call_sid: str, outcome: str, message: str = "", recording: str = ""):
    async with _autodial_lock:
        item = _autodial_state["active"].pop(call_sid, None)
        if not item:
            return
        _autodial_state["completed"] += 1
    lead = item.get("lead", {})
    await _autodial_broadcast({
        "type": "call_finished",
        "call_sid": call_sid,
        "name": lead.get("Name", ""),
        "phone": item.get("phone", ""),
        "status": f"Test: {_status_from_outcome(outcome)}",
        "outcome": outcome,
        "recording": recording,
        "message": message or "GPT lead simulation finished; no real call placed and sheet not updated",
        "update": {"success": True, "method": "test_mode_no_sheet_update"},
    })


def _followup_test_reply(scenario: str, history: list[dict]) -> dict:
    turns = sum(1 for item in history if item.get("speaker") == "lead")
    scenario = (scenario or "remembers_context").lower()
    if turns <= 1:
        if scenario == "forgot_context":
            return {"text": "I do not remember, what was this about?", "done": False, "outcome": "conversation"}
        if scenario == "not_interested":
            return {"text": "We are not interested anymore, thanks.", "done": True, "outcome": "not_interested"}
        if scenario == "asks_questions":
            return {"text": "Yes, I remember. How does the call coverage actually work?", "done": False, "outcome": "conversation"}
        return {"text": "Yes, I remember speaking with you about missed calls.", "done": False, "outcome": "conversation"}
    if turns <= 2 and scenario == "asks_questions":
        return {"text": "Can you send details and maybe set up a short demo?", "done": True, "outcome": "interested"}
    if turns <= 2 and scenario == "forgot_context":
        return {"text": "Okay, send me the details again by email.", "done": True, "outcome": "interested"}
    return {"text": "That sounds fine, send me the next step.", "done": True, "outcome": "interested"}


async def _run_simulated_agent_call(call_sid: str, lead: dict, phone: str, scenario: str, endpoint: str):
    cfg = _runtime_agent_config(lead.get("call_mode", "outbound") or "outbound")
    status_queue = asyncio.Queue()
    conversation: list[dict] = [{"role": "system", "content": _sim_agent_system_prompt(cfg, lead)}]
    tokens_in = 0
    tokens_out = 0
    sim_started = time.time()
    stop_requested = False
    end_phrases = [p.strip().lower() for p in (getattr(cfg, "end_call_phrases", "") or "").split(",") if p.strip()]

    history: list[dict] = []
    outcome = "conversation"

    async def agent_turn() -> str:
        nonlocal tokens_in, tokens_out, stop_requested
        response = await _sim_agent_text_response(cfg, conversation, status_queue)
        text = response or ""
        clean = text.replace("[HANGUP]", "").strip()
        tokens_in += sum(len(m.get("content", "")) for m in conversation) // 4
        tokens_out += len(text) // 4
        if clean:
            conversation.append({"role": "assistant", "content": clean})
        low = clean.lower()
        if "[hangup]" in text.lower() or any(p in low for p in end_phrases):
            stop_requested = True
        return await _drain_sim_events(call_sid, status_queue)

    try:
        await status_queue.put({"type": "status", "state": "active", "message": f"GPT lead test: {scenario}"})
        await _drain_sim_events(call_sid, status_queue)

        openings = _sim_initial_utterances(lead, scenario)
        lead_text = openings.pop(0) if openings else "Hello?"
        for _turn in range(14):
            if not lead_text:
                break
            history.append({"speaker": "lead", "text": lead_text})
            await status_queue.put({
                "type": "transcript_final", "speaker": "prospect", "text": lead_text,
                "ts": datetime.now().strftime("%H:%M:%S"),
            })
            conversation.append({"role": "user", "content": lead_text})
            last_agent = await agent_turn()
            if last_agent:
                history.append({"speaker": "agent", "text": last_agent})
            if stop_requested:
                break
            if not last_agent and openings:
                lead_text = openings.pop(0)
                await asyncio.sleep(0.4)
                continue
            if not last_agent:
                break
            if lead.get("call_mode") == "follow_up" and not endpoint:
                reply = _followup_test_reply(scenario, history)
            else:
                reply = await _simulated_lead_reply(lead, scenario, history, endpoint)
            lead_text = reply.get("text", "")
            outcome = reply.get("outcome") or outcome
            if reply.get("done"):
                if lead_text:
                    history.append({"speaker": "lead", "text": lead_text})
                    await status_queue.put({
                        "type": "transcript_final", "speaker": "prospect", "text": lead_text,
                        "ts": datetime.now().strftime("%H:%M:%S"),
                    })
                    conversation.append({"role": "user", "content": lead_text})
                    last_agent = await agent_turn()
                    if last_agent:
                        history.append({"speaker": "agent", "text": last_agent})
                break
            await asyncio.sleep(0.4)
        final_outcome = outcome
        transcript_file = _save_sim_transcript(lead, phone, call_sid, scenario, history, final_outcome)
        recording = transcript_file
        await status_queue.put({"type": "status", "state": "ended", "message": f"GPT lead test finished: {final_outcome}"})
        await _drain_sim_events(call_sid, status_queue)
        try:
            from call_history import record_call
            from metrics import RATES
            ended = time.time()
            duration_s = round(max(0.1, ended - sim_started), 1)
            llm_cost = (
                tokens_in / 1_000_000 * RATES.get(f"{cfg.model}_in", RATES["gpt-4o-mini_in"]) +
                tokens_out / 1_000_000 * RATES.get(f"{cfg.model}_out", RATES["gpt-4o-mini_out"])
            )
            call_entry = {
                **_lead_time_fields(lead, ended),
                "call_sid": call_sid,
                "business": lead.get("Name", ""),
                "phone": phone,
                "city": lead.get("City", ""),
                "state": lead.get("State", ""),
                "category": lead.get("Category", ""),
                "call_mode": lead.get("call_mode", "outbound") or "outbound",
                "follow_up_id": lead.get("follow_up_id", ""),
                "answered": bool(history),
                "outcome": final_outcome,
                "turns": sum(1 for item in history if item.get("speaker") == "agent"),
                "interrupts": 0,
                "duration_s": duration_s,
                "latency": {"stt_ms": 0, "llm_ms": 0, "tts_ms": 0, "total_ms": 0, "total_p95_ms": 0, "samples": 0},
                "cost": {
                    "twilio": 0.0,
                    "stt": 0.0,
                    "llm": round(llm_cost, 5),
                    "tts": 0.0,
                    "total": round(llm_cost, 5),
                    "per_min": round(llm_cost / (duration_s / 60), 4) if duration_s else 0.0,
                    "duration_min": round(duration_s / 60, 3),
                    "duration_s": duration_s,
                },
                "recording": recording,
                "transcript_file": transcript_file,
                "test_mode": True,
                "sim_scenario": scenario,
            }
            record_call(call_entry)
            if call_entry.get("follow_up_id") and recording:
                try:
                    from followups import mark_call_recording
                    updated = mark_call_recording(call_sid, recording)
                    if updated:
                        await _followup_broadcast({"type": "follow_up_updated", "follow_up": updated, "message": "Follow-up recording saved"})
                except Exception as e:
                    print(f"[FOLLOWUP] simulated recording link failed: {e}")
            asyncio.create_task(_analyze_call_for_follow_up(call_entry))
        except Exception as e:
            print(f"[SIM HISTORY] failed: {e}")
        await _finish_sim_call(call_sid, final_outcome, recording=recording)
    except Exception as e:
        async with _autodial_lock:
            _autodial_state["active"].pop(call_sid, None)
            _autodial_state["failed"] += 1
        await _autodial_broadcast({
            "type": "call_failed",
            "call_sid": call_sid,
            "name": lead.get("Name", ""),
            "phone": phone,
            "error": str(e),
            "message": "GPT lead simulation failed",
        })


async def _autodial_loop():
    await _autodial_broadcast({"type": "started", "message": "Auto dialer started"})
    seen = set()

    try:
        while True:
            async with _autodial_lock:
                running = _autodial_state["running"]
                room = _autodial_state["concurrency"] - len(_autodial_state["active"])
                done = not _autodial_state["queue"] and not _autodial_state["active"]
                base_url = _autodial_state["base_url"]
                test_mode = _autodial_state.get("test_mode", False)
                sim_scenario = _autodial_state.get("sim_scenario", "mixed")
                sim_endpoint = _autodial_state.get("sim_endpoint", "")

            if not running:
                await _autodial_broadcast({"type": "stopped", "message": "Auto dialer stopped"})
                break
            if done:
                async with _autodial_lock:
                    _autodial_state["running"] = False
                    target_total = _autodial_state.get("target_total", 0)
                    eligible_total = _autodial_state.get("eligible_total", 0)
                msg = "Auto dialer finished requested call limit" if target_total and eligible_total > target_total else "Auto dialer finished all eligible leads"
                await _autodial_broadcast({"type": "completed", "message": msg})
                break

            launched = 0
            while room > 0:
                lead = None
                async with _autodial_lock:
                    while _autodial_state["queue"]:
                        candidate = _autodial_state["queue"].pop(0)
                        key = _phone_key(candidate.get("Phone", ""))
                        if not key or key in seen or not _is_autodial_eligible(candidate):
                            _autodial_state["skipped"] += 1
                            continue
                        if any(_phone_key(item.get("phone", "")) == key for item in _autodial_state["active"].values()):
                            _autodial_state["skipped"] += 1
                            continue
                        seen.add(key)
                        lead = candidate
                        break

                if not lead:
                    break

                phone = _format_us_phone(lead.get("Phone", ""))
                try:
                    if test_mode:
                        call_sid = f"SIM{uuid.uuid4().hex[:24]}"
                        async with _autodial_lock:
                            _autodial_state["active"][call_sid] = {
                                "lead": lead,
                                "phone": phone,
                                "mode": "test",
                                "engine": "GPT Lead Test",
                                "state": "testing",
                                "started_at": time.time(),
                            }
                        asyncio.create_task(_run_simulated_agent_call(call_sid, lead, phone, sim_scenario, sim_endpoint))
                    else:
                        call_sid = await _start_agent_call(phone, lead, base_url)
                        engine = "GPT-Live"
                        async with _autodial_lock:
                            _autodial_state["active"][call_sid] = {
                                "lead": lead,
                                "phone": phone,
                                "mode": "live",
                                "engine": engine,
                                "started_at": time.time(),
                            }
                        await _update_lead_record(lead.get("Name", ""), phone, "Calling", "Auto dialer started call", lead)
                    await _autodial_broadcast({
                        "type": "call_started",
                        "call_sid": call_sid,
                        "name": lead.get("Name", ""),
                        "phone": phone,
                        "mode": "test" if test_mode else "live",
                        "engine": "GPT Lead Test" if test_mode else engine,
                        "message": "GPT lead simulation started" if test_mode else "Auto dialer started call",
                    })
                    launched += 1
                    room -= 1
                    await asyncio.sleep(0.2 if test_mode else 0.7)  # avoid bursting Twilio/API callbacks
                except Exception as e:
                    async with _autodial_lock:
                        _autodial_state["failed"] += 1
                    if not test_mode:
                        await _update_lead_record(lead.get("Name", ""), phone, "Call Failed", str(e), lead)
                    await _autodial_broadcast({
                        "type": "call_failed",
                        "name": lead.get("Name", ""),
                        "phone": phone,
                        "error": str(e),
                    })
                    room -= 1

            if not launched:
                await asyncio.sleep(1)
    finally:
        async with _autodial_lock:
            _autodial_state["task"] = None


async def _autodial_finish_call(call_sid: str, outcome: str = "", call_status: str = "", recording: str = "", notes_extra: str = ""):
    async with _autodial_lock:
        item = _autodial_state["active"].pop(call_sid, None)
        if not item:
            lead_meta = _call_leads.get(call_sid, {})
            follow_up_id = lead_meta.get("follow_up_id")
            if follow_up_id and call_status and not outcome:
                from followups import complete_call_attempt
                result_status = "failed"
                result_text = f"Call {call_status}; no live media stream was opened, so no transcript or recording was created."
                updated = complete_call_attempt(follow_up_id, {
                    "call_sid": call_sid,
                    "outcome": call_status,
                    "phone": lead_meta.get("Phone", ""),
                    "business": lead_meta.get("Name", ""),
                }, {
                    "status": result_status,
                    "result": result_text,
                    "reason": "Twilio ended before the AI media stream produced an analyzable call.",
                })
                if updated:
                    await _followup_broadcast({"type": "follow_up_failed", "follow_up": updated, "call_sid": call_sid, "message": result_text})
            return
        _autodial_state["completed"] += 1

    lead = item.get("lead", {})
    phone = item.get("phone", lead.get("Phone", ""))
    status = _status_from_outcome(outcome, call_status)
    notes = f"Auto dial outcome: {outcome or call_status or 'completed'}"
    if notes_extra:
        notes += f"; {notes_extra}"
    if recording:
        notes += f"; recording: {recording}"
    result = await _update_lead_record(lead.get("Name", ""), phone, status, notes, lead)
    await _autodial_broadcast({
        "type": "call_finished",
        "call_sid": call_sid,
        "name": lead.get("Name", ""),
        "phone": phone,
        "status": status,
        "outcome": outcome or call_status,
        "recording": recording,
        "update": result,
    })


@app.post("/api/agent/autodial/start")
async def autodial_start(request: Request):
    data = await request.json()
    base_url = (data.get("base_url") or load_agent_config().base_url or "").rstrip("/")
    concurrency = max(1, min(15, int(data.get("concurrency", 1))))
    test_mode = bool(data.get("test_mode", False))
    sim_scenario = str(data.get("sim_scenario") or "mixed").strip() or "mixed"
    sim_endpoint = str(data.get("sim_endpoint") or "").strip()
    call_limit = max(1, min(1000, int(data.get("call_limit") or data.get("test_limit") or 5)))

    if test_mode:
        leads = _default_sim_leads(call_limit)
        eligible = leads
        skipped = 0
    else:
        try:
            leads = await _fetch_leads_from_sheet()
            eligible = [lead for lead in leads if _is_autodial_eligible(lead)]
            skipped = len(leads) - len(eligible)
        except HTTPException:
            raise
        except Exception as e:
            raise HTTPException(502, f"Could not load leads from Google Sheets: {e}")

    if test_mode:
        if not OPENAI_API_KEY:
            raise HTTPException(500, "OPENAI_API_KEY not set for GPT lead test mode")
        eligible_total = len(eligible)
        eligible = eligible[:call_limit]
        skipped = max(0, len(leads) - len(eligible)) if leads else 0
    elif not base_url:
        raise HTTPException(400, "base_url required")
    else:
        eligible_total = len(eligible)
        eligible = eligible[:call_limit]
    if not eligible:
        raise HTTPException(400, "no eligible leads found (only blank/New/Retry/Queued statuses are called)")

    async with _autodial_lock:
        if _autodial_state["running"]:
            return JSONResponse({"success": True, "already_running": True, **_autodial_snapshot()})

        _autodial_state.update({
            "running": True,
            "concurrency": concurrency,
            "base_url": base_url,
            "queue": eligible,
            "active": {},
            "completed": 0,
            "failed": 0,
            "skipped": skipped,
            "call_limit": call_limit,
            "eligible_total": eligible_total,
            "target_total": len(eligible),
            "started_at": time.time(),
            "events": [],
            "test_mode": test_mode,
            "sim_scenario": sim_scenario,
            "sim_endpoint": sim_endpoint,
        })
        _autodial_state["task"] = asyncio.create_task(_autodial_loop())

    return JSONResponse({"success": True, **_autodial_snapshot()})


@app.post("/api/agent/autodial/stop")
async def autodial_stop():
    async with _autodial_lock:
        _autodial_state["running"] = False
        _autodial_state["queue"] = []
    await _autodial_broadcast({"type": "stopping", "message": "Stopping after active calls finish"})
    return JSONResponse({"success": True, **_autodial_snapshot()})


@app.post("/api/agent/simulated-lead/respond")
async def simulated_lead_respond(request: Request):
    """Custom endpoint contract for test-mode lead simulators."""
    data = await request.json()
    lead = data.get("lead") or {}
    scenario = str(data.get("scenario") or "mixed")
    history = data.get("history") or []
    return JSONResponse(await _simulated_lead_reply(lead, scenario, history, endpoint=""))


@app.get("/api/agent/autodial/status")
async def autodial_status():
    return JSONResponse(_autodial_snapshot())


@app.get("/api/agent/autodial/events")
async def autodial_events():
    async def stream():
        q = asyncio.Queue()
        _autodial_listeners.add(q)
        try:
            for evt in _autodial_state["events"][-25:]:
                yield f"data: {json.dumps(evt)}\n\n"
            yield f"data: {json.dumps({'type': 'status', 'snapshot': _autodial_snapshot()})}\n\n"
            while True:
                try:
                    evt = await asyncio.wait_for(q.get(), timeout=30)
                    yield f"data: {json.dumps(evt)}\n\n"
                except asyncio.TimeoutError:
                    yield "data: {\"type\":\"ping\"}\n\n"
        finally:
            _autodial_listeners.discard(q)

    return StreamingResponse(
        stream(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "Connection": "keep-alive", "X-Accel-Buffering": "no"},
    )


@app.get("/api/agent/twiml")
@app.post("/api/agent/twiml")
async def agent_twiml(request: Request):
    """
    TwiML returned to Twilio when the outbound call connects.
    Opens a bidirectional Media Stream WebSocket to this server.
    """
    from xml.sax.saxutils import escape

    params = dict(request.query_params)
    # query_params are ALREADY decoded by Starlette — re-encode once for the URL
    phone    = urllib.parse.quote(params.get("phone", ""),    safe="")
    name     = urllib.parse.quote(params.get("name", ""),     safe="")
    city     = urllib.parse.quote(params.get("city", ""),     safe="")
    category = urllib.parse.quote(params.get("category", ""), safe="")
    timezone = urllib.parse.quote(params.get("timezone", ""), safe="")
    call_mode = urllib.parse.quote(params.get("call_mode", "outbound"), safe="")
    follow_up_id = urllib.parse.quote(params.get("follow_up_id", ""), safe="")
    lead_context = urllib.parse.quote(params.get("lead_context", ""), safe="")

    cfg = load_agent_config()
    base_url = cfg.base_url.rstrip("/")

    if not base_url:
        return Response(
            content='<?xml version="1.0" encoding="UTF-8"?><Response><Say>Agent base URL is not configured.</Say></Response>',
            media_type="text/xml",
        )

    ws_base = base_url.replace("https://", "wss://").replace("http://", "ws://")
    stream_url = (
        f"{ws_base}/ws/agent/stream"
        f"?phone={phone}&amp;name={name}&amp;city={city}&amp;category={category}"
        f"&amp;timezone={timezone}&amp;call_mode={call_mode}&amp;follow_up_id={follow_up_id}"
        f"&amp;lead_context={lead_context}"
    )

    def xml_esc(s: str) -> str:
        return (str(s or "").replace("&", "&amp;").replace("<", "&lt;")
                .replace(">", "&gt;").replace('"', "&quot;"))

    twiml = (
        '<?xml version="1.0" encoding="UTF-8"?>'
        '<Response>'
        '<Connect>'
        f'<Stream url="{stream_url}">'
        f'<Parameter name="phone" value="{xml_esc(params.get("phone",""))}" />'
        f'<Parameter name="name" value="{xml_esc(params.get("name",""))}" />'
        f'<Parameter name="city" value="{xml_esc(params.get("city",""))}" />'
        f'<Parameter name="category" value="{xml_esc(params.get("category",""))}" />'
        f'<Parameter name="timezone" value="{xml_esc(params.get("timezone",""))}" />'
        f'<Parameter name="call_mode" value="{xml_esc(params.get("call_mode","outbound"))}" />'
        f'<Parameter name="follow_up_id" value="{xml_esc(params.get("follow_up_id",""))}" />'
        f'<Parameter name="lead_context" value="{xml_esc(params.get("lead_context",""))}" />'
        '</Stream>'
        '</Connect>'
        '</Response>'
    )
    return Response(content=twiml, media_type="text/xml")

@app.post("/api/agent/call-status")
async def agent_call_status(request: Request):
    form        = await request.form()
    call_sid    = form.get("CallSid", "")
    call_status = form.get("CallStatus", "")
    print(f"[CALL STATUS] {call_sid} → {call_status}")

    if call_sid not in _agent_queues:
        _agent_queues[call_sid] = asyncio.Queue()

    if call_status in ("initiated", "ringing"):
        await _agent_queues[call_sid].put({
            "type": "status", "state": "connecting", "message": "Ringing…"
        })
    elif call_status in ("completed", "failed", "busy", "no-answer", "canceled"):
        await _agent_queues[call_sid].put({
            "type": "status", "state": "ended", "message": f"Call {call_status}"
        })
        if call_sid not in _agent_handlers:
            await _autodial_finish_call(call_sid, call_status=call_status)

    return Response(content="", media_type="text/plain")


@app.post("/api/agent/recording-status")
async def agent_recording_status(request: Request):
    form = await request.form()
    call_sid = form.get("CallSid", "")
    recording_sid = form.get("RecordingSid", "")
    recording_url = form.get("RecordingUrl", "")
    recording_status = form.get("RecordingStatus", "")
    print(f"[RECORDING STATUS] {call_sid} {recording_status} {recording_sid}")

    if recording_status and recording_status != "completed":
        return Response(content="", media_type="text/plain")

    handler = _agent_handlers.get(call_sid)
    lead_name = handler.lead_info.get("Name", "") if handler else ""
    if not lead_name:
        item = _autodial_state.get("active", {}).get(call_sid, {})
        lead_name = item.get("lead", {}).get("Name", "")
    if not lead_name:
        lead_name = _call_leads.get(call_sid, {}).get("Name", "")

    try:
        filename = await _download_twilio_recording(call_sid, recording_sid, recording_url, lead_name)
        if filename:
            if handler:
                handler.recording_file = filename
            from call_history import update_recording_file
            if not update_recording_file(call_sid, filename):
                _pending_recordings[call_sid] = filename
            follow_up_id = ""
            if handler:
                follow_up_id = handler.lead_info.get("follow_up_id", "")
            if not follow_up_id:
                follow_up_id = _call_leads.get(call_sid, {}).get("follow_up_id", "")
            if follow_up_id:
                try:
                    from followups import mark_call_recording
                    updated = mark_call_recording(call_sid, filename)
                    if updated:
                        await _followup_broadcast({"type": "follow_up_updated", "follow_up": updated, "message": "Follow-up recording saved"})
                except Exception as e:
                    print(f"[FOLLOWUP] recording link failed: {e}")
            await _autodial_broadcast({
                "type": "recording_ready",
                "call_sid": call_sid,
                "name": lead_name,
                "recording": filename,
                "message": "Twilio dual-channel recording saved",
            })
    except Exception as e:
        print(f"[RECORDING DOWNLOAD] failed for {call_sid}: {e}")

    return Response(content="", media_type="text/plain")


@app.post("/api/agent/end/{call_sid}")
async def agent_end_call(call_sid: str):
    """Frontend-triggered hangup."""
    if not TWILIO_ACCOUNT_SID:
        raise HTTPException(500, "Twilio credentials not set")
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            await client.post(
                f"https://api.twilio.com/2010-04-01/Accounts/{TWILIO_ACCOUNT_SID}/Calls/{call_sid}.json",
                auth=(TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN),
                data={"Status": "completed"},
            )
    except Exception as e:
        print(f"End call error: {e}")
    return JSONResponse({"success": True})


@app.websocket("/ws/agent/stream")
async def agent_stream(websocket: WebSocket):
    """
    Twilio connects here when the media stream starts.
    This is NOT the browser — it's Twilio's servers streaming call audio.
    """
    await websocket.accept()

    params = dict(websocket.query_params)
    lead_info = {
        "Name":     urllib.parse.unquote(params.get("name", "")),
        "Phone":    urllib.parse.unquote(params.get("phone", "")),
        "City":     urllib.parse.unquote(params.get("city", "")),
        "Category": urllib.parse.unquote(params.get("category", "")),
        "Timezone": urllib.parse.unquote(params.get("timezone", "")),
        "call_mode": urllib.parse.unquote(params.get("call_mode", "outbound")),
        "follow_up_id": urllib.parse.unquote(params.get("follow_up_id", "")),
    }
    raw_lead_context = urllib.parse.unquote(params.get("lead_context", ""))
    if raw_lead_context:
        try:
            lead_context = json.loads(raw_lead_context)
            if isinstance(lead_context, dict):
                lead_info["LeadContext"] = lead_context
        except Exception:
            pass
    if lead_info.get("call_mode") == "follow_up" and lead_info.get("follow_up_id"):
        try:
            from followups import get_follow_up
            lead_info["FollowUp"] = get_follow_up(lead_info["follow_up_id"])
        except Exception as e:
            print(f"[FOLLOWUP] context load failed: {e}")

    cfg          = _runtime_agent_config(lead_info.get("call_mode", "outbound") or "outbound")
    status_queue = asyncio.Queue()

    handler = GPTLiveCallHandler(
        twilio_ws    = websocket,
        cfg          = cfg,
        lead_info    = lead_info,
        status_queue = status_queue,
    )

    async def _register():
        while not handler.call_sid and not handler._stop:
            await asyncio.sleep(0.1)
        if handler.call_sid:
            _agent_handlers[handler.call_sid] = handler

    asyncio.create_task(_register())

    async def _relay_status():
        """Forward status/transcript events to the per-call SSE queue."""
        while True:
            try:
                event = await asyncio.wait_for(status_queue.get(), timeout=60)
                sid   = handler.call_sid or "unknown"

                # Buffer events
                if sid not in _agent_events:
                    _agent_events[sid] = []
                _agent_events[sid].append(event)

                # Push to SSE queue if registered
                q = _agent_queues.get(sid)
                if q:
                    await q.put(event)

                if event.get("type") in ("status", "transcript", "transcript_final"):
                    await _autodial_note_call_event(sid, event)

                if handler._stop:
                    break
            except asyncio.TimeoutError:
                if handler._stop:
                    break
            except Exception as e:
                print(f"Relay error: {e}")
                break

    try:
        await asyncio.gather(handler.run(), _relay_status())
    finally:
        try:
            from call_history import record_call
            snap = handler.metrics.snapshot()
            recording_file = _pending_recordings.pop(handler.call_sid, None) or getattr(handler, "recording_file", None)
            call_entry = {
                **_lead_time_fields(handler.lead_info, snap["cost"].get("ended_ts") or time.time()),
                "call_sid":   handler.call_sid,
                "business":   handler.lead_info.get("Name", ""),
                "phone":      handler.lead_info.get("Phone", ""),
                "city":       handler.lead_info.get("City", ""),
                "state":      handler.lead_info.get("State", ""),
                "category":   handler.lead_info.get("Category", ""),
                "call_mode":  handler.lead_info.get("call_mode", "outbound") or "outbound",
                "follow_up_id": handler.lead_info.get("follow_up_id", ""),
                "voice_engine": getattr(cfg, "voice_engine", "gpt_live"),
                "live_model": getattr(cfg, "live_model", "gpt-live-1"),
                "live_voice": getattr(cfg, "live_voice", "gleam"),
                "answered":   handler.metrics.turns > 0,
                "outcome":    handler.outcome,
                "turns":      snap["turns"],
                "interrupts": snap["interrupts"],
                "duration_s": snap["cost"]["duration_s"],
                "latency":    snap["latency"],
                "cost":       snap["cost"],
                "live_seconds": snap.get("live_seconds", 0.0),
                "live_usage_source": snap.get("live_usage_source", "none"),
                "recording": recording_file,
                "recording_source": "twilio_dual_channel" if recording_file and str(recording_file).endswith("_twilio.wav") else "local_stream",
                "followup_note": getattr(handler, "followup_note", ""),
                "end_reason": getattr(handler, "end_reason", ""),
            }
            record_call(call_entry)
            if call_entry.get("follow_up_id") and recording_file:
                try:
                    from followups import mark_call_recording
                    updated = mark_call_recording(handler.call_sid, recording_file)
                    if updated:
                        await _followup_broadcast({"type": "follow_up_updated", "follow_up": updated, "message": "Follow-up recording saved"})
                except Exception as e:
                    print(f"[FOLLOWUP] recording link failed: {e}")
            asyncio.create_task(_analyze_call_for_follow_up(call_entry))
            if handler.call_sid and (not recording_file or not str(recording_file).endswith("_twilio.wav")):
                asyncio.create_task(_recover_twilio_recording(
                    handler.call_sid,
                    handler.lead_info.get("Name", ""),
                ))
            asyncio.create_task(_store_twilio_actual_price(handler.call_sid))
            await _autodial_finish_call(
                handler.call_sid,
                outcome=handler.outcome,
                recording=recording_file or "",
                notes_extra=getattr(handler, "followup_note", ""),
            )
        except Exception as e:
            print(f"[HISTORY] failed: {e}")
        finally:
            if handler.call_sid:
                _agent_handlers.pop(handler.call_sid, None)


@app.get("/api/agent/events/{call_sid}")
async def agent_events(call_sid: str):
    """
    SSE stream — browser connects here to receive live status + transcript.
    Replays buffered events first, then streams new ones.
    """
    async def stream():
        # Replay buffered events
        for evt in _agent_events.get(call_sid, []):
            yield f"data: {json.dumps(evt)}\n\n"

        # Stream new events
        q = _agent_queues.get(call_sid)
        if not q:
            yield f"data: {json.dumps({'type':'status','state':'ended','message':'Call ended'})}\n\n"
            return

        while True:
            try:
                evt = await asyncio.wait_for(q.get(), timeout=30)
                print(f"[SSE OUT] {evt}")
                yield f"data: {json.dumps(evt)}\n\n"
                if evt.get("type") == "status" and evt.get("state") == "ended":
                    _agent_queues.pop(call_sid, None)
                    _agent_events.pop(call_sid, None)
                    break
            except asyncio.TimeoutError:
                yield "data: {\"type\":\"ping\"}\n\n"
            except Exception:
                break

    return StreamingResponse(
        stream(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )


@app.post("/api/agent/config/reset")
async def reset_agent_config():
    from agent_config import AgentConfig
    cfg = load_agent_config()
    fresh = AgentConfig(enabled=cfg.enabled, base_url=cfg.base_url)
    _save_agent_config(fresh)
    return JSONResponse({"success": True})

@app.websocket("/ws/agent/listen/{call_sid}")
async def agent_listen(websocket: WebSocket, call_sid: str):
    await websocket.accept()
    handler = _agent_handlers.get(call_sid)
    if not handler:
        await websocket.send_text(json.dumps({"error": "call not found"}))
        await websocket.close()
        return

    handler.listeners.add(websocket)
    print(f"[LISTEN] browser attached to {call_sid}")
    try:
        while True:
            await websocket.receive_text()   # keepalive
    except Exception:
        pass
    finally:
        handler.listeners.discard(websocket)
        print(f"[LISTEN] browser detached from {call_sid}")

@app.get("/api/agent/metrics/info")
async def agent_metrics_info():
    """Static reference card data + presets for the metrics panel."""
    from metrics import COMPONENT_INFO, PRESETS, RATES
    cfg = load_agent_config()
    model_key = getattr(cfg, "live_model", "gpt-live-1") or "gpt-live-1"
    model_info = COMPONENT_INFO["model"].get(model_key, COMPONENT_INFO["model"]["gpt-4o-mini"])
    transcriber_info = {
        "name": "Built into GPT-Live",
        "provider": "OpenAI",
        "typical_latency_ms": 0,
        "cost_per_min": 0.0,
        "metric_label": "Separate STT",
        "metric_value": "None",
    }
    voice_info = {
        "name": f"GPT-Live {getattr(cfg, 'live_voice', 'gleam')}",
        "provider": "OpenAI",
        "typical_latency_ms": 0,
        "cost_per_min": 0.0,
        "metric_label": "Separate TTS",
        "metric_value": "None",
    }

    est_per_min = (
        RATES["twilio_voice_us"] + RATES["twilio_media_stream"]
        + transcriber_info["cost_per_min"]
        + model_info["cost_per_min"]
        + voice_info["cost_per_min"]
    )
    est_latency = (
        transcriber_info["typical_latency_ms"]
        + model_info["typical_latency_ms"]
        + voice_info["typical_latency_ms"]
    )

    return JSONResponse({
        "engine": "gpt_live",
        "transcriber": transcriber_info,
        "model":       model_info,
        "voice":       voice_info,
        "estimated": {
            "cost_per_min": round(est_per_min, 4),
            "latency_ms":   est_latency,
            "endpointing_ms": 0,
        },
        "presets": {k: v["label"] for k, v in PRESETS.items()},
        "rates": RATES,
    })


@app.post("/api/agent/preset/{preset_key}")
async def agent_apply_preset(preset_key: str):
    from agent_config import apply_preset
    cfg = apply_preset(load_agent_config(), preset_key)
    _save_agent_config(cfg)
    return JSONResponse({"success": True, "config": cfg.model_dump()})        

@app.get("/api/agent/call-price/{call_sid}")
async def agent_call_price(call_sid: str):
    """
    Fetch the real Twilio price for a completed call.
    Twilio populates `price` a few seconds after the call ends, so this may
    return null on the first attempt — the frontend retries.
    """
    if not TWILIO_ACCOUNT_SID:
        raise HTTPException(500, "Twilio credentials not set")
    try:
        price = await _fetch_twilio_call_price(call_sid)
        return JSONResponse({
            "price": price,
            "unit": "USD",
        })
    except Exception as e:
        return JSONResponse({"price": None, "error": str(e)})

@app.get("/api/analytics")
async def get_analytics(days: int = 30):
    from call_history import summarise
    return JSONResponse(summarise(days))


@app.get("/api/analytics/counts")
async def get_analytics_counts(days: int = 30):
    return JSONResponse(_analytics_data_snapshot(days))


@app.post("/api/analytics/calls/outcomes")
async def analytics_update_call_outcomes(request: Request):
    payload = await request.json()
    result = _apply_call_outcome_updates(payload, updated_by="operator", require_evidence=False)
    status = 200 if result.get("updated", 0) else 400
    return JSONResponse(result, status_code=status)


@app.post("/api/analytics/transcribe")
async def analytics_transcribe(request: Request):
    data = await request.json()
    recording = data.get("recording", "")
    was_cached = _transcript_cache_path(recording).exists()
    result = await _transcribe_recording(recording)
    result["cached"] = was_cached
    return JSONResponse(result)


@app.post("/api/analytics/ask")
async def analytics_ask(request: Request):
    data = await request.json()
    recording = data.get("recording", "")
    question = data.get("question", "")
    result = await _ask_transcript_agent(recording, question)
    clean_answer, actions = _parse_analyst_action_blocks(result.get("answer", ""))
    actions = _guard_outcome_update_actions(actions, question)
    action_results = await _execute_analyst_actions(actions, source_call=result.get("call") or None)
    result["answer"] = (clean_answer or result.get("answer", "") or "Action processed.") + _analyst_action_result_summary(action_results)
    if action_results:
        result["actions"] = action_results
    return JSONResponse(result)


@app.get("/api/follow-ups")
async def followups_list(status: str = ""):
    from followups import list_follow_ups
    items = list_follow_ups(status=status)
    return JSONResponse({"follow_ups": items, "counts": _followup_counts(items)})


@app.get("/api/follow-ups/events")
async def followups_events():
    async def stream():
        q = asyncio.Queue()
        _followup_listeners.add(q)
        try:
            from followups import list_follow_ups
            items = list_follow_ups()
            yield f"data: {json.dumps({'type': 'snapshot', 'counts': _followup_counts(items), 'follow_ups': items})}\n\n"
            while True:
                try:
                    evt = await asyncio.wait_for(q.get(), timeout=30)
                    yield f"data: {json.dumps(evt)}\n\n"
                except asyncio.TimeoutError:
                    yield "data: {\"type\":\"ping\"}\n\n"
        finally:
            _followup_listeners.discard(q)

    return StreamingResponse(
        stream(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "Connection": "keep-alive", "X-Accel-Buffering": "no"},
    )


@app.post("/api/follow-ups/apply-json")
async def followups_apply_json(request: Request):
    from followups import update_follow_up
    payload = await request.json()
    data = _extract_follow_up_update_json(payload.get("json") or payload.get("text") or payload.get("payload") or payload)
    if not data:
        raise HTTPException(400, "valid follow-up JSON required")
    selected_id = str(payload.get("follow_up_id") or payload.get("selected_id") or "").strip()
    if selected_id and not data.get("id") and not data.get("follow_up_id"):
        data["id"] = selected_id

    follow_up_id = _match_follow_up_for_update(data)
    if not follow_up_id:
        raise HTTPException(400, "follow-up id required, or include matching phone, email, or business")

    updates = {k: v for k, v in data.items() if k not in {"id", "follow_up_id"}}
    if not updates:
        raise HTTPException(400, "no update fields found")
    item = update_follow_up(follow_up_id, updates)
    if not item:
        raise HTTPException(404, "follow-up not found")
    if str(item.get("status") or "").lower() == "cancelled":
        await _followup_broadcast({"type": "follow_up_deleted", "follow_up_id": follow_up_id, "follow_up": item, "message": "Follow-up deleted"})
        return JSONResponse({"success": True, "deleted": True, "follow_up_id": follow_up_id})
    await _followup_broadcast({"type": "follow_up_updated", "follow_up": item, "message": "Follow-up JSON applied"})
    return JSONResponse({"success": True, "follow_up": item})


@app.get("/api/follow-ups/{follow_up_id}")
async def followups_get(follow_up_id: str):
    from followups import get_follow_up
    item = get_follow_up(follow_up_id)
    if not item:
        raise HTTPException(404, "follow-up not found")
    previous_call = _find_call(item.get("previous_call_id") or "")
    latest_call = _find_call(item.get("call_id") or "")
    return JSONResponse({"follow_up": item, "previous_call": previous_call, "latest_call": latest_call})


@app.patch("/api/follow-ups/{follow_up_id}")
async def followups_patch(follow_up_id: str, request: Request):
    from followups import update_follow_up
    payload = await request.json()
    item = update_follow_up(follow_up_id, payload)
    if not item:
        raise HTTPException(404, "follow-up not found")
    await _followup_broadcast({"type": "follow_up_updated", "follow_up": item, "message": "Follow-up updated"})
    return JSONResponse({"success": True, "follow_up": item})


@app.post("/api/follow-ups/{follow_up_id}/cancel")
async def followups_cancel(follow_up_id: str):
    from followups import delete_follow_up
    item = delete_follow_up(follow_up_id)
    if not item:
        raise HTTPException(404, "follow-up not found")
    await _followup_broadcast({"type": "follow_up_deleted", "follow_up_id": follow_up_id, "follow_up": item, "message": "Follow-up deleted"})
    return JSONResponse({"success": True, "deleted": True, "follow_up_id": follow_up_id})


@app.post("/api/follow-ups/{follow_up_id}/call")
async def followups_call(follow_up_id: str, request: Request):
    from followups import get_follow_up, mark_call_started, update_follow_up
    item = get_follow_up(follow_up_id)
    if not item:
        raise HTTPException(404, "follow-up not found")
    if item.get("status") == "calling" and item.get("call_id"):
        return JSONResponse({"success": True, "already_calling": True, "follow_up": item, "call_sid": item.get("call_id")})
    if item.get("status") == "cancelled":
        raise HTTPException(400, f"follow-up is {item.get('status')}")

    payload = await request.json()
    cfg = load_agent_config()
    base_url = (payload.get("base_url") or cfg.base_url or "").rstrip("/")
    phone = item.get("phone") or payload.get("phone") or ""
    if not phone:
        raise HTTPException(400, "follow-up phone required")
    if not base_url:
        raise HTTPException(400, "base_url required")

    lead = {
        "Name": item.get("business") or "",
        "Phone": phone,
        "City": "",
        "Category": "service business",
        "Timezone": item.get("lead_timezone") or item.get("lead_timezone_iana") or "",
        "call_mode": "follow_up",
        "follow_up_id": follow_up_id,
    }
    try:
        call_sid = await _start_agent_call(phone, lead, base_url, call_mode="follow_up", follow_up_id=follow_up_id)
    except HTTPException:
        raise
    except Exception as e:
        failed = update_follow_up(follow_up_id, {"status": "failed", "result": str(e)})
        if failed:
            await _followup_broadcast({"type": "follow_up_updated", "follow_up": failed, "message": "Follow-up call failed to start"})
        raise

    updated = mark_call_started(follow_up_id, call_sid)
    await _followup_broadcast({"type": "follow_up_call_started", "follow_up": updated, "call_sid": call_sid, "message": "Follow-up call started"})
    runtime_cfg = _runtime_agent_config("follow_up")
    return JSONResponse({
        "success": True,
        "call_sid": call_sid,
        "follow_up": updated,
        "voice_engine": getattr(runtime_cfg, "voice_engine", "gpt_live"),
        "live_model": getattr(runtime_cfg, "live_model", "gpt-live-1"),
        "live_voice": getattr(runtime_cfg, "live_voice", "gleam"),
    })


@app.post("/api/follow-ups/{follow_up_id}/test-call")
async def followups_test_call(follow_up_id: str, request: Request):
    raise HTTPException(400, "Follow-Up Agent is GPT-Live only; legacy simulated follow-up calls are disabled")
    from followups import get_follow_up, mark_call_started
    item = get_follow_up(follow_up_id)
    if not item:
        raise HTTPException(404, "follow-up not found")
    payload = await request.json()
    scenario = str(payload.get("scenario") or load_agent_config().followup_test_scenario or "remembers_context")
    call_sid = f"SIMFU{uuid.uuid4().hex[:22]}"
    _agent_queues[call_sid] = asyncio.Queue()
    _agent_events[call_sid] = []
    lead = {
        "Name": item.get("business") or "Follow-Up Test",
        "Phone": item.get("phone") or "+15550000000",
        "City": "Test City",
        "State": "IN",
        "Category": "service business",
        "Timezone": "Eastern",
        "Status": "Queued",
        "call_mode": "follow_up",
        "follow_up_id": follow_up_id,
        "FollowUp": item,
    }
    _call_leads[call_sid] = {**lead, "test_mode": True}
    updated = mark_call_started(follow_up_id, call_sid)
    await _followup_broadcast({"type": "follow_up_call_started", "follow_up": updated, "call_sid": call_sid, "message": "Test follow-up started"})
    asyncio.create_task(_run_simulated_agent_call(call_sid, lead, lead["Phone"], scenario, endpoint=""))
    runtime_cfg = _runtime_agent_config("follow_up")
    return JSONResponse({
        "success": True,
        "test_mode": True,
        "call_sid": call_sid,
        "follow_up": updated,
        "voice_engine": getattr(runtime_cfg, "voice_engine", "gpt_live"),
    })


@app.post("/api/follow-ups/analyze-call")
async def followups_analyze_call(request: Request):
    payload = await request.json()
    call = _find_call(str(payload.get("call_sid") or payload.get("recording") or ""))
    if not call:
        raise HTTPException(404, "call not found")
    await _analyze_call_for_follow_up(call)
    from followups import find_by_previous_call_id
    item = find_by_previous_call_id(call.get("call_sid") or call.get("recording") or "")
    return JSONResponse({"success": True, "follow_up": item})


@app.get("/api/email/status")
async def email_status(request: Request):
    token = _load_email_token()
    return JSONResponse({
        "configured": _email_oauth_configured(),
        "authorized": bool(token.get("refresh_token") or token.get("access_token")),
        "from_email": PUBLIC_CONTACT_EMAIL,
        "scope": _GMAIL_SEND_SCOPE,
        "redirect_uri": _email_redirect_uri(request),
    })


@app.get("/api/email/auth-url")
async def email_auth_url(request: Request):
    if not _email_oauth_configured():
        raise HTTPException(500, "LynkFlow_Console_ID and LynkFlow_Console_Secret are not set")

    now = time.time()
    for state, item in list(_EMAIL_OAUTH_STATES.items()):
        if float(item.get("expires_at") or 0) < now:
            _EMAIL_OAUTH_STATES.pop(state, None)

    state = secrets.token_urlsafe(24)
    redirect_uri = _email_redirect_uri(request)
    _EMAIL_OAUTH_STATES[state] = {"expires_at": now + 600, "redirect_uri": redirect_uri}
    params = {
        "client_id": LYNKFLOW_CONSOLE_ID,
        "redirect_uri": redirect_uri,
        "response_type": "code",
        "scope": _GMAIL_SEND_SCOPE,
        "access_type": "offline",
        "prompt": "consent",
        "include_granted_scopes": "true",
        "state": state,
    }
    return JSONResponse({
        "auth_url": "https://accounts.google.com/o/oauth2/v2/auth?" + urllib.parse.urlencode(params),
        "redirect_uri": redirect_uri,
    })


@app.get("/api/email/oauth/callback")
async def email_oauth_callback(request: Request):
    if not _email_oauth_configured():
        raise HTTPException(500, "Email OAuth is not configured")
    params = request.query_params
    if params.get("error"):
        return Response(
            content=f"<h2>Gmail connection failed</h2><p>{params.get('error')}</p>",
            media_type="text/html",
        )
    code = params.get("code")
    state = params.get("state")
    state_item = _EMAIL_OAUTH_STATES.pop(state or "", None)
    if not code or not state_item:
        raise HTTPException(400, "invalid OAuth callback")
    redirect_uri = state_item.get("redirect_uri") or _email_redirect_uri(request)

    async with httpx.AsyncClient(timeout=20) as client:
        resp = await client.post(
            "https://oauth2.googleapis.com/token",
            data={
                "client_id": LYNKFLOW_CONSOLE_ID,
                "client_secret": LYNKFLOW_CONSOLE_SECRET,
                "code": code,
                "grant_type": "authorization_code",
                "redirect_uri": redirect_uri,
            },
        )
    if resp.status_code != 200:
        raise HTTPException(502, f"OAuth token exchange failed: {resp.text}")

    token = resp.json()
    existing = _load_email_token()
    if not token.get("refresh_token") and existing.get("refresh_token"):
        token["refresh_token"] = existing["refresh_token"]
    token["expires_at"] = time.time() + int(token.get("expires_in") or 3600)
    token["connected_at"] = datetime.now().isoformat(timespec="seconds")
    _save_email_token(token)
    return Response(
        content=(
            "<h2>Gmail connected</h2>"
            "<p>You can close this tab and return to Lynkflow.</p>"
            "<script>setTimeout(function(){ window.close(); }, 1200);</script>"
        ),
        media_type="text/html",
    )


@app.post("/api/email/send")
async def email_send(request: Request):
    data = await request.json()
    try:
        result = await _send_gmail_message(
            to_email=data.get("to", ""),
            subject=data.get("subject", ""),
            html=data.get("html", ""),
            text=data.get("text", ""),
        )
    except HTTPException as e:
        if e.status_code >= 500:
            try:
                from followups import note_email_failed
                changed = note_email_failed(data.get("to", ""), data.get("subject", ""), str(e.detail))
                for item in changed:
                    await _followup_broadcast({"type": "follow_up_updated", "follow_up": item, "message": "Verified email send failure recorded"})
            except Exception as log_err:
                print(f"[EMAIL] follow-up email failure update failed: {log_err}")
        raise
    _log_email_sent(data.get("to", ""), data.get("subject", ""), result)
    try:
        from followups import note_email_sent
        changed = note_email_sent(data.get("to", ""), data.get("subject", ""))
        for item in changed:
            await _followup_broadcast({"type": "follow_up_updated", "follow_up": item, "message": "Follow-up email marked sent"})
    except Exception as e:
        print(f"[EMAIL] follow-up email status update failed: {e}")
    return JSONResponse(result)


@app.get("/api/analyst/chats")
async def analyst_chats():
    data = _load_analyst_chats()
    chats = [
        {k: chat.get(k) for k in ("id", "title", "created_at", "updated_at")}
        | {"message_count": len(chat.get("messages", [])), "attachment_count": len(chat.get("attachments", []))}
        for chat in sorted(data.get("chats", []), key=lambda c: c.get("updated_at", ""), reverse=True)
    ]
    return JSONResponse({"chats": chats})


@app.get("/api/analyst/recordings")
async def analyst_recordings(days: int = 365, q: str = ""):
    return JSONResponse({"recordings": _recording_catalog(days=days, q=q)})


@app.post("/api/analyst/chats")
async def analyst_create_chat(request: Request):
    payload = await request.json()
    data = _load_analyst_chats()
    chat = _new_analyst_chat(payload.get("title") or "New chat")
    data.setdefault("chats", []).append(chat)
    _save_analyst_chats(data)
    return JSONResponse(chat)


@app.get("/api/analyst/chats/{chat_id}")
async def analyst_get_chat(chat_id: str):
    data = _load_analyst_chats()
    return JSONResponse(_find_analyst_chat(data, chat_id))


@app.patch("/api/analyst/chats/{chat_id}")
async def analyst_update_chat(chat_id: str, request: Request):
    payload = await request.json()
    data = _load_analyst_chats()
    chat = _find_analyst_chat(data, chat_id)
    title = str(payload.get("title") or "").strip()
    if title:
        chat["title"] = title[:80]
        chat["updated_at"] = datetime.now().isoformat(timespec="seconds")
    if "attachments" in payload and isinstance(payload["attachments"], list):
        clean = []
        seen = set()
        for a in payload["attachments"][:30]:
            rec = Path(str(a.get("recording", ""))).name
            if not rec or rec in seen:
                continue
            seen.add(rec)
            clean.append({
                "recording": rec,
                "business": str(a.get("business", ""))[:120],
                "phone": str(a.get("phone", ""))[:40],
                "outcome": str(a.get("outcome", ""))[:40],
                "duration_s": a.get("duration_s", 0),
                "date": str(a.get("date", ""))[:40],
            })
        chat["attachments"] = clean
        chat["updated_at"] = datetime.now().isoformat(timespec="seconds")
    _save_analyst_chats(data)
    return JSONResponse(chat)


@app.post("/api/analyst/chats/{chat_id}/attachments")
async def analyst_add_attachment(chat_id: str, request: Request):
    payload = await request.json()
    data = _load_analyst_chats()
    chat = _find_analyst_chat(data, chat_id)
    existing = chat.setdefault("attachments", [])
    existing_recs = {a.get("recording") for a in existing}
    attachments = payload.get("attachments") if isinstance(payload.get("attachments"), list) else [payload]
    for a in attachments:
        rec = Path(str(a.get("recording", ""))).name
        if not rec or rec in existing_recs:
            continue
        _safe_recording_path(rec)
        existing.append({
            "recording": rec,
            "business": str(a.get("business", ""))[:120],
            "phone": str(a.get("phone", ""))[:40],
            "outcome": str(a.get("outcome", ""))[:40],
            "duration_s": a.get("duration_s", 0),
            "date": str(a.get("date", ""))[:40],
        })
        existing_recs.add(rec)
    chat["attachments"] = existing[:30]
    chat["updated_at"] = datetime.now().isoformat(timespec="seconds")
    _save_analyst_chats(data)
    return JSONResponse(chat)


@app.delete("/api/analyst/chats/{chat_id}/attachments/{recording}")
async def analyst_remove_attachment(chat_id: str, recording: str):
    data = _load_analyst_chats()
    chat = _find_analyst_chat(data, chat_id)
    rec = Path(urllib.parse.unquote(recording)).name
    chat["attachments"] = [a for a in chat.get("attachments", []) if a.get("recording") != rec]
    chat["updated_at"] = datetime.now().isoformat(timespec="seconds")
    _save_analyst_chats(data)
    return JSONResponse(chat)


@app.delete("/api/analyst/chats/{chat_id}")
async def analyst_delete_chat(chat_id: str):
    data = _load_analyst_chats()
    before = len(data.get("chats", []))
    data["chats"] = [c for c in data.get("chats", []) if c.get("id") != chat_id]
    if len(data["chats"]) == before:
        raise HTTPException(404, "chat not found")
    _save_analyst_chats(data)
    return JSONResponse({"success": True})


def _analyst_action_result_summary(results: list[dict]) -> str:
    if not results:
        return ""
    lines = ["", "**Action Results**"]
    for result in results:
        action_type = result.get("type") or "action"
        if action_type == _ANALYST_ACTION_UPDATE_CALL_OUTCOMES:
            lines.append(f"- UPDATE_CALL_OUTCOMES: {result.get('updated', 0)} updated, {result.get('failed', 0)} failed.")
            for item in (result.get("results") or [])[:12]:
                business = item.get("business") or item.get("call_id") or item.get("call_sid") or "call"
                if item.get("executed"):
                    lines.append(f"- {business}: {item.get('previous_outcome') or 'unknown'} -> {item.get('outcome')}")
                else:
                    lines.append(f"- {business}: failed, {item.get('error') or 'unknown error'}")
            extra = len(result.get("results") or []) - 12
            if extra > 0:
                lines.append(f"- {extra} more result(s) not shown.")
        elif result.get("executed"):
            lines.append(f"- {action_type}: executed.")
        else:
            lines.append(f"- {action_type}: failed, {result.get('error') or 'unknown error'}")
    return "\n".join(lines)


@app.post("/api/analyst/chats/{chat_id}/message")
async def analyst_message(chat_id: str, request: Request):
    payload = await request.json()
    question = str(payload.get("message") or "").strip()
    if not question:
        raise HTTPException(400, "message required")
    days = _normalise_analytics_days(payload.get("days") or 30)
    data = _load_analyst_chats()
    chat = _find_analyst_chat(data, chat_id)
    now = datetime.now().isoformat(timespec="seconds")
    chat.setdefault("messages", []).append({"role": "user", "content": question, "ts": now})
    result = await _ask_global_analyst(chat, question, days)
    clean_answer, actions = _parse_analyst_action_blocks(result["answer"])
    actions = _guard_outcome_update_actions(actions, question)
    action_results = await _execute_analyst_actions(actions)
    context = {**result["context"]}
    if action_results:
        context["actions"] = action_results
    display_answer = clean_answer or ("Action processed." if actions else result["answer"])
    display_answer = (display_answer + _analyst_action_result_summary(action_results)).strip()
    chat["messages"].append({"role": "assistant", "content": display_answer, "ts": datetime.now().isoformat(timespec="seconds"), "context": context})
    if chat.get("title") == "New chat":
        chat["title"] = question[:60]
    chat["updated_at"] = datetime.now().isoformat(timespec="seconds")
    _save_analyst_chats(data)
    return JSONResponse({"chat": chat, "answer": display_answer, "context": context})

@app.get("/api/recordings")
async def list_recordings():
    files = sorted(_REC_DIR.glob("*.wav"), key=lambda p: p.stat().st_mtime, reverse=True)
    return JSONResponse([{
        "name": f.name,
        "url":  f"/recordings/{f.name}",
        "size_kb": round(f.stat().st_size / 1024),
        "mtime": f.stat().st_mtime,
    } for f in files[:200]])
