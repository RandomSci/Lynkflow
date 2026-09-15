"""
Persistent call history for the analytics page.
Stored as newline-delimited JSON at call_history.jsonl next to this file.
"""

import json
import time
from datetime import datetime, timedelta
from pathlib import Path
from statistics import median

_HISTORY_FILE = Path(__file__).parent / "call_history.jsonl"
_MAX_RECORDS = 5000


def record_call(entry: dict) -> None:
    """Append one finished call. Trims the file when it grows too large."""
    entry.setdefault("ts", time.time())
    entry.setdefault("date", datetime.now().isoformat(timespec="seconds"))
    try:
        with _HISTORY_FILE.open("a") as f:
            f.write(json.dumps(entry) + "\n")
    except Exception as e:
        print(f"[HISTORY] write failed: {e}")
        return

    # Trim occasionally
    try:
        lines = _HISTORY_FILE.read_text().splitlines()
        if len(lines) > _MAX_RECORDS:
            _HISTORY_FILE.write_text("\n".join(lines[-_MAX_RECORDS:]) + "\n")
    except Exception:
        pass


def update_twilio_price(call_sid: str, price: float) -> bool:
    """Patch a stored call with the actual Twilio price once Twilio exposes it."""
    if not call_sid or price is None or not _HISTORY_FILE.exists():
        return False
    try:
        lines = _HISTORY_FILE.read_text().splitlines()
        changed = False
        out = []
        for line in lines:
            if not line.strip():
                continue
            try:
                entry = json.loads(line)
            except Exception:
                out.append(line)
                continue
            if entry.get("call_sid") == call_sid:
                cost = entry.setdefault("cost", {})
                old = float(cost.get("twilio") or 0.0)
                delta = float(price) - old
                cost["twilio"] = round(float(price), 5)
                cost["twilio_actual"] = True
                cost["total"] = round(float(cost.get("total") or 0.0) + delta, 5)
                mins = float(cost.get("duration_min") or 0.0)
                if mins > 0:
                    cost["per_min"] = round(cost["total"] / mins, 4)
                changed = True
            out.append(json.dumps(entry))
        if changed:
            _HISTORY_FILE.write_text("\n".join(out) + "\n")
        return changed
    except Exception as e:
        print(f"[HISTORY] twilio price update failed: {e}")
        return False


def update_recording_file(call_sid: str, recording: str) -> bool:
    """Patch a stored call with the authoritative recording file."""
    if not call_sid or not recording or not _HISTORY_FILE.exists():
        return False
    try:
        lines = _HISTORY_FILE.read_text().splitlines()
        changed = False
        out = []
        for line in lines:
            if not line.strip():
                continue
            try:
                entry = json.loads(line)
            except Exception:
                out.append(line)
                continue
            if entry.get("call_sid") == call_sid:
                entry["recording"] = recording
                entry["recording_source"] = "twilio_dual_channel"
                changed = True
            out.append(json.dumps(entry))
        if changed:
            _HISTORY_FILE.write_text("\n".join(out) + "\n")
        return changed
    except Exception as e:
        print(f"[HISTORY] recording update failed: {e}")
        return False


def load_calls(days: int = 30) -> list:
    if not _HISTORY_FILE.exists():
        return []
    cutoff = time.time() - days * 86400
    out = []
    try:
        for line in _HISTORY_FILE.read_text().splitlines():
            if not line.strip():
                continue
            try:
                d = json.loads(line)
                if d.get("ts", 0) >= cutoff:
                    out.append(d)
            except Exception:
                continue
    except Exception as e:
        print(f"[HISTORY] read failed: {e}")
    return out


def summarise(days: int = 30) -> dict:
    calls = load_calls(days)
    if not calls:
        return {
            "empty": True, "days": days,
            "totals": {"calls": 0, "connected": 0, "conversations": 0,
                       "ivr": 0, "interested": 0, "minutes": 0.0, "cost": 0.0},
            "rates": {"connect": 0, "conversation": 0, "interest": 0},
            "cost": {"twilio": 0, "stt": 0, "llm": 0, "tts": 0,
                     "total": 0, "per_call": 0, "per_min": 0,
                     "per_conversation": 0, "per_interested": 0},
            "latency": {"stt": 0, "llm": 0, "tts": 0, "total": 0, "p95": 0},
            "daily": [], "outcomes": {}, "recent": [],
        }

    total     = len(calls)
    connected = sum(1 for c in calls if c.get("answered"))
    ivr       = sum(1 for c in calls if c.get("outcome") == "ivr")
    convos    = sum(1 for c in calls if c.get("turns", 0) >= 2 and c.get("outcome") != "ivr")
    interested = sum(1 for c in calls if c.get("outcome") == "interested")

    minutes = sum(c.get("duration_s", 0) for c in calls) / 60.0

    def csum(k): return sum(c.get("cost", {}).get(k, 0) for c in calls)
    c_twilio, c_stt = csum("twilio"), csum("stt")
    c_llm, c_tts    = csum("llm"), csum("tts")
    c_gpt_live = csum("gpt_live")
    c_total = c_twilio + c_stt + c_llm + c_tts + c_gpt_live

    def lat(k):
        vals = [c["latency"][k] for c in calls
                if c.get("latency", {}).get(k)]
        return round(median(vals)) if vals else 0

    p95_vals = sorted(c["latency"]["total_p95_ms"] for c in calls
                      if c.get("latency", {}).get("total_p95_ms"))
    p95 = p95_vals[min(len(p95_vals) - 1, int(len(p95_vals) * 0.95))] if p95_vals else 0

    # Per-day rollup
    buckets = {}
    for c in calls:
        day = datetime.fromtimestamp(c.get("ts", 0)).strftime("%Y-%m-%d")
        b = buckets.setdefault(day, {"date": day, "calls": 0, "connected": 0,
                                     "conversations": 0, "cost": 0.0, "minutes": 0.0})
        b["calls"] += 1
        if c.get("answered"): b["connected"] += 1
        if c.get("turns", 0) >= 2 and c.get("outcome") != "ivr": b["conversations"] += 1
        b["cost"]    += sum(c.get("cost", {}).get(k, 0) for k in ("twilio","stt","llm","tts","gpt_live"))
        b["minutes"] += c.get("duration_s", 0) / 60.0
    daily = sorted(buckets.values(), key=lambda x: x["date"])
    for d in daily:
        d["cost"]    = round(d["cost"], 4)
        d["minutes"] = round(d["minutes"], 1)

    outcomes = {}
    for c in calls:
        o = c.get("outcome") or "unknown"
        outcomes[o] = outcomes.get(o, 0) + 1

    recent = sorted(calls, key=lambda c: c.get("ts", 0), reverse=True)[:25]

    def safe_div(a, b): return round(a / b, 4) if b else 0.0

    return {
        "empty": False,
        "days": days,
        "totals": {
            "calls": total, "connected": connected, "conversations": convos,
            "ivr": ivr, "interested": interested,
            "minutes": round(minutes, 1), "cost": round(c_total, 4),
        },
        "rates": {
            "connect":      round(connected / total * 100, 1),
            "conversation": round(convos / total * 100, 1),
            "interest":     round(interested / total * 100, 1),
        },
        "cost": {
            "twilio": round(c_twilio, 4), "stt": round(c_stt, 4),
            "llm": round(c_llm, 4),       "tts": round(c_tts, 4),
            "gpt_live": round(c_gpt_live, 4),
            "total": round(c_total, 4),
            "per_call":         safe_div(c_total, total),
            "per_min":          safe_div(c_total, minutes),
            "per_conversation": safe_div(c_total, convos),
            "per_interested":   safe_div(c_total, interested),
        },
        "latency": {
            "stt": lat("stt_ms"), "llm": lat("llm_ms"),
            "tts": lat("tts_ms"), "total": lat("total_ms"), "p95": p95,
        },
        "daily": daily,
        "outcomes": outcomes,
        "recent": recent,
    }
