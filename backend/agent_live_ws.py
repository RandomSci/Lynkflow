"""
GPT-Live Twilio Media Streams handler.

Production path:
  Twilio (mulaw 8kHz) <-> GPT-Live (pcmu 8kHz)

This keeps code responsible for telephony/recording/events while GPT-Live owns
turn-taking, interruptions, speech, and conversation behavior.
"""

import asyncio
import base64
import json
import os
import re
import time
from datetime import datetime
from pathlib import Path
from typing import Optional
import audioop
import wave

import httpx
import websockets
from dotenv import load_dotenv
from fastapi import WebSocket, WebSocketDisconnect
from metrics import CallMetrics

load_dotenv()

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
TWILIO_ACCOUNT_SID = os.getenv("TWILIO_ACCOUNT_SID")
TWILIO_AUTH_TOKEN = os.getenv("TWILIO_AUTH_TOKEN")


TONE_NOTES = {
    "professional": "Be calm, concise, and businesslike.",
    "friendly": "Be warm and conversational without sounding fake.",
    "direct": "Be brief and straight to the point.",
}


def _clean_filename(s: str) -> str:
    text = re.sub(r"[^A-Za-z0-9_. -]+", "", str(s or "")).strip()
    return re.sub(r"\s+", " ", text)[:42] or "GPT Live Call"


class GPTLiveCallHandler:
    def __init__(self, twilio_ws: WebSocket, cfg, lead_info: dict, status_queue: asyncio.Queue):
        self.twilio_ws = twilio_ws
        self.cfg = cfg
        self.lead_info = lead_info
        self.status_queue = status_queue
        self.listeners: set = set()

        self.stream_sid: Optional[str] = None
        self.call_sid: Optional[str] = None
        self.live_ws = None
        self.live_session_id = ""
        self.live_started = False
        self._pending_audio: list[str] = []
        self._stop = False
        self._cleaned = False

        self.metrics = CallMetrics(model="gpt-live-1")
        self.outcome = "no_answer"
        self.followup_note = ""
        self.recording_file = None

        self._rec_agent = bytearray()
        self._rec_prospect = bytearray()
        self._fanout_buf: list = []
        self._fanout_inflight = 0
        self._fanout_flush_task = None

        self._input_text = ""
        self._output_text = ""
        self._last_speaker = ""
        self._agent_turn_open = False
        self._last_status = ""
        self._live_usage_seconds = 0.0
        self._live_usage_source = "local_duration_fallback"
        self.end_phrases = [
            p.strip().lower()
            for p in (getattr(cfg, "end_call_phrases", "") or "").split(",")
            if p.strip()
        ]

    async def run(self):
        await self._push_status("connecting", "Connecting GPT-Live...")
        if not OPENAI_API_KEY:
            await self._push_status("ended", "OPENAI_API_KEY not set")
            self._stop = True
            return

        try:
            await self._connect_live()
            await asyncio.gather(self._twilio_loop(), self._live_loop())
        finally:
            await self._cleanup()

    def _instructions(self) -> str:
        business = self.lead_info.get("Name") or "the business"
        city = self.lead_info.get("City") or ""
        category = self.lead_info.get("Category") or "service business"
        timezone = self.lead_info.get("Timezone") or ""
        context = f"Lead context: {business}, a {category}"
        if city:
            context += f" in {city}"
        if timezone:
            context += f". Timezone: {timezone}"
        tone = TONE_NOTES.get(getattr(self.cfg, "tone", "professional"), TONE_NOTES["professional"])
        return (
            f"{getattr(self.cfg, 'system_prompt', '')}\n\n"
            f"{context}.\n"
            f"Tone: {tone}\n\n"
            "Live voice behavior:\n"
            "- You are on a phone call. Keep replies short and natural.\n"
            "- Listen while the other person speaks; stop when interrupted.\n"
            "- Answer the exact question first, then continue naturally.\n"
            "- Do not repeat a previous line or restart the call.\n"
            "- Do not pitch until a decision maker has allowed the 30-second pitch.\n"
            "- If they decline, want to end, or ask to be removed, politely end.\n"
            "- Never speak bracketed control tokens aloud."
        )

    async def _connect_live(self):
        headers = {"Authorization": f"Bearer {OPENAI_API_KEY}"}
        try:
            self.live_ws = await websockets.connect(
                "wss://api.openai.com/v1/live/sessions",
                additional_headers=headers,
                ping_interval=20,
            )
        except TypeError:
            self.live_ws = await websockets.connect(
                "wss://api.openai.com/v1/live/sessions",
                extra_headers=headers,
                ping_interval=20,
            )

        await self.live_ws.send(json.dumps({
            "type": "session.start",
            "event_id": "start_gpt_live",
            "session": {
                "model": getattr(self.cfg, "live_model", "gpt-live-1") or "gpt-live-1",
                "instructions": self._instructions(),
                "audio": {
                    "format": {"type": "audio/pcmu", "rate": 8000},
                    "output": {"voice": getattr(self.cfg, "live_voice", "gleam") or "gleam"},
                },
                "store": False,
            },
        }))

    async def _twilio_loop(self):
        try:
            async for raw in self.twilio_ws.iter_text():
                if self._stop:
                    break
                data = json.loads(raw)
                await self._on_twilio_event(data)
        except WebSocketDisconnect:
            pass
        finally:
            self._stop = True
            await self._close_live()

    async def _live_loop(self):
        try:
            async for raw in self.live_ws:
                if self._stop:
                    break
                try:
                    event = json.loads(raw)
                except Exception:
                    continue
                await self._on_live_event(event)
        except Exception as e:
            if not self._stop:
                print(f"[GPT-LIVE] loop error: {e}")
                await self._push_status("ended", f"GPT-Live error: {e}")
        finally:
            self._stop = True

    async def _on_twilio_event(self, data: dict):
        evt = data.get("event")
        if evt == "connected":
            await self._push_status("connected", "Call connected")
            return

        if evt == "start":
            self.stream_sid = data.get("streamSid")
            start = data.get("start", {})
            self.call_sid = start.get("callSid")
            self.metrics.call_sid = self.call_sid or ""
            cp = start.get("customParameters", {}) or {}
            if cp:
                self.lead_info = {
                    "Name": cp.get("name", ""),
                    "Phone": cp.get("phone", ""),
                    "City": cp.get("city", ""),
                    "Category": cp.get("category", ""),
                    "Timezone": cp.get("timezone", ""),
                }
            print(f"[GPT-LIVE STREAM START] {self.call_sid} lead={self.lead_info}")
            await self._push_status("active", "GPT-Live active")
            return

        if evt == "media":
            payload = data.get("media", {}).get("payload", "")
            if not payload:
                return
            raw = base64.b64decode(payload)
            self._record_prospect_frame(raw)
            self._fanout_nowait(payload, "prospect")
            await self._send_live_audio(payload)
            return

        if evt == "stop":
            print("[GPT-LIVE STREAM STOP]")
            self._stop = True
            await self._close_live()

    async def _send_live_audio(self, payload_b64: str):
        if not self.live_started:
            self._pending_audio.append(payload_b64)
            self._pending_audio = self._pending_audio[-250:]
            return
        try:
            await self.live_ws.send(json.dumps({
                "type": "session.input_audio.append",
                "audio": payload_b64,
            }))
        except Exception as e:
            print(f"[GPT-LIVE SEND AUDIO] {e}")

    async def _on_live_event(self, event: dict):
        typ = event.get("type", "")
        if typ == "session.started":
            self.live_started = True
            self.live_session_id = event.get("session", {}).get("id", "")
            await self._push_status("listening", "GPT-Live listening")
            for payload in self._pending_audio:
                await self._send_live_audio(payload)
            self._pending_audio.clear()
            return

        if typ == "session.input_transcript.delta":
            delta = event.get("delta", "")
            if delta:
                if self._last_speaker == "agent":
                    await self._finalize_transcript("agent")
                self._last_speaker = "prospect"
                self._input_text += delta
                text = self._input_text.strip()
                if text:
                    self.outcome = "conversation" if self.outcome == "no_answer" else self.outcome
                    await self._push_partial("prospect", text)
            return

        if typ == "session.output_audio.delta":
            delta = event.get("delta", "")
            if delta:
                if not self._agent_turn_open:
                    self._agent_turn_open = True
                    self.metrics.turns += 1
                    await self._push_status("speaking", "GPT-Live speaking")
                await self._send_twilio_audio(delta)
            return

        if typ == "session.output_transcript.delta":
            delta = event.get("delta", "")
            if delta:
                if self._last_speaker == "prospect":
                    await self._finalize_transcript("prospect")
                self._last_speaker = "agent"
                self._output_text += delta
                text = self._output_text.replace("[HANGUP]", "").strip()
                if text:
                    await self._push_partial("agent", text)
                low = self._output_text.lower()
                if "[hangup]" in low or any(p in low for p in self.end_phrases):
                    asyncio.create_task(self._delayed_hangup())
            return

        if typ == "session.usage.updated":
            usage = event.get("usage", {}) or {}
            self._live_usage_seconds = float(usage.get("seconds") or self._live_usage_seconds or 0.0)
            if self._live_usage_seconds:
                self._live_usage_source = "openai_session_usage"
            return

        if typ == "session.closed":
            usage = event.get("usage", {}) or {}
            self._live_usage_seconds = float(usage.get("seconds") or self._live_usage_seconds or 0.0)
            if self._live_usage_seconds:
                self._live_usage_source = "openai_session_closed"
            self._stop = True
            return

        if typ == "error":
            err = event.get("error", {}) or {}
            msg = err.get("message") or json.dumps(err)
            print(f"[GPT-LIVE ERROR] {msg}")
            await self._push_status("ended", f"GPT-Live error: {msg}")

    async def _send_twilio_audio(self, payload_b64: str):
        if not self.stream_sid:
            return
        try:
            raw = base64.b64decode(payload_b64)
        except Exception:
            return
        for i in range(0, len(raw), 160):
            frame_raw = raw[i:i + 160]
            if not frame_raw:
                continue
            frame = base64.b64encode(frame_raw).decode("ascii")
            try:
                await self.twilio_ws.send_text(json.dumps({
                    "event": "media",
                    "streamSid": self.stream_sid,
                    "media": {"payload": frame},
                }))
            except Exception as e:
                print(f"[GPT-LIVE TWILIO AUDIO] {e}")
                return
            self._record_agent_frame(frame_raw)
            self._fanout_nowait(frame, "agent")

    async def _delayed_hangup(self):
        await asyncio.sleep(1.2)
        if not self._stop:
            await self._hangup()

    async def _hangup(self):
        self._stop = True
        await self._close_live()
        if not self.call_sid or not TWILIO_ACCOUNT_SID:
            return
        try:
            async with httpx.AsyncClient(timeout=10) as client:
                await client.post(
                    f"https://api.twilio.com/2010-04-01/Accounts/{TWILIO_ACCOUNT_SID}/Calls/{self.call_sid}.json",
                    auth=(TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN),
                    data={"Status": "completed"},
                )
        except Exception as e:
            print(f"[GPT-LIVE HANGUP ERROR] {e}")

    async def _close_live(self):
        if self.live_ws:
            try:
                await self.live_ws.send(json.dumps({"type": "session.close"}))
            except Exception:
                pass
            try:
                await self.live_ws.close()
            except Exception:
                pass

    async def _push_status(self, state: str, message: str):
        if message != self._last_status:
            self._last_status = message
            await self.status_queue.put({"type": "status", "state": state, "message": message})

    async def _push_transcript(self, speaker: str, text: str):
        await self.status_queue.put({
            "type": "transcript",
            "speaker": speaker,
            "text": text,
            "ts": datetime.now().strftime("%H:%M:%S"),
        })

    async def _push_partial(self, speaker: str, text: str):
        await self.status_queue.put({
            "type": "transcript_partial",
            "speaker": speaker,
            "text": text,
            "ts": datetime.now().strftime("%H:%M:%S"),
        })

    async def _finalize_transcript(self, speaker: str):
        if speaker == "agent":
            text = self._output_text.replace("[HANGUP]", "").strip()
            self._output_text = ""
            self._agent_turn_open = False
        else:
            text = self._input_text.strip()
            self._input_text = ""
        if text:
            await self.status_queue.put({
                "type": "transcript_final",
                "speaker": speaker,
                "text": text,
                "ts": datetime.now().strftime("%H:%M:%S"),
            })

    def _record_prospect_frame(self, frame: bytes):
        self._rec_prospect.extend(frame)
        self._rec_agent.extend(b"\xff" * len(frame))

    def _record_agent_frame(self, frame: bytes):
        self._rec_agent.extend(frame)
        self._rec_prospect.extend(b"\xff" * len(frame))

    def _save_recording(self) -> str | None:
        if not self._rec_agent and not self._rec_prospect:
            return None
        rec_dir = Path(__file__).parent / "recordings"
        rec_dir.mkdir(exist_ok=True)
        name = _clean_filename(self.lead_info.get("Name") or "GPT Live Call")
        suffix = (self.call_sid or "")[-6:] or os.urandom(3).hex()
        filename = f"{datetime.now().strftime('%Y%m%d_%H%M%S')}_{name}_{suffix}.wav"
        path = rec_dir / filename

        a = bytes(self._rec_agent)
        p = bytes(self._rec_prospect)
        n = max(len(a), len(p))
        a = a.ljust(n, b"\xff")
        p = p.ljust(n, b"\xff")
        try:
            pcm_agent = audioop.ulaw2lin(a, 2)
            pcm_prospect = audioop.ulaw2lin(p, 2)
            mixed = audioop.add(pcm_agent, pcm_prospect, 2)
            with wave.open(str(path), "wb") as wav:
                wav.setnchannels(1)
                wav.setsampwidth(2)
                wav.setframerate(8000)
                wav.writeframes(mixed)
            return filename
        except Exception as e:
            print(f"[GPT-LIVE RECORDING] failed: {e}")
            return None

    def _fanout_nowait(self, payload_b64: str, who: str):
        if not self.listeners:
            self._fanout_buf.clear()
            return
        self._fanout_buf.append((who, payload_b64))
        if len(self._fanout_buf) > 30:
            self._fanout_buf = self._fanout_buf[-10:]
        if len(self._fanout_buf) >= 8:
            self._flush_fanout_nowait()
        elif not self._fanout_flush_task or self._fanout_flush_task.done():
            self._fanout_flush_task = asyncio.create_task(self._flush_fanout_soon())

    async def _flush_fanout_soon(self):
        await asyncio.sleep(0.12)
        self._flush_fanout_nowait()

    def _flush_fanout_nowait(self):
        if not self._fanout_buf or self._fanout_inflight:
            return
        batch, self._fanout_buf = self._fanout_buf, []
        self._fanout_inflight += 1
        asyncio.create_task(self._flush_fanout(batch))

    async def _flush_fanout(self, batch):
        try:
            if not self.listeners or not batch:
                return
            msg = json.dumps({"batch": [{"t": w, "a": a} for w, a in batch]})
            for ws in list(self.listeners):
                try:
                    await ws.send_text(msg)
                except Exception:
                    self.listeners.discard(ws)
        finally:
            self._fanout_inflight = max(0, self._fanout_inflight - 1)

    async def _cleanup(self):
        if self._cleaned:
            return
        self._cleaned = True
        self._stop = True
        self.metrics.ended = time.time()
        if not self._live_usage_seconds:
            self._live_usage_seconds = max(0.0, self.metrics.ended - self.metrics.started)
        self.metrics.live_seconds = self._live_usage_seconds
        self.metrics.live_usage_source = self._live_usage_source
        await self._finalize_transcript("prospect")
        await self._finalize_transcript("agent")
        self.recording_file = self._save_recording()
        snap = self.metrics.snapshot()
        try:
            await self.status_queue.put({"type": "metrics", **snap})
        except Exception:
            pass
