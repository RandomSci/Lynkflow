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
TWILIO_CALLER_ID = os.getenv("TWILIO_CALLER_ID", "")


TONE_NOTES = {
    "professional": "Be calm, concise, and businesslike.",
    "friendly": "Be warm and conversational without sounding fake.",
    "direct": "Be brief and straight to the point.",
}


IVR_MARKERS = [
    "press 1", "press one", "press 2", "press two", "press 3", "press three",
    "press 4", "press four", "press 5", "press five", "press 6", "press six",
    "press 7", "press seven", "press 8", "press eight", "press 9", "press nine",
    "press 0", "press zero", "press pound", "press the pound", "press star",
    "press the star", "press #", "press *", "press any key", "keypad",
    "enter your", "enter the", "enter a", "enter zip", "enter your zip",
    "zip code", "postal code", "extension", "extension number", "dial by name",
    "main menu", "menu options", "please listen", "options have changed",
    "office is currently closed", "office is closed", "currently closed",
    "operators are busy", "all of our operators are busy", "after normal business hours",
    "directory", "company directory", "coordinated directory",
    "for sales", "for service", "for billing", "for emergency service",
    "say or press", "to continue", "to repeat", "hold for", "currently assisting",
    "automated system", "automated attendant", "auto attendant", "answering service",
    "connect you to your local", "connect you to a local", "local technician",
    "local text section", "local tech",
]


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
        self._out_audio_queue: asyncio.Queue[bytes | None] = asyncio.Queue()
        self._out_audio_task = None
        self._stop = False
        self._ending = False
        self._cleaned = False

        self.metrics = CallMetrics(model="gpt-live-1")
        self.outcome = "no_answer"
        self.followup_note = ""
        self.recording_file = None

        self._rec_agent_pcm = bytearray()
        self._rec_prospect_pcm = bytearray()
        self._rec_started_at = time.time()
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
        self._call_started_at = time.time()
        self._last_input_transcript_at = 0.0
        self._last_clear_at = 0.0
        self._audio_clear_seq = 0
        self._prospect_loud_frames = 0
        self._barge_vad_threshold = 2600
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
            await asyncio.gather(self._twilio_loop(), self._live_loop(), self._watchdog())
        finally:
            await self._cleanup()

    async def _watchdog(self):
        while not self._stop:
            await asyncio.sleep(1)
            now = time.time()
            elapsed = now - self._call_started_at
            if self.call_sid and self.outcome == "no_answer" and elapsed > max(25, getattr(self.cfg, "silence_timeout_s", 20)):
                print("[GPT-LIVE WATCHDOG] no human transcript detected")
                await self._push_status("ended", "No human detected")
                await self._hangup()
                return
            max_duration = getattr(self.cfg, "max_duration_s", 300) or 300
            if elapsed > max_duration and self.outcome not in {"conversation", "interested", "callback"}:
                print("[GPT-LIVE WATCHDOG] max duration without useful conversation")
                await self._push_status("ended", "Max duration reached")
                await self._hangup()
                return

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
        callback = (getattr(self.cfg, "callback_number", "") or TWILIO_CALLER_ID or "the number I called from").strip()
        return (
            f"{getattr(self.cfg, 'system_prompt', '')}\n\n"
            f"{context}.\n"
            f"Your callback number: {callback}.\n"
            f"Tone: {tone}\n\n"
            "Live voice behavior:\n"
            "- You are on a phone call. Keep replies short and natural.\n"
            "- Listen while the other person speaks; stop when interrupted.\n"
            "- Answer the exact question first, then continue naturally.\n"
            "- Do not repeat a previous line or restart the call.\n"
            "- Do not pitch until a decision maker has allowed the 30-second pitch.\n"
            "- Never invent phone numbers, emails, prices, company details, or names. Use only the callback number above.\n"
            "- If leaving a message, give your name, Lynkflow, the callback number above, and a brief reason.\n"
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
            self._rec_started_at = time.time()
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
            if self._agent_turn_open:
                try:
                    rms = audioop.rms(audioop.ulaw2lin(raw, 2), 2)
                except Exception:
                    rms = 0
                if rms > self._barge_vad_threshold:
                    self._prospect_loud_frames += 1
                    if self._prospect_loud_frames >= 6:
                        await self._clear_twilio_output("barge-in")
                        self._prospect_loud_frames = 0
                else:
                    self._prospect_loud_frames = max(0, self._prospect_loud_frames - 1)
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
            if not self._out_audio_task or self._out_audio_task.done():
                self._out_audio_task = asyncio.create_task(self._pump_twilio_audio())
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
                    self._last_input_transcript_at = time.time()
                    if self._is_ivr_text(text):
                        await self._handle_ivr_detected(text)
                        return
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
                try:
                    await self._out_audio_queue.put(base64.b64decode(delta))
                except Exception:
                    pass
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
                    print(f"[GPT-LIVE END PHRASE] {text[:120]}")
                    await self._hangup("end phrase")
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

    async def _pump_twilio_audio(self):
        sent_frames = 0
        playback_started = None
        buf = b""
        while not self._stop:
            chunk = await self._out_audio_queue.get()
            if chunk is None:
                return
            buf += chunk
            n = (len(buf) // 160) * 160
            seq = self._audio_clear_seq
            for i in range(0, n, 160):
                if self._stop:
                    return
                if seq != self._audio_clear_seq:
                    buf = b""
                    break
                frame_raw = buf[i:i + 160]
                if not frame_raw:
                    continue
                if not self.stream_sid:
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
                if playback_started is None:
                    playback_started = time.time()
                sent_frames += 1
                self._record_agent_frame(frame_raw)
                self._fanout_nowait(frame, "agent")
                target_elapsed = sent_frames * 0.02
                delay = target_elapsed - (time.time() - playback_started)
                if delay > 0:
                    await asyncio.sleep(delay)
            buf = buf[n:]
            if not buf and self._out_audio_queue.empty():
                self._agent_turn_open = False

    async def _clear_twilio_output(self, reason: str, force: bool = False):
        now = time.time()
        if not force and now - self._last_clear_at < 0.7:
            return
        self._last_clear_at = now
        self._audio_clear_seq += 1
        while True:
            try:
                self._out_audio_queue.get_nowait()
            except asyncio.QueueEmpty:
                break
        if self.stream_sid:
            try:
                await self.twilio_ws.send_text(json.dumps({
                    "event": "clear",
                    "streamSid": self.stream_sid,
                }))
                print(f"[GPT-LIVE] cleared Twilio audio ({reason})")
            except Exception as e:
                print(f"[GPT-LIVE CLEAR ERROR] {e}")
        self._agent_turn_open = False

    def _is_ivr_text(self, text: str) -> bool:
        low = text.lower()
        return any(marker in low for marker in IVR_MARKERS)

    async def _handle_ivr_detected(self, text: str):
        print(f"[GPT-LIVE IVR] {text[:120]}")
        self.outcome = "ivr"
        await self._push_partial("prospect", text)
        await self._push_status("ended", "Phone tree / IVR detected")
        await self._clear_twilio_output("ivr", force=True)
        await self._hangup("ivr")

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
            await self._hangup("delayed")

    async def _hangup(self, reason: str = ""):
        if self._ending:
            return
        self._ending = True
        if reason:
            print(f"[GPT-LIVE HANGUP] {reason}")
        try:
            await self._push_status("ended", f"Ending call{': ' + reason if reason else ''}")
        except Exception:
            pass
        try:
            await self._clear_twilio_output(reason or "hangup", force=True)
        except Exception:
            pass
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
        try:
            await self._out_audio_queue.put(None)
        except Exception:
            pass
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
        self._mix_recording_frame(frame, self._rec_prospect_pcm)

    def _record_agent_frame(self, frame: bytes):
        self._mix_recording_frame(frame, self._rec_agent_pcm)

    def _mix_recording_frame(self, frame: bytes, lane: bytearray):
        try:
            pcm = audioop.ulaw2lin(frame, 2)
        except Exception:
            return
        offset = max(0, int((time.time() - self._rec_started_at) * 8000) * 2)
        end = offset + len(pcm)
        if len(lane) < end:
            lane.extend(b"\x00" * (end - len(lane)))
        existing = bytes(lane[offset:end])
        try:
            mixed = audioop.add(existing, pcm, 2)
        except Exception:
            mixed = pcm
        lane[offset:end] = mixed

    def _save_recording(self) -> str | None:
        if not self._rec_agent_pcm and not self._rec_prospect_pcm:
            return None
        rec_dir = Path(__file__).parent / "recordings"
        rec_dir.mkdir(exist_ok=True)
        name = _clean_filename(self.lead_info.get("Name") or "GPT Live Call")
        suffix = (self.call_sid or "")[-6:] or os.urandom(3).hex()
        filename = f"{datetime.now().strftime('%Y%m%d_%H%M%S')}_{name}_{suffix}.wav"
        path = rec_dir / filename

        try:
            n = max(len(self._rec_agent_pcm), len(self._rec_prospect_pcm))
            agent = bytes(self._rec_agent_pcm).ljust(n, b"\x00")
            prospect = bytes(self._rec_prospect_pcm).ljust(n, b"\x00")
            stereo = audioop.tostereo(prospect, 2, 1.0, 0.0)
            stereo = audioop.add(stereo, audioop.tostereo(agent, 2, 0.0, 1.0), 2)
            with wave.open(str(path), "wb") as wav:
                wav.setnchannels(2)
                wav.setsampwidth(2)
                wav.setframerate(8000)
                wav.writeframes(stereo)
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
        try:
            await self._out_audio_queue.put(None)
        except Exception:
            pass
        if not self._live_usage_seconds:
            self._live_usage_seconds = max(0.0, self.metrics.ended - self.metrics.started)
        self.metrics.live_seconds = self._live_usage_seconds
        self.metrics.live_usage_source = self._live_usage_source
        await self._finalize_transcript("prospect")
        await self._finalize_transcript("agent")
        if not self.recording_file:
            self.recording_file = self._save_recording()
        snap = self.metrics.snapshot()
        try:
            await self.status_queue.put({"type": "metrics", **snap})
        except Exception:
            pass
