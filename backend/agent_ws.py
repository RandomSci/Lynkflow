"""
AI Agent WebSocket handler for Twilio Media Streams.

Flow:
  Twilio (mulaw 8kHz) -> Deepgram STT -> GPT -> ElevenLabs Flash TTS -> Twilio
"""

import asyncio
import base64
import json
import os
import time
from datetime import datetime
from typing import Optional

import httpx
import websockets
from dotenv import load_dotenv
from fastapi import WebSocket, WebSocketDisconnect

load_dotenv()

OPENAI_API_KEY     = os.getenv("OPENAI_API_KEY")
ELEVENLABS_API_KEY = os.getenv("ELEVENLABS_API_KEY")
DEEPGRAM_API_KEY   = os.getenv("DEEPGRAM_API_KEY")
TWILIO_ACCOUNT_SID = os.getenv("TWILIO_ACCOUNT_SID")
TWILIO_AUTH_TOKEN  = os.getenv("TWILIO_AUTH_TOKEN")


TONE_NOTES = {
    "professional": "Be confident, concise, and businesslike.",
    "friendly":     "Be warm and conversational, like talking to a neighbour.",
    "direct":       "Be brief and straight to the point. Cut anything that isn't essential.",
}


def render_template(text: str, lead: dict) -> str:
    """Replace {business}, {city}, {category} placeholders with lead data."""
    return (
        text.replace("{business}", lead.get("Name") or "your business")
            .replace("{city}",     lead.get("City") or "")
            .replace("{category}", lead.get("Category") or "your business")
    )


class AgentCallHandler:
    def __init__(self, twilio_ws: WebSocket, cfg, lead_info: dict, status_queue: asyncio.Queue):
        self.twilio_ws    = twilio_ws
        self.cfg          = cfg
        self.lead_info    = lead_info
        self.status_queue = status_queue

        self.stream_sid: Optional[str] = None
        self.call_sid:   Optional[str] = None

        self.conversation: list = []
        self.is_speaking  = False
        self.dg_ws        = None
        self._stop        = False
        self._last_audio  = time.time()
        self._call_start  = time.time()

        self.end_phrases = [
            p.strip().lower()
            for p in (cfg.end_call_phrases or "").split(",")
            if p.strip()
        ]

    # ── Entry ────────────────────────────────────────────────────────────────

    async def run(self):
        await self._push_status("connecting", "Connecting…")
        await self._connect_deepgram()
        asyncio.create_task(self._watchdog())

        try:
            async for raw in self.twilio_ws.iter_text():
                if self._stop:
                    break
                await self._on_twilio_event(json.loads(raw))
        except WebSocketDisconnect:
            pass
        finally:
            await self._cleanup()
            await self._push_status("ended", "Call ended")

    async def _watchdog(self):
        """Enforce silence timeout and max call duration."""
        while not self._stop:
            await asyncio.sleep(2)
            now = time.time()
            if self.cfg.max_duration_s and (now - self._call_start) > self.cfg.max_duration_s:
                print("[WATCHDOG] max duration reached")
                await self._hangup()
                break
            if (self.cfg.silence_timeout_s
                    and not self.is_speaking
                    and (now - self._last_audio) > self.cfg.silence_timeout_s):
                print("[WATCHDOG] silence timeout")
                await self._hangup()
                break

    # ── Twilio events ────────────────────────────────────────────────────────

    async def _on_twilio_event(self, data: dict):
        evt = data.get("event")

        if evt == "connected":
            await self._push_status("connected", "Call connected")

        elif evt == "start":
            self.stream_sid = data.get("streamSid")
            self.call_sid   = data.get("start", {}).get("callSid")
            print(f"[STREAM START] {self.call_sid}")
            await self._push_status("active", "Call active")
            await self._begin_conversation()

        elif evt == "media":
            self._last_audio = time.time()
            if self.dg_ws and (not self.is_speaking or self.cfg.allow_interruption):
                payload = data.get("media", {}).get("payload", "")
                if payload:
                    try:
                        await self.dg_ws.send(base64.b64decode(payload))
                    except Exception:
                        pass

        elif evt == "stop":
            print("[STREAM STOP]")
            self._stop = True
            await self._push_status("ended", "Call ended")

    # ── Conversation ─────────────────────────────────────────────────────────

    async def _begin_conversation(self):
        tone_note = TONE_NOTES.get(self.cfg.tone, "Be natural and confident.")
        business  = self.lead_info.get("Name") or "your business"
        city      = self.lead_info.get("City") or ""
        category  = self.lead_info.get("Category") or "plumbing business"

        context = f"\n\nLEAD CONTEXT: {business}, a {category}" + (f" in {city}." if city else ".")
        system  = render_template(self.cfg.system_prompt, self.lead_info) \
                  + context + f"\n\nTONE: {tone_note}"

        opening = render_template(self.cfg.first_message, self.lead_info)

        self.conversation = [
            {"role": "system",    "content": system},
            {"role": "assistant", "content": opening},
        ]
        await self._push_transcript("agent", opening)
        await self._speak(opening)

    async def _handle_transcript(self, text: str):
        if not text.strip() or self._stop:
            return

        await self._push_transcript("prospect", text)
        self.conversation.append({"role": "user", "content": text})

        response = await self._gpt()
        if not response:
            return

        hangup = "[HANGUP]" in response
        clean  = response.replace("[HANGUP]", "").strip()

        # Detect configured end-call phrases
        low = clean.lower()
        if any(p in low for p in self.end_phrases):
            hangup = True

        if clean:
            self.conversation.append({"role": "assistant", "content": clean})
            await self._push_transcript("agent", clean)
            await self._speak(clean)

        if hangup:
            await asyncio.sleep(1)
            await self._hangup()

    # ── GPT ──────────────────────────────────────────────────────────────────

    async def _gpt(self) -> Optional[str]:
        try:
            async with httpx.AsyncClient(timeout=20) as c:
                r = await c.post(
                    "https://api.openai.com/v1/chat/completions",
                    headers={"Authorization": f"Bearer {OPENAI_API_KEY}"},
                    json={
                        "model":       self.cfg.model,
                        "messages":    self.conversation,
                        "max_tokens":  self.cfg.max_tokens,
                        "temperature": self.cfg.temperature,
                    },
                )
                return r.json()["choices"][0]["message"]["content"].strip()
        except Exception as e:
            print(f"[GPT ERROR] {e}")
            return None

    # ── TTS ──────────────────────────────────────────────────────────────────

    async def _speak(self, text: str):
        self.is_speaking = True
        await self._push_status("speaking", "Agent speaking…")

        try:
            async with httpx.AsyncClient(timeout=30) as c:
                r = await c.post(
                    f"https://api.elevenlabs.io/v1/text-to-speech/{self.cfg.voice_id}/stream"
                    f"?output_format=ulaw_8000",
                    headers={"xi-api-key": ELEVENLABS_API_KEY,
                             "Content-Type": "application/json"},
                    json={
                        "text":     text,
                        "model_id": "eleven_flash_v2_5",
                        "voice_settings": {
                            "stability":        self.cfg.stability,
                            "similarity_boost": self.cfg.similarity_boost,
                            "style":            self.cfg.style,
                            "speed":            self.cfg.speaking_rate,
                        },
                    },
                )

                if r.status_code != 200:
                    print(f"[TTS ERROR] {r.status_code}: {r.text[:300]}")
                    return

                audio = r.content
                print(f"[TTS OK] {len(audio)} bytes")

                for i in range(0, len(audio), 160):
                    if not self.is_speaking or self._stop:
                        break
                    await self.twilio_ws.send_text(json.dumps({
                        "event":     "media",
                        "streamSid": self.stream_sid,
                        "media":     {"payload": base64.b64encode(audio[i:i+160]).decode()},
                    }))
                    await asyncio.sleep(0.02)

        except Exception as e:
            print(f"[TTS EXCEPTION] {e}")
        finally:
            self.is_speaking = False
            self._last_audio = time.time()
            if not self._stop:
                await self._push_status("listening", "Listening…")

    async def _stop_speaking(self):
        self.is_speaking = False
        if self.stream_sid:
            try:
                await self.twilio_ws.send_text(json.dumps({
                    "event": "clear", "streamSid": self.stream_sid,
                }))
            except Exception:
                pass

    # ── Deepgram ─────────────────────────────────────────────────────────────

    async def _connect_deepgram(self):
        if not DEEPGRAM_API_KEY:
            print("[DG] DEEPGRAM_API_KEY not set — agent cannot hear")
            return

        url = (
            "wss://api.deepgram.com/v1/listen"
            "?encoding=mulaw&sample_rate=8000&channels=1&model=nova-2"
            f"&endpointing={self.cfg.endpointing_ms}"
            f"&utterance_end_ms={self.cfg.utterance_end_ms}"
            "&interim_results=true"
        )
        headers = {"Authorization": f"Token {DEEPGRAM_API_KEY}"}

        try:
            self.dg_ws = await websockets.connect(
                url, additional_headers=headers, ping_interval=30
            )
        except TypeError:
            try:
                self.dg_ws = await websockets.connect(
                    url, extra_headers=headers, ping_interval=30
                )
            except Exception as e:
                print(f"[DG] connect error: {e}")
                return
        except Exception as e:
            print(f"[DG] connect error: {e}")
            return

        print("[DG] connected")
        asyncio.create_task(self._deepgram_listener())

    async def _deepgram_listener(self):
        try:
            async for msg in self.dg_ws:
                if self._stop:
                    break
                data = json.loads(msg)
                if data.get("type") != "Results":
                    continue

                alts = data.get("channel", {}).get("alternatives", [])
                text = alts[0].get("transcript", "").strip() if alts else ""
                if not text:
                    continue

                # Barge-in
                if self.is_speaking and self.cfg.allow_interruption:
                    print("[DG] barge-in")
                    await self._stop_speaking()

                if data.get("is_final"):
                    print(f"[DG] final: {text}")
                    asyncio.create_task(self._handle_transcript(text))

        except Exception as e:
            print(f"[DG] listener error: {e}")

    # ── Utilities ────────────────────────────────────────────────────────────

    async def _hangup(self):
        self._stop = True
        if not self.call_sid or not TWILIO_ACCOUNT_SID:
            return
        try:
            async with httpx.AsyncClient(timeout=10) as c:
                await c.post(
                    f"https://api.twilio.com/2010-04-01/Accounts/{TWILIO_ACCOUNT_SID}/Calls/{self.call_sid}.json",
                    auth=(TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN),
                    data={"Status": "completed"},
                )
        except Exception as e:
            print(f"[HANGUP ERROR] {e}")

    async def _push_status(self, state: str, message: str):
        await self.status_queue.put({"type": "status", "state": state, "message": message})

    async def _push_transcript(self, speaker: str, text: str):
        await self.status_queue.put({
            "type": "transcript", "speaker": speaker, "text": text,
            "ts": datetime.now().strftime("%H:%M:%S"),
        })

    async def _cleanup(self):
        self._stop = True
        if self.dg_ws:
            try:
                await self.dg_ws.close()
            except Exception:
                pass