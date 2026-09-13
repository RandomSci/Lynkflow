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
        self._speak_seq   = 0

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
            # ALWAYS forward audio — Deepgram closes the socket if it goes
            # ~10s without data. Barge-in is handled in the listener instead.
            if self.dg_ws:
                payload = data.get("media", {}).get("payload", "")
                if payload:
                    try:
                        await self.dg_ws.send(base64.b64decode(payload))
                    except Exception as e:
                        print(f"[DG SEND] {e}")

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
        print(f"[LEAD] {self.lead_info}")
        print(f"[OPENING] {opening}")

        self.conversation = [
            {"role": "system",    "content": system},
            {"role": "assistant", "content": opening},
        ]
        await self._push_transcript("agent", opening)
        self._speak_seq += 1
        await self._speak_chunk(opening, self._speak_seq)

    async def _handle_transcript(self, text: str):
        if not text.strip() or self._stop:
            return

        await self._push_transcript("prospect", text)
        self.conversation.append({"role": "user", "content": text})

        response = await self._respond()
        if not response:
            return

        clean = response.replace("[HANGUP]", "").strip()
        if clean:
            self.conversation.append({"role": "assistant", "content": clean})
            await self._push_transcript("agent", clean)

        low = clean.lower()
        if "[HANGUP]" in response or any(p in low for p in self.end_phrases):
            await asyncio.sleep(1)
            await self._hangup()

    # ── GPT + TTS streaming pipeline ─────────────────────────────────────────

    async def _respond(self) -> Optional[str]:
        """
        Streams GPT tokens, and as soon as a full sentence is ready it is sent
        to TTS. This overlaps generation with speech so the caller hears the
        first words while the rest is still being written.
        """
        full = ""
        buf  = ""
        self._speak_seq += 1
        seq = self._speak_seq
        first_chunk = True

        try:
            async with httpx.AsyncClient(timeout=30) as c:
                async with c.stream(
                    "POST",
                    "https://api.openai.com/v1/chat/completions",
                    headers={"Authorization": f"Bearer {OPENAI_API_KEY}"},
                    json={
                        "model":       self.cfg.model,
                        "messages":    self.conversation,
                        "max_tokens":  self.cfg.max_tokens,
                        "temperature": self.cfg.temperature,
                        "stream":      True,
                    },
                ) as resp:
                    async for line in resp.aiter_lines():
                        if self._stop or seq != self._speak_seq:
                            return None
                        if not line.startswith("data: "):
                            continue
                        payload = line[6:]
                        if payload == "[DONE]":
                            break
                        try:
                            delta = json.loads(payload)["choices"][0]["delta"]
                            tok = delta.get("content", "")
                        except Exception:
                            continue
                        if not tok:
                            continue

                        full += tok
                        buf  += tok

                        # Flush on sentence boundary (or early on first chunk)
                        limit = 25 if first_chunk else 90
                        if (any(buf.rstrip().endswith(p) for p in ".!?") and len(buf.strip()) > 12) \
                           or len(buf) > limit:
                            chunk = buf.strip()
                            buf = ""
                            first_chunk = False
                            await self._speak_chunk(chunk, seq)

        except Exception as e:
            print(f"[GPT ERROR] {e}")

        if buf.strip() and not self._stop and seq == self._speak_seq:
            await self._speak_chunk(buf.strip(), seq)

        return full.strip() or None

    async def _speak_chunk(self, text: str, seq: int):
        """Stream one sentence to ElevenLabs and pipe audio straight to Twilio."""
        if self._stop or seq != self._speak_seq:
            return

        self.is_speaking = True
        await self._push_status("speaking", "Agent speaking…")

        t0 = time.time()
        sent = 0
        try:
            async with httpx.AsyncClient(timeout=30) as c:
                async with c.stream(
                    "POST",
                    f"https://api.elevenlabs.io/v1/text-to-speech/{self.cfg.voice_id}/stream"
                    f"?output_format=ulaw_8000&optimize_streaming_latency=4",
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
                ) as r:
                    if r.status_code != 200:
                        body = await r.aread()
                        print(f"[TTS ERROR] {r.status_code}: {body[:200]}")
                        return

                    leftover = b""
                    async for raw in r.aiter_bytes(1600):
                        if self._stop or seq != self._speak_seq or not self.is_speaking:
                            print("[TTS] cancelled mid-stream")
                            return
                        data = leftover + raw
                        n = (len(data) // 160) * 160
                        leftover = data[n:]
                        for i in range(0, n, 160):
                            if self._stop or seq != self._speak_seq or not self.is_speaking:
                                return
                            await self.twilio_ws.send_text(json.dumps({
                                "event":     "media",
                                "streamSid": self.stream_sid,
                                "media":     {"payload": base64.b64encode(data[i:i+160]).decode()},
                            }))
                            sent += 1
                            await asyncio.sleep(0.019)

            print(f"[TTS] {sent} frames in {time.time()-t0:.2f}s :: {text[:50]}")

        except Exception as e:
            print(f"[TTS EXCEPTION] {e}")
        finally:
            if seq == self._speak_seq:
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
        asyncio.create_task(self._deepgram_keepalive())

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

                # Barge-in — cancel current speech immediately
                if self.is_speaking and self.cfg.allow_interruption and len(text) > 2:
                    print(f"[DG] barge-in: {text}")
                    self._speak_seq += 1        # invalidates in-flight TTS
                    await self._stop_speaking()

                if data.get("is_final"):
                    print(f"[DG] final: {text}")
                    asyncio.create_task(self._handle_transcript(text))

        except Exception as e:
            print(f"[DG] listener error: {e}")

    async def _deepgram_keepalive(self):
        """Deepgram closes idle sockets — send a KeepAlive every 5s."""
        while not self._stop and self.dg_ws:
            await asyncio.sleep(5)
            try:
                await self.dg_ws.send(json.dumps({"type": "KeepAlive"}))
            except Exception:
                break

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