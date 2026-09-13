"""
AI Agent WebSocket handler for Twilio Media Streams.

Flow:
  Twilio (mulaw 8kHz) → Deepgram STT → GPT → ElevenLabs Flash TTS → Twilio

The browser UI is a spectator: it receives status/transcript via SSE.
Audio never touches the browser when agent mode is active.
"""

import asyncio
import base64
import json
import os
from datetime import datetime
from typing import Optional

import httpx
import websockets
from fastapi import WebSocket, WebSocketDisconnect

OPENAI_API_KEY  = os.getenv("OPENAI_API_KEY")
ELEVENLABS_API_KEY = os.getenv("ELEVENLABS_API_KEY")
DEEPGRAM_API_KEY   = os.getenv("DEEPGRAM_API_KEY")
TWILIO_ACCOUNT_SID = os.getenv("TWILIO_ACCOUNT_SID")
TWILIO_AUTH_TOKEN  = os.getenv("TWILIO_AUTH_TOKEN")


# ── System prompt (uses validated 150+ call script as foundation) ────────────

def build_system_prompt(lead: dict, tone: str) -> str:
    business = lead.get("Name", "your business")
    city     = lead.get("City", "")
    category = lead.get("Category", "plumbing business")
    context  = f"Lead: {business}, a {category}{' in ' + city if city else ''}."

    tone_notes = {
        "professional": "Be confident, concise, and businesslike.",
        "friendly":     "Be warm and conversational, like talking to a neighbour.",
        "direct":       "Be brief and straight to the point. Cut anything that isn't essential.",
    }.get(tone, "Be confident and natural.")

    return f"""You are Odelyn, a sales agent for Lynkflow calling plumbing and trade businesses cold.
Your ONLY goal this call: book a 10-minute Zoom demo.

{context}
Tone instruction: {tone_notes}

RULES:
- Keep every response to 1-3 sentences MAX. Never monologue.
- Sound like a real person, not a script reader.
- React specifically to what the prospect just said.
- When genuine interest appears, pivot immediately to booking a Zoom.
- After 3 clear rejections with no opening, exit gracefully.
- Never admit you are an AI unless directly and sincerely asked — if asked, say you are a real person.
- If the call should end (clear DNC, call closed, prospect hung up), end your final line with [HANGUP].

OPENING — say this first:
"Hi, am I reaching the owner of {business}? Yeah so I'll be honest with you — this is a cold call. Do you want to hang up now or give me 30 seconds and then you can decide?"

CORE PITCH (use naturally, not word-for-word):
We help plumbers make sure they never miss a customer call — 24/7 AI that answers every call automatically when you're on a job and texts you a full summary instantly. No contracts, cancel anytime.

PRICE: $350 setup, $100 a month. One saved job pays for months of the service.

CLOSE: "What day works best for a quick 10-minute Zoom? I'll send the link right after."

OBJECTION REPLIES (short, in your own words):
- Not interested → "Fair — quick question though: when you miss a call on a job, where does that lead go?"
- We have voicemail → "80% of callers hang up before they leave a message. They call your competitor instead."
- Too expensive → "One missed job usually costs more than the whole month. It pays for itself the first week."
- Think about it → "Of course — what part do you want to think through? I can answer it now."
- Already have receptionist → "Great — we cover overflow and after-hours she can't get to. Worth a quick look?"
- Is this AI → "No, I'm a real person. I'm calling because we help plumbers stop losing jobs to missed calls."
"""


# ── Handler ──────────────────────────────────────────────────────────────────

class AgentCallHandler:
    """
    Manages one outbound AI agent call end-to-end.
    Instantiated once per Twilio Media Stream WebSocket connection.
    """

    def __init__(
        self,
        twilio_ws: WebSocket,
        voice_id: str,
        model: str,
        tone: str,
        lead_info: dict,
        status_queue: asyncio.Queue,
    ):
        self.twilio_ws    = twilio_ws
        self.voice_id     = voice_id
        self.model        = model
        self.tone         = tone
        self.lead_info    = lead_info
        self.status_queue = status_queue

        self.stream_sid: Optional[str] = None
        self.call_sid:   Optional[str] = None

        self.conversation: list = []
        self.is_speaking  = False
        self.dg_ws        = None
        self._stop        = False

    # ── Entry point ──────────────────────────────────────────────────────────

    async def run(self):
        await self._push_status("connecting", "Connecting…")
        await self._connect_deepgram()

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

    # ── Twilio events ─────────────────────────────────────────────────────────

    async def _on_twilio_event(self, data: dict):
        evt = data.get("event")

        if evt == "connected":
            await self._push_status("connected", "Call connected")

        elif evt == "start":
            self.stream_sid = data.get("streamSid")
            self.call_sid   = data.get("start", {}).get("callSid")
            await self._push_status("active", "Call active")
            await self._begin_conversation()

        elif evt == "media":
            if not self.is_speaking and self.dg_ws:
                payload = data.get("media", {}).get("payload", "")
                if payload:
                    try:
                        await self.dg_ws.send(base64.b64decode(payload))
                    except Exception:
                        pass

        elif evt == "stop":
            self._stop = True

    # ── Conversation ─────────────────────────────────────────────────────────

    async def _begin_conversation(self):
        system = build_system_prompt(self.lead_info, self.tone)
        business = self.lead_info.get("Name", "your business")

        opening = (
            f"Hi, am I reaching the owner of {business}? "
            f"Yeah so I'll be honest with you — this is a cold call. "
            f"Do you want to hang up now or give me 30 seconds and then you can decide?"
        )

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
                        "model":       self.model,
                        "messages":    self.conversation,
                        "max_tokens":  180,
                        "temperature": 0.75,
                    },
                )
                return r.json()["choices"][0]["message"]["content"].strip()
        except Exception as e:
            print(f"GPT error: {e}")
            return None

    # ── ElevenLabs TTS ───────────────────────────────────────────────────────

    async def _speak(self, text: str):
        self.is_speaking = True
        await self._push_status("speaking", "Agent speaking…")

        try:
            async with httpx.AsyncClient(timeout=30) as c:
                r = await c.post(
                    f"https://api.elevenlabs.io/v1/text-to-speech/{self.voice_id}/stream",
                    headers={
                        "xi-api-key":   ELEVENLABS_API_KEY,
                        "Content-Type": "application/json",
                    },
                    json={
                        "text":       text,
                        "model_id":   "eleven_flash_v2_5",
                        "output_format": "ulaw_8000",   # native Twilio format — no conversion needed
                        "voice_settings": {
                            "stability":        0.5,
                            "similarity_boost": 0.75,
                            "style":            0.1,
                        },
                    },
                )

                if r.status_code != 200:
                    print(f"ElevenLabs {r.status_code}: {r.text[:200]}")
                    return

                # Stream in 20 ms chunks (160 bytes at 8 kHz μ-law)
                audio = r.content
                for i in range(0, len(audio), 160):
                    if not self.is_speaking or self._stop:
                        break
                    chunk = audio[i : i + 160]
                    await self.twilio_ws.send_text(json.dumps({
                        "event":     "media",
                        "streamSid": self.stream_sid,
                        "media":     {"payload": base64.b64encode(chunk).decode()},
                    }))
                    await asyncio.sleep(0.02)

        except Exception as e:
            print(f"TTS error: {e}")
        finally:
            self.is_speaking = False
            if not self._stop:
                await self._push_status("listening", "Listening…")

    async def _stop_speaking(self):
        """Barge-in: cancel current audio immediately."""
        self.is_speaking = False
        if self.stream_sid:
            try:
                await self.twilio_ws.send_text(json.dumps({
                    "event":     "clear",
                    "streamSid": self.stream_sid,
                }))
            except Exception:
                pass

    # ── Deepgram STT ─────────────────────────────────────────────────────────

    async def _connect_deepgram(self):
        if not DEEPGRAM_API_KEY:
            print("DEEPGRAM_API_KEY not set — STT disabled")
            return

        url = (
            "wss://api.deepgram.com/v1/listen"
            "?encoding=mulaw&sample_rate=8000&channels=1"
            "&endpointing=400&interim_results=false"
            "&utterance_end_ms=1200"
        )
        try:
            self.dg_ws = await websockets.connect(
                url,
                extra_headers={"Authorization": f"Token {DEEPGRAM_API_KEY}"},
                ping_interval=30,
            )
            asyncio.create_task(self._deepgram_listener())
        except Exception as e:
            print(f"Deepgram connect error: {e}")

    async def _deepgram_listener(self):
        try:
            async for msg in self.dg_ws:
                if self._stop:
                    break
                data = json.loads(msg)

                if data.get("type") == "Results":
                    alts = data.get("channel", {}).get("alternatives", [])
                    text = alts[0].get("transcript", "").strip() if alts else ""
                    is_final = data.get("is_final", False)

                    # Barge-in detection
                    if text and self.is_speaking:
                        await self._stop_speaking()

                    if text and is_final:
                        asyncio.create_task(self._handle_transcript(text))

        except Exception as e:
            print(f"Deepgram listener error: {e}")

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
            print(f"Hangup error: {e}")

    async def _push_status(self, state: str, message: str):
        await self.status_queue.put({
            "type":    "status",
            "state":   state,
            "message": message,
        })

    async def _push_transcript(self, speaker: str, text: str):
        await self.status_queue.put({
            "type":    "transcript",
            "speaker": speaker,
            "text":    text,
            "ts":      datetime.now().strftime("%H:%M:%S"),
        })

    async def _cleanup(self):
        self._stop = True
        if self.dg_ws:
            try:
                await self.dg_ws.close()
            except Exception:
                pass