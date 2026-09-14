"""
AI Agent WebSocket handler for Twilio Media Streams.

Flow:
  Twilio (mulaw 8kHz) -> Deepgram STT -> GPT -> ElevenLabs Flash TTS -> Twilio
"""

import asyncio
import base64
import json
import os
import re
import time
from datetime import datetime
from typing import Optional
import audioop
import httpx
import websockets
from dotenv import load_dotenv
from fastapi import WebSocket, WebSocketDisconnect
from metrics import CallMetrics

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
    business = clean_business_name_for_speech(lead.get("Name") or "your business")
    return (
        text.replace("{business}", business)
            .replace("{city}",     lead.get("City") or "")
            .replace("{category}", lead.get("Category") or "your business")
    )


def clean_business_name_for_speech(name: str) -> str:
    """Make scraped business names easier for TTS to pronounce on phone audio."""
    text = str(name or "").strip()
    if not text:
        return "your business"

    text = text.replace("&", " and ")
    text = re.sub(r"[,|]+", " ", text)
    replacements = {
        r"\bHVAC\b": "H V A C",
        r"\bLLC\b": "L L C",
        r"\bLTD\b": "limited",
        r"\bINC\b": "incorporated",
        r"\bCO\b": "company",
    }
    for pattern, repl in replacements.items():
        text = re.sub(pattern, repl, text, flags=re.IGNORECASE)
    return re.sub(r"\s+", " ", text).strip()


class AgentCallHandler:
    def __init__(self, twilio_ws: WebSocket, cfg, lead_info: dict, status_queue: asyncio.Queue):
        self.twilio_ws    = twilio_ws
        self.cfg          = cfg
        self.lead_info    = lead_info
        self.status_queue = status_queue
        self._ivr_hits = 0
        self.listeners: set = set()      # browser WebSockets listening in
        self._last_partial = 0.0
        self._fanout_buf: list = []
        self._fanout_inflight = 0
        self._fanout_flush_task = None
        self._speak_started = 0.0
        self._nudges = 0
        self._last_turn = time.time()
        self._prospect_spoke = False        
        self.metrics = CallMetrics(model=cfg.model)
        self._t_stt_done = 0.0
        self._t_llm_first = 0.0
        self._t_tts_first = 0.0        
        self.stream_sid: Optional[str] = None
        self.call_sid:   Optional[str] = None
        self.conversation: list = []
        self.is_speaking  = False
        self.dg_ws        = None
        self._stop        = False
        self._last_audio  = time.time()
        self._last_voice_audio = time.time()
        self._call_start  = time.time()
        self._awaiting_response_since = None
        self._max_duration_extended = False
        self._startup_audio_seen = False
        self._opening_started = False
        self._initial_turn_task = None
        self._turn_lock = asyncio.Lock()
        self._last_handled_text = ""
        self._last_handled_at = 0.0
        self._awaiting_callback_time = False
        self.followup_note = ""
        self._speak_seq     = 0
        self._loud_frames   = 0      # consecutive frames above threshold
        self._vad_threshold = 2500    # RMS level that counts as speech
        self.outcome = "no_answer"        
        self._machine_kind = ""
        self._vm_left = False
        self._voicemail_task = None
        self._last_dg_text = 0.0        
        self._vad_threshold = 3200        
        self._rec_agent = bytearray()
        self._rec_prospect = bytearray()        
        self.recording_file = None
        self._script_step = "owner_check"
        self._gatekeeper_mode = False
        self._gatekeeper_contact_asked = False
        self._decision_maker_confirmed = False

        self.end_phrases = [
            p.strip().lower()
            for p in (cfg.end_call_phrases or "").split(",")
            if p.strip()
        ]

    # ── Entry ────────────────────────────────────────────────────────────────

    # Phone trees — cannot leave a message, hang up
    MENU_MARKERS = [
        "press one", "press two", "press three", "press zero",
        "press 1", "press 2", "press 3", "press 0",
        "press any key", "any key to continue", "key to continue",
        "did not detect a key", "didn't detect a key", "key press",
        "press pound", "press the pound", "press star", "press the star",
        "press #", "press *", "enter your", "say or press",
        "dial by name", "extension number", "main menu",
        "for emergency service", "for sales", "for billing",
        "please listen as our options", "options have changed",
        "please hold", "your call is important",
        "currently assisting other customers",
        "automated system", "automated attendant", "auto attendant",
    ]

    # Voicemail — CAN leave a message
    VOICEMAIL_MARKERS = [
        "at the tone", "after the tone", "after the beep",
        "record your message", "leave your name", "leave a message",
        "leave your message", "forwarded to voice mail", "forwarded to voicemail",
        "is not available", "unable to take your call",
        "can't take your call", "cannot take your call",
        "mailbox is full", "brief description",
        "call you back as soon as", "we'll get back to you",
    ]

    ANNOUNCEMENT_MARKERS = [
        "this call may be recorded", "this call is being recorded",
        "call may be recorded", "call is being recorded",
        "may be recorded", "being recorded", "recorded for quality",
        "quality assurance", "quality of service", "quality of customer care",
        "ensure quality", "training purposes", "monitoring purposes",
        "please wait while", "while we connect", "while i connect",
        "your call will be connected", "connecting your call",
        "ringing", "ring ring", "phone ringing", "dial tone",
    ]

    HUMAN_GREETING_MARKERS = [
        "how may i help", "how can i help", "how may we help", "how can we help",
        "how may i assist", "how can i assist", "how may we assist", "how can we assist",
        "you're speaking with", "you are speaking with", "speaking",
        "can i help", "may i help", "how can i direct", "how may i direct",
    ]

    def _classify_machine(self, text: str) -> str:
        """Returns 'voicemail', 'menu', or '' for a human."""
        low = text.lower()
        if self._looks_like_human_greeting(low):
            return ""
        if any(m in low for m in self.MENU_MARKERS):
            return "menu"
        if any(m in low for m in self.VOICEMAIL_MARKERS):
            return "voicemail"
        if any(m in low for m in self.ANNOUNCEMENT_MARKERS):
            return "announcement"
        return ""

    def _looks_like_human_greeting(self, text: str) -> bool:
        low = text.lower()
        if any(m in low for m in self.VOICEMAIL_MARKERS):
            return False
        if any(m in low for m in self.HUMAN_GREETING_MARKERS):
            return True
        # Receptionists often say: "Thank you for calling X, this is Frankie..."
        return bool(re.search(r"\b(thank you|thanks) for calling\b.+\b(how (may|can)|this is)\b", low))

    def _looks_like_business_greeting_only(self, text: str) -> bool:
        low = text.lower()
        if any(m in low for m in self.MENU_MARKERS + self.VOICEMAIL_MARKERS):
            return False
        if self._looks_like_human_greeting(low):
            return False
        return "thank you for calling" in low or "thanks for calling" in low

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

    def _mark_listening_for_response(self):
        self._awaiting_response_since = time.time()

    def _recent_prospect_activity(self, now: float, grace_s: float = 4.0) -> bool:
        last = self._last_dg_text or 0.0
        return last > 0 and (now - last) <= grace_s

    def _is_engaged_human_conversation(self) -> bool:
        return (
            self._prospect_spoke
            and not self._machine_kind
            and self.outcome in {"conversation", "interested", "not_interested"}
        )

    async def _watchdog(self):
        """Silence nudges, silence timeout, and max call duration."""
        NUDGES = [
            "Hello, can you hear me okay?",
            "Sorry, I think we might have a bad connection. Are you still there?",
        ]
        while not self._stop:
            await asyncio.sleep(1)
            now = time.time()
            quiet = now - self._last_turn

            if self.cfg.max_duration_s and (now - self._call_start) > self.cfg.max_duration_s:
                if self._is_engaged_human_conversation():
                    if not self._max_duration_extended:
                        self._max_duration_extended = True
                        print("[WATCHDOG] max duration reached during engaged conversation; continuing")
                    if not self.is_speaking and quiet > max(45, self.cfg.silence_timeout_s * 2):
                        print("[WATCHDOG] engaged call went quiet after max duration")
                        await self._hangup()
                        break
                else:
                    print("[WATCHDOG] max duration reached")
                    await self._hangup()
                    break

            if (self.cfg.silence_timeout_s and not self.is_speaking
                    and not self._prospect_spoke and self._awaiting_response_since
                    and (now - self._awaiting_response_since) > self.cfg.silence_timeout_s
                    and not self._recent_prospect_activity(now)):
                print("[WATCHDOG] no response after agent line")
                await self._push_status("ended", "No response")
                await self._hangup()
                break

            # Nudge on dead air — only when the agent isn't speaking and the
            # conversation has actually started
            if (not self.is_speaking and self._prospect_spoke and quiet > 9
                    and self._nudges < len(NUDGES)):
                line = NUDGES[self._nudges]
                self._nudges += 1
                self._last_turn = now
                print(f"[NUDGE {self._nudges}] {line}")
                self.conversation.append({"role": "assistant", "content": line})
                await self._push_transcript("agent", line)
                self._speak_seq += 1
                await self._speak_chunk(line, self._speak_seq)
                continue

            # Give up after both nudges go unanswered
            if self._nudges >= len(NUDGES) and quiet > 8:
                print("[WATCHDOG] no response after nudges — hanging up")
                await self._hangup()
                break

            if (self.cfg.silence_timeout_s and not self.is_speaking and self._prospect_spoke
                    and (now - max(self._last_turn, self._last_voice_audio, self._last_dg_text or 0.0)) > self.cfg.silence_timeout_s):
                print("[WATCHDOG] silence timeout")
                await self._hangup()
                break

    # ── Twilio events ────────────────────────────────────────────────────────

    def _fanout_nowait(self, payload_b64: str, who: str):
        """Buffer frames; flush in batches. Drops frames if listeners lag."""
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

    def _flush_fanout_nowait(self):
        if not self._fanout_buf or self._fanout_inflight:
            return
        batch, self._fanout_buf = self._fanout_buf, []
        self._fanout_inflight += 1
        asyncio.create_task(self._flush_fanout(batch))

    async def _flush_fanout_soon(self):
        await asyncio.sleep(0.12)
        self._flush_fanout_nowait()

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
            if self._fanout_buf and self.listeners:
                if len(self._fanout_buf) >= 8:
                    self._flush_fanout_nowait()
                elif not self._fanout_flush_task or self._fanout_flush_task.done():
                    self._fanout_flush_task = asyncio.create_task(self._flush_fanout_soon())

    async def _on_twilio_event(self, data: dict):
        evt = data.get("event")

        if evt == "connected":
            await self._push_status("connected", "Call connected")

        elif evt == "start":
            self.stream_sid = data.get("streamSid")
            start = data.get("start", {})
            self.call_sid = start.get("callSid")
            self.metrics.call_sid = self.call_sid or ""            
            cp = start.get("customParameters", {}) or {}
            if cp:
                self.lead_info = {
                    "Name":     cp.get("name", ""),
                    "Phone":    cp.get("phone", ""),
                    "City":     cp.get("city", ""),
                    "Category": cp.get("category", ""),
                }
            print(f"[STREAM START] {self.call_sid} lead={self.lead_info}")
            await self._push_status("active", "Call active")
            if not self._initial_turn_task or self._initial_turn_task.done():
                self._initial_turn_task = asyncio.create_task(self._begin_when_ready())

        elif evt == "media":
            self._last_audio = time.time()
            payload = data.get("media", {}).get("payload", "")
            if not payload:
                return
            raw = base64.b64decode(payload)
            self._rec_prospect.extend(raw)            
            if self.listeners:
                self._fanout_nowait(payload, "prospect")

            try:
                pcm = audioop.ulaw2lin(raw, 2)
                rms = audioop.rms(pcm, 2)
            except Exception:
                pcm = None
                rms = 0
            if rms > self._vad_threshold:
                self._last_voice_audio = self._last_audio
                if not self._opening_started:
                    self._startup_audio_seen = True

            # Grace period — never let barge-in kill the agent's first words.
            # Echo and trailing speech from the prospect cause false triggers.
            grace_ok = (time.time() - self._speak_started) > 0.9
            if self.is_speaking and self.cfg.allow_interruption and grace_ok:
                if rms > self._vad_threshold:
                    self._loud_frames += 1
                    if self._loud_frames >= 10:     # ~200ms      # ~60ms of speech
                        print(f"[VAD] barge-in (rms={rms})")
                        self._speak_seq += 1
                        await self._stop_speaking()
                        self._loud_frames = 0
                else:
                    self._loud_frames = max(0, self._loud_frames - 1)

            if self.dg_ws:
                try:
                    await self.dg_ws.send(raw)
                except Exception as e:
                    print(f"[DG SEND] {e}")

        elif evt == "stop":
            print("[STREAM STOP] media stream closed")
            self._stop = True

    # ── Conversation ─────────────────────────────────────────────────────────

    def _system_message(self) -> str:
        tone_note = TONE_NOTES.get(self.cfg.tone, "Be natural and confident.")
        business  = self.lead_info.get("Name") or "your business"
        city      = self.lead_info.get("City") or ""
        category  = self.lead_info.get("Category") or "plumbing business"

        context = f"\n\nLEAD CONTEXT: {business}, a {category}" + (f" in {city}." if city else ".")
        return render_template(self.cfg.system_prompt, self.lead_info) + context + f"\n\nTONE: {tone_note}"

    def _ensure_conversation_context(self):
        if not self.conversation:
            self.conversation = [{"role": "system", "content": self._system_message()}]

    async def _begin_when_ready(self):
        self._ensure_conversation_context()
        await self._push_status("listening", "Listening for answer...")
        deadline = time.time() + max(4, self.cfg.silence_timeout_s)

        while not self._stop and not self._opening_started and not self._prospect_spoke:
            if time.time() > deadline:
                print("[STARTUP] no answer transcript detected")
                await self._push_status("ended", "No human detected")
                await self._hangup()
                return
            await asyncio.sleep(0.2)

    async def _begin_conversation(self):
        if self._opening_started:
            return

        opening = render_template(self.cfg.first_message, self.lead_info)
        print(f"[LEAD] {self.lead_info}")
        print(f"[OPENING] {opening}")

        self._ensure_conversation_context()
        self.conversation.append({"role": "assistant", "content": opening})
        self._script_step = "owner_check"
        self._opening_started = True
        await asyncio.sleep(0.6)
        await self._push_transcript("agent", opening)
        self._speak_seq += 1
        self._last_turn = time.time()
        await self._speak_chunk(opening, self._speak_seq)

    async def _handle_transcript(self, text: str):
        if not text.strip() or self._stop:
            return

        low = text.lower().strip(" .,?!")
        if self._turn_lock.locked() and (
                self.is_speaking or self._is_ambiguous_greeting(text)
                or "can you hear me" in low or low in {"hello", "hi", "hey"}):
            print(f"[TURN] dropping overlapping transcript: {text[:80]}")
            return

        async with self._turn_lock:
            now = time.time()
            norm = re.sub(r"\s+", " ", low)
            if norm and norm == self._last_handled_text and (now - self._last_handled_at) < 4:
                print(f"[TURN] dropping duplicate transcript: {text[:80]}")
                return
            self._last_handled_text = norm
            self._last_handled_at = now
            await self._handle_transcript_locked(text)

    async def _handle_transcript_locked(self, text: str):
        if not text.strip() or self._stop:
            return

        if self._voicemail_task and not self._voicemail_task.done() and self._looks_like_human_greeting(text):
            print(f"[MACHINE] cancelled voicemail; human greeting detected: {text[:80]}")
            self._voicemail_task.cancel()
            self._voicemail_task = None
            self._machine_kind = ""
            self.outcome = "conversation"

        kind = self._classify_machine(text)

        if kind == "announcement":
            print(f"[MACHINE] announcement: {text[:80]}")
            self._startup_audio_seen = True
            if self.is_speaking:
                self._speak_seq += 1
                await self._stop_speaking()
            return

        if kind == "menu":
            print(f"[MACHINE] phone tree: {text[:60]}")
            self.outcome = "ivr"
            await self._push_status("ended", "Phone tree — cannot leave message")
            await self._hangup()
            return

        if kind == "voicemail":
            if self._vm_left or (self._voicemail_task and not self._voicemail_task.done()):
                return
            self._machine_kind = "voicemail"
            print(f"[MACHINE] voicemail: {text[:60]}")
            if self.cfg.voicemail_enabled:
                self._voicemail_task = asyncio.create_task(self._leave_voicemail())
            else:
                self.outcome = "voicemail"
                await self._push_status("ended", "Voicemail — skipped")
                await self._hangup()
            return

        if self._looks_like_business_greeting_only(text):
            print(f"[GREETING] waiting for more context: {text[:80]}")
            self._startup_audio_seen = True
            return

        self._ensure_conversation_context()
        self._awaiting_response_since = None
        self._last_turn = time.time()
        self._nudges = 0
        self._prospect_spoke = True
        if self.outcome == "no_answer":
            self.outcome = "conversation"

        self.conversation.append({"role": "user", "content": text})

        if self._gatekeeper_mode and await self._handle_gatekeeper_transcript(text):
            return

        if await self._maybe_scripted_opening_response(text):
            return

        if not self._decision_maker_confirmed:
            if await self._handle_unconfirmed_transcript(text):
                return

        self._opening_started = True
        response = await self._respond()

        # Record latency for this turn
        if self._t_stt_done and self._t_llm_first and self._t_tts_first:
            stt_lat = 0.0
            llm_lat = self._t_llm_first - self._t_stt_done
            tts_lat = self._t_tts_first - self._t_llm_first
            self.metrics.record_turn(stt_lat, max(0, llm_lat), max(0, tts_lat))
            print(f"[LATENCY] llm={llm_lat*1000:.0f}ms tts={tts_lat*1000:.0f}ms "
                  f"total={(llm_lat+tts_lat)*1000:.0f}ms")
            self._t_llm_first = 0.0
            self._t_tts_first = 0.0

        if not response:
            return

        clean = response.replace("[HANGUP]", "").strip()
        if clean:
            self.conversation.append({"role": "assistant", "content": clean})

        low = clean.lower()
        # Classify interest from what the agent asked for
        if any(k in low for k in ("what day works", "callback", "spell that",
                                  "best number to reach", "our team will reach out")):
            self.outcome = "interested"
        elif any(k in low for k in ("no problem at all", "have a great day",
                                    "i'll let you go")):
            if self.outcome != "interested":
                self.outcome = "not_interested"

        if "[HANGUP]" in response or any(p in low for p in self.end_phrases):
            await asyncio.sleep(1)
            await self._hangup()

    def _is_hard_no(self, text: str) -> bool:
        low = text.lower()
        return any(p in low for p in (
            "not interested", "no thanks", "no thank you", "we're good", "we are good",
            "take me off", "remove me", "don't call", "do not call", "stop calling",
            "wrong number", "goodbye", "bye",
        ))

    def _needs_flexible_response(self, text: str) -> bool:
        low = text.lower()
        return any(k in low for k in (
            "who", "what", "why", "how", "price", "cost", "much", "ai", "robot",
            "where", "number", "email", "info", "information",
        ))

    def _looks_like_gatekeeper(self, text: str) -> bool:
        low = text.lower()
        if any(k in low for k in (
            "how may i help", "how can i help", "how may we help", "how can we help",
            "how may i assist", "how can i assist", "can i help", "may i help",
            "how can i direct", "how may i direct", "what can i get you",
            "what can i help", "how can i get you", "this is", "speaking with",
        )):
            return True
        return bool(re.search(r"\b(great|good) day at\b", low))

    def _confirms_owner(self, text: str) -> bool:
        low = text.lower().strip(" .,?!")
        return (
            low in {
                "yes", "yeah", "yep", "speaking", "this is he", "this is she",
                "this is him", "this is her", "this is me", "that's me", "that is me",
            }
            or any(k in low for k in (
                "i'm the owner", "i am the owner", "owner speaking", "this is the owner",
                "i handle that", "i make those decisions", "i'm the manager", "i am the manager",
            ))
        )

    def _denies_owner(self, text: str) -> bool:
        low = text.lower().strip(" .,?!")
        if low in {"no", "nope", "nah"}:
            return True
        return any(k in low for k in (
            "not the owner", "not the decision maker", "not a decision maker",
            "i'm just", "i am just", "receptionist", "front desk", "office",
            "employee", "staff", "assistant", "i can take a message",
            "take a message", "pass along", "pass a message",
        ))

    def _owner_unavailable(self, text: str) -> bool:
        low = text.lower()
        return any(k in low for k in (
            "isn't here", "is not here", "not here", "isn't available", "isnt available",
            "is not available", "not available", "unavailable", "owner isn't available",
            "owner is not available", "owner's not available", "out right now",
            "away right now", "he's out", "she's out", "he is out", "she is out",
            "owner is out", "owner's out",
        ))

    def _lead_timezone(self) -> str:
        for key in ("Timezone", "Time Zone", "timezone", "time_zone", "TZ", "tz"):
            val = str(self.lead_info.get(key, "")).strip()
            if val:
                return val
        return ""

    def _requests_better_time(self, text: str) -> bool:
        low = text.lower()
        return any(k in low for k in (
            "better time", "call back", "callback", "try later", "another time",
            "not right now", "busy right now", "too busy", "later",
        ))

    def _has_callback_time_detail(self, text: str) -> bool:
        low = text.lower().strip(" .,?!")
        vague = {
            "a better time", "a better time to call", "better time", "better time to call",
            "call back", "call back later", "later", "another time", "try later",
        }
        if low in vague:
            return False
        if re.search(r"\b\d{1,2}(:\d{2})?\s*(am|pm|a\.m\.|p\.m\.)?\b", low):
            return True
        return any(k in low for k in (
            "morning", "afternoon", "evening", "noon", "lunch", "tomorrow", "today",
            "monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday",
            "next week", "this week", "after", "before",
        ))

    async def _save_callback_time_and_end(self, text: str):
        tz = self._lead_timezone()
        when = text.strip()
        self.outcome = "callback"
        self.followup_note = f"Callback requested: {when}"
        if tz:
            self.followup_note += f" ({tz})"
        line = f"Got it, I'll note that {when}"
        if tz:
            line += f" {tz}"
        line += ". Thanks for your help, have a good day. [HANGUP]"
        await self._send_agent_line(line)

    def _is_ambiguous_greeting(self, text: str) -> bool:
        low = text.lower().strip(" .,?!")
        return low in {
            "hi", "hello", "hey", "good morning", "good afternoon", "good evening",
            "yes", "yeah", "yep", "uh huh", "okay", "ok",
        }

    async def _handle_unconfirmed_transcript(self, text: str) -> bool:
        """Keep pre-confirmation turns safe: route first, never pitch."""
        if self._confirms_owner(text):
            self._decision_maker_confirmed = True
            self._gatekeeper_mode = False
            self._script_step = "done"
            self.conversation.append({
                "role": "system",
                "content": (
                    "The last reply confirmed this is the owner or decision maker. "
                    "Continue naturally using the pitch reference. Keep it brief and ask one clear next-step question."
                ),
            })
            return False

        self._script_step = "done"

        if self._is_hard_no(text):
            await self._send_agent_line("No problem, thanks for your time. Have a good day. [HANGUP]")
            return True

        if self._awaiting_callback_time:
            if self._has_callback_time_detail(text):
                await self._save_callback_time_and_end(text)
            else:
                await self._send_agent_line("Sure, what day and time is usually best?")
            return True

        if self._requests_better_time(text):
            self._awaiting_callback_time = True
            await self._send_agent_line("Sure, what day and time is usually best?")
            return True

        if self._owner_unavailable(text):
            self._gatekeeper_mode = True
            self._gatekeeper_contact_asked = True
            await self._send_agent_line(
                "No worries. What's the best way to reach the owner or office manager directly?"
            )
            return True

        if self._looks_like_gatekeeper(text) or self._denies_owner(text):
            self._gatekeeper_mode = True
            line = "No problem. Is the owner or office manager available?"
            if not self._opening_started:
                line = "Hi, this is Anna with Lynkflow. Is the owner or office manager available?"
            await self._send_agent_line(
                line
            )
            return True

        if self._asks_agent_identity(text):
            await self._send_agent_line(
                "This is Anna with Lynkflow. I was trying to reach whoever handles customer calls for the business. Is that you?"
            )
            return True

        if self._asks_reason(text):
            await self._send_agent_line(
                "It's about customer calls for the business, but I only want to speak with whoever handles that. Is that you or someone else?"
            )
            return True

        if self._is_ambiguous_greeting(text):
            await self._send_agent_line(
                "Hi, this is Anna with Lynkflow. Are you the owner or office manager?"
            )
            return True

        await self._send_agent_line(
            "Hi, this is Anna with Lynkflow. Are you the owner or the person who handles customer calls?"
        )
        return True

    async def _maybe_scripted_opening_response(self, text: str) -> bool:
        """Keep the first two turns on track before handing off to GPT."""
        if self._script_step not in ("owner_check", "pitch", "hook"):
            return False

        if self._is_hard_no(text):
            self._script_step = "done"
            await self._send_agent_line("No problem at all, have a great day! [HANGUP]")
            return True

        if self._script_step == "owner_check":
            if self._confirms_owner(text):
                self._script_step = "done"
                self._gatekeeper_mode = False
                self._decision_maker_confirmed = True
                self.conversation.append({
                    "role": "system",
                    "content": (
                        "The last reply confirmed this is the owner or decision maker. "
                        "Continue naturally using the pitch reference. Keep it brief and ask one clear next-step question."
                    ),
                })
                return False

            if self._owner_unavailable(text):
                self._script_step = "done"
                self._gatekeeper_mode = True
                self._gatekeeper_contact_asked = True
                await self._send_agent_line(
                    "No worries. What's the best way to reach them directly - phone, email, or a better time to call?"
                )
                return True

            if self._looks_like_gatekeeper(text) or self._denies_owner(text):
                return await self._handle_unconfirmed_transcript(text)

            if self._needs_flexible_response(text):
                return await self._handle_unconfirmed_transcript(text)

            return await self._handle_unconfirmed_transcript(text)

        if self._script_step == "pitch" and self._looks_like_gatekeeper(text):
            self._script_step = "done"
            self._gatekeeper_mode = True
            await self._send_agent_line("No problem. Is the owner or office manager available?")
            return True
        if self._needs_flexible_response(text):
            if not self._decision_maker_confirmed:
                return await self._handle_unconfirmed_transcript(text)
            self._script_step = "done"
            return False

        if self._script_step == "pitch":
            self._script_step = "hook"
            await self._send_agent_line(
                "We help plumbing businesses stop losing jobs to missed calls. "
                "We build an AI system that answers your calls automatically and books jobs while you're on site."
            )
            return True

        self._script_step = "done"
        await self._send_agent_line(
            "Would a quick 10 minute call with our team be worth it to see if it fits your business?"
        )
        return True

    def _asks_reason(self, text: str) -> bool:
        low = text.lower()
        return any(k in low for k in (
            "what's this about", "what is this about", "what is it about", "what's it about",
            "tell me more", "more info", "bit more", "a bit more", "what for", "why are you calling",
            "regarding", "in regards to", "what do you need",
        ))

    def _offers_transfer_or_message(self, text: str) -> bool:
        low = text.lower()
        return any(k in low for k in (
            "take a message", "pass your info", "pass your information", "pass it along",
            "get you to the right person", "right person", "send a message", "relay a message",
        ))

    def _asks_agent_identity(self, text: str) -> bool:
        low = text.lower()
        return any(k in low for k in (
            "your name", "who are you", "who's calling", "who is calling",
            "company name", "where are you calling from", "number for callback",
        ))

    async def _handle_gatekeeper_transcript(self, text: str) -> bool:
        if self._confirms_owner(text):
            self._gatekeeper_mode = False
            self._decision_maker_confirmed = True
            self._script_step = "done"
            self.conversation.append({
                "role": "system",
                "content": (
                    "The last reply confirmed this is the owner or decision maker. "
                    "Continue naturally using the pitch reference. Keep it brief and ask one clear next-step question."
                ),
            })
            return False

        if self._is_hard_no(text):
            await self._send_agent_line("No problem, I'll try another time. Thanks for your help, have a good day! [HANGUP]")
            return True

        if self._awaiting_callback_time:
            if self._has_callback_time_detail(text):
                await self._save_callback_time_and_end(text)
            else:
                await self._send_agent_line("Sure, what day and time is usually best?")
            return True

        if self._requests_better_time(text):
            self._awaiting_callback_time = True
            await self._send_agent_line("Sure, what day and time is usually best?")
            return True

        if self._asks_agent_identity(text):
            cb = self._callback_number() or "the number I called from"
            await self._send_agent_line(
                f"My name is Anna with Lynkflow, and the best callback is {cb}. What's the best way to reach the owner or office manager directly?"
            )
            self._gatekeeper_contact_asked = True
            return True

        if self._asks_reason(text):
            await self._send_agent_line(
                "It's about customer calls for the business. "
                "What's the best way to reach the owner or office manager directly?"
            )
            self._gatekeeper_contact_asked = True
            return True

        if self._owner_unavailable(text) or self._offers_transfer_or_message(text) or self._denies_owner(text):
            if self._gatekeeper_contact_asked:
                await self._send_agent_line(
                    "Sure, please let them know Anna from Lynkflow called about customer calls for the business. "
                    "What's the best callback number or email for them?"
                )
            else:
                await self._send_agent_line(
                    "No worries. What's the best way to reach them directly - phone, email, or a better time to call?"
                )
                self._gatekeeper_contact_asked = True
            return True

        if not self._gatekeeper_contact_asked:
            await self._send_agent_line(
                "What's the best way to reach the owner or office manager - phone, email, or a better time to call?"
            )
            self._gatekeeper_contact_asked = True
            return True

        await self._send_agent_line(
            "Got it. Please let them know Anna from Lynkflow called about customer calls for the business. Thanks for your help, have a good day! [HANGUP]"
        )
        return True

    async def _send_agent_line(self, line: str):
        clean = line.replace("[HANGUP]", "").strip()
        if clean:
            self._ensure_conversation_context()
            self._opening_started = True
            self.conversation.append({"role": "assistant", "content": clean})
            await self._push_transcript("agent", clean)
            self.metrics.turns += 1
            self._speak_seq += 1
            await self._speak_chunk(clean, self._speak_seq)

        low = clean.lower()
        if any(k in low for k in ("no problem at all", "have a great day", "i'll let you go")):
            if self.outcome != "interested":
                self.outcome = "not_interested"
        if "[HANGUP]" in line or any(p in low for p in self.end_phrases):
            await asyncio.sleep(0.8)
            await self._hangup()

    # ── Voicemail ────────────────────────────────────────────────────────────

    def _callback_number(self) -> str:
        return (self.cfg.callback_number or os.getenv("TWILIO_CALLER_ID", "")).strip()

    @staticmethod
    def _space_digits(num: str) -> str:
        """+19786843590 -> 9 7 8. 6 8 4. 3 5 9 0 — makes TTS read it clearly."""
        d = "".join(ch for ch in num if ch.isdigit())
        if len(d) == 11 and d.startswith("1"):
            d = d[1:]
        if len(d) != 10:
            return " ".join(d)
        return f"{' '.join(d[:3])}. {' '.join(d[3:6])}. {' '.join(d[6:])}"

    async def _leave_voicemail(self):
        """Wait for the beep, deliver the message, then hang up."""
        await self._push_status("speaking", "Waiting for beep…")

        # Let the greeting finish — watch for a gap in incoming speech
        self._last_dg_text = time.time()
        deadline = time.time() + 25
        while time.time() < deadline and not self._stop:
            await asyncio.sleep(0.4)
            if time.time() - self._last_dg_text > 2.0:
                break

        if self._stop:
            return

        await asyncio.sleep(0.8)   # pause after the beep

        if self._machine_kind != "voicemail":
            return

        self._vm_left = True
        self.outcome = "voicemail_left"

        cb = self._callback_number()
        msg = (self.cfg.voicemail_message
               .replace("{business}", self.lead_info.get("Name") or "your business")
               .replace("{callback_spaced}", self._space_digits(cb))
               .replace("{callback}", cb))

        print(f"[VOICEMAIL] leaving message ({len(msg)} chars)")
        await self._push_transcript("agent", f"[voicemail] {msg}")

        self._speak_seq += 1
        await self._speak_chunk(msg, self._speak_seq)
        await asyncio.sleep(1.5)

        await self._hangup()            

    # ── GPT + TTS streaming pipeline ─────────────────────────────────────────

    # ── Streaming pipeline: GPT tokens -> ElevenLabs WS -> Twilio ────────────

    async def _respond(self) -> Optional[str]:
        """
        Opens one ElevenLabs WebSocket, streams GPT tokens into it as they
        arrive, and forwards audio to Twilio continuously. Single generation
        means no seams between sentences.
        """
        self._speak_seq += 1
        seq = self._speak_seq
        full = ""

        ws_url = (
            f"wss://api.elevenlabs.io/v1/text-to-speech/{self.cfg.voice_id}/stream-input"
            f"?model_id=eleven_flash_v2_5"
            f"&output_format=ulaw_8000"
            f"&auto_mode=true"
            f"&inactivity_timeout=20"
        )

        tts_ws = None
        audio_task = None
        t_start = time.time()

        try:
            headers = {"xi-api-key": ELEVENLABS_API_KEY}
            try:
                tts_ws = await websockets.connect(ws_url, additional_headers=headers)
            except TypeError:
                tts_ws = await websockets.connect(ws_url, extra_headers=headers)

            # Initialise the stream
            await tts_ws.send(json.dumps({
                "text": " ",
                "voice_settings": {
                    "stability":        self.cfg.stability,
                    "similarity_boost": self.cfg.similarity_boost,
                    "style":            self.cfg.style,
                    "speed":            self.cfg.speaking_rate,
                },
            }))

            self.is_speaking = True
            self._speak_started = time.time()
            await self._push_status("speaking", "Agent speaking…")

            # Pump audio out in the background while text streams in
            audio_task = asyncio.create_task(self._pump_tts_audio(tts_ws, seq, t_start))

            # Stream GPT tokens straight into the TTS socket
            pending_tts = ""
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
                            break
                        if not line.startswith("data: "):
                            continue
                        payload = line[6:]
                        if payload == "[DONE]":
                            break
                        try:
                            tok = json.loads(payload)["choices"][0]["delta"].get("content", "")
                        except Exception:
                            continue
                        if not tok:
                            continue

                        if not full:
                            self._t_llm_first = time.time()
                        full += tok

                        # Never speak the control token
                        speakable = tok.replace("[HANGUP]", "")
                        if speakable:
                            pending_tts += speakable
                            if self._should_flush_tts_text(pending_tts):
                                try:
                                    await tts_ws.send(json.dumps({"text": pending_tts.strip() + " "}))
                                    pending_tts = ""
                                except Exception:
                                    break

                        # Throttled UI update
                        now = time.time()
                        if now - self._last_partial > 0.15:
                            self._last_partial = now
                            await self.status_queue.put({
                                "type": "transcript_partial", "speaker": "agent",
                                "text": full.replace("[HANGUP]", "").strip(),
                                "ts": datetime.now().strftime("%H:%M:%S"),
                            })

            # Signal end of input
            if seq == self._speak_seq and not self._stop:
                try:
                    if pending_tts.strip():
                        await tts_ws.send(json.dumps({"text": pending_tts.strip() + " "}))
                    await tts_ws.send(json.dumps({"text": ""}))
                except Exception:
                    pass

            # Let the audio finish draining
            if audio_task:
                try:
                    await asyncio.wait_for(audio_task, timeout=45)
                except asyncio.TimeoutError:
                    audio_task.cancel()

        except Exception as e:
            print(f"[PIPELINE ERROR] {e}")
        finally:
            if audio_task and not audio_task.done():
                audio_task.cancel()
            if tts_ws:
                try:
                    await tts_ws.close()
                except Exception:
                    pass
            if seq == self._speak_seq:
                self.is_speaking = False
                self._last_audio = time.time()
                self._last_turn = time.time()
                self._mark_listening_for_response()
                if not self._stop:
                    await self._push_status("listening", "Listening…")

        final = full.strip().replace("[HANGUP]", "").strip()
        if final:
            await self.status_queue.put({
                "type": "transcript_final", "speaker": "agent",
                "text": final, "ts": datetime.now().strftime("%H:%M:%S"),
            })

        # Rough token accounting
        convo_chars = sum(len(m.get("content", "")) for m in self.conversation)
        self.metrics.tokens_in  += convo_chars // 4
        self.metrics.tokens_out += len(full) // 4
        self.metrics.tts_chars  += len(final)

        return full.strip() or None

    @staticmethod
    def _should_flush_tts_text(text: str) -> bool:
        stripped = text.strip()
        if not stripped:
            return False
        if len(stripped) < 35 and not stripped.endswith(('.', '?', '!')):
            return False
        if stripped.endswith(('.', '?', '!', ';', ':')):
            return True
        if len(stripped) >= 80 and text[-1].isspace():
            return True
        if len(stripped) >= 120:
            return True
        return False

    async def _pump_tts_audio(self, tts_ws, seq: int, t_start: float):
        """Read audio frames off the TTS socket and pace them into Twilio."""
        sent = 0
        first = True
        buf = b""
        playback_started = None

        try:
            async for raw in tts_ws:
                if self._stop or seq != self._speak_seq or not self.is_speaking:
                    return
                try:
                    msg = json.loads(raw)
                except Exception:
                    continue

                audio_b64 = msg.get("audio")
                if audio_b64:
                    if first:
                        first = False
                        self._t_tts_first = time.time()
                        print(f"[TTS] first audio {(time.time()-t_start)*1000:.0f}ms")

                    buf += base64.b64decode(audio_b64)

                    # Emit whole 20ms frames (160 bytes of mulaw at 8kHz)
                    n = (len(buf) // 160) * 160
                    for i in range(0, n, 160):
                        if self._stop or seq != self._speak_seq or not self.is_speaking:
                            return
                        frame = base64.b64encode(buf[i:i+160]).decode()
                        await self.twilio_ws.send_text(json.dumps({
                            "event":     "media",
                            "streamSid": self.stream_sid,
                            "media":     {"payload": frame},
                        }))
                        if playback_started is None:
                            playback_started = time.time()
                        if self.listeners:
                            self._fanout_nowait(frame, "agent")
                        sent += 1
                        self._record_agent_frame(buf[i:i+160])
                        await self._pace_tts_audio(sent, playback_started)
                    buf = buf[n:]

                if msg.get("isFinal"):
                    break

            # Flush any tail
            if buf and not self._stop and seq == self._speak_seq:
                tail = buf.ljust(160, b"\xff")[:160]
                frame = base64.b64encode(tail).decode()
                await self.twilio_ws.send_text(json.dumps({
                    "event": "media", "streamSid": self.stream_sid,
                    "media": {"payload": frame},
                }))
                if playback_started is None:
                    playback_started = time.time()
                self._record_agent_frame(tail)
                sent += 1
                await self._pace_tts_audio(sent, playback_started, force=True)

            if self._fanout_buf:
                self._flush_fanout_nowait()

            print(f"[TTS] {sent} frames ({sent*0.02:.1f}s audio) in {time.time()-t_start:.2f}s")

            # Hold the speaking flag for any audio still buffered in Twilio
            await asyncio.sleep(0.3)

        except asyncio.CancelledError:
            raise
        except Exception as e:
            print(f"[TTS PUMP] {e}")

    async def _speak_chunk(self, text: str, seq: int):
        """One-shot TTS for fixed text (opening line, voicemail)."""
        if self._stop or seq != self._speak_seq:
            return

        self.is_speaking = True
        self._speak_started = time.time()
        self.metrics.tts_chars += len(text)
        await self._push_status("speaking", "Agent speaking…")

        t0 = time.time()
        sent = 0
        ws_url = (
            f"wss://api.elevenlabs.io/v1/text-to-speech/{self.cfg.voice_id}/stream-input"
            f"?model_id=eleven_flash_v2_5&output_format=ulaw_8000&auto_mode=true"
        )
        tts_ws = None
        try:
            headers = {"xi-api-key": ELEVENLABS_API_KEY}
            try:
                tts_ws = await websockets.connect(ws_url, additional_headers=headers)
            except TypeError:
                tts_ws = await websockets.connect(ws_url, extra_headers=headers)

            await tts_ws.send(json.dumps({
                "text": " ",
                "voice_settings": {
                    "stability":        self.cfg.stability,
                    "similarity_boost": self.cfg.similarity_boost,
                    "style":            self.cfg.style,
                    "speed":            self.cfg.speaking_rate,
                },
            }))
            await tts_ws.send(json.dumps({"text": text}))
            await tts_ws.send(json.dumps({"text": ""}))

            buf = b""
            playback_started = None
            async for raw in tts_ws:
                if self._stop or seq != self._speak_seq or not self.is_speaking:
                    return
                try:
                    msg = json.loads(raw)
                except Exception:
                    continue
                if msg.get("audio"):
                    buf += base64.b64decode(msg["audio"])
                    n = (len(buf) // 160) * 160
                    for i in range(0, n, 160):
                        if self._stop or seq != self._speak_seq or not self.is_speaking:
                            return
                        frame = base64.b64encode(buf[i:i+160]).decode()
                        await self.twilio_ws.send_text(json.dumps({
                            "event": "media", "streamSid": self.stream_sid,
                            "media": {"payload": frame},
                        }))
                        if playback_started is None:
                            playback_started = time.time()
                        if self.listeners:
                            self._fanout_nowait(frame, "agent")
                        sent += 1
                        self._record_agent_frame(buf[i:i+160])                        
                        await self._pace_tts_audio(sent, playback_started)
                    buf = buf[n:]
                if msg.get("isFinal"):
                    break

            if buf and not self._stop and seq == self._speak_seq:
                tail = buf.ljust(160, b"\xff")[:160]
                frame = base64.b64encode(tail).decode()
                await self.twilio_ws.send_text(json.dumps({
                    "event": "media", "streamSid": self.stream_sid,
                    "media": {"payload": frame},
                }))
                if playback_started is None:
                    playback_started = time.time()
                self._record_agent_frame(tail)
                sent += 1
                await self._pace_tts_audio(sent, playback_started, force=True)

            print(f"[TTS] {sent} frames ({sent*0.02:.1f}s) in {time.time()-t0:.2f}s :: {text[:45]}")

        except Exception as e:
            print(f"[TTS ERROR] {e}")
        finally:
            if tts_ws:
                try:
                    await tts_ws.close()
                except Exception:
                    pass
            if seq == self._speak_seq:
                await asyncio.sleep(0.3)
                self.is_speaking = False
                self._last_audio = time.time()
                self._last_turn = time.time()
                self._mark_listening_for_response()
                if not self._stop:
                    await self._push_status("listening", "Listening…")                

    def _record_agent_frame(self, frame: bytes):
        """Pad agent audio so the mixed WAV preserves call timing."""
        if len(self._rec_agent) < len(self._rec_prospect):
            self._rec_agent.extend(b"\xff" * (len(self._rec_prospect) - len(self._rec_agent)))
        self._rec_agent.extend(frame)

    async def _pace_tts_audio(self, sent_frames: int, started_at: float, force: bool = False):
        """Keep Twilio/browser monitor audio close to real-time playback."""
        if not started_at:
            return
        if not force and sent_frames % 5:
            return
        target_elapsed = sent_frames * 0.02
        delay = target_elapsed - (time.time() - started_at)
        if delay > 0:
            await asyncio.sleep(delay)


    async def _stop_speaking(self):
        self.is_speaking = False
        await self.status_queue.put({
            "type": "transcript_cancel", "speaker": "agent",
        })
        if self.stream_sid:
            try:
                await self.twilio_ws.send_text(json.dumps({
                    "event": "clear", "streamSid": self.stream_sid,
                }))
                print("[TTS] buffer cleared")
            except Exception as e:
                print(f"[CLEAR ERROR] {e}")

    def _save_recording(self):
        """Mix both legs into one WAV. Audio is already in memory — free."""
        import audioop, wave
        from pathlib import Path

        if not self._rec_prospect and not self._rec_agent:
            return None

        rec_dir = Path(__file__).parent / "recordings"
        rec_dir.mkdir(exist_ok=True)

        try:
            pro = audioop.ulaw2lin(bytes(self._rec_prospect), 2)
            agt = audioop.ulaw2lin(bytes(self._rec_agent), 2)

            # Pad to equal length, then mix
            n = max(len(pro), len(agt))
            pro = pro.ljust(n, b"\x00")
            agt = agt.ljust(n, b"\x00")
            mixed = audioop.add(pro, agt, 2)

            name = (self.lead_info.get("Name") or "unknown")[:40]
            name = "".join(c if c.isalnum() or c in " -_" else "" for c in name).strip()
            stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            path = rec_dir / f"{stamp}_{name or 'call'}_{(self.call_sid or '')[-6:]}.wav"

            with wave.open(str(path), "wb") as w:
                w.setnchannels(1)
                w.setsampwidth(2)
                w.setframerate(8000)
                w.writeframes(mixed)

            kb = path.stat().st_size / 1024
            print(f"[RECORDING] {path.name} ({kb:.0f} KB, {len(mixed)/16000:.1f}s)")
            return path.name

        except Exception as e:
            print(f"[RECORDING] failed: {e}")
            return None                

    # ── Deepgram ─────────────────────────────────────────────────────────────

    async def _connect_deepgram(self):
        if not DEEPGRAM_API_KEY:
            print("[DG] DEEPGRAM_API_KEY not set — agent cannot hear")
            return

        url = (
            "wss://api.deepgram.com/v1/listen"
            "?encoding=mulaw&sample_rate=8000&channels=1&model=nova-3&language=en-US"
            "&smart_format=true&punctuate=true&filler_words=false"
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

                self._last_dg_text = time.time()

                # Barge-in — cancel current speech immediately
                # "hello", "who's calling", "yeah" etc. are the prospect
                # engaging, not interrupting. Require a real interruption.
                FILLER = {"hello", "hi", "hey", "yeah", "yes", "no", "okay", "ok",
                          "uh", "um", "mhmm", "hmm", "sure", "right", "what"}
                words = text.lower().strip(" .,?!").split()
                is_filler = len(words) <= 2 and all(w in FILLER for w in words)

                grace_ok = (time.time() - self._speak_started) > 1.2
                if (self.is_speaking and self.cfg.allow_interruption
                        and grace_ok and not is_filler and len(text) > 8):
                    print(f"[DG] barge-in: {text}")
                    self.metrics.interrupts += 1
                    self._speak_seq += 1        # invalidates in-flight TTS
                    await self._stop_speaking()

                if data.get("is_final"):
                    self._t_stt_done = time.time()
                    print(f"[DG] final: {text}")
                    await self.status_queue.put({
                        "type": "transcript_final", "speaker": "prospect",
                        "text": text, "ts": datetime.now().strftime("%H:%M:%S"),
                    })
                    asyncio.create_task(self._handle_transcript(text))
                else:
                    await self.status_queue.put({
                        "type": "transcript_partial", "speaker": "prospect",
                        "text": text, "ts": datetime.now().strftime("%H:%M:%S"),
                    })

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
        if getattr(self, "_cleaned", False):
            return
        self.recording_file = self._save_recording()        
        self._stop = True
        self.metrics.ended = time.time()
        snap = self.metrics.snapshot()
        print(f"[METRICS] {snap}")
        try:
            await self.status_queue.put({"type": "metrics", **snap})
        except Exception:
            pass
        self.metrics.ended = time.time()
        snap = self.metrics.snapshot()
        print(f"[METRICS] {snap}")
        try:
            await self.status_queue.put({"type": "metrics", **snap})
        except Exception:
            pass
        if self.dg_ws:
            try:
                await self.dg_ws.close()
            except Exception:
                pass
