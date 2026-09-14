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
        self._ivr_hits = 0
        self.listeners: set = set()      # browser WebSockets listening in
        self._last_partial = 0.0
        self._fanout_buf: list = []
        self._fanout_inflight = 0
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
        self._call_start  = time.time()
        self._speak_seq     = 0
        self._loud_frames   = 0      # consecutive frames above threshold
        self._vad_threshold = 2500    # RMS level that counts as speech
        self.outcome = "no_answer"        

        self.end_phrases = [
            p.strip().lower()
            for p in (cfg.end_call_phrases or "").split(",")
            if p.strip()
        ]

    # ── Entry ────────────────────────────────────────────────────────────────

    IVR_MARKERS = [
        "press one", "press two", "press three", "press zero",
        "press 1", "press 2", "press 3", "press 0",
        "dial by name", "extension number", "main menu",
        "for quality assurance", "may be monitored", "may be recorded",
        "please listen", "options have changed", "please hold",
        "leave a message", "after the tone", "business hours are",
        "answering service", "please stay on the line",
        "unable to take your call", "leave your name",
        "brief description", "call you back as soon as",
        "thank you for calling", "we are currently closed",
        "at the tone", "record your message",
    ]

    def _looks_like_ivr(self, text: str) -> bool:
        low = text.lower()
        return any(m in low for m in self.IVR_MARKERS)

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

    async def _watchdog(self):
        """Silence nudges, silence timeout, and max call duration."""
        NUDGES = [
            "Hello, can you hear me okay?",
            "Sorry, I think we might have a bad connection. Are you still there?",
        ]
        while not self._stop:
            await asyncio.sleep(1)
            now = time.time()

            if self.cfg.max_duration_s and (now - self._call_start) > self.cfg.max_duration_s:
                print("[WATCHDOG] max duration reached")
                await self._hangup()
                break

            # Nudge on dead air — only when the agent isn't speaking and the
            # conversation has actually started
            quiet = now - self._last_turn
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

            if (self.cfg.silence_timeout_s and not self.is_speaking
                    and (now - self._last_audio) > self.cfg.silence_timeout_s):
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
        if len(self._fanout_buf) >= 25:
            batch, self._fanout_buf = self._fanout_buf, []
            if self._fanout_inflight < 3:      # never queue more than 3 batches
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
            await self._begin_conversation()

        elif evt == "media":
            self._last_audio = time.time()
            payload = data.get("media", {}).get("payload", "")
            if not payload:
                return
            raw = base64.b64decode(payload)
            if self.listeners:
                self._fanout_nowait(payload, "prospect")

            # Grace period — never let barge-in kill the agent's first words.
            # Echo and trailing speech from the prospect cause false triggers.
            grace_ok = (time.time() - self._speak_started) > 0.9
            if self.is_speaking and self.cfg.allow_interruption and grace_ok:
                try:
                    pcm = audioop.ulaw2lin(raw, 2)
                    rms = audioop.rms(pcm, 2)
                except Exception:
                    rms = 0
                if rms > self._vad_threshold:
                    self._loud_frames += 1
                    if self._loud_frames >= 6:      # ~120ms      # ~60ms of speech
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
        self._last_turn = time.time()
        await self._speak_chunk(opening, self._speak_seq)

    async def _handle_transcript(self, text: str):
        if not text.strip() or self._stop:
            return

        self._last_turn = time.time()
        self._nudges = 0
        self._prospect_spoke = True        
        if self.outcome == "no_answer":
            self.outcome = "conversation"        

        if self._looks_like_ivr(text):
            self._ivr_hits += 1
            print(f"[IVR] detected ({self._ivr_hits}): {text[:60]}")
            if self._ivr_hits >= 1:
                print("[IVR] phone tree confirmed — hanging up")
                self.outcome = "ivr"
                await self._push_status("ended", "IVR / phone tree")
                await self._hangup()
            return

        self.conversation.append({"role": "user", "content": text})

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

                        if not full:
                            self._t_llm_first = time.time()
                        full += tok
                        buf  += tok
                        # Throttle partial updates to ~7/sec instead of per-token
                        now = time.time()
                        if now - self._last_partial > 0.15:
                            self._last_partial = now
                            await self.status_queue.put({
                                "type": "transcript_partial", "speaker": "agent",
                                "text": full, "ts": datetime.now().strftime("%H:%M:%S"),
                            })

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

        # Rough token accounting — 4 chars per token is the usual approximation
        convo_chars = sum(len(m.get("content", "")) for m in self.conversation)
        self.metrics.tokens_in  += convo_chars // 4
        self.metrics.tokens_out += len(full) // 4

        final = full.strip().replace("[HANGUP]", "").strip()
        if final and len(final) > 1:
            await self.status_queue.put({
                "type": "transcript_final", "speaker": "agent",
                "text": final.replace("[HANGUP]", "").strip(),
                "ts": datetime.now().strftime("%H:%M:%S"),
            })
        return final or None

    async def _speak_chunk(self, text: str, seq: int):
        """Stream one sentence to ElevenLabs and pipe audio straight to Twilio."""
        if self._stop or seq != self._speak_seq:
            return

        self.is_speaking = True
        self._speak_started = time.time()
        await self._push_status("speaking", "Agent speaking…")

        t0 = time.time()
        sent = 0
        first_byte = 0.0
        self.metrics.tts_chars += len(text)
        print(f"[TTS] voice={self.cfg.voice_id}")
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
                        if not first_byte:
                            first_byte = time.time()
                            self._t_tts_first = first_byte
                        if self._stop or seq != self._speak_seq or not self.is_speaking:
                            print("[TTS] cancelled mid-stream")
                            return
                        data = leftover + raw
                        n = (len(data) // 160) * 160
                        leftover = data[n:]
                        for i in range(0, n, 160):
                            if self._stop or seq != self._speak_seq or not self.is_speaking:
                                return
                            frame_b64 = base64.b64encode(data[i:i+160]).decode()
                            await self.twilio_ws.send_text(json.dumps({
                                "event":     "media",
                                "streamSid": self.stream_sid,
                                "media":     {"payload": frame_b64},
                            }))
                            if self.listeners:
                                self._fanout_nowait(frame_b64, "agent")
                            sent += 1

                            # Pace in 10-frame blocks (200ms of audio).
                            # Keeps roughly realtime without per-frame jitter,
                            # and stays ahead enough that playback never gaps.
                            if sent % 10 == 0:
                                await asyncio.sleep(0.16)

                        if self._fanout_buf:
                            batch, self._fanout_buf = self._fanout_buf, []
                            asyncio.create_task(self._flush_fanout(batch))

            print(f"[TTS] {sent} frames in {time.time()-t0:.2f}s :: {text[:50]}")

        except Exception as e:
            print(f"[TTS EXCEPTION] {e}")
        finally:
            if seq == self._speak_seq:
                await asyncio.sleep(0.25)
                self.is_speaking = False
                self._last_turn = time.time()

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

                # Barge-in — cancel current speech immediately
                grace_ok = (time.time() - self._speak_started) > 0.9
                if (self.is_speaking and self.cfg.allow_interruption
                        and grace_ok and len(text) > 4):
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