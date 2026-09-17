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
from agent_config import PUBLIC_CONTACT_EMAIL
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

AGENT_END_MARKERS = [
    "thanks for your help, have a good day", "thank you for your help, have a good day",
    "thanks for your time, have a good day", "thank you for your time, have a good day",
    "appreciate your help, have a good day", "that's everything, have a good day",
    "that is everything, have a good day", "i'll let you go", "i will let you go",
    "i'll try another time", "i will try another time", "try another time",
    "thanks for your time", "thank you for your time", "have a good day",
    "have a great day", "have a good one", "have a great one", "take care",
    "goodbye", "bye now", "talk soon", "all set", "we're all set",
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
        self._live_reconnects = 0
        self._stream_started = asyncio.Event()
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
        self._last_output_transcript_at = 0.0
        self._last_activity_at = time.time()
        self._last_prospect_audio_at = 0.0
        self._last_prospect_voice_at = 0.0
        self._prospect_voice_observed = False
        self._ending_started_at = 0.0
        self._hangup_after_agent_turn = False
        self._no_transcript_audio_warned = False
        self.end_reason = ""
        self._last_clear_at = 0.0
        self._audio_clear_seq = 0
        self._prospect_loud_frames = 0
        self._barge_vad_threshold = 2600
        self._followup_no_answer_warned = False
        self._followup_idle_warned = False
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

        tasks = []
        try:
            twilio_task = asyncio.create_task(self._twilio_loop())
            watchdog_task = asyncio.create_task(self._watchdog())
            tasks.extend([twilio_task, watchdog_task])

            try:
                await asyncio.wait_for(self._stream_started.wait(), timeout=10)
            except asyncio.TimeoutError:
                await self._push_status("ended", "Twilio stream did not start")
                self._stop = True
                return

            if not self._stop:
                await self._connect_live()
                live_task = asyncio.create_task(self._live_loop())
                tasks.append(live_task)

            await asyncio.gather(*tasks)
        finally:
            for task in tasks:
                if not task.done():
                    task.cancel()
            await self._cleanup()

    async def _watchdog(self):
        while not self._stop:
            await asyncio.sleep(1)
            now = time.time()
            elapsed = now - self._call_started_at
            is_followup = self.lead_info.get("call_mode") == "follow_up"
            silence_timeout = max(30, getattr(self.cfg, "silence_timeout_s", 10) or 10)
            no_transcript_timeout = max(45, silence_timeout)
            if is_followup:
                silence_timeout = max(45, silence_timeout)
                no_transcript_timeout = max(75, silence_timeout + 20)
            if self._ending and self._ending_started_at and (now - self._ending_started_at) > 6:
                print("[GPT-LIVE WATCHDOG] ending state exceeded 6s; forcing local close")
                self._stop = True
                await self._close_live()
                return
            if self.call_sid and self.outcome == "no_answer" and elapsed > no_transcript_timeout:
                recent_remote_audio = self._last_prospect_audio_at and (now - self._last_prospect_audio_at) < 12
                recent_remote_voice = self._last_prospect_voice_at and (now - self._last_prospect_voice_at) < max(25, silence_timeout)
                if recent_remote_audio or recent_remote_voice:
                    if not self._no_transcript_audio_warned:
                        self._no_transcript_audio_warned = True
                        await self._push_status("listening", "Remote audio heard; waiting for GPT-Live transcript")
                    continue
                if self._prospect_voice_observed:
                    if not self._no_transcript_audio_warned:
                        self._no_transcript_audio_warned = True
                        await self._push_status("listening", "Customer voice heard; keeping call open while waiting for GPT-Live transcript")
                    continue
                print("[GPT-LIVE WATCHDOG] no transcript after answer window")
                await self._push_status("ended", "No transcript after answer window")
                await self._hangup("no transcript after answer window")
                return
            if self.call_sid and self.outcome != "no_answer" and not self._agent_turn_open:
                last_activity = max(
                    self._last_activity_at,
                    self._last_input_transcript_at,
                    self._last_output_transcript_at,
                    self._last_prospect_audio_at,
                    self._last_prospect_voice_at,
                )
                idle = now - last_activity
                if idle > silence_timeout:
                    if is_followup and not self._followup_idle_warned:
                        self._followup_idle_warned = True
                        await self._push_status("listening", "Conversation is quiet; keeping follow-up call open")
                        continue
                    if is_followup and idle < silence_timeout + 20:
                        continue
                    print(f"[GPT-LIVE WATCHDOG] idle after conversation ({idle:.1f}s)")
                    await self._hangup("idle after conversation")
                    return
            max_duration = getattr(self.cfg, "max_duration_s", 300) or 300
            if elapsed > max_duration and self.outcome not in {"conversation", "interested", "callback"}:
                recent_customer_activity = self._last_prospect_audio_at and (now - self._last_prospect_audio_at) < 20
                if recent_customer_activity:
                    await self._push_status("listening", "Max duration reached but customer audio is active; not dropping call")
                    continue
                print("[GPT-LIVE WATCHDOG] max duration without useful conversation")
                await self._push_status("ended", "Max duration reached")
                await self._hangup("max duration")
                return

    def _follow_up_instructions(self) -> str:
        follow_up = self.lead_info.get("FollowUp") or {}
        if self.lead_info.get("call_mode") != "follow_up" or not follow_up:
            return ""
        previous_name = follow_up.get("previous_contact_name") or follow_up.get("contact_name")
        if str(previous_name or "").strip().lower() in {"unknown", "name unknown", "name not provided", "not provided", "none", "null"}:
            previous_name = None
        previous_role = follow_up.get("previous_contact_role") or follow_up.get("contact_role")
        context = {
            "business": follow_up.get("business"),
            "phone": follow_up.get("phone"),
            "previous_contact_role": previous_role,
            "previous_contact_name": previous_name,
            "current_contact_role": follow_up.get("current_contact_role"),
            "current_contact_name": follow_up.get("current_contact_name"),
            "email": follow_up.get("email"),
            "agent_summary": follow_up.get("agent_summary"),
            "context_summary": follow_up.get("context_summary"),
            "details": follow_up.get("details"),
            "pain_point": follow_up.get("pain_point"),
            "current_solution": follow_up.get("current_solution"),
            "interest_signal": follow_up.get("interest_signal"),
            "previous_action": follow_up.get("previous_action"),
            "email_delivery_status": follow_up.get("email_delivery_status"),
            "email_received": follow_up.get("email_received"),
            "prospect_reported_not_received": follow_up.get("prospect_reported_not_received"),
            "email_status_details": follow_up.get("email_status_details"),
            "next_action": follow_up.get("next_action"),
            "follow_up_goal": follow_up.get("follow_up_goal"),
            "scheduled_for": follow_up.get("scheduled_for"),
            "previous_call_date": follow_up.get("previous_call_date"),
            "previous_call_local_display": follow_up.get("previous_call_local_display"),
            "lead_timezone": follow_up.get("lead_timezone"),
        }
        context = {k: v for k, v in context.items() if v not in (None, "")}
        safe_context = json.dumps(context, ensure_ascii=False)
        tone = TONE_NOTES.get(getattr(self.cfg, "tone", "professional"), TONE_NOTES["professional"])
        callback = (getattr(self.cfg, "callback_number", "") or TWILIO_CALLER_ID or "the number I called from").strip()
        contact_name = context.get("previous_contact_name")
        contact_role = context.get("previous_contact_role")
        contact = contact_name or contact_role or "the person I spoke with earlier"
        preferred_opening = (getattr(self.cfg, "first_message", "") or "").replace("{business}", context.get("business") or "the business").replace("{contact}", contact).replace("{contact_name}", contact_name or contact).replace("{contact_role}", contact_role or contact)
        if contact_name:
            opening_rule = f"Your first response after their greeting should ask for {contact_name} by name, then say you are following up from Lynkflow."
        elif contact_role:
            opening_rule = f"Your first response after their greeting should ask whether you are speaking with the {contact_role}, then say you are following up from Lynkflow."
        else:
            opening_rule = "Your first response after their greeting should say you are following up from Lynkflow about the prior conversation."
        system_prompt = str(getattr(self.cfg, "system_prompt", "") or "")
        system_prompt = system_prompt.replace("professional receptionists", "AI voice receptionists")
        system_prompt = system_prompt.replace("Professional receptionists", "AI voice receptionists")
        system_prompt = system_prompt.replace("human receptionists", "AI voice receptionists")
        system_prompt = system_prompt.replace("Human receptionists", "AI voice receptionists")
        return (
            f"{system_prompt}\n\n"
            "# FOLLOW-UP CONTEXT\n"
            f"{safe_context}\n\n"
            "# OBJECTIVE\n"
            "Naturally reconnect, briefly reference the earlier conversation, and work toward follow_up_goal.\n"
            "If they remember, continue from there. If they do not remember, give a short reminder from agent_summary or context_summary and ask if now is still okay.\n\n"
            "# OPENING\n"
            f"Preferred opening: {preferred_opening}\n"
            f"{opening_rule}\n"
            "Never open by asking whether the owner or office manager is available.\n\n"
            "# RULES\n"
            "- You are on a live phone call. Listen while the other person speaks and stop when interrupted.\n"
            "- Previous contact identity comes only from previous_contact_name and previous_contact_role. Never use current_contact_name as the previous contact.\n"
            "- If the previous contact name is unknown, do not ask for a name. Use previous_contact_role if available.\n"
            "- Do not ask for the owner or office manager as a default opener. Use previous_contact_name or previous_contact_role if available.\n"
            "- Do not restart the cold-call permission process or repeat the full cold pitch.\n"
            "- Do not invent names, emails, promises, demo requests, pain points, or prior actions. Unknown means unknown.\n"
            "- Lynkflow is an AI voice receptionist / AI call handling system. Do not describe it as a human-staffed receptionist service.\n"
            "- Do not say an email was sent unless email_delivery_status is exactly email_sent.\n"
            "- Do not say the prospect received an email unless email_received is true.\n"
            "- If prospect_reported_not_received is true, preserve email_delivery_status and ask whether they checked spam/junk or prefer another email. Do not say the email failed unless email_delivery_status is email_failed.\n"
            "- If email was only requested, generated, ready, or unknown, say only that you are following up on the information conversation.\n"
            "- If they want a demo or callback, handle it naturally and collect the needed details.\n"
            "- Do not leave voicemail. If you hit voicemail or an automated system, end politely.\n"
            "- If they are not interested or ask not to be contacted, respect it and end professionally.\n"
            "- Keep replies short and conversational. Never pressure them.\n"
            "- Never speak bracketed control tokens aloud. Use [HANGUP] only as a silent control token when ending.\n"
            f"Your approved email address: {PUBLIC_CONTACT_EMAIL}.\n"
            f"Your callback number: {callback}.\n"
            f"Tone: {tone}"
        )

    def _instructions(self) -> str:
        follow_up_prompt = self._follow_up_instructions()
        if follow_up_prompt:
            return follow_up_prompt
        business = self.lead_info.get("Name") or "the business"
        city = self.lead_info.get("City") or ""
        category = self.lead_info.get("Category") or "service business"
        timezone = self.lead_info.get("Timezone") or ""
        context = f"Lead context: {business}, a {category}"
        if city:
            context += f" in {city}"
        if timezone:
            context += f". Timezone: {timezone}"
        lead_context = self.lead_info.get("LeadContext") or {}
        if lead_context:
            context += f". Extra lead context JSON: {json.dumps(lead_context, ensure_ascii=False)}"
        tone = TONE_NOTES.get(getattr(self.cfg, "tone", "professional"), TONE_NOTES["professional"])
        callback = (getattr(self.cfg, "callback_number", "") or TWILIO_CALLER_ID or "the number I called from").strip()
        return (
            f"{getattr(self.cfg, 'system_prompt', '')}\n\n"
            f"{context}.\n"
            f"Your callback number: {callback}.\n"
            f"Your approved email address: {PUBLIC_CONTACT_EMAIL}.\n"
            f"Tone: {tone}\n\n"
            "Live voice behavior:\n"
            "- You are on a phone call. Keep replies short and natural.\n"
            "- Listen while the other person speaks; stop when interrupted.\n"
            "- Do not end the call just because there is a pause. Wait for the other person after asking a question.\n"
            "- Answer the exact question first, then continue naturally.\n"
            "- Do not repeat a previous line or restart the call.\n"
            "- Do not pitch until a decision maker has allowed the 30-second pitch.\n"
            "- Never invent phone numbers, emails, prices, company details, or names. Use only the callback number and approved email above.\n"
            f"- If asked for your email, give exactly {PUBLIC_CONTACT_EMAIL}. Do not say Anna at Lynkflow dot com, info at Lynkflow dot com, or any other address.\n"
            "- Do not give a website verbally on calls. If they ask for a website or link, say you can send more information by email and ask for the best email address.\n"
            "- Receptionist/staff contact capture: if they offer an email address, direct number, direct contact, callback time, or say you can send an email, accept it. Ask for the missing contact detail, confirm spelling/digits/time, then thank them and end.\n"
            "- Message-taking is different: if they only offer to take your message or ask for your details, ask once for the best email, direct number, or callback time instead. Only end without collecting info if they refuse or cannot provide it.\n"
            "- Do not leave voicemail. If staff insists on taking your details after you asked for direct contact, provide only your name, Lynkflow, the callback number above, and the approved email above.\n"
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

    async def _reconnect_live(self, reason: str):
        if self._stop or self._ending:
            return False
        self._live_reconnects += 1
        wait_s = min(5, 0.5 * self._live_reconnects)
        await self._push_status("connecting", f"Reconnecting GPT-Live after {reason}")
        try:
            await self._close_live(send_close=False)
        except Exception:
            pass
        if self._out_audio_task and not self._out_audio_task.done():
            try:
                await asyncio.wait_for(self._out_audio_task, timeout=0.5)
            except Exception:
                self._out_audio_task.cancel()
        self._out_audio_queue = asyncio.Queue()
        self.live_ws = None
        self.live_started = False
        self.live_session_id = ""
        await asyncio.sleep(wait_s)
        try:
            await self._connect_live()
            return True
        except Exception as e:
            print(f"[GPT-LIVE RECONNECT] failed after {reason}: {e}")
            await self._push_status("listening", "GPT-Live reconnect failed; keeping phone call open")
            return False

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
        while not self._stop:
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
                if self._stop or self._ending:
                    break
                print(f"[GPT-LIVE] loop error: {e}")
                await self._reconnect_live("loop error")
                continue
            if self._stop or self._ending:
                break
            await self._reconnect_live("session closed")

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
                existing_follow_up = self.lead_info.get("FollowUp")
                lead_context = self.lead_info.get("LeadContext")
                raw_lead_context = cp.get("lead_context", "")
                if raw_lead_context:
                    try:
                        parsed_context = json.loads(raw_lead_context)
                        if isinstance(parsed_context, dict):
                            lead_context = parsed_context
                    except Exception:
                        pass
                follow_up_id = cp.get("follow_up_id", "")
                if not existing_follow_up and cp.get("call_mode") == "follow_up" and follow_up_id:
                    try:
                        from followups import get_follow_up
                        existing_follow_up = get_follow_up(follow_up_id)
                    except Exception as e:
                        print(f"[GPT-LIVE FOLLOWUP] context load failed: {e}")
                self.lead_info = {
                    "Name": cp.get("name", ""),
                    "Phone": cp.get("phone", ""),
                    "City": cp.get("city", ""),
                    "Category": cp.get("category", ""),
                    "Timezone": cp.get("timezone", ""),
                    "call_mode": cp.get("call_mode", ""),
                    "follow_up_id": follow_up_id,
                    "FollowUp": existing_follow_up,
                    "LeadContext": lead_context,
                }
            print(f"[GPT-LIVE STREAM START] {self.call_sid} lead={self.lead_info}")
            self._stream_started.set()
            await self._push_status("active", "GPT-Live active")
            return

        if evt == "media":
            payload = data.get("media", {}).get("payload", "")
            if not payload:
                return
            raw = base64.b64decode(payload)
            self._record_prospect_frame(raw)
            self._fanout_nowait(payload, "prospect")
            try:
                rms = audioop.rms(audioop.ulaw2lin(raw, 2), 2)
            except Exception:
                rms = 0
            if rms > 120:
                self._last_prospect_audio_at = time.time()
            if rms > 550:
                self._last_prospect_voice_at = self._last_prospect_audio_at or time.time()
                self._prospect_voice_observed = True
            if self._agent_turn_open:
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
                    self._last_activity_at = self._last_input_transcript_at
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
                    self._last_output_transcript_at = time.time()
                    self._last_activity_at = self._last_output_transcript_at
                    await self._push_partial("agent", text)
                low = self._output_text.lower()
                if "[hangup]" in low or self._is_agent_end_text(low):
                    print(f"[GPT-LIVE END PHRASE] {text[:120]}")
                    self._hangup_after_agent_turn = True
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
            if self._ending:
                self._stop = True
            return

        if typ == "error":
            err = event.get("error", {}) or {}
            msg = err.get("message") or json.dumps(err)
            print(f"[GPT-LIVE ERROR] {msg}")
            await self._push_status("connecting", f"GPT-Live error, reconnecting: {msg}")
            await self._close_live(send_close=False)

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
                if self._hangup_after_agent_turn and not self._ending:
                    self._hangup_after_agent_turn = False
                    asyncio.create_task(self._delayed_hangup())

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

    def _is_agent_end_text(self, text: str) -> bool:
        low = text.lower()
        return any(p in low for p in self.end_phrases) or any(marker in low for marker in AGENT_END_MARKERS)

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
            await self._hangup("agent finished end phrase")

    async def _hangup(self, reason: str = ""):
        if self._ending:
            return
        self._ending = True
        self._ending_started_at = time.time()
        self.end_reason = reason or self.end_reason or "local hangup"
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
        if self.call_sid and TWILIO_ACCOUNT_SID:
            try:
                async with httpx.AsyncClient(timeout=4) as client:
                    resp = await client.post(
                        f"https://api.twilio.com/2010-04-01/Accounts/{TWILIO_ACCOUNT_SID}/Calls/{self.call_sid}.json",
                        auth=(TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN),
                        data={"Status": "completed"},
                    )
                    print(f"[GPT-LIVE TWILIO COMPLETE] {self.call_sid} status={resp.status_code}")
            except Exception as e:
                print(f"[GPT-LIVE HANGUP ERROR] {e}")
        self._stop = True
        await self._close_live()
        try:
            await self.twilio_ws.close()
        except Exception:
            pass

    async def _close_live(self, send_close: bool = True):
        try:
            await self._out_audio_queue.put(None)
        except Exception:
            pass
        if self.live_ws:
            if send_close:
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
