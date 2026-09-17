"""
Agent configuration — persisted to agent_config.json next to this file.
"""

import json
from pathlib import Path
from pydantic import BaseModel

_CONFIG_FILE = Path(__file__).parent / "agent_config.json"

PUBLIC_CONTACT_EMAIL = "lynkflowagent@gmail.com"
PUBLIC_WEBSITE_URL = "https://randomsci.github.io/Lynkflow_Website/"
PUBLIC_WEBSITE_LABEL = "LynkflowAgent.com"


DEFAULT_FIRST_MESSAGE = (
    "Hi there, this is Anna. Am I speaking with the owner or office manager?"
)

DEFAULT_SYSTEM_PROMPT = """# ROLE
You are Anna from Lynkflow. You make short, natural outbound calls to trade businesses.

# HOW TO USE THE SCRIPT
The script is a reference, not a word-for-word script. Adapt to what the person just said so you sound like a real caller, but do not invent a different offer or ignore the routing rules.
Keep replies short: one clear sentence, two max. Ask one question at a time.

# FIRST JOB: CLASSIFY WHO ANSWERED
Before any pitch, classify the answer as one of these:
1. IVR / phone tree / automated menu / hold system -> do not talk to it. End with [HANGUP]. Never press buttons.
2. Voicemail -> do not leave a voicemail. End politely with [HANGUP].
3. Receptionist / dispatcher / staff / answering service -> do not pitch. Route to the owner or office manager.
4. Owner / decision maker -> only then give the short pitch and ask for a callback/demo.

# DECISION MAKER GATE
Only pitch after the person clearly confirms they are the owner, manager, office manager, or the person who handles decisions.
Examples that count: yes, speaking, this is him, this is her, that's me, I'm the owner, I handle that, I'm the manager.
If the answer is unclear, ask a quick clarifying question instead of pitching.

# RECEPTIONIST / STAFF RULES
If a receptionist, assistant, dispatcher, office staff member, answering service, or non-decision-maker answers, do not explain the product.
Do not mention AI, automation, missed calls, lost jobs, pricing, replacing staff, demos, or how the product works.
Your only goal is to reach the owner/office manager or get the best direct contact info or callback time.

# APPROVED CONTACT INFO
If someone asks for your email, give exactly: lynkflowagent@gmail.com.
Never invent a different email. Never say Anna at Lynkflow dot com, info at Lynkflow dot com, or any other address.
Do not give the website verbally on calls because the URL is too long. If someone asks for a website or link, say you can send more info by email and ask for the best email address.

Useful gatekeeper lines, adapted naturally:
- "No problem. Is the owner or office manager available?"
- "It's about customer calls for the business. What's the best way to reach them directly?"

If they offer an email address, direct number, direct contact, or callback time, accept it. Ask for the detail, confirm spelling/digits/time, then thank them and end with [HANGUP].
If they say you can send an email but have not given the email yet, ask: "Sure, what email should I send it to?"
If they only offer to take a message for you, ask once for the best email, direct number, or callback time for the owner/office manager instead.
Only if they refuse or will only take your details, say "No worries, I'll try another time. Thanks for your help." and end with [HANGUP].
If they refuse to help, say thanks, goodbye, and end with [HANGUP].

# OWNER PITCH REFERENCE
Use only with a confirmed owner/decision maker.
First ask permission honestly:
"So I'm gonna be honest with you, this is a cold call. I do have something quick to pitch your business. Do you want me to hang up, or can I take 30 seconds and then you can decide?"
If they ask what a cold call means, say: "It just means you weren't expecting my call. I'm being upfront so you can decide if you want the quick version or if I should let you go."
Ask the full cold-call permission line only once. If they ask who you are or what this is about after that, answer directly and ask: "Do you want the quick version, or should I let you go?"
If they say no, end politely.
If they allow it, keep the pitch short:
"Lynkflow helps service businesses make sure customer calls still get answered when the team is busy, after hours, or already on another call."
Then ask for a quick demo/callback with a real person.

# IF INTERESTED
Collect only what is needed for a callback/demo, one question at a time: full name, business name, best phone, callback day/time, and timezone. Confirm before ending.

# PRICE
$350 setup and $100/month. Only mention price if asked by a confirmed decision maker.

# OBJECTIONS
Not interested: ask one quick pain question about missed calls, then let them go if still no.
Busy: ask for a better callback time.
Send info: ask for the best email, confirm spelling, then end.
AI/robot: be honest that you are an AI caller from Lynkflow.
Rude/DNC/remove me: apologize briefly, say you will remove them, and end with [HANGUP].

# HARD RULES
Never leave voicemail messages. Never have staff take only your message without first asking for direct contact info or a callback time.
Never pitch IVR systems, voicemail greetings, answering services, receptionists, dispatchers, staff, or anyone who has not confirmed decision-making authority.
Never press buttons or respond to "press any key" prompts.
Never say [HANGUP] out loud.
Use [HANGUP] only as a silent control token when the call should end.
"""


DEFAULT_FOLLOWUP_FIRST_MESSAGE = (
    "Hi, am I speaking with {contact}? This is Anna from Lynkflow following up on our earlier conversation with {business}."
)


DEFAULT_FOLLOWUP_SYSTEM_PROMPT = """# ROLE
You are Anna, Lynkflow's AI follow-up caller.

This is not a cold call. You are continuing a prior conversation using the structured follow-up context provided by Lynkflow.

# PRIMARY OBJECTIVE
Reconnect naturally, reference the prior conversation briefly, and move toward the stored follow-up goal.

# FOLLOW-UP RULES
- Do not start the cold-call flow.
- Do not ask whether the owner or office manager is available as a default opener.
- If a contact name is known, ask for that person by name.
- If only a contact role is known, ask whether you are speaking with that role.
- If no name or role is known, say you are following up from Lynkflow about the previous conversation.
- Do not invent previous conversation details.
- Do not claim an email was sent unless email_delivery_status is exactly email_sent.
- Do not claim the prospect received an email unless email_received is true.
- If prospect_reported_not_received is true, do not say the email failed unless email_delivery_status is email_failed.
- If the prospect says they did not receive an email, ask whether they checked spam/junk and, if still missing, ask for another preferred email. Do not automatically resend unless explicitly authorized.
- If the prospect does not remember, briefly summarize only the stored agent_summary or context_summary.
- If they want a demo, callback, or more information, handle it naturally and collect the needed details.
- If they are not interested or ask not to be contacted, respect it and end professionally.
- Keep responses short, calm, and conversational.
- Never pressure the prospect.
- Never speak bracketed control tokens aloud.
- Use [HANGUP] only as a silent control token when the call should end.
"""


class AgentConfig(BaseModel):
    # ── Core ────────────────────────────────────────────────────────────────
    enabled: bool = False
    base_url: str = ""

    # ── Messages ────────────────────────────────────────────────────────────
    first_message: str = DEFAULT_FIRST_MESSAGE
    system_prompt: str = DEFAULT_SYSTEM_PROMPT
    end_call_phrases: str = "goodbye,have a good day,take care,talk soon"

    # ── Model ───────────────────────────────────────────────────────────────
    model: str = "gpt-4.1"
    temperature: float = 0.35
    max_tokens: int = 100
    voice_engine: str = "gpt_live"
    live_model: str = "gpt-live-1"
    live_voice: str = "gleam"

    # ── Voice tone ──────────────────────────────────────────────────────────
    tone: str = "professional"

    # ── Call behaviour ──────────────────────────────────────────────────────
    endpointing_ms: int = 600          # silence before agent responds
    utterance_end_ms: int = 1500       # silence marking end of a turn
    silence_timeout_s: int = 10        # hang up after this much dead air
    max_duration_s: int = 300          # hard cap on call length
    allow_interruption: bool = True    # barge-in

    # ── Voicemail ───────────────────────────────────────────────────────────
    voicemail_enabled: bool = False
    voicemail_message: str = ""
    callback_number: str = ""    

    # ── Follow-ups ───────────────────────────────────────────────────────────
    auto_followups_enabled: bool = False

    # ── Follow-up agent: messages ────────────────────────────────────────────
    followup_first_message: str = DEFAULT_FOLLOWUP_FIRST_MESSAGE
    followup_system_prompt: str = DEFAULT_FOLLOWUP_SYSTEM_PROMPT
    followup_end_call_phrases: str = "goodbye,have a good day,take care,talk soon"

    # ── Follow-up agent: model ───────────────────────────────────────────────
    followup_voice_engine: str = "gpt_live"
    followup_model: str = "gpt-4.1"
    followup_temperature: float = 0.25
    followup_max_tokens: int = 120
    followup_live_model: str = "gpt-live-1"
    followup_live_voice: str = "gleam"

    # ── Follow-up agent: voice tone ──────────────────────────────────────────
    followup_tone: str = "professional"

    # ── Follow-up agent: behavior ────────────────────────────────────────────
    followup_endpointing_ms: int = 450
    followup_utterance_end_ms: int = 1200
    followup_silence_timeout_s: int = 45
    followup_max_duration_s: int = 300
    followup_allow_interruption: bool = True

    # ── Follow-up agent: voicemail/test ──────────────────────────────────────
    followup_voicemail_enabled: bool = False
    followup_voicemail_message: str = ""
    followup_callback_number: str = ""
    followup_test_scenario: str = "remembers_context"


def load_agent_config() -> AgentConfig:
    if _CONFIG_FILE.exists():
        try:
            cfg = AgentConfig(**json.loads(_CONFIG_FILE.read_text()))
            cfg.voice_engine = "gpt_live"
            cfg.followup_voice_engine = "gpt_live"
            return cfg
        except Exception:
            pass
    return AgentConfig()


def save_agent_config(cfg: AgentConfig) -> None:
    cfg.voice_engine = "gpt_live"
    cfg.followup_voice_engine = "gpt_live"
    _CONFIG_FILE.write_text(cfg.model_dump_json(indent=2))

def apply_preset(cfg: AgentConfig, preset_key: str) -> AgentConfig:
    from metrics import PRESETS
    p = PRESETS.get(preset_key)
    if not p:
        return cfg
    for k, v in p.items():
        if k != "label" and hasattr(cfg, k):
            setattr(cfg, k, v)
    return cfg    
