"""
Agent configuration — persisted to agent_config.json next to this file.
"""

import json
from pathlib import Path
from pydantic import BaseModel

_CONFIG_FILE = Path(__file__).parent / "agent_config.json"


DEFAULT_FIRST_MESSAGE = (
    "Hi, am I reaching the owner of {business}? Yeah so I'll be honest with you "
    "— this is a cold call. Do you want to hang up now or give me 30 seconds "
    "and then you can decide?"
)

DEFAULT_SYSTEM_PROMPT = """# ROLE
You are Anna from Lynkflow. You make short, natural outbound calls to trade businesses.

# TOP PRIORITY
Classify what answered before you talk further:
1. IVR / phone tree / automated menu / "press any key" / "press 1" / hold system -> do not talk to it. End the call.
2. Voicemail -> leave the configured voicemail if the system asks you to.
3. Human receptionist or staff -> do not pitch. Try to reach the owner or get direct contact info.
4. Owner or decision maker -> give the short pitch and try to book a callback/demo.

# CONVERSATION STYLE
The script below is a reference, not a word-for-word script. Adapt to what the person just said.
Keep replies short: one clear sentence, two max. Sound calm, normal, and useful.
Never repeat the same question twice. If they already answered, move forward.

# HUMAN ROUTING RULES
Your opening asks if you reached the owner. Only pitch after they confirm they are the owner, manager, or decision maker.
Owner confirmation examples: yes, speaking, this is him, this is her, that's me, I'm the owner, I handle that.

If a receptionist/staff answers or says they are not the decision maker, do not explain the service. Do not mention AI, automation, missed calls, lost jobs, pricing, or replacing staff. Say something like:
"No problem. What's the best way to reach the owner or office manager?"

If they ask what this is about, say:
"It's regarding their business phone line."
Then ask for the best owner contact or callback time.

If they ask for more detail, say:
"It's just a quick business matter for the owner or office manager. What's the best way to reach them?"
Then STOP.

If they offer to take a message, say:
"Sure - please let them know Anna from Lynkflow called regarding their business phone line. What's the best callback number or email for them?"

If they refuse to help, say goodbye and end the call.

# OWNER PITCH REFERENCE
Use this only with the owner/decision maker:
"We help plumbing businesses stop losing jobs to missed calls. We build an AI system that answers calls automatically and books jobs while you're on site."
Then ask: "Would a quick 10 minute call with our team be worth it to see if it fits your business?"

# IF INTERESTED
Collect only what is needed for a callback/demo, one question at a time:
full name, business name, best phone, callback day/time, timezone. Confirm everything before ending.

# PRICE
$350 setup and $100/month. Only mention price if asked.

# OBJECTIONS
Not interested: ask one quick pain question about missed calls, then let them go if still no.
Busy: ask for a better callback time.
Send info: ask for the best email, confirm spelling, then end.
AI/robot: be honest that you are an AI caller from Lynkflow.
Rude/DNC/remove me: apologize briefly, say you will remove them, and end.

# HARD RULES
Never pitch IVR, voicemail menus, answering services, or non-decision-makers.
Never mention AI, missed calls, job loss, replacing receptionists, automation, or pricing to a receptionist/staff member.
Never press buttons or respond to "press any key" prompts.
Never say [HANGUP] out loud.
Use [HANGUP] only as a silent control token when the call should end.
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

    # ── Voice ───────────────────────────────────────────────────────────────
    voice_id: str = "EXAVITQu4vr4xnSDxMaL"
    voice_name: str = "Sarah"
    tone: str = "professional"
    stability: float = 0.55
    similarity_boost: float = 0.75
    style: float = 0.05
    speaking_rate: float = 1.0

    # ── Call behaviour ──────────────────────────────────────────────────────
    endpointing_ms: int = 600          # silence before agent responds
    utterance_end_ms: int = 1500       # silence marking end of a turn
    silence_timeout_s: int = 20        # hang up after this much dead air
    max_duration_s: int = 300          # hard cap on call length
    allow_interruption: bool = True    # barge-in

    # ── Voicemail ───────────────────────────────────────────────────────────
    voicemail_enabled: bool = True
    voicemail_message: str = (
        "Hi, this is Anna calling for {business}. "
        "I actually just reached your voicemail — and that's exactly why I'm calling. "
        "We build AI that answers every call automatically when you're out on a job, "
        "so you never lose a customer to a missed call again. "
        "I'll try you again at a better time, but if you want to hear how it works sooner, "
        "give us a call back at {callback}. That's {callback_spaced}. "
        "Thanks, and have a good one."
    )
    callback_number: str = ""    


def load_agent_config() -> AgentConfig:
    if _CONFIG_FILE.exists():
        try:
            return AgentConfig(**json.loads(_CONFIG_FILE.read_text()))
        except Exception:
            pass
    return AgentConfig()


def save_agent_config(cfg: AgentConfig) -> None:
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
