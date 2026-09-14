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

DEFAULT_SYSTEM_PROMPT = """You are Odelyn, a sales agent for Lynkflow calling plumbing and trade businesses cold.
Your ONLY goal this call: book a 10-minute Zoom demo.

RULES:
- Keep every response to 1-3 sentences MAX. Never monologue.
- Sound like a real person, not a script reader.
- React specifically to what the prospect just said.
- When genuine interest appears, pivot immediately to booking a Zoom.
- After 3 clear rejections with no opening, exit gracefully.
- Never admit you are an AI unless directly and sincerely asked.

CORE PITCH (use naturally, not word-for-word):
We help plumbers make sure they never miss a customer call — 24/7 AI that answers
every call automatically when you're on a job and texts you a full summary instantly.
No contracts, cancel anytime.

PRICE: $350 setup, $100 a month. One saved job pays for months of the service.

CLOSE: "What day works best for a quick 10-minute Zoom? I'll send the link right after."

OBJECTION REPLIES (short, in your own words):
- Not interested -> "Fair — quick question though: when you miss a call on a job, where does that lead go?"
- We have voicemail -> "80% of callers hang up before they leave a message. They call your competitor instead."
- Too expensive -> "One missed job usually costs more than the whole month. It pays for itself the first week."
- Think about it -> "Of course — what part do you want to think through? I can answer it now."
- Already have receptionist -> "Great — we cover overflow and after-hours she can't get to. Worth a quick look?"
- Is this AI -> "No, I'm a real person. I'm calling because we help plumbers stop losing jobs to missed calls."
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
    model: str = "gpt-4o-mini"
    temperature: float = 0.75
    max_tokens: int = 180

    # ── Voice ───────────────────────────────────────────────────────────────
    voice_id: str = "EXAVITQu4vr4xnSDxMaL"
    voice_name: str = "Sarah"
    tone: str = "professional"
    stability: float = 0.5
    similarity_boost: float = 0.75
    style: float = 0.1
    speaking_rate: float = 1.0

    # ── Call behaviour ──────────────────────────────────────────────────────
    endpointing_ms: int = 400          # silence before agent responds
    utterance_end_ms: int = 1200       # silence marking end of a turn
    silence_timeout_s: int = 20        # hang up after this much dead air
    max_duration_s: int = 300          # hard cap on call length
    allow_interruption: bool = True    # barge-in


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