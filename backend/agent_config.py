"""
Agent configuration — persisted to agent_config.json next to this file.
"""

import json
from pathlib import Path
from pydantic import BaseModel

_CONFIG_FILE = Path(__file__).parent / "agent_config.json"


class AgentConfig(BaseModel):
    enabled: bool = False
    voice_id: str = "EXAVITQu4vr4xnSDxMaL"   # ElevenLabs voice
    voice_name: str = "Sarah"
    model: str = "gpt-4o-mini"                  # gpt-4o-mini | gpt-4o
    tone: str = "professional"                   # professional | friendly | direct
    speaking_rate: float = 1.0
    base_url: str = ""                           # ngrok / production URL for TwiML callback


def load_agent_config() -> AgentConfig:
    if _CONFIG_FILE.exists():
        try:
            return AgentConfig(**json.loads(_CONFIG_FILE.read_text()))
        except Exception:
            pass
    return AgentConfig()


def save_agent_config(cfg: AgentConfig) -> None:
    _CONFIG_FILE.write_text(cfg.model_dump_json(indent=2))