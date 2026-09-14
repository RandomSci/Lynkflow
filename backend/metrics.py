"""
Per-call latency and cost tracking.

Rates are per-minute unless noted and live in RATES below — edit them to match
your actual plan. Defaults are list prices as of early 2026; verify against your
own invoices before trusting the totals.
"""

import time
from dataclasses import dataclass, field
from statistics import median
from typing import Optional


# ── Rate card (USD) ──────────────────────────────────────────────────────────

RATES = {
    # Twilio
    "twilio_voice_us":     0.0140,   # per minute, outbound US
    "twilio_media_stream": 0.0040,   # per minute

    # Deepgram streaming
    "deepgram_nova3":      0.0077,   # per minute
    "deepgram_nova2":      0.0059,

    # OpenAI — per 1M tokens, converted at runtime
    "gpt-4o-mini_in":      0.150,
    "gpt-4o-mini_out":     0.600,
    "gpt-4o_in":           2.500,
    "gpt-4o_out":         10.000,

    # ElevenLabs — per 1000 characters
    "eleven_flash_v2_5":   0.030,
    "eleven_turbo_v2_5":   0.050,
}

# Reference figures shown in the UI cards
COMPONENT_INFO = {
    "transcriber": {
        "name": "Nova 3", "provider": "Deepgram · English",
        "typical_latency_ms": 330, "cost_per_min": RATES["deepgram_nova3"],
        "metric_label": "Accuracy", "metric_value": "~3% WER",
    },
    "model": {
        "gpt-4o-mini": {
            "name": "GPT-4o Mini", "provider": "OpenAI",
            "typical_latency_ms": 480, "cost_per_min": 0.004,
            "metric_label": "Intelligence", "metric_value": "13",
        },
        "gpt-4o": {
            "name": "GPT-4o", "provider": "OpenAI",
            "typical_latency_ms": 720, "cost_per_min": 0.045,
            "metric_label": "Intelligence", "metric_value": "22",
        },
    },
    "voice": {
        "name": "ElevenLabs Flash", "provider": "eleven_flash_v2_5",
        "typical_latency_ms": 190, "cost_per_min": 0.024,
        "metric_label": "Humanness", "metric_value": "88",
    },
}


# ── Presets ──────────────────────────────────────────────────────────────────

PRESETS = {
    "balanced": {
        "label": "Balanced",
        "model": "gpt-4o-mini", "endpointing_ms": 300, "utterance_end_ms": 1000,
        "temperature": 0.6, "max_tokens": 120,
        "stability": 0.5, "similarity_boost": 0.75, "style": 0.1,
    },
    "ultra_fast": {
        "label": "Ultra Fast",
        "model": "gpt-4o-mini", "endpointing_ms": 150, "utterance_end_ms": 700,
        "temperature": 0.5, "max_tokens": 80,
        "stability": 0.4, "similarity_boost": 0.7, "style": 0.0,
    },
    "high_intelligence": {
        "label": "High Intelligence",
        "model": "gpt-4o", "endpointing_ms": 400, "utterance_end_ms": 1200,
        "temperature": 0.7, "max_tokens": 200,
        "stability": 0.55, "similarity_boost": 0.8, "style": 0.15,
    },
    "cost_saver": {
        "label": "Cost Saver",
        "model": "gpt-4o-mini", "endpointing_ms": 500, "utterance_end_ms": 1400,
        "temperature": 0.5, "max_tokens": 70,
        "stability": 0.5, "similarity_boost": 0.7, "style": 0.0,
    },
}


# ── Per-call tracker ─────────────────────────────────────────────────────────

@dataclass
class CallMetrics:
    call_sid: str = ""
    started: float = field(default_factory=time.time)
    ended: Optional[float] = None

    # Latency samples in ms
    stt_ms:   list = field(default_factory=list)
    llm_ms:   list = field(default_factory=list)
    tts_ms:   list = field(default_factory=list)
    total_ms: list = field(default_factory=list)

    # Usage counters
    tokens_in:   int = 0
    tokens_out:  int = 0
    tts_chars:   int = 0
    turns:       int = 0
    interrupts:  int = 0

    model: str = "gpt-4o-mini"

    # ── recording ────────────────────────────────────────────────────────────

    def record_turn(self, stt: float, llm: float, tts: float):
        if stt: self.stt_ms.append(stt * 1000)
        if llm: self.llm_ms.append(llm * 1000)
        if tts: self.tts_ms.append(tts * 1000)
        self.total_ms.append((stt + llm + tts) * 1000)
        self.turns += 1

    def duration_min(self) -> float:
        end = self.ended or time.time()
        return max(0.0, (end - self.started) / 60.0)

    # ── cost ─────────────────────────────────────────────────────────────────

    def costs(self) -> dict:
        mins = self.duration_min()

        twilio = (RATES["twilio_voice_us"] + RATES["twilio_media_stream"]) * mins
        stt    = RATES["deepgram_nova3"] * mins

        m = self.model
        llm = (
            self.tokens_in  / 1_000_000 * RATES.get(f"{m}_in",  RATES["gpt-4o-mini_in"]) +
            self.tokens_out / 1_000_000 * RATES.get(f"{m}_out", RATES["gpt-4o-mini_out"])
        )
        tts = self.tts_chars / 1000 * RATES["eleven_flash_v2_5"]

        total = twilio + stt + llm + tts
        return {
            "twilio": round(twilio, 5),
            "stt":    round(stt, 5),
            "llm":    round(llm, 5),
            "tts":    round(tts, 5),
            "total":  round(total, 5),
            "per_min": round(total / mins, 4) if mins > 0.01 else 0.0,
            "duration_min": round(mins, 3),
            "duration_s":   round(mins * 60, 1),
        }

    # ── latency ──────────────────────────────────────────────────────────────

    def latency(self) -> dict:
        def med(xs): return round(median(xs)) if xs else 0
        def p95(xs):
            if not xs: return 0
            s = sorted(xs)
            return round(s[min(len(s) - 1, int(len(s) * 0.95))])

        return {
            "stt_ms":       med(self.stt_ms),
            "llm_ms":       med(self.llm_ms),
            "tts_ms":       med(self.tts_ms),
            "total_ms":     med(self.total_ms),
            "total_p95_ms": p95(self.total_ms),
            "samples":      len(self.total_ms),
        }

    def snapshot(self) -> dict:
        return {
            "call_sid":   self.call_sid,
            "latency":    self.latency(),
            "cost":       self.costs(),
            "turns":      self.turns,
            "interrupts": self.interrupts,
            "tokens_in":  self.tokens_in,
            "tokens_out": self.tokens_out,
            "tts_chars":  self.tts_chars,
        }
