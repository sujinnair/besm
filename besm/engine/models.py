from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Literal

Domain = Literal["commodity", "financial", "legal", "health", "urban", "market"]


@dataclass(slots=True)
class RawSignalEvent:
    event_id: str
    node_id: str
    domain: Domain
    content_hash: str  # SHA-256
    source_url: str
    raw_text: str
    metadata: dict  # publication_date, issuing_authority, …
    t_signal: datetime  # UTC


@dataclass(slots=True)
class ButterflyChain:
    chain_id: str
    signal_id: str
    domain: Domain
    source: str
    t_signal: datetime
    t_impact_predicted: datetime
    t_impact_actual: datetime | None
    confidence_score: float
    embedding: list[float]
    outcome_confirmed: bool
    similarity_score: float | None = None  # populated during search


@dataclass(slots=True)
class ScoredSignal:
    signal_id: str
    domain: Domain
    raw_text: str
    embedding: list[float]
    t_signal: datetime
    t_impact_predicted: datetime
    lead_time_hours: float
    confidence_score: float  # 0–100
    top_similar_chains: list[ButterflyChain] = field(default_factory=list)
    metadata: dict = field(default_factory=dict)
    suppressed: bool = False
    suppression_reason: str | None = None
