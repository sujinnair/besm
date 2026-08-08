from __future__ import annotations

from typing import TypedDict

from besm.engine.models import ScoredSignal


class AgentOutput(TypedDict):
    domain: str
    confidence: float
    alert_approved: bool
    summary: str
    impact_description: str
    recommended_action: str


class AlertPayload(TypedDict):
    alert_id: str
    signal_id: str
    summary: str
    impact_description: str
    lead_time_display: str
    recommended_action: str
    confidence_score: float
    domain: str
    t_created: str  # ISO 8601
    attachments: list[str]


class ReasoningState(TypedDict):
    signal: ScoredSignal
    domain: str
    confidence_override: float | None
    agent_outputs: list[AgentOutput]
    final_confidence: float
    alert_approved: bool
    alert_payload: AlertPayload | None
