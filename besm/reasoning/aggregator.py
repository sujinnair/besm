from __future__ import annotations

import uuid
from datetime import datetime, timezone

from besm.reasoning.state import AgentOutput, AlertPayload, ReasoningState


def aggregator_node(state: ReasoningState) -> ReasoningState:
    outputs = state["agent_outputs"]
    if not outputs:
        return {
            **state,
            "final_confidence": 0.0,
            "alert_approved": False,
            "alert_payload": None,
        }

    best: AgentOutput = max(outputs, key=lambda o: o["confidence"])
    final_confidence = (
        state["confidence_override"]
        if state.get("confidence_override") is not None
        else best["confidence"]
    )
    alert_approved = best["alert_approved"] and final_confidence >= 40.0

    signal = state["signal"]
    lead_time = signal.lead_time_hours
    lead_display = _format_lead_time(lead_time)

    payload: AlertPayload | None = None
    if alert_approved:
        payload = AlertPayload(
            alert_id=str(uuid.uuid4()),
            signal_id=signal.signal_id,
            summary=best["summary"],
            impact_description=best["impact_description"],
            lead_time_display=lead_display,
            recommended_action=best["recommended_action"],
            confidence_score=final_confidence,
            domain=best["domain"],
            t_created=datetime.now(timezone.utc).isoformat(),
            attachments=[],
        )

    return {
        **state,
        "final_confidence": final_confidence,
        "alert_approved": alert_approved,
        "alert_payload": payload,
    }


def _format_lead_time(hours: float) -> str:
    if hours < 1:
        return "< 1 hour"
    if hours < 24:
        return f"~{int(hours)} hours"
    days = hours / 24
    if days < 2:
        return "~1 day"
    return f"~{int(days)} days"
