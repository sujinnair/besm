from besm.reasoning.state import AlertPayload


def format_alert(payload: AlertPayload) -> str:
    summary = payload["summary"][:160]
    domain = payload["domain"].upper()
    impact = payload["impact_description"]
    window = payload["lead_time_display"]
    action = payload["recommended_action"]
    confidence = int(payload["confidence_score"])

    return (
        f"[{domain}] {summary}\n"
        f"Impact: {impact}\n"
        f"Window: {window}\n"
        f"Action: {action}\n"
        f"Confidence: {confidence}%"
    )
