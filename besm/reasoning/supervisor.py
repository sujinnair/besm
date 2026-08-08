from __future__ import annotations

from besm.engine.models import Domain
from besm.reasoning.state import ReasoningState

_DOMAIN_KEYWORDS: dict[Domain, list[str]] = {
    "commodity": [
        "mandi",
        "arrival",
        "onion",
        "tomato",
        "pulse",
        "grain",
        "rainfall",
        "import duty",
        "mrp",
        "edible oil",
        "kharif",
        "rabi",
        "agmarknet",
    ],
    "financial": [
        "inr",
        "usd",
        "forex",
        "brent",
        "crude",
        "fuel",
        "petrol",
        "diesel",
        "ipo",
        "gmp",
        "gold",
        "sovereign",
        "currency",
        "exchange rate",
    ],
    "legal": [
        "gazette",
        "subsidy",
        "passport",
        "rto",
        "visa",
        "vfs",
        "slot",
        "compensation",
        "sla",
        "power outage",
        "dgca",
        "flight delay",
        "notification",
        "scheme",
        "benefit",
    ],
    "health": [
        "flu",
        "fever",
        "cough",
        "viral",
        "aqi",
        "air quality",
        "pollution",
        "dengue",
        "outbreak",
        "symptom",
        "epidemic",
    ],
    "urban": [
        "traffic",
        "congestion",
        "crowd",
        "venue",
        "road",
        "corridor",
        "jam",
        "accident",
        "waterlogging",
    ],
    "market": [
        "zoning",
        "clu",
        "infrastructure",
        "metro",
        "highway",
        "skill",
        "salary",
        "job",
        "naukri",
        "business gap",
        "liquor license",
    ],
}

_CLASSIFICATION_THRESHOLD = 0.60


def classify_domain(signal_text: str) -> tuple[Domain, float]:
    text = signal_text.lower()
    scores: dict[Domain, int] = {d: 0 for d in _DOMAIN_KEYWORDS}

    for domain, keywords in _DOMAIN_KEYWORDS.items():
        for kw in keywords:
            if kw in text:
                scores[domain] += 1

    total = sum(scores.values())
    if total == 0:
        return "multi", 0.0

    best_domain = max(scores, key=lambda d: scores[d])
    confidence = scores[best_domain] / total
    return best_domain, confidence


def supervisor_node(state: ReasoningState) -> ReasoningState:
    domain, confidence = classify_domain(state["signal"].raw_text)

    if confidence >= _CLASSIFICATION_THRESHOLD:
        return {**state, "domain": domain, "agent_outputs": []}

    # Fan-out: mark domain as "multi" — graph will route to all agents
    return {**state, "domain": "multi", "agent_outputs": []}


def route_after_supervisor(state: ReasoningState) -> str:
    return state["domain"]  # "commodity" | "financial" | ... | "multi"
