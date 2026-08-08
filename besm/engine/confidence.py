from datetime import datetime, timezone

from besm.engine.models import ButterflyChain

_BASE_SCORE = 50.0
_HIGH_SIMILARITY_THRESHOLD = 0.7
_HIGH_SIMILARITY_BONUS = 15.0
_LOW_HISTORY_PENALTY = 20.0
_LOW_HISTORY_MIN_MATCHES = 3
_CROSS_VALIDATION_BONUS = 15.0
_CROSS_VALIDATION_MIN_SOURCES = 2
_DOMAIN_BONUS = 15.0
_RECENCY_PENALTY = 10.0
_RECENCY_CUTOFF_YEARS = 2


def calculate_confidence(
    top_chains: list[ButterflyChain],
    independent_source_count: int,
    t_signal: datetime,
    domain_bonus: bool = False,
) -> float:
    score = _BASE_SCORE

    qualifying = [
        c
        for c in top_chains
        if (c.similarity_score or 0.0) > _HIGH_SIMILARITY_THRESHOLD
    ]

    if qualifying:
        avg_sim = sum(c.similarity_score for c in qualifying) / len(qualifying)  # type: ignore[arg-type]
        if avg_sim > _HIGH_SIMILARITY_THRESHOLD:
            score += _HIGH_SIMILARITY_BONUS

    if len(qualifying) < _LOW_HISTORY_MIN_MATCHES:
        score -= _LOW_HISTORY_PENALTY

    if independent_source_count >= _CROSS_VALIDATION_MIN_SOURCES:
        score += _CROSS_VALIDATION_BONUS

    if domain_bonus:
        score += _DOMAIN_BONUS

    if qualifying:
        most_recent = max(c.t_signal for c in qualifying)
        now = t_signal.tzinfo and t_signal or t_signal.replace(tzinfo=timezone.utc)
        most_recent = (
            most_recent.tzinfo
            and most_recent
            or most_recent.replace(tzinfo=timezone.utc)
        )
        age_years = (now - most_recent).days / 365.25
        if age_years > _RECENCY_CUTOFF_YEARS:
            score -= _RECENCY_PENALTY

    return max(0.0, min(100.0, score))
