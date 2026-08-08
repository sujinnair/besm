from datetime import datetime


def calculate_lead_time(t_signal: datetime, t_impact_predicted: datetime) -> float:
    """Returns lead time in hours. Negative or zero means discard."""
    delta = t_impact_predicted - t_signal
    return delta.total_seconds() / 3600
