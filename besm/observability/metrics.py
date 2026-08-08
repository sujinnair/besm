from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path


class MetricsCollector:
    def __init__(self, metrics_file: str) -> None:
        self._path = Path(metrics_file)
        self._path.parent.mkdir(parents=True, exist_ok=True)

    def record(self, event: str, **kwargs) -> None:
        entry = {
            "ts": datetime.now(timezone.utc).isoformat(),
            "event": event,
            **kwargs,
        }
        with self._path.open("a") as f:
            f.write(json.dumps(entry) + "\n")

    def signal_processed(
        self,
        signal_id: str,
        domain: str,
        confidence: float,
        lead_time_hours: float,
        suppressed: bool,
        latency_s: float,
    ) -> None:
        self.record(
            "signal_processed",
            signal_id=signal_id,
            domain=domain,
            confidence=confidence,
            lead_time_hours=lead_time_hours,
            suppressed=suppressed,
            latency_s=latency_s,
        )

    def alert_delivered(self, alert_id: str, channel: str, success: bool) -> None:
        self.record(
            "alert_delivered",
            alert_id=alert_id,
            channel=channel,
            success=success,
        )

    def node_fetch(self, node_id: str, success: bool, latency_s: float) -> None:
        self.record(
            "node_fetch",
            node_id=node_id,
            success=success,
            latency_s=latency_s,
        )
