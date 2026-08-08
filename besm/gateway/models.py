from __future__ import annotations

from dataclasses import dataclass


@dataclass(slots=True)
class DeliveryResult:
    success: bool
    channel: str
    error: str | None = None
