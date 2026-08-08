from abc import ABC, abstractmethod

from besm.gateway.models import DeliveryResult
from besm.reasoning.state import AlertPayload


class ChannelAdapter(ABC):
    async def aclose(self) -> None:
        pass

    @abstractmethod
    async def send(self, payload: AlertPayload, message: str) -> DeliveryResult: ...
