from datetime import datetime, timedelta, timezone

import structlog
from qdrant_client import QdrantClient
from qdrant_client.models import (
    DatetimeRange,
    FieldCondition,
    Filter,
    FilterSelector,
)

log = structlog.get_logger()

_SIGNALS = "signals"
_RETENTION_YEARS = 3


def run_retention_cleanup(client: QdrantClient) -> None:
    cutoff = datetime.now(timezone.utc) - timedelta(days=_RETENTION_YEARS * 365)
    deleted = client.delete(
        collection_name=_SIGNALS,
        points_selector=FilterSelector(
            filter=Filter(
                must=[
                    FieldCondition(
                        key="t_signal",
                        range=DatetimeRange(lt=cutoff),
                    ),
                    FieldCondition(
                        key="outcome_confirmed",
                        match={"value": True},
                    ),
                ]
            )
        ),
    )
    log.info(
        "retention_cleanup_complete", cutoff=cutoff.isoformat(), result=str(deleted)
    )
