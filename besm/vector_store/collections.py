from qdrant_client import QdrantClient
from qdrant_client.models import (
    Distance,
    PayloadSchemaType,
    VectorParams,
)

_VECTOR_SIZE = 768
_SIGNALS = "signals"
_CHAINS = "butterfly_chains"


def ensure_collections(client: QdrantClient) -> None:
    existing = {c.name for c in client.get_collections().collections}
    if _SIGNALS not in existing:
        client.create_collection(
            collection_name=_SIGNALS,
            vectors_config=VectorParams(size=_VECTOR_SIZE, distance=Distance.COSINE),
        )
        client.create_payload_index(_SIGNALS, "signal_id", PayloadSchemaType.KEYWORD)
        client.create_payload_index(_SIGNALS, "domain", PayloadSchemaType.KEYWORD)
        client.create_payload_index(_SIGNALS, "source_url", PayloadSchemaType.KEYWORD)
        client.create_payload_index(_SIGNALS, "content_hash", PayloadSchemaType.KEYWORD)
        client.create_payload_index(
            _SIGNALS, "outcome_confirmed", PayloadSchemaType.BOOL
        )
        client.create_payload_index(
            _SIGNALS, "confidence_score", PayloadSchemaType.FLOAT
        )

    if _CHAINS not in existing:
        client.create_collection(
            collection_name=_CHAINS,
            vectors_config=VectorParams(size=_VECTOR_SIZE, distance=Distance.COSINE),
        )
        client.create_payload_index(_CHAINS, "signal_id", PayloadSchemaType.KEYWORD)
        client.create_payload_index(_CHAINS, "domain", PayloadSchemaType.KEYWORD)
        client.create_payload_index(
            _CHAINS, "outcome_confirmed", PayloadSchemaType.BOOL
        )
        client.create_payload_index(
            _CHAINS, "confidence_score", PayloadSchemaType.FLOAT
        )
