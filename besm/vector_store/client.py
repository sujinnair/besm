from functools import lru_cache
from pathlib import Path

from qdrant_client import QdrantClient


@lru_cache(maxsize=1)
def get_client(path: str) -> QdrantClient:
    Path(path).mkdir(parents=True, exist_ok=True)
    return QdrantClient(path=path)
