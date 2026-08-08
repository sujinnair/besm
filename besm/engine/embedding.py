from sentence_transformers import SentenceTransformer

_cache: dict[str, SentenceTransformer] = {}


def _model(model_name: str) -> SentenceTransformer:
    if model_name not in _cache:
        _cache.clear()
        _cache[model_name] = SentenceTransformer(model_name)
    return _cache[model_name]


def embed(text: str, model_name: str) -> list[float]:
    return _model(model_name).encode(text, convert_to_numpy=True).tolist()
