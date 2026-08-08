import os
from pathlib import Path

import structlog
from dotenv import load_dotenv

log = structlog.get_logger()

_DEFAULT_SECRETS_PATH = Path(".env")


def load_secrets(path: Path | None = None) -> None:
    secrets_path = path or _DEFAULT_SECRETS_PATH
    if secrets_path.exists():
        load_dotenv(dotenv_path=secrets_path, override=False)
        _check_gitignore(secrets_path)
    else:
        log.warning("secrets_file_missing", path=str(secrets_path))


def _check_gitignore(secrets_path: Path) -> None:
    gitignore = Path(".gitignore")
    if not gitignore.exists():
        log.warning("gitignore_missing", secrets_path=str(secrets_path))
        return
    entries = gitignore.read_text().splitlines()
    normalized = {e.strip().lstrip("/") for e in entries if e.strip()}
    if secrets_path.name not in normalized and str(secrets_path) not in normalized:
        log.warning(
            "secrets_not_in_gitignore",
            secrets_path=str(secrets_path),
            advice="Add .env to .gitignore to prevent credential exposure",
        )


def require_env(key: str) -> str:
    value = os.getenv(key)
    if not value:
        raise RuntimeError(f"Required environment variable not set: {key!r}")
    return value
