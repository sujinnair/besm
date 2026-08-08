from __future__ import annotations

import logging
import sys

import structlog


def configure_logging(level: str, log_file: str) -> None:
    structlog.configure(
        processors=[
            structlog.stdlib.add_log_level,
            structlog.stdlib.add_logger_name,
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            structlog.processors.JSONRenderer(),
        ],
        wrapper_class=structlog.stdlib.BoundLogger,
        context_class=dict,
        logger_factory=structlog.stdlib.LoggerFactory(),
    )

    numeric_level = getattr(logging, level.upper(), logging.INFO)

    file_handler = logging.FileHandler(log_file)
    file_handler.setLevel(numeric_level)

    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setLevel(numeric_level)

    root = logging.getLogger()
    root.setLevel(numeric_level)
    root.addHandler(file_handler)
    root.addHandler(stream_handler)
