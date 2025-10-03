from __future__ import annotations

import json
import logging
import os
from typing import Any, Dict


class JsonLogFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:  # noqa: D401
        payload: Dict[str, Any] = {
            "level": record.levelname,
            "name": record.name,
            "message": record.getMessage(),
        }
        # Attach structured extras if present
        if hasattr(record, "__dict__"):
            extras = {
                k: v
                for k, v in record.__dict__.items()
                if k
                not in {
                    "name",
                    "msg",
                    "args",
                    "levelname",
                    "levelno",
                    "pathname",
                    "filename",
                    "module",
                    "exc_info",
                    "exc_text",
                    "stack_info",
                    "lineno",
                    "funcName",
                    "created",
                    "msecs",
                    "relativeCreated",
                    "thread",
                    "threadName",
                    "processName",
                    "process",
                }
            }
            if extras:
                payload.update(extras)
        return json.dumps(payload, ensure_ascii=False)


def configure_logging(level: str | int = None) -> None:
    level = level or os.getenv("LOG_LEVEL", "INFO")
    if isinstance(level, str):
        level = getattr(logging, level.upper(), logging.INFO)

    root = logging.getLogger()
    # Clear existing handlers to avoid duplicate logs in notebooks
    root.handlers.clear()
    handler = logging.StreamHandler()
    handler.setFormatter(JsonLogFormatter())
    root.addHandler(handler)
    root.setLevel(level)

