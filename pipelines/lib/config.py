from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any, Dict

try:
    import yaml  # type: ignore
except Exception:  # pragma: no cover
    yaml = None  # type: ignore

logger = logging.getLogger(__name__)


def load_config(path: str | Path) -> Dict[str, Any]:
    """Load job configuration from YAML or JSON.

    Uses file extension to determine parser. Logs a concise summary.
    """
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(f"Config not found: {p}")

    if p.suffix.lower() in {".yml", ".yaml"}:
        if yaml is None:
            raise RuntimeError("pyyaml is required to parse YAML configs")
        data = yaml.safe_load(p.read_text(encoding="utf-8")) or {}
    elif p.suffix.lower() == ".json":
        data = json.loads(p.read_text(encoding="utf-8"))
    else:
        raise ValueError(f"Unsupported config format: {p.suffix}")

    logger.info("config_loaded", extra={"event": "config_loaded", "path": str(p), "keys": list(data.keys())})
    return data

