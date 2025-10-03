from __future__ import annotations

import logging
from dataclasses import dataclass, asdict
from typing import Dict

from pyspark.sql import DataFrame

logger = logging.getLogger(__name__)


@dataclass
class Metrics:
    input_rows: int = 0
    output_rows_total_claim: int = 0
    output_rows_fraud_by_state: int = 0
    rejects: int = 0
    dq_violations: int = 0

    def log(self) -> None:
        logger.info("metrics", extra={"event": "metrics", **asdict(self)})


def count(df: DataFrame) -> int:
    return int(df.count())

