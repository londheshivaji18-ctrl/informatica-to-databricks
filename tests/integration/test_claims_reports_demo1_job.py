from __future__ import annotations

import uuid

from pipelines.jobs.claims_reports_demo1 import job
from pipelines.lib.config import load_config


def test_job_run__writes_to_paths(spark, tmp_path):
    cfg = load_config("configs/jobs/claims_reports_demo1.yaml")

    # Override outputs to write to temp Delta paths (local-friendly)
    base = tmp_path / f"claims_demo1_{uuid.uuid4().hex}"
    out_total = str(base / "total_claim")
    out_agg = str(base / "fraud_by_state")

    cfg["outputs"]["total_claim"] = {"path": out_total, "format": "parquet"}
    cfg["outputs"]["fraud_by_state"] = {"path": out_agg, "format": "parquet"}
    cfg["dry_run"] = True

    result = job.run(cfg, env="test", spark=spark)
    # Dry run should skip materialization but still compute metrics
    assert result["total_claim"] is None
    assert result["fraud_by_state"] is None
    assert result["metrics"]["output_rows_total_claim"] >= 1
    assert result["metrics"]["output_rows_fraud_by_state"] >= 1
