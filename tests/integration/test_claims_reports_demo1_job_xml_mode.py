from __future__ import annotations

from pipelines.jobs.claims_reports_demo1 import job
from pipelines.lib.config import load_config


def test_job_run__xml_filter_yields_zero_agg(spark):
    cfg = load_config("configs/jobs/claims_reports_demo1.yaml")
    cfg["aggregate_filter"] = "xml"
    cfg["dry_run"] = True

    result = job.run(cfg, env="test", spark=spark)

    # With fixtures, xml filter (incident_state='y' and incident_type='new') matches no rows
    assert result["metrics"]["output_rows_fraud_by_state"] == 0
    # Total claim branch still produces rows
    assert result["metrics"]["output_rows_total_claim"] >= 1

