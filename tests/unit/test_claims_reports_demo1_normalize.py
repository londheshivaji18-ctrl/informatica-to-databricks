from __future__ import annotations

from pipelines.jobs.claims_reports_demo1 import transforms as T


def test_normalize_columns__applies_rename_map(spark):
    df = spark.sql("SELECT 1 AS ClaimNum, 2 AS Claim_policy_number, 'x' AS incident_type")
    out = T.normalize_columns(df)
    assert set(out.columns) == {"claim_num", "claim_policy_number", "incident_type"}

