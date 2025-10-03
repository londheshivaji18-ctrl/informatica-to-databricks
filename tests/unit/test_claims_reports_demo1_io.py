from __future__ import annotations

from pipelines.jobs.claims_reports_demo1 import io as IO


def test_read_claims__loads_columns_and_rows(spark):
    df = IO.read_claims(spark, "tests/fixtures/claims.csv")
    assert {"ClaimNum", "Claim_policy_number", "incident_date", "incident_type"}.issubset(set(df.columns))
    assert df.count() >= 1


def test_read_policy__loads_columns_and_rows(spark):
    df = IO.read_policy(spark, "tests/fixtures/policy.csv")
    assert {"policy_number", "policy_bind_date", "policy_state", "policy_type"}.issubset(set(df.columns))
    assert df.count() >= 1


def test_read_member__loads_columns_and_rows(spark):
    df = IO.read_member(spark, "tests/fixtures/member.csv")
    assert {"Ins_Mem_policy_number", "smoker", "age"}.issubset(set(df.columns))
    assert df.count() >= 1

