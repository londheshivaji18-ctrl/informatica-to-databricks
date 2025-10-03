from __future__ import annotations

from pipelines.jobs.claims_reports_demo1 import io as IO


def test_read_sources_end_to_end__fixture_paths(spark):
    claims = IO.read_claims(spark, "tests/fixtures/claims.csv")
    policy = IO.read_policy(spark, "tests/fixtures/policy.csv")
    member = IO.read_member(spark, "tests/fixtures/member.csv")

    # Basic sanity checks
    assert claims.count() > 0
    assert policy.count() > 0
    assert member.count() > 0

    assert {"ClaimNum", "Claim_policy_number"}.issubset(claims.columns)
    assert {"policy_number", "policy_type"}.issubset(policy.columns)
    assert {"Ins_Mem_policy_number", "smoker"}.issubset(member.columns)

