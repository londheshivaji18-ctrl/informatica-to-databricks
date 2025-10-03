from __future__ import annotations

from pipelines.jobs.claims_reports_demo1 import io as IO
from pipelines.jobs.claims_reports_demo1 import transforms as T


def test_pipeline_end_to_end__fixture_inputs(spark):
    claims = IO.read_claims(spark, "tests/fixtures/claims.csv")
    policy = IO.read_policy(spark, "tests/fixtures/policy.csv")
    member = IO.read_member(spark, "tests/fixtures/member.csv")

    vehicle = T.filter_vehicle_policies(policy)
    joined_policy = T.join_claims_policy(claims, vehicle)
    enriched = T.join_claims_member_left(joined_policy, member)

    total = T.select_total_claim_columns(T.mask_pii_columns(enriched))
    assert total.count() == 2

    fraud_smoker = T.filter_fraud_smoker(enriched)
    agg = T.aggregate_incidents_by_state_type(fraud_smoker)
    assert agg.count() == 1

