from __future__ import annotations

from chispa import assert_df_equality

from pipelines.jobs.claims_reports_demo1 import transforms as T


def test_filter_vehicle_policies(spark):
    df = spark.sql(
        "SELECT * FROM VALUES (1, 'vehicle'), (2, 'home'), (3, 'VeHiCle') AS data(policy_number, policy_type)"
    )
    out = T.filter_vehicle_policies(df)
    assert out.count() == 2


def test_join_and_aggregate__fraud_smoker_by_state_type(spark):
    claims = spark.read.option("header", True).csv("tests/fixtures/claims.csv")
    policy = spark.read.option("header", True).csv("tests/fixtures/policy.csv")
    member = spark.read.option("header", True).csv("tests/fixtures/member.csv")

    vehicle_policies = T.filter_vehicle_policies(policy)
    claims_policy = T.join_claims_policy(claims, vehicle_policies)
    claims_full = T.join_claims_member_left(claims_policy, member)

    total_claim = T.select_total_claim_columns(T.mask_pii_columns(claims_full))
    assert total_claim.count() == 2  # policy 100003 filtered out

    fraud_smoker = T.filter_fraud_smoker(claims_full)
    agg = T.aggregate_incidents_by_state_type(fraud_smoker)

    expected = spark.sql(
        "SELECT CAST('CA' AS string) AS incident_state, CAST('Single Vehicle Collision' AS string) AS incident_type, CAST(1 AS bigint) AS Count_Incident"
    )
    assert_df_equality(agg, expected, ignore_row_order=True, ignore_column_order=False, ignore_nullable=True)

