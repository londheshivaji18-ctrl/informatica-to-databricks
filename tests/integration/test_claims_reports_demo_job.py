from __future__ import annotations

import yaml

from pipelines.jobs.claims_reports_demo import transforms as T


def test_end_to_end_transforms_with_config(spark):
    with open("configs/jobs/claims_reports_demo.yaml", "r", encoding="utf-8") as fh:
        conf = yaml.safe_load(fh)

    claims = spark.read.option("header", True).csv(conf["inputs"]["claims"])
    policy = spark.read.option("header", True).csv(conf["inputs"]["policy"])
    member = spark.read.option("header", True).csv(conf["inputs"]["member"])

    vehicle_policies = T.filter_vehicle_policies(policy)
    claims_policy = T.join_claims_policy(claims, vehicle_policies)
    claims_full = T.join_claims_member_left(claims_policy, member)
    total_claim = T.select_total_claim_columns(T.mask_pii_columns(claims_full))
    fraud_smoker = T.filter_fraud_smoker(claims_full)
    agg = T.aggregate_fraud_by_state_type(fraud_smoker)

    assert total_claim.count() == 2
    assert agg.count() == 1

