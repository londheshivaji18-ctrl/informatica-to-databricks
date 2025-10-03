# Databricks notebook source

# 1) Header
# Purpose: Apply business transforms for Claims Reports Demo (v1) and prepare DataFrames for writing.
# Inputs: claims, policy, member
# Outputs: df_total_claim, df_fraud_by_state (not written here)
# Owner: data-eng
# Reviewed: 2025-10-01

# 2) Imports
from __future__ import annotations

# stdlib
import logging
from typing import Dict

# third-party
# databricks/spark
from pyspark.sql import SparkSession

# local
from pipelines.jobs.claims_reports_demo1 import io as IO
from pipelines.jobs.claims_reports_demo1 import transforms as TFM
from pipelines.lib.config import load_config
from pipelines.lib.logging_utils import configure_logging


# 3) Parameters (widgets/job params)
try:
    dbutils.widgets.text("p_env", "dev", "Environment (dev|stg|prod)")
    dbutils.widgets.text("p_config", "", "Config file path (YAML/JSON)")
    dbutils.widgets.text("p_claims", "tests/fixtures/claims.csv", "Claims source (path or catalog.schema.table)")
    dbutils.widgets.text("p_policy", "tests/fixtures/policy.csv", "Policy source (path or catalog.schema.table)")
    dbutils.widgets.text("p_member", "tests/fixtures/member.csv", "Member source (path or catalog.schema.table)")
    ENV = dbutils.widgets.get("p_env")
    P_CONFIG = dbutils.widgets.get("p_config")
    SRC_CLAIMS = dbutils.widgets.get("p_claims")
    SRC_POLICY = dbutils.widgets.get("p_policy")
    SRC_MEMBER = dbutils.widgets.get("p_member")
except Exception:
    ENV = "dev"
    P_CONFIG = ""
    SRC_CLAIMS = "tests/fixtures/claims.csv"
    SRC_POLICY = "tests/fixtures/policy.csv"
    SRC_MEMBER = "tests/fixtures/member.csv"


# 4) Constants & schemas
REQUIRED_TOTAL_COLS = {
    "Claim_policy_number",
    "incident_state",
    "policy_number",
}
REQUIRED_AGG_COLS = {"incident_state", "incident_type", "Count_Incident"}


# 5) Helpers
def assert_columns(df, expected: set[str], name: str) -> None:
    missing = expected - set(df.columns)
    assert not missing, f"{name}: Missing columns: {missing}"


# 6) Main flow
configure_logging()
log = logging.getLogger("nb")
spark = SparkSession.getActiveSession() or SparkSession.builder.appName("claims_reports_demo1_transform").getOrCreate()
log.info("run_start", extra={"event": "run_start", "env": ENV})

conf: Dict[str, str] = {}
if P_CONFIG:
    conf = load_config(P_CONFIG)
    SRC_CLAIMS = conf.get("inputs", {}).get("claims", SRC_CLAIMS)
    SRC_POLICY = conf.get("inputs", {}).get("policy", SRC_POLICY)
    SRC_MEMBER = conf.get("inputs", {}).get("member", SRC_MEMBER)

df_claims_raw = IO.read_claims(spark, SRC_CLAIMS)
df_policy_raw = IO.read_policy(spark, SRC_POLICY)
df_member_raw = IO.read_member(spark, SRC_MEMBER)

vehicle_policies_df = TFM.filter_vehicle_policies(df_policy_raw)
claims_policy_df = TFM.join_claims_policy(df_claims_raw, vehicle_policies_df)
claims_full_df = TFM.join_claims_member_left(claims_policy_df, df_member_raw)

df_total_claim = TFM.select_total_claim_columns(TFM.mask_pii_columns(claims_full_df))
# Choose aggregation filter by config if provided
agg_mode = "flags"
if P_CONFIG:
    agg_mode = (conf.get("aggregate_filter") or "flags").lower()
df_agg_input = TFM.filter_for_aggregation(claims_full_df, mode=agg_mode)
df_fraud_by_state = TFM.aggregate_incidents_by_state_type(df_agg_input)

# 7) Validation & outputs (asserts, counts)
assert_columns(df_total_claim, REQUIRED_TOTAL_COLS, "total_claim")
assert_columns(df_fraud_by_state, REQUIRED_AGG_COLS, "fraud_by_state")

log.info(
    "stage_counts",
    extra={
        "event": "stage_counts",
        "claims_raw": df_claims_raw.count(),
        "policy_raw": df_policy_raw.count(),
        "member_raw": df_member_raw.count(),
        "vehicle_policies": vehicle_policies_df.count(),
        "claims_policy": claims_policy_df.count(),
        "claims_full": claims_full_df.count(),
        "total_claim": df_total_claim.count(),
        "fraud_by_state": df_fraud_by_state.count(),
        "agg_mode": agg_mode,
    },
)

# 8) Summary
print("=== Transform Summary ===")
print(f"env: {ENV}")
print(f"total_claim rows: {df_total_claim.count()}")
print(f"fraud_by_state rows: {df_fraud_by_state.count()}")
