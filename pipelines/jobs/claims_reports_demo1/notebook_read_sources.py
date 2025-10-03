# Databricks notebook source

# 1) Header
# Purpose: Read source files/tables for Claims Reports Demo (v1) using widgets or config.
# Inputs: claims, policy, member (CSV paths or UC 3-part table names)
# Outputs: none (read/validate only)
# Owner: data-eng
# Reviewed: 2025-10-01

# 2) Imports
# stdlib
from __future__ import annotations
import logging
from typing import Dict

# third-party
# databricks/spark
from pyspark.sql import SparkSession

# local
from pipelines.jobs.claims_reports_demo1 import io as IO
from pipelines.lib.config import load_config
from pipelines.lib.logging_utils import configure_logging


# 3) Parameters (widgets/job params)
try:  # Widgets are available inside Databricks
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
except Exception:  # Running locally (pytest, IDE)
    ENV = "dev"
    P_CONFIG = ""
    SRC_CLAIMS = "tests/fixtures/claims.csv"
    SRC_POLICY = "tests/fixtures/policy.csv"
    SRC_MEMBER = "tests/fixtures/member.csv"


# 4) Constants & schemas
REQUIRED_CLAIMS_COLS = {
    "ClaimNum",
    "Claim_policy_number",
    "incident_date",
    "incident_type",
}
REQUIRED_POLICY_COLS = {"policy_number", "policy_bind_date", "policy_state", "policy_type"}
REQUIRED_MEMBER_COLS = {"Ins_Mem_policy_number", "smoker", "age"}


# 5) Helpers (small functions)
def assert_columns(df, expected: set[str], name: str) -> None:
    missing = expected - set(df.columns)
    assert not missing, f"{name}: Missing columns: {missing}"


# 6) Main flow
configure_logging()
log = logging.getLogger("nb")
spark = SparkSession.getActiveSession() or SparkSession.builder.appName("claims_reports_demo1_read").getOrCreate()
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

IO.log_df(df_claims_raw, "claims_raw")
IO.log_df(df_policy_raw, "policy_raw")
IO.log_df(df_member_raw, "member_raw")


# 7) Validation & outputs (asserts, counts)
assert_columns(df_claims_raw, REQUIRED_CLAIMS_COLS, "claims")
assert_columns(df_policy_raw, REQUIRED_POLICY_COLS, "policy")
assert_columns(df_member_raw, REQUIRED_MEMBER_COLS, "member")

rows = {
    "claims": df_claims_raw.count(),
    "policy": df_policy_raw.count(),
    "member": df_member_raw.count(),
}
log.info("inputs_loaded", extra={"event": "inputs_loaded", **rows})


# 8) Summary (printed artifacts)
print("=== Read Sources Summary ===")
print(f"env:   {ENV}")
print(f"claims: {SRC_CLAIMS} (rows={rows['claims']})")
print(f"policy: {SRC_POLICY} (rows={rows['policy']})")
print(f"member: {SRC_MEMBER} (rows={rows['member']})")
