from __future__ import annotations

# stdlib
import logging
from typing import List, Dict

# third-party
# databricks/spark
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

logger = logging.getLogger(__name__)


def normalize_policy_type(df: DataFrame) -> DataFrame:
    """Add a normalized policy type column for robust filtering.

    Why: Flat files can contain inconsistent casing; lowercasing ensures stable filters.
    """
    return df.withColumn("policy_type_norm", F.lower(F.col("policy_type")))


def filter_vehicle_policies(policy_df: DataFrame) -> DataFrame:
    """Keep only policies with type == 'vehicle' (case-insensitive)."""
    df = normalize_policy_type(policy_df)
    filtered = df.filter(F.col("policy_type_norm") == F.lit("vehicle"))
    return filtered.drop("policy_type_norm")


def join_claims_policy(claim_df: DataFrame, policy_df: DataFrame) -> DataFrame:
    """Inner join claims to policy on policy numbers (Normal Join)."""
    return claim_df.join(
        policy_df, claim_df["Claim_policy_number"] == policy_df["policy_number"], "inner"
    )


def join_claims_member_left(claims_enriched_df: DataFrame, member_df: DataFrame) -> DataFrame:
    """Left join enriched claims to member by policy number (Detail Outer in Informatica)."""
    return claims_enriched_df.join(
        member_df,
        claims_enriched_df["Claim_policy_number"] == member_df["Ins_Mem_policy_number"],
        "left",
    )


def filter_fraud_smoker(df: DataFrame) -> DataFrame:
    """Filter rows with fraud_reported == 'Y' and smoker == 'Y' (case-insensitive)."""
    return df.filter(
        (F.upper(F.col("fraud_reported")) == F.lit("Y")) & (F.upper(F.col("smoker")) == F.lit("Y"))
    )


def aggregate_incidents_by_state_type(df: DataFrame) -> DataFrame:
    """Aggregate incident counts by state and type (COUNT on incident_state)."""
    return (
        df.groupBy("incident_state", "incident_type")
        .agg(F.count(F.col("incident_state")).alias("Count_Incident"))
        .orderBy("incident_state", "incident_type")
    )


def select_total_claim_columns(df: DataFrame) -> DataFrame:
    """Project the Total_Claim_FF_Full target columns if available.

    Keeps the original column casing to support parity checks.
    """
    cols: List[str] = [
        "ClaimNum",
        "Claim_policy_number",
        "incident_date",
        "incident_type",
        "collision_type",
        "incident_severity",
        "authorities_contacted",
        "incident_state",
        "incident_city",
        "incident_location",
        "incident_hour_of_the_day",
        "number_of_vehicles_involved",
        "property_damage",
        "bodily_injuries",
        "witnesses",
        "police_report_available",
        "total_claim_amount",
        "injury_claim",
        "property_claim",
        "vehicle_claim",
        "auto_make",
        "auto_model",
        "auto_year",
        "fraud_reported",
        # Policy
        "policy_number",
        "policy_bind_date",
        "policy_state",
        "policy_csl",
        "policy_premium_received",
        "policy_annual_premium",
        "umbrella_limit",
        "policy_type",
        # Member
        "months_as_customer",
        "age",
        "insured_zip",
        "insured_sex",
        "insured_education_level",
        "insured_occupation",
        "insured_hobbies",
        "insured_relationship",
        "smoker",
    ]
    available = [c for c in cols if c in df.columns]
    return df.select(*available)


def mask_pii_columns(df: DataFrame) -> DataFrame:
    """Placeholder for PII masking.

    TODO(@data-eng, 2025-10-15): implement tokenization or FPE per policy.
    """
    logger.warning("pii_masking_todo", extra={"event": "pii_masking_todo"})
    return df


def normalize_columns(df: DataFrame, rename_map: Dict[str, str] | None = None) -> DataFrame:
    """Normalize column names to snake_case using a rename map.

    Any columns not in the explicit map are lowercased. Use at domain boundaries,
    keeping internal transforms parity-friendly.
    """
    rename_map = rename_map or {
        "ClaimNum": "claim_num",
        "Claim_policy_number": "claim_policy_number",
        "Count_Incident": "count_incident",
    }
    return df.select([F.col(c).alias(rename_map.get(c, c.lower())) for c in df.columns])


def filter_incident_state_new(df: DataFrame) -> DataFrame:
    """XML-defined filter: incident_state='y' AND incident_type='new' (case-insensitive)."""
    return df.filter(
        (F.lower(F.col("incident_state")) == F.lit("y")) & (F.lower(F.col("incident_type")) == F.lit("new"))
    )


def filter_for_aggregation(df: DataFrame, *, mode: str = "flags") -> DataFrame:
    """Select rows for aggregation based on configured mode.

    - mode='flags': fraud_reported='Y' AND smoker='Y'
    - mode='xml': incident_state='y' AND incident_type='new'
    """
    m = (mode or "flags").lower()
    if m == "xml":
        return filter_incident_state_new(df)
    return filter_fraud_smoker(df)
