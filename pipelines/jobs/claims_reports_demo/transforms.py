from __future__ import annotations

import logging
from typing import Iterable, List

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

logger = logging.getLogger(__name__)


def normalize_policy_type(df: DataFrame) -> DataFrame:
    return df.withColumn("policy_type_norm", F.lower(F.col("policy_type")))


def filter_vehicle_policies(policy_df: DataFrame) -> DataFrame:
    df = normalize_policy_type(policy_df)
    filtered = df.filter(F.col("policy_type_norm") == F.lit("vehicle"))
    return filtered.drop("policy_type_norm")


def join_claims_policy(claim_df: DataFrame, policy_df: DataFrame) -> DataFrame:
    # Normal (inner) join on policy_number
    return claim_df.join(
        policy_df, claim_df["Claim_policy_number"] == policy_df["policy_number"], "inner"
    )


def join_claims_member_left(
    claims_enriched_df: DataFrame, member_df: DataFrame
) -> DataFrame:
    # Detail outer join => left join from claims_enriched_df to member_df
    return claims_enriched_df.join(
        member_df, claims_enriched_df["Claim_policy_number"] == member_df["Ins_Mem_policy_number"], "left"
    )


def filter_fraud_smoker(df: DataFrame) -> DataFrame:
    # Align to typical Y/N flags
    return df.filter((F.upper(F.col("fraud_reported")) == F.lit("Y")) & (F.upper(F.col("smoker")) == F.lit("Y")))


def aggregate_fraud_by_state_type(df: DataFrame) -> DataFrame:
    return (
        df.groupBy("incident_state", "incident_type")
        .agg(F.count(F.col("incident_state")).alias("Count_Incident"))
        .orderBy("incident_state", "incident_type")
    )


def select_total_claim_columns(df: DataFrame) -> DataFrame:
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
        # From policy
        "policy_number",
        "policy_bind_date",
        "policy_state",
        "policy_csl",
        "policy_premium_received",
        "policy_annual_premium",
        "umbrella_limit",
        "policy_type",
        # From member
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
    # TODO: implement proper PII masking strategy (tokenization/format-preserving)
    logger.warning("pii_masking_todo", extra={"event": "pii_masking_todo"})
    return df

