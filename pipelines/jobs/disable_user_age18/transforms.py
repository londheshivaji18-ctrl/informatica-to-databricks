from __future__ import annotations

from typing import Dict, Tuple

from pyspark.sql import DataFrame, functions as F, types as T


def rename_contract_columns(df_contracts: DataFrame) -> DataFrame:
    """Standardise column names from contract snapshot extracts."""

    rename_map = {
        "USER_ID": "user_id",
        "EMAIL": "email",
        "obj_id": "obj_id",
        "agre_num1": "agreement_number",
        "party_id": "party_id",
        "brth_dt": "birth_dt",
        "src_rec_eff_strt_ts": "src_rec_eff_start_ts",
    }
    for old, new in rename_map.items():
        if old in df_contracts.columns:
            df_contracts = df_contracts.withColumnRenamed(old, new)
    return df_contracts


def join_contracts_with_profiles(df_contracts: DataFrame, df_profiles: DataFrame) -> DataFrame:
    """Reproduce the Informatica joiner on USER_ID = OBJ_ID."""

    df_contracts = rename_contract_columns(df_contracts)
    df_profiles = rename_contract_columns(df_profiles)

    return df_contracts.join(
        df_profiles,
        df_contracts["obj_id"] == df_profiles["user_id"],
        "inner",
    )


def compute_age_years(
    df: DataFrame,
    *,
    birth_column: str = "birth_dt",
    reference_date: F.Column | None = None,
) -> DataFrame:
    """Mirror the Informatica DATE_DIFF logic using Spark functions."""

    ref_col = reference_date if reference_date is not None else F.current_date()
    df_cast = df.withColumn(birth_column, F.to_date(F.col(birth_column)))
    df_with_age = df_cast.withColumn(
        "age_years",
        F.floor(F.abs(F.months_between(ref_col, F.col(birth_column)) / F.lit(12.0))).cast(T.IntegerType()),
    )
    return df_with_age


def filter_known_birthdates(df: DataFrame, birth_column: str = "birth_dt") -> Tuple[DataFrame, int]:
    """Filter out rows without a birthdate while exposing reject counts."""

    filtered = df.filter(F.col(birth_column).isNotNull())
    rejected = df.count() - filtered.count()
    return filtered, rejected


def split_age_groups(df: DataFrame, *, threshold: int = 18) -> Tuple[DataFrame, DataFrame]:
    """Return (< age threshold, >= age threshold)."""

    return df.filter(F.col("age_years") < threshold), df.filter(F.col("age_years") >= threshold)


def add_request_headers(
    df_eligible: DataFrame,
    *,
    bearer_token: str,
    header_values: Dict[str, str],
    request_prefix: str,
    run_ts: str,
) -> DataFrame:
    """Attach HTTP headers expected by the disable API."""

    df = df_eligible.withColumn("authorization", F.lit(f"Bearer {bearer_token}"))
    df = df.withColumn("accept", F.lit("*/*"))
    df = df.withColumn("x_token_owner_type", F.lit("system"))
    df = df.withColumn(
        "x_request_id",
        F.concat_ws(
            "-",
            F.lit(request_prefix),
            F.col("email"),
            F.lit(run_ts),
        ),
    )
    for key, value in header_values.items():
        df = df.withColumn(key, F.lit(value))
    return df


def mask_pii_stub(df: DataFrame, columns: list[str] | None = None) -> DataFrame:
    """Placeholder for future PII masking to satisfy governance requirements."""

    columns = columns or ["email"]
    # TODO(@data-eng, 2025-12-31): Replace stub with irreversible hashing for PII columns.
    for column in columns:
        if column in df.columns:
            df = df.withColumn(
                f"{column}_masked",
                F.format_string("%016x", F.xxhash64(F.col(column).cast(T.StringType()))),
            )
    return df


__all__ = [
    "rename_contract_columns",
    "join_contracts_with_profiles",
    "compute_age_years",
    "filter_known_birthdates",
    "split_age_groups",
    "add_request_headers",
    "mask_pii_stub",
]
