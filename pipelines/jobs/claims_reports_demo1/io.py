from __future__ import annotations

# stdlib
import logging
import os
from typing import Iterable

# third-party
# databricks/spark
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F, types as T

log = logging.getLogger(__name__)


def _is_table_name(source: str) -> bool:
    """Heuristic to decide if a source string is a UC table name.

    Treat strings that look like file paths or URIs as files; otherwise as tables.
    """
    s = source.strip()
    if not s:
        return False
    if s.endswith((".csv", ".parquet", ".json", ".delta")):
        return False
    if any(proto in s for proto in ("dbfs:/", "s3://", "abfss://", "file:/")):
        return False
    if os.path.sep in s or "/" in s:
        return False
    # A UC table name should have 2 dots: catalog.schema.table
    return s.count(".") == 2


def _read_csv(spark: SparkSession, path: str) -> DataFrame:
    return (
        spark.read.option("header", True)
        .option("inferSchema", True)
        .option("quote", '"')
        .option("escape", '"')
        .csv(path)
    )


def _drop_header_row(df: DataFrame, id_col: str) -> DataFrame:
    """Drop duplicated header rows found in some demo CSVs.

    If the CSV contains an extra header line (common in flat-file exports), this filters
    rows where the identifier column literally equals the column name.
    """
    if id_col not in df.columns:
        return df
    return df.filter(F.col(id_col).cast(T.StringType()) != F.lit(id_col))


def read_claims(spark: SparkSession, source: str) -> DataFrame:
    """Read claims input from a file path or UC table.

    - CSV path: header=True, inferSchema=True
    - Table: `spark.table(source)`
    Also removes duplicated header rows if present.
    """
    if _is_table_name(source):
        df = spark.table(source)
        log.info("claims_read_table", extra={"event": "claims_read_table", "table": source, "rows": df.count()})
        return df
    df = _read_csv(spark, source)
    df = _drop_header_row(df, "ClaimNum")
    log.info("claims_read_csv", extra={"event": "claims_read_csv", "path": source, "rows": df.count()})
    return df


def read_policy(spark: SparkSession, source: str) -> DataFrame:
    if _is_table_name(source):
        df = spark.table(source)
        log.info("policy_read_table", extra={"event": "policy_read_table", "table": source, "rows": df.count()})
        return df
    df = _read_csv(spark, source)
    df = _drop_header_row(df, "policy_number")
    log.info("policy_read_csv", extra={"event": "policy_read_csv", "path": source, "rows": df.count()})
    return df


def read_member(spark: SparkSession, source: str) -> DataFrame:
    if _is_table_name(source):
        df = spark.table(source)
        log.info("member_read_table", extra={"event": "member_read_table", "table": source, "rows": df.count()})
        return df
    df = _read_csv(spark, source)
    df = _drop_header_row(df, "Ins_Mem_policy_number")
    log.info("member_read_csv", extra={"event": "member_read_csv", "path": source, "rows": df.count()})
    return df


def log_df(df: DataFrame, name: str) -> None:
    log.info("dataframe_stats", extra={"event": "dataframe_stats", "name": name, "rows": df.count(), "cols": len(df.columns)})

