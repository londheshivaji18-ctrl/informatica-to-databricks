from __future__ import annotations

import argparse
import logging
from pathlib import Path
from typing import Dict

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from pipelines.lib.config import load_config
from pipelines.lib.logging_utils import configure_logging
from pipelines.lib.metrics import Metrics, count
from pipelines.jobs.claims_reports_demo.transforms import (
    aggregate_fraud_by_state_type,
    filter_fraud_smoker,
    filter_vehicle_policies,
    join_claims_member_left,
    join_claims_policy,
    mask_pii_columns,
    select_total_claim_columns,
)

logger = logging.getLogger(__name__)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Claims Reports Demo job")
    parser.add_argument(
        "--config",
        required=True,
        help="Path to YAML/JSON config file",
    )
    return parser.parse_args()


def read_csv(spark: SparkSession, path: str) -> DataFrame:
    return (
        spark.read.option("header", True)
        .option("inferSchema", True)
        .option("quote", '"')
        .option("escape", '"')
        .csv(path)
    )


def write_table(df: DataFrame, full_name: str, mode: str = "overwrite") -> None:
    logger.info("write_table", extra={"event": "write_table", "table": full_name, "mode": mode})
    (
        df.write.format("delta")
        .mode(mode)
        .option("overwriteSchema", True)
        .saveAsTable(full_name)
    )


def build_table_name(conf: Dict[str, str]) -> str:
    return f"{conf['catalog']}.{conf['schema']}.{conf['table']}"


def main() -> None:
    configure_logging()
    args = parse_args()
    conf = load_config(args.config)

    spark = SparkSession.builder.appName("claims_reports_demo").getOrCreate()
    spark.conf.set("spark.sql.shuffle.partitions", conf.get("shuffle_partitions", 200))

    # Inputs
    claims_df = read_csv(spark, conf["inputs"]["claims"])  # Claim_DF
    policy_df = read_csv(spark, conf["inputs"]["policy"])  # PolicyFFVersion
    member_df = read_csv(spark, conf["inputs"]["member"])  # Insured_Member_FF

    metrics = Metrics()
    metrics.input_rows = count(claims_df)
    logger.info(
        "inputs_loaded",
        extra={
            "event": "inputs_loaded",
            "claims_rows": metrics.input_rows,
            "policy_rows": count(policy_df),
            "member_rows": count(member_df),
        },
    )

    # Transform pipeline mirrors Informatica mapping
    vehicle_policies_df = filter_vehicle_policies(policy_df)

    # Track rejects from policy filter (non-vehicle policies)
    metrics.rejects += max(count(policy_df) - count(vehicle_policies_df), 0)

    claims_policy_df = join_claims_policy(claims_df, vehicle_policies_df)

    # Claims with no matching vehicle policy (inner join drops these)
    # Approximated by anti-join count
    dropped_by_join = count(
        claims_df.join(vehicle_policies_df, claims_df["Claim_policy_number"] == vehicle_policies_df["policy_number"], "left_anti")
    )
    metrics.rejects += dropped_by_join

    claims_full_df = join_claims_member_left(claims_policy_df, member_df)

    total_claim_df = select_total_claim_columns(mask_pii_columns(claims_full_df))
    metrics.output_rows_total_claim = count(total_claim_df)

    # Fraud & smoker branch -> aggregate by state/type
    fraud_smoker_df = filter_fraud_smoker(claims_full_df)
    agg_df = aggregate_fraud_by_state_type(fraud_smoker_df)
    metrics.output_rows_fraud_by_state = count(agg_df)

    # Simple DQ check: incident_state not null
    dq_viol = count(fraud_smoker_df.filter(F.col("incident_state").isNull()))
    metrics.dq_violations += dq_viol

    # Writes
    total_claim_tbl = build_table_name(conf["outputs"]["total_claim"])  # Unity Catalog 3-part
    fraud_by_state_tbl = build_table_name(conf["outputs"]["fraud_by_state"])  # Unity Catalog 3-part

    write_table(total_claim_df, total_claim_tbl, mode=conf.get("write_mode", "overwrite"))
    write_table(agg_df, fraud_by_state_tbl, mode=conf.get("write_mode", "overwrite"))

    metrics.log()
    logger.info(
        "job_completed",
        extra={
            "event": "job_completed",
            "total_claim_table": total_claim_tbl,
            "fraud_by_state_table": fraud_by_state_tbl,
        },
    )


if __name__ == "__main__":
    main()
