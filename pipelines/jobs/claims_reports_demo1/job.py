from __future__ import annotations

# stdlib
import argparse
import logging
from dataclasses import asdict
from datetime import datetime
from typing import Any, Dict

# third-party
# databricks/spark
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

# local
from pipelines.jobs.claims_reports_demo1 import io as IO
from pipelines.jobs.claims_reports_demo1 import transforms as T
from pipelines.lib.config import load_config
from pipelines.lib.logging_utils import configure_logging
from pipelines.lib.metrics import Metrics, count

log = logging.getLogger(__name__)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Claims Reports Demo v1 job")
    parser.add_argument("--p_config", required=True, help="Path to YAML/JSON job config")
    parser.add_argument("--p_env", default="dev", help="Environment name (dev|stg|prod)")
    parser.add_argument("--p_run_date", default=datetime.utcnow().strftime("%Y-%m-%d"), help="ISO run date")
    return parser.parse_args()


def build_table_name(conf: Dict[str, str]) -> str:
    return f"{conf['catalog']}.{conf['schema']}.{conf['table']}"


def write_output(df: DataFrame, conf: Dict[str, Any], *, mode: str = "overwrite") -> str:
    """Write a DataFrame either to a UC table (3-part) or to a Delta path.

    Returns the artifact identifier (table name or path).
    """
    fmt = conf.get("format", "delta")
    if {"catalog", "schema", "table"}.issubset(conf.keys()):
        table = build_table_name(conf)
        assert table.count(".") == 2, "Unity Catalog 3-part name required"
        log.info("write_table", extra={"event": "write_table", "table": table, "mode": mode, "format": fmt})
        (
            df.write.format(fmt)
            .mode(mode)
            .option("overwriteSchema", True)
            .saveAsTable(table)
        )
        return table
    path = conf.get("path")
    assert path, "Output must include either catalog/schema/table or a path"
    log.info("write_path", extra={"event": "write_path", "path": path, "mode": mode, "format": fmt})
    (
        df.write.format(fmt)
        .mode(mode)
        .option("overwriteSchema", True)
        .save(path)
    )
    return path


def assert_required_columns(df: DataFrame, required: set[str], *, name: str) -> None:
    missing = required - set(df.columns)
    assert not missing, f"Contract failed for {name}: missing columns {missing}"


def run(conf: Dict[str, Any], *, env: str = "dev", spark: SparkSession | None = None) -> Dict[str, Any]:
    configure_logging()
    spark = spark or SparkSession.builder.appName("claims_reports_demo1").getOrCreate()
    spark.conf.set("spark.sql.shuffle.partitions", conf.get("shuffle_partitions", 200))

    log.info("run_start", extra={"event": "run_start", "env": env})

    # Read inputs
    claims_df = IO.read_claims(spark, conf["inputs"]["claims"])  # Claim_DF
    policy_df = IO.read_policy(spark, conf["inputs"]["policy"])  # PolicyFFVersion
    member_df = IO.read_member(spark, conf["inputs"]["member"])  # Insured_Member_FF

    metrics = Metrics()
    metrics.input_rows = count(claims_df)
    log.info(
        "inputs_loaded",
        extra={
            "event": "inputs_loaded",
            "claims_rows": metrics.input_rows,
            "policy_rows": count(policy_df),
            "member_rows": count(member_df),
        },
    )

    # Transform pipeline mirrors mapping
    vehicle_policies_df = T.filter_vehicle_policies(policy_df)

    # Rejects from filter (non-vehicle policies)
    metrics.rejects += max(count(policy_df) - count(vehicle_policies_df), 0)

    claims_policy_df = T.join_claims_policy(claims_df, vehicle_policies_df)

    # Claims dropped by inner join (approx via anti join)
    dropped_by_join = count(
        claims_df.join(vehicle_policies_df, claims_df["Claim_policy_number"] == vehicle_policies_df["policy_number"], "left_anti")
    )
    metrics.rejects += dropped_by_join

    claims_full_df = T.join_claims_member_left(claims_policy_df, member_df)

    total_claim_df = T.select_total_claim_columns(T.mask_pii_columns(claims_full_df))
    metrics.output_rows_total_claim = count(total_claim_df)

    # Choose aggregation input filter per config: 'flags' (default) or 'xml'
    agg_mode = (conf.get("aggregate_filter") or "flags").lower()
    agg_input_df = T.filter_for_aggregation(claims_full_df, mode=agg_mode)
    agg_df = T.aggregate_incidents_by_state_type(agg_input_df)
    metrics.output_rows_fraud_by_state = count(agg_df)

    # Simple DQ: non-null group keys
    dq_viol = count(agg_input_df.filter(F.col("incident_state").isNull() | F.col("incident_type").isNull()))
    metrics.dq_violations += dq_viol

    # Contracts
    assert_required_columns(
        total_claim_df,
        {
            "Claim_policy_number",
            "policy_number",
            "incident_state",
            "total_claim_amount",
        },
        name="total_claim",
    )
    assert_required_columns(
        agg_df,
        {"incident_state", "incident_type", "Count_Incident"},
        name="fraud_by_state",
    )

    # Optionally skip writes for dry runs or test environments
    if conf.get("dry_run", False):
        log.info("dry_run_skip_writes", extra={"event": "dry_run_skip_writes"})
        metrics.log()
        return {"total_claim": None, "fraud_by_state": None, "metrics": asdict(metrics)}

    # Writes
    outputs = conf["outputs"]
    total_claim_art = write_output(total_claim_df, outputs["total_claim"], mode=conf.get("write_mode", "overwrite"))
    fraud_by_state_art = write_output(agg_df, outputs["fraud_by_state"], mode=conf.get("write_mode", "overwrite"))

    metrics.log()
    log.info(
        "job_completed",
        extra={
            "event": "job_completed",
            "total_claim": total_claim_art,
            "fraud_by_state": fraud_by_state_art,
            **asdict(metrics),
        },
    )
    return {
        "total_claim": total_claim_art,
        "fraud_by_state": fraud_by_state_art,
        "metrics": asdict(metrics),
    }


def main() -> None:
    args = parse_args()
    conf = load_config(args.p_config)
    run(conf, env=args.p_env)


if __name__ == "__main__":
    main()
