from __future__ import annotations

import argparse
import logging
import os
from dataclasses import asdict, dataclass
from datetime import datetime
from typing import Any, Dict, Iterable, List

from pyspark.sql import DataFrame, SparkSession, functions as F

from pipelines.lib.config import load_config
from pipelines.lib.logging_utils import configure_logging
from pipelines.jobs.disable_user_age18.api import DisableApiClient, DisableApiResponse
from pipelines.jobs.disable_user_age18 import transforms as T

log = logging.getLogger(__name__)


@dataclass
class DisableUserMetrics:
    input_rows: int = 0
    output_rows: int = 0
    underage_rows: int = 0
    rejects: int = 0
    dq_violations: int = 0

    def log(self) -> None:
        log.info("metrics", extra={"event": "metrics", **asdict(self)})


def log_df(df: DataFrame, name: str) -> None:
    log.info(
        "dataframe_stats",
        extra={"event": "dataframe_stats", "name": name, "rows": df.count(), "cols": len(df.columns)},
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Disable users aged 18+ via external API")
    parser.add_argument("--p_config", required=True, help="Path to YAML/JSON job config")
    parser.add_argument("--p_env", default="dev", help="Environment name (dev|stg|prod)")
    parser.add_argument("--p_run_date", default=datetime.utcnow().strftime("%Y-%m-%d"), help="ISO run date")
    return parser.parse_args()


def resolve_secret(entry: Dict[str, str], *, default: str = "") -> str:
    scope = entry.get("scope")
    key = entry.get("key")
    env = entry.get("env")
    literal = entry.get("value")

    if scope and key:
        try:
            from pyspark.dbutils import DBUtils  # type: ignore

            spark = SparkSession.getActiveSession()
            if spark is not None:
                dbutils = DBUtils(spark)
                return dbutils.secrets.get(scope=scope, key=key)
        except Exception:
            log.warning(
                "secret_lookup_failed",
                extra={"event": "secret_lookup_failed", "scope": scope, "key": key},
            )
    if env:
        return os.getenv(env, default) or default
    if literal is not None:
        return literal
    return default


def load_contract_snapshot(spark: SparkSession, source_conf: Dict[str, Any]) -> DataFrame:
    if "agreements_sql" in source_conf:
        return spark.sql(source_conf["agreements_sql"])

    df_agreement = spark.table(source_conf["agreements_table"])
    df_role = spark.table(source_conf["party_role_table"])
    df_party = spark.table(source_conf["party_snapshot_table"])

    df_joined = (
        df_role.alias("role")
        .join(df_agreement.alias("agre"), F.col("role.agre_num") == F.col("agre.agre_num"), "inner")
        .join(df_party.alias("party"), F.col("role.party_id") == F.col("party.party_id"), "inner")
        .select(
            F.col("agre.obj_id"),
            F.col("role.agre_num").alias("agre_num1"),
            F.col("role.party_id"),
            F.col("role.agre_role_mstr_cd"),
            F.col("agre.stts_dscr"),
            F.col("party.unq_party_id"),
            F.col("party.brth_dt"),
            F.col("party.src_rec_eff_strt_ts"),
        )
    )

    role_filters = source_conf.get("role_codes")
    if role_filters:
        df_joined = df_joined.filter(F.col("agre_role_mstr_cd").isin(role_filters))

    status_filters = source_conf.get("agreement_status")
    if status_filters:
        df_joined = df_joined.filter(F.col("stts_dscr").isin(status_filters))

    return df_joined


def load_user_profiles(spark: SparkSession, source_conf: Dict[str, Any]) -> DataFrame:
    if "user_profiles_sql" in source_conf:
        return spark.sql(source_conf["user_profiles_sql"]).select("USER_ID", "EMAIL")
    return spark.table(source_conf["user_profiles_table"]).select("USER_ID", "EMAIL")


def prepare_requests(
    df_eligible: DataFrame,
    *,
    cfg: Dict[str, Any],
    bearer_token: str,
    run_ts: str,
) -> DataFrame:
    header_values = {
        "x_ibm_client_id": cfg["headers"].get("x_ibm_client_id"),
        "x_ibm_client_secret": cfg["headers"].get("x_ibm_client_secret"),
        "x_customer_id": cfg["headers"].get("x_customer_id"),
        "x_aai_ope_id": cfg["headers"].get("x_aai_ope_id"),
    }
    header_values = {k: v for k, v in header_values.items() if v}
    request_prefix = cfg.get("request_prefix", "ETL_DISABLE")
    df_headers = T.add_request_headers(
        df_eligible,
        bearer_token=bearer_token,
        header_values=header_values,
        request_prefix=request_prefix,
        run_ts=run_ts,
    )
    return df_headers


def execute_api_requests(
    spark: SparkSession,
    df_requests: DataFrame,
    client: DisableApiClient,
) -> DataFrame:
    request_columns = [
        "user_id",
        "email",
        "agreement_number",
        "party_id",
        "authorization",
        "accept",
        "x_ibm_client_id",
        "x_ibm_client_secret",
        "x_request_id",
        "x_customer_id",
        "x_aai_ope_id",
        "x_token_owner_type",
    ]
    selected_cols = [col for col in request_columns if col in df_requests.columns]

    responses: List[DisableApiResponse] = []
    for row in df_requests.select(*selected_cols).toLocalIterator():
        payload = row.asDict(recursive=True)
        payload["request_id"] = payload.get("x_request_id", "")
        response = client.invoke(payload)
        responses.append(response)

    if not responses:
        schema = "user_id string, email string, request_id string, status_code int, item_output string, system_output string"
        return spark.createDataFrame([], schema=schema)

    response_rows = [
        (r.user_id, r.email, r.request_id, r.status_code, r.item_output, r.system_output)
        for r in responses
    ]
    return spark.createDataFrame(
        response_rows,
        schema="user_id string, email string, request_id string, status_code int, item_output string, system_output string",
    )


def build_table_name(conf: Dict[str, str]) -> str:
    return f"{conf['catalog']}.{conf['schema']}.{conf['table']}"


def write_delta(df: DataFrame, table_name: str, *, mode: str = "overwrite") -> None:
    assert table_name.count(".") == 2, "Unity Catalog 3-part name required"
    log.info("write_delta", extra={"event": "write_delta", "table": table_name, "mode": mode})
    (
        df.write.format("delta")
        .mode(mode)
        .option("overwriteSchema", True)
        .saveAsTable(table_name)
    )


def compute_metrics(
    df_source: DataFrame,
    df_joined: DataFrame,
    df_underage: DataFrame,
    df_eligible: DataFrame,
    *,
    missing_births: int,
) -> DisableUserMetrics:
    metrics = DisableUserMetrics()
    source_count = df_source.count()
    joined_count = df_joined.count()
    metrics.input_rows = joined_count
    metrics.rejects = max(source_count - joined_count, 0) + missing_births
    metrics.underage_rows = df_underage.count()
    metrics.output_rows = df_eligible.count()
    metrics.dq_violations = df_eligible.filter(F.col("email").isNull()).count()
    return metrics


def main() -> None:
    configure_logging()
    args = parse_args()
    config = load_config(args.p_config)

    spark = SparkSession.builder.appName("disable_user_age18").getOrCreate()
    spark.conf.set("spark.sql.shuffle.partitions", config.get("spark", {}).get("shuffle_partitions", 200))

    job_conf = config.get("job", {})
    env = args.p_env
    run_ts = datetime.utcnow().strftime("%Y%m%d%H%M%S")

    log.info(
        "run_start",
        extra={"event": "run_start", "env": env, "run_ts": run_ts, "config": args.p_config},
    )

    df_source = load_contract_snapshot(spark, config["sources"]).cache()
    df_profiles = load_user_profiles(spark, config["sources"]).cache()

    df_joined = T.join_contracts_with_profiles(df_source, df_profiles).cache()
    log_df(df_joined, "joined_contract_profiles")

    df_with_age = T.compute_age_years(df_joined).withColumnRenamed("agre_num1", "agreement_number").cache()

    df_birth_filtered, missing_births = T.filter_known_birthdates(df_with_age, birth_column="birth_dt")
    df_birth_filtered = df_birth_filtered.cache()

    age_threshold = job_conf.get("age_threshold", 18)
    df_underage, df_eligible = T.split_age_groups(df_birth_filtered, threshold=age_threshold)
    df_underage = df_underage.cache()
    df_eligible = df_eligible.cache()

    bearer_token = resolve_secret(config["auth"]["bearer_token"], default="")
    header_conf = {
        key: resolve_secret(value, default="") if isinstance(value, dict) else value
        for key, value in config["auth"].get("headers", {}).items()
    }
    job_conf.setdefault("headers", {})
    job_conf["headers"].update(header_conf)

    df_requests = prepare_requests(df_eligible, cfg=job_conf, bearer_token=bearer_token, run_ts=run_ts).cache()

    api_conf = config["http"]
    client = DisableApiClient(
        base_url_prefix=api_conf["base_url_prefix"],
        base_url_suffix=api_conf.get("base_url_suffix", ""),
        timeout_seconds=api_conf.get("timeout_seconds", 10.0),
    )
    df_responses = execute_api_requests(spark, df_requests, client).cache()

    metrics = compute_metrics(
        df_source,
        df_joined,
        df_underage,
        df_requests,
        missing_births=missing_births,
    )
    metrics.log()

    df_underage_out = T.mask_pii_stub(df_underage)
    df_eligible_log = T.mask_pii_stub(df_eligible)

    disable_table = build_table_name(config["outputs"]["disable_response"])
    eligible_table = build_table_name(config["outputs"]["eligible_audit"])
    underage_table = build_table_name(config["outputs"]["underage_audit"])

    required_disable_cols = {"user_id", "email", "request_id", "status_code", "item_output", "system_output"}
    missing_disable_cols = required_disable_cols - set(df_responses.columns)
    assert not missing_disable_cols, f"Contract failed: missing {missing_disable_cols} for disable output"

    required_audit_cols = {"agreement_number", "party_id", "birth_dt", "age_years"}
    assert required_audit_cols.issubset(set(df_eligible_log.columns)), "Audit output missing required columns"

    write_mode = job_conf.get("write_mode", "overwrite")
    response_count = df_responses.count()
    eligible_count = df_eligible.count()
    underage_count = df_underage.count()

    if response_count:
        write_delta(df_responses, disable_table, mode=write_mode)
    write_delta(df_eligible_log, eligible_table, mode=write_mode)
    write_delta(df_underage_out, underage_table, mode=write_mode)

    log.info(
        "run_complete",
        extra={
            "event": "run_complete",
            "disable_table": disable_table,
            "eligible_table": eligible_table,
            "underage_table": underage_table,
        },
    )

    print("=== Outputs ===")
    print(f"table: {disable_table}")
    print(f"eligible_table: {eligible_table}")
    print(f"underage_table: {underage_table}")
    print(f"rows_disable: {response_count}")
    print(f"rows_eligible: {eligible_count}")
    print(f"rows_underage: {underage_count}")


if __name__ == "__main__":
    main()
