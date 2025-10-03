from __future__ import annotations

from pyspark.sql import functions as F

from pipelines.jobs.disable_user_age18 import transforms as T
from pipelines.jobs.disable_user_age18.api import DisableApiResponse
from pipelines.jobs.disable_user_age18.job import (
    compute_metrics,
    execute_api_requests,
    load_contract_snapshot,
    load_user_profiles,
    prepare_requests,
)


class StubDisableClient:
    def __init__(self) -> None:
        self.calls = []

    def invoke(self, payload):  # type: ignore[override]
        self.calls.append(payload)
        return DisableApiResponse(
            user_id=payload.get("user_id", ""),
            email=payload.get("email", ""),
            request_id=payload.get("request_id", ""),
            status_code=204,
            item_output="{}",
            system_output="ok",
        )


def test_disable_pipeline_happy_path(spark):
    spark.createDataFrame(
        [
            ("obj-1", "agre-1", "party-1", "PRIMARY", "Active", "UNQ-1", "2000-01-01", "2025-09-01"),
        ],
        [
            "obj_id",
            "agre_num1",
            "party_id",
            "agre_role_mstr_cd",
            "stts_dscr",
            "unq_party_id",
            "brth_dt",
            "src_rec_eff_strt_ts",
        ],
    ).createOrReplaceTempView("contracts_source")

    spark.createDataFrame(
        [
            ("USER-1", "user@example.com"),
        ],
        ["USER_ID", "EMAIL"],
    ).createOrReplaceTempView("user_profiles_source")

    sources = {
        "agreements_sql": "SELECT * FROM contracts_source",
        "user_profiles_sql": "SELECT * FROM user_profiles_source",
        "role_codes": ["PRIMARY"],
        "agreement_status": ["Active"],
    }

    df_source = load_contract_snapshot(spark, sources)
    df_profiles = load_user_profiles(spark, sources)

    df_joined = T.join_contracts_with_profiles(df_source, df_profiles)
    df_with_age = (
        T.compute_age_years(df_joined, birth_column="brth_dt", reference_date=F.to_date(F.lit("2025-09-30")))
        .withColumnRenamed("agre_num1", "agreement_number")
        .cache()
    )

    df_filtered, missing_births = T.filter_known_birthdates(df_with_age, "birth_dt")
    df_underage, df_eligible = T.split_age_groups(df_filtered, threshold=18)

    job_conf = {"headers": {}, "request_prefix": "ETL_DISABLE"}
    df_requests = prepare_requests(df_eligible, cfg=job_conf, bearer_token="fake-token", run_ts="20250930")

    client = StubDisableClient()
    df_responses = execute_api_requests(spark, df_requests, client)

    assert df_responses.count() == 1
    row = df_responses.collect()[0]
    assert row.status_code == 204
    assert client.calls  # ensure API invoked

    metrics = compute_metrics(
        df_source,
        df_joined,
        df_underage,
        df_requests,
        missing_births=missing_births,
    )
    assert metrics.output_rows == 1
    assert metrics.underage_rows == 0
    assert metrics.rejects == 0
