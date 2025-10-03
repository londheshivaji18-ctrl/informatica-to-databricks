from __future__ import annotations

from datetime import date

from chispa import assert_df_equality
from pyspark.sql import functions as F, types as T

from pipelines.jobs.disable_user_age18 import transforms as T_fn


def test_compute_age_years(spark):
    df_input = spark.createDataFrame(
        [
            ("u1", "2008-09-30"),
            ("u2", "1980-06-15"),
        ],
        ["user_id", "birth_dt"],
    )
    reference_col = F.to_date(F.lit("2025-09-30"))
    df_result = T_fn.compute_age_years(df_input, birth_column="birth_dt", reference_date=reference_col)

    schema = T.StructType(
        [
            T.StructField("user_id", T.StringType(), True),
            T.StructField("birth_dt", T.DateType(), True),
            T.StructField("age_years", T.IntegerType(), True),
        ]
    )
    expected = spark.createDataFrame(
        [
            ("u1", date(2008, 9, 30), 17),
            ("u2", date(1980, 6, 15), 45),
        ],
        schema=schema,
    )

    assert_df_equality(
        df_result.select("user_id", "birth_dt", "age_years"),
        expected,
        ignore_row_order=True,
        ignore_nullable=True,
    )


def test_split_age_groups(spark):
    df_input = spark.createDataFrame(
        [
            ("u1", 17),
            ("u2", 18),
            ("u3", 25),
        ],
        ["user_id", "age_years"],
    )

    df_under, df_over = T_fn.split_age_groups(df_input, threshold=18)

    assert df_under.count() == 1
    assert df_over.count() == 2


def test_add_request_headers(spark):
    df_input = spark.createDataFrame(
        [
            ("u1", "user@example.com"),
        ],
        ["user_id", "email"],
    )

    df_headers = T_fn.add_request_headers(
        df_input,
        bearer_token="token123",
        header_values={"x_ibm_client_id": "cid", "x_customer_id": "cust"},
        request_prefix="ETL_DISABLE",
        run_ts="20250930",
    )

    row = df_headers.collect()[0]
    assert row.authorization == "Bearer token123"
    assert row.x_ibm_client_id == "cid"
    assert row.x_customer_id == "cust"
    assert row.x_request_id == "ETL_DISABLE-user@example.com-20250930"
    assert row.x_token_owner_type == "system"


def test_mask_pii_stub(spark):
    df_input = spark.createDataFrame([("user@example.com",)], ["email"])
    df_masked = T_fn.mask_pii_stub(df_input)
    row = df_masked.collect()[0]
    assert row.email_masked is not None
    assert row.email_masked != row.email


def test_join_contracts_with_profiles(spark):
    df_contracts = spark.createDataFrame(
        [
            ("obj-1", "agre-1", "party-1"),
            ("obj-2", "agre-2", "party-2"),
        ],
        ["obj_id", "agre_num1", "party_id"],
    )
    df_profiles = spark.createDataFrame(
        [
            ("obj-1", "user1@example.com"),
            ("obj-2", "user2@example.com"),
        ],
        ["USER_ID", "EMAIL"],
    )

    df_joined = T_fn.join_contracts_with_profiles(df_contracts, df_profiles)
    assert df_joined.count() == 2
