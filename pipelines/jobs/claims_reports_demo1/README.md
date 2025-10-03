# Claims Reports Demo v1

Databricks-ready notebooks and a CLI job that read claims/policy/member inputs, apply business transforms, and write outputs to Unity Catalog Delta tables or local Parquet paths.

## Layout
- `io.py` — reusable readers for claims/policy/member (file or UC table).
- `transforms.py` — vehicle policy filter, joins, fraud/smoker filter (or XML mode), aggregation, projection, PII mask stub.
- `notebook_read_sources.py` — widgets to read inputs and validate schemas.
- `notebook_apply_transforms.py` — widgets to apply transforms and preview outputs.
- `job.py` — CLI entrypoint for scheduled runs.
- Config: `configs/jobs/claims_reports_demo1.yaml`.

## Local Run Recipes

1) Dry run (compute only, no writes)
- Edit config to include `dry_run: true`, then run:
```
python -m pipelines.jobs.claims_reports_demo1.job --p_config configs/jobs/claims_reports_demo1.yaml --p_env dev
```

2) Write to local Parquet (no Delta dependency)
- Create a local override YAML (e.g., `configs/jobs/claims_reports_demo1.local.yaml`):
```
inputs:
  claims: tests/fixtures/claims.csv
  policy: tests/fixtures/policy.csv
  member: tests/fixtures/member.csv
outputs:
  total_claim: { path: tmp/claims_demo1/total_claim, format: parquet }
  fraud_by_state: { path: tmp/claims_demo1/fraud_by_state, format: parquet }
write_mode: overwrite
shuffle_partitions: 1
aggregate_filter: flags   # flags | xml
```
- Run:
```
python -m pipelines.jobs.claims_reports_demo1.job --p_config configs/jobs/claims_reports_demo1.local.yaml
```
- Inspect outputs:
```
pyspark
spark.read.parquet('tmp/claims_demo1/total_claim').show()
```

## Databricks UC Run (Delta)
- Use the default config `configs/jobs/claims_reports_demo1.yaml` with UC 3‑part names:
  - `dev.insurance.total_claim_ff_full`
  - `dev.insurance.fraud_smoker_by_state`
- Submit in a Databricks notebook or task:
```
python -m pipelines.jobs.claims_reports_demo1.job --p_config dbfs:/configs/claims_reports_demo1.yaml --p_env prod
```
- Ensure the cluster has Unity Catalog access and Delta enabled (default on Databricks).

## Options
- `aggregate_filter`: `flags` (fraud_reported='Y' & smoker='Y') or `xml` (incident_state='y' & incident_type='new').
- Pre-write asserts enforce required columns; metrics logged: `input_rows`, `output_rows_*`, `rejects`, `dq_violations`.
